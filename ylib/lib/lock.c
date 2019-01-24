

#include <pthread.h>
#include <sys/time.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "ylock.h"
#include "sysutil.h"
#include "adt.h"
#include "schedule.h"
#include "net_global.h"
#include "sdfs_conf.h"
#include "dbg.h"

#define DBG_SYLOCK

extern int srv_running;
extern int rdma_running;

typedef enum {
        LOCK_CO = 10,
        LOCK_NO,
} lock_type_t; 

#define RET_MAGIC 0x866aa9f0

typedef struct {
        struct list_head hook;
        char lock_type;
        char schedule_type;
        task_t task;
        sem_t sem;
        int magic;
        int retval;
} lock_wait_t;

int sy_rwlock_init(sy_rwlock_t *rwlock, const char *name)
{
        int ret;

        (void) name;
        
        ret = pthread_rwlock_init(&rwlock->lock, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sy_spin_init(&rwlock->spin);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        INIT_LIST_HEAD(&rwlock->queue);

        rwlock->priority = 0;
#if LOCK_DEBUG
        rwlock->last_unlock = 0;
        rwlock->count = 0;
#endif
        
        if (name) {
                if (strlen(name) > MAX_LOCK_NAME - 1)
                        YASSERT(0);

                strcpy(rwlock->name, name);
        } else {
                rwlock->name[0] = '\0';
        }

        return 0;
err_ret:
        return ret;
}

int sy_rwlock_destroy(sy_rwlock_t *rwlock)
{
        int ret;
        lock_wait_t *lock_wait = NULL;
        struct list_head list, *pos, *n;

        DWARN("destroy %p\n", rwlock);

        INIT_LIST_HEAD(&list);
        ret = sy_spin_lock(&rwlock->spin);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = pthread_rwlock_destroy(&rwlock->lock);
        if (unlikely(ret))
                GOTO(err_lock, ret);

        list_splice_init(&rwlock->queue, &list);

        sy_spin_unlock(&rwlock->spin);

        list_for_each_safe(pos, n, &list) {
                lock_wait = (void *)pos;
                list_del(pos);

                lock_wait->retval = EBUSY;
                lock_wait->magic = RET_MAGIC;
                if (lock_wait->schedule_type == LOCK_CO) {
                        schedule_resume(&lock_wait->task, 0, NULL);
                } else {
                        sem_post(&lock_wait->sem);
                }
        }

        return 0;
err_lock:
        sy_spin_unlock(&rwlock->spin);
err_ret:
        return ret;
}

STATIC int __sy_rwlock_trylock__(sy_rwlock_t *rwlock, char type, task_t *task)
{
        int ret;

        //YASSERT((int)rwlock->lock.__data.__readers >= 0);
        if (type == 'r')
                ret = pthread_rwlock_tryrdlock(&rwlock->lock);
        else {
                YASSERT(type == 'w');
                ret = pthread_rwlock_trywrlock(&rwlock->lock);
                if (ret == 0) {
                        (void) task;
#if LOCK_DEBUG
                        if (task) {
                                rwlock->writer = *task;
                        } else {
                                schedule_task_given(&rwlock->writer);
                        }
#endif
                }
        }

        //YASSERT((int)rwlock->lock.__data.__readers >= 0);
        
        return ret;
}

STATIC int __sy_rwlock_trylock(sy_rwlock_t *rwlock, char type)
{
        int ret;

        if (list_empty(&rwlock->queue)) {
                ret =  __sy_rwlock_trylock__(rwlock, type, NULL);
                if (unlikely(ret))
                        goto err_ret;

#if 1
                if (rwlock->priority && type == 'w') {
                        DBUG("priority %u, cleanup\n", rwlock->priority);
                        rwlock->priority = 0;
                }
#endif
        } else {
#if 0
                ret =  __sy_rwlock_trylock__(rwlock, type, NULL);
                if (unlikely(ret))
                        goto err_ret;
#else
                if (rwlock->priority < 128 && type == 'r') {
                        ret =  __sy_rwlock_trylock__(rwlock, type, NULL);
                        if (unlikely(ret))
                                goto err_ret;

                        rwlock->priority++;
                        DBUG("priority %u, increased\n", rwlock->priority);
                } else {
                        ret = EBUSY;
                        goto err_ret;
                }
#endif
        }

        return 0;
err_ret:
        return ret;
}

STATIC void __sy_rwlock_register(sy_rwlock_t *rwlock, char type, lock_wait_t *lock_wait)
{
        int ret;

        if (schedule_running()) {
                lock_wait->task = schedule_task_get();
                lock_wait->schedule_type = LOCK_CO;
        } else {
                lock_wait->task.taskid = __gettid();
                lock_wait->task.scheduleid = getpid();
                lock_wait->task.fingerprint = TASK_THREAD;
                lock_wait->schedule_type = LOCK_NO;

                ret = sem_init(&lock_wait->sem, 0, 0);
                YASSERT(ret == 0);
        }

        lock_wait->magic = 0;
        lock_wait->retval = 0;
        lock_wait->lock_type = type;
        list_add_tail(&lock_wait->hook, &rwlock->queue);
}

STATIC int __sy_rwlock_wait(sy_rwlock_t *rwlock, lock_wait_t *lock_wait, char type, int tmo)
{
        int ret;

        DBUG("lock_wait %c\n", type);

        (void) rwlock;

        if (lock_wait->schedule_type == LOCK_CO) {
                ANALYSIS_BEGIN(0);
                ret = schedule_yield(type == 'r' ? "rdlock" : "rwlock", NULL, lock_wait);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }

                ANALYSIS_END(0, IO_WARN, NULL);
        } else {
                ret = _sem_timedwait1(&lock_wait->sem, tmo != -1 ? tmo : _get_timeout() * 6);
                YASSERT(ret == 0);
        }

        ret = lock_wait->retval;
        if (unlikely(ret))
                GOTO(err_ret, ret);

#if LOCK_DEBUG
        DINFO("locked %p %c count %d, writer %d\n", rwlock, lock_wait->lock_type, rwlock->count, rwlock->lock.__data.__cur_writer);
#endif
        
        return 0;
err_ret:
        return ret;
}

STATIC int __sy_rwlock_lock(sy_rwlock_t *rwlock, char type, int tmo)
{
        int ret, retry = 0;
        lock_wait_t lock_wait;
        //char *name = type == 'r' ? "rdlock" : "rwlock";

        if (ng.daemon) {
                YASSERT(schedule_status() != SCHEDULE_STATUS_IDLE);
        }
        
        ANALYSIS_BEGIN(0);

retry:
        ret = sy_spin_lock(&rwlock->spin);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __sy_rwlock_trylock(rwlock, type);
        if (unlikely(ret)) {
                if (ret == EBUSY || ret == EWOULDBLOCK) {
#if LOCK_DEBUG
                        DINFO("wait %p %c count %d, retry %u\n", rwlock, type, rwlock->count, retry);
                        YASSERT(rwlock->count > 0);
                        rwlock->count++;
#endif

                        __sy_rwlock_register(rwlock, type, &lock_wait);

                        sy_spin_unlock(&rwlock->spin);

                        ret = __sy_rwlock_wait(rwlock, &lock_wait, type, tmo);

                        ret = sy_spin_lock(&rwlock->spin);
                        YASSERT(ret == 0);
                        
                        YASSERT(!list_exists(&lock_wait.hook, &rwlock->queue));

                        sy_spin_unlock(&rwlock->spin);
                        
                        if (unlikely(ret)) {
                                if (ret == ESTALE) {//dangerous here
                                        YASSERT(lock_wait.retval == 0);
                                        YASSERT(schedule_running());
                                        sy_rwlock_unlock(rwlock);
                                }

                                GOTO(err_ret, ret);
                        }

                        YASSERT(lock_wait.magic == (int)RET_MAGIC);

                        retry++;
                        goto retry;
                } else
                        GOTO(err_lock, ret);
        } else {
#if LOCK_DEBUG
                YASSERT(rwlock->count >= 0);
                rwlock->count++;
                DINFO("locked %p %c count %d, writer %d\n", rwlock, type,
                      rwlock->count, rwlock->lock.__data.__cur_writer);
#endif
                sy_spin_unlock(&rwlock->spin);
        }

        ANALYSIS_END(0, 1000 * 50, rwlock->name);

        return 0;
err_lock:
        sy_spin_unlock(&rwlock->spin);
err_ret:
        return ret;
}

int sy_rwlock_rdlock(sy_rwlock_t *rwlock)
{
        return __sy_rwlock_lock(rwlock, 'r', -1);
}

inline int sy_rwlock_wrlock(sy_rwlock_t *rwlock)
{
        return __sy_rwlock_lock(rwlock, 'w', -1);
}

int sy_rwlock_timedwrlock(sy_rwlock_t *rwlock, int sec)
{
        return __sy_rwlock_lock(rwlock, 'w', sec);
}


STATIC int __sy_rwlock_trylock1(sy_rwlock_t *rwlock, char type)
{
        int ret;
        char *name = type == 'r' ? "rdlock" : "rwlock";

        ANALYSIS_BEGIN(0);
        ret = sy_spin_lock(&rwlock->spin);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __sy_rwlock_trylock(rwlock, type);
        if (unlikely(ret)) {
                if (ret == EBUSY || ret == EWOULDBLOCK)
                        goto err_lock;
                else
                        GOTO(err_lock, ret);
        }

#if LOCK_DEBUG
        YASSERT(rwlock->count >= 0);
        rwlock->count++;
        DINFO("trylock %p count %d\n", rwlock, rwlock->count);
#endif
        sy_spin_unlock(&rwlock->spin);

        ANALYSIS_END(0, 1000 * 50, name);

        return 0;
err_lock:
        sy_spin_unlock(&rwlock->spin);
err_ret:
        return ret;
}

inline int sy_rwlock_tryrdlock(sy_rwlock_t *rwlock)
{
        return __sy_rwlock_trylock1(rwlock, 'r');
}


inline int sy_rwlock_trywrlock(sy_rwlock_t *rwlock)
{
        return __sy_rwlock_trylock1(rwlock, 'w');
}

void sy_rwlock_unlock(sy_rwlock_t *rwlock)
{
        int ret, locked = 0;
        lock_wait_t *lock_wait = NULL;
        struct list_head list, *pos, *n;

        INIT_LIST_HEAD(&list);

        ret = sy_spin_lock(&rwlock->spin);
        if (unlikely(ret))
                UNIMPLEMENTED(__WARN__);

        //YASSERT((int)rwlock->lock.__data.__readers >= 0);
        ret = pthread_rwlock_unlock(&rwlock->lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__WARN__);

        //YASSERT((int)rwlock->lock.__data.__readers >= 0);
#if LOCK_DEBUG
        int empty = list_empty(&rwlock->queue);
        
        memset(&rwlock->writer, 0x0, sizeof(rwlock->writer));

        rwlock->count--;
        DINFO("unlock %p count %d, locked %u, empty %d, writer %d\n", rwlock,
              rwlock->count, locked, empty, rwlock->lock.__data.__cur_writer);

        YASSERT(rwlock->count >= 0);

        rwlock->last_unlock = gettime();
#endif

#if 0
        list_for_each_safe(pos, n, &rwlock->queue) {
                lock_wait = (void *)pos;
                /* rwlock->__writer maybe not real writer, but this thread who try lock the rwlock */

#if 0
                ret = __sy_rwlock_trylock__(rwlock, lock_wait->lock_type, &lock_wait->task);
                if (unlikely(ret)) {
                        break;
                }
#endif

#if LOCK_DEBUG
                DINFO("resume %p %c count %d \n", rwlock,
                      lock_wait->lock_type, rwlock->count);
#endif
                
                list_del(pos);
                list_add_tail(&lock_wait->hook, &list);
                locked++;
        }
#else
        (void) locked;
        (void) pos;
        (void) n;

        list_splice_init(&rwlock->queue, &list);
#endif

        sy_spin_unlock(&rwlock->spin);

        list_for_each_safe(pos, n, &list) {
                lock_wait = (void *)pos;
                list_del(pos);

                lock_wait->retval = 0;
                lock_wait->magic = RET_MAGIC;
                if (lock_wait->schedule_type == LOCK_CO) {
                        schedule_resume(&lock_wait->task, 0, NULL);
                } else {
                        sem_post(&lock_wait->sem);
                }
        }

        return;
}

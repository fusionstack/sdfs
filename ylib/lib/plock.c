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
#include "plock.h"
#include "variable.h"
#include "dbg.h"

#define DBG_SYLOCK

extern int srv_running;

#ifdef PLOCK_NEW

#define RET_MAGIC 0x866aa9f0

#if 0
#define PLOCK_DMSG
#endif

typedef struct {
        struct list_head hook;
        char lock_type;
        task_t task;
        int magic;
        int retval;
        int prio;
        time_t lock_time;
} lock_wait_t;

STATIC void __plock_check(plock_t *rwlock)
{
        if (unlikely(rwlock->thread == -1)) {
                rwlock->thread = variable_thread();
                YASSERT(rwlock->thread >= gloconf.main_loop_threads);
        } else {
                YASSERT(rwlock->thread == variable_thread());
        }
}

int plock_init(plock_t *rwlock, const char *name)
{
        INIT_LIST_HEAD(&rwlock->queue);

        (void) name;
        rwlock->priority = 0;
        rwlock->writer = -1;
        rwlock->readers = 0;
        rwlock->thread = -1;
#if LOCK_DEBUG
        rwlock->last_unlock = 0;
        rwlock->count = 0;
        if (name) {
                if (strlen(name) > MAX_LOCK_NAME - 1)
                        YASSERT(0);

                strcpy(rwlock->name, name);
        } else {
                rwlock->name[0] = '\0';
        }
#endif

        return 0;
//err_ret:
//        return ret;
}

int plock_destroy(plock_t *rwlock)
{
        lock_wait_t *lock_wait = NULL;
        struct list_head list, *pos, *n;

        DBUG("destroy %p\n", rwlock);

        INIT_LIST_HEAD(&list);

        YASSERT(rwlock->readers == 0 && rwlock->writer == -1);
        list_splice_init(&rwlock->queue, &list);

        list_for_each_safe(pos, n, &list) {
                lock_wait = (void *)pos;
                list_del(pos);

                lock_wait->retval = EBUSY;
                lock_wait->magic = RET_MAGIC;
                schedule_resume(&lock_wait->task, 0, NULL);
        }

        return 0;
}

STATIC int __plock_trylock(plock_t *rwlock, char type, char force, task_t *task)
{
        int ret;

        __plock_check(rwlock);

        if (type == 'r') {
                if (rwlock->writer == -1 && (list_empty(&rwlock->queue) || force)) {
                        rwlock->readers++;
                        YASSERT(rwlock->readers > 0);
#ifdef PLOCK_DMSG
                        DINFO("read lock reader %u\n", rwlock->readers);
#endif
                } else {
#ifdef PLOCK_DMSG
                        DWARN("lock fail, readers %u writer task[%u]\n", rwlock->readers, rwlock->writer);
#endif
                        ret = EBUSY;
                        goto err_ret;
                }
        } else {
                if (rwlock->readers == 0 && rwlock->writer == -1) {
                        if (task)
                                rwlock->writer = task->taskid;
                        else
                                rwlock->writer = schedule_taskid();
                        YASSERT(rwlock->writer != -1);
#ifdef PLOCK_DMSG
                        DINFO("write lock, task[%u]\n", rwlock->writer);
#endif
                } else {
#ifdef PLOCK_DMSG
                        DWARN("lock fail, readers %u writer task[%u]\n", rwlock->readers, rwlock->writer);
#endif
                        ret = EBUSY;
                        goto err_ret;
                }
        }

        return 0;
err_ret:
        return ret;
}

STATIC int __plock_unlock(plock_t *rwlock)
{
        if (rwlock->writer != -1) {
                YASSERT(rwlock->readers == 0);
#ifdef PLOCK_DMSG
                DINFO("unlock, writer task[%u]\n", rwlock->writer);
#endif

                rwlock->writer =  -1;
        } else {
                YASSERT(rwlock->readers > 0);
                rwlock->readers--;
#ifdef PLOCK_DMSG
                DINFO("unlock, readers %u\n", rwlock->readers);
#endif
        }

        return 0;
}

#if 0
STATIC void __plock_register(plock_t *rwlock, char type, lock_wait_t *lock_wait, int prio)
{
        (void) prio;

        YASSERT(schedule_running());
        lock_wait->task = schedule_task_get();
        lock_wait->magic = 0;
        lock_wait->retval = 0;
        lock_wait->prio = 0;
        lock_wait->lock_type = type;
        lock_wait->lock_time = gettime();

        list_add_tail(&lock_wait->hook, &rwlock->queue);
}

#else

STATIC void __plock_register(plock_t *rwlock, char type, lock_wait_t *lock_wait, int prio)
{
        int found = 0, count = 0;
        lock_wait_t *tmp;
        struct list_head *pos;
        
        YASSERT(schedule_running());
        lock_wait->task = schedule_task_get();
        lock_wait->magic = 0;
        lock_wait->retval = 0;
        lock_wait->prio = prio;
        lock_wait->lock_type = type;
        lock_wait->lock_time = gettime();

        if (unlikely(prio)) {
                list_for_each(pos, &rwlock->queue) {
                        tmp = (void *)pos;
                        if (tmp->prio == 0) {
                                found = 1;
                                list_add_tail(&lock_wait->hook, pos);
                                break;
                        }

                        count++;
                }

                if (count > 0) {
                        DWARN("seek count %u\n", count);
                }
                
                if (found == 0) {
                        list_add_tail(&lock_wait->hook, &rwlock->queue);
                }
        } else {
                list_add_tail(&lock_wait->hook, &rwlock->queue);
        }
}

#endif

STATIC void __plock_timeout_check(void *args)
{
        int ret, count = 0, lock_timeout = 0;
        plock_t *rwlock = args;
        lock_wait_t *lock_wait;
        struct list_head list, *pos, *n;

        if (rwlock->writer != -1) {
#if LOCK_DEBUG
                DWARN("lock %p, writer %d, readers %u count %u, write locked, last %u\n", rwlock,
                      rwlock->writer, rwlock->readers, rwlock->count, rwlock->last_unlock);
                schedule_backtrace();
#endif
                return;
        } else {
#if LOCK_DEBUG
                DWARN("lock %p, writer %d, readers %u count %u, read locked, last %u\n", rwlock,
                      rwlock->writer, rwlock->readers, rwlock->count, rwlock->last_unlock);

                schedule_backtrace();
#endif
        }

        INIT_LIST_HEAD(&list);

        list_for_each_safe(pos, n, &rwlock->queue) {
                lock_wait = (void *)pos;

                if (lock_wait->lock_type == 'r') {
                        ret = __plock_trylock(rwlock, lock_wait->lock_type, 1, &lock_wait->task);
                        if (unlikely(ret))
                                break;

                        list_del(pos);
                        list_add_tail(&lock_wait->hook, &list);
                        count++;

#if LOCK_DEBUG
                        DWARN("force lock %p, writer %d, readers %u count %u, last %u %d\n", rwlock,
                                        rwlock->writer, rwlock->readers, rwlock->count, rwlock->last_unlock, count);
#endif
                        schedule_backtrace();                        
                }
        }

        list_for_each_safe(pos, n, &list) {
                lock_wait = (void *)pos;
                list_del(pos);

                lock_wait->retval = 0;
                lock_wait->magic = RET_MAGIC;
                schedule_resume(&lock_wait->task, 0, NULL);
        }

#if 1
        list_for_each_safe(pos, n, &rwlock->queue) {
                lock_wait = (void *)pos;

                if (lock_wait->lock_time + gloconf.rpc_timeout  <= gettime()) {
                        list_del(pos);
                        lock_wait->retval = ETIMEDOUT;
                        lock_wait->magic = RET_MAGIC;
                        schedule_resume(&lock_wait->task, ETIMEDOUT, NULL);
                        lock_timeout++;
#if LOCK_DEBUG
                        DWARN("lock timeout %p, writer %d, readers %u count %u, last %u scheudle[%u] task[%u] type %d %d\n", rwlock,
                                        rwlock->writer, rwlock->readers, rwlock->count, rwlock->last_unlock,
                                        lock_wait->lock_type, lock_wait->task.scheduleid, lock_wait->task.taskid,
                                        lock_timeout);
#endif
                }
        }
#endif
}

STATIC int __plock_wait(lock_wait_t *lock_wait, char type, int tmo, plock_t *rwlock)
{
        int ret;

        (void) tmo;

        DBUG("lock_wait %c\n", type);

        ANALYSIS_BEGIN(0);
        ret = schedule_yield1(type == 'r' ? "rdplock" : "wrplock", NULL,
                              rwlock, __plock_timeout_check, 180);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        ANALYSIS_END(0, IO_WARN, NULL);

        ret = lock_wait->retval;
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

STATIC int __plock_lock(plock_t *rwlock, char type, int tmo, int prio)
{
        int ret;
        lock_wait_t lock_wait;
        char *name = type == 'r' ? "rdlock" : "rwlock";

        if (ng.daemon) {
                YASSERT(schedule_status() != SCHEDULE_STATUS_IDLE);
        }

        __plock_check(rwlock);

        ANALYSIS_BEGIN(0);

        ret = __plock_trylock(rwlock, type, 0, NULL);
        if (unlikely(ret)) {
                if (ret == EBUSY || ret == EWOULDBLOCK) {
                        __plock_register(rwlock, type, &lock_wait, prio);

#if LOCK_DEBUG
                        rwlock->count++;
#endif
                        ret = __plock_wait(&lock_wait, type, tmo, rwlock);
                        if (unlikely(ret)) {
                        YASSERT(lock_wait.magic == (int)RET_MAGIC);
                                GOTO(err_ret, ret);
                        }

                        YASSERT(lock_wait.magic == (int)RET_MAGIC);
                        goto out;
                } else
                        GOTO(err_ret, ret);
        } else {
#if LOCK_DEBUG
                rwlock->count++;
#endif
        }

        ANALYSIS_END(0, 1000 * 50, name);

out:
#if ENABLE_SCHEDULE_LOCK_CHECK
        schedule_lock_set(1, 0);
#endif

        return 0;
err_ret:
        return ret;
}

int plock_rdlock(plock_t *rwlock)
{
        return __plock_lock(rwlock, 'r', -1, 0);
}

inline int plock_wrlock(plock_t *rwlock)
{
        return __plock_lock(rwlock, 'w', -1, 0);
}

inline int plock_wrlock_prio(plock_t *rwlock, int prio)
{
        return __plock_lock(rwlock, 'w', -1, prio);
}

int plock_timedwrlock(plock_t *rwlock, int sec)
{
        return __plock_lock(rwlock, 'w', sec, 0);
}

STATIC int __plock_trylock1(plock_t *rwlock, char type)
{
        int ret;
        char *name = type == 'r' ? "rdlock" : "rwlock";

        __plock_check(rwlock);

        ANALYSIS_BEGIN(0);
        ret = __plock_trylock(rwlock, type, 0, NULL);
        if (unlikely(ret)) {
                goto err_ret;
        }

#if LOCK_DEBUG
        rwlock->count++;
#endif

        ANALYSIS_END(0, 1000 * 50, name);

#if ENABLE_SCHEDULE_LOCK_CHECK
        schedule_lock_set(1, 0);
#endif
        
        return 0;
err_ret:
        return ret;
}

inline int plock_tryrdlock(plock_t *rwlock)
{
        return __plock_trylock1(rwlock, 'r');
}


inline int plock_trywrlock(plock_t *rwlock)
{
        return __plock_trylock1(rwlock, 'w');
}

void IO_FUNC plock_unlock(plock_t *rwlock)
{
        int ret;
        lock_wait_t *lock_wait = NULL;
        struct list_head list, *pos, *n;

        __plock_check(rwlock);

        ret = __plock_unlock(rwlock);
        if (unlikely(ret))
                UNIMPLEMENTED(__WARN__);

        if (list_empty(&rwlock->queue)) {
                goto out;
        }

        INIT_LIST_HEAD(&list);

        list_for_each_safe(pos, n, &rwlock->queue) {
                lock_wait = (void *)pos;
                ret = __plock_trylock(rwlock, lock_wait->lock_type, 1, &lock_wait->task);
                if (unlikely(ret))
                        break;

                list_del(pos);
                list_add_tail(&lock_wait->hook, &list);
        }

        list_for_each_safe(pos, n, &list) {
                lock_wait = (void *)pos;
                list_del(pos);

                lock_wait->retval = 0;
                lock_wait->magic = RET_MAGIC;
                schedule_resume(&lock_wait->task, 0, NULL);
        }

out:
#if LOCK_DEBUG
        rwlock->last_unlock = gettime();
        rwlock->count--;
        DBUG("lock count %d\n", rwlock->count);
        YASSERT(rwlock->count >= 0);
#endif

#if ENABLE_SCHEDULE_LOCK_CHECK
        schedule_lock_set(-1, 0);
#endif
        
        return;
}

#else /*old code*/

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

STATIC void __plock_check(plock_t *rwlock)
{
        if (unlikely(rwlock->thread == -1)) {
                rwlock->thread = variable_thread();
        } else {
                YASSERT(rwlock->thread == variable_thread());
        }
}


int plock_init(plock_t *rwlock)
{
        int ret;

        ret = pthread_rwlock_init(&rwlock->lock, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sy_spin_init(&rwlock->spin);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        INIT_LIST_HEAD(&rwlock->queue);

        rwlock->priority = 0;
        rwlock->thread = variable_thread();
#if LOCK_DEBUG
        rwlock->last_unlock = 0;
        rwlock->count = 0;
#endif

        return 0;
err_ret:
        return ret;
}

int plock_destroy(plock_t *rwlock)
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

STATIC int __plock_trylock__(plock_t *rwlock, char type)
{
        if (type == 'r')
                return pthread_rwlock_tryrdlock(&rwlock->lock);
        else {
                YASSERT(type == 'w');
                return pthread_rwlock_trywrlock(&rwlock->lock);
        }
}

STATIC int __plock_trylock(plock_t *rwlock, char type)
{
        int ret;

        __plock_check(rwlock);

        if (list_empty(&rwlock->queue)) {
                ret =  __plock_trylock__(rwlock, type);
                if (unlikely(ret))
                        goto err_ret;

                if (rwlock->priority && type == 'w') {
                        DBUG("priority %u, cleanup\n", rwlock->priority);
                        rwlock->priority = 0;
                }
        } else {
                if (rwlock->priority < 128 && type == 'r') {
                        ret =  __plock_trylock__(rwlock, type);
                        if (unlikely(ret))
                                goto err_ret;

                        rwlock->priority++;
                        DBUG("priority %u, increased\n", rwlock->priority);
                } else {
                        ret = EBUSY;
                        goto err_ret;
                }
        }

        return 0;
err_ret:
        return ret;
}

STATIC void __plock_register(plock_t *rwlock, char type, lock_wait_t *lock_wait)
{
        int ret;

        if (schedule_running()) {
                lock_wait->task = schedule_task_get();
                lock_wait->schedule_type = LOCK_CO;
        } else {
                lock_wait->task.taskid = getpid();
                lock_wait->task.scheduleid = __gettid();
                lock_wait->task.figerprint = 0;

                ret = sem_init(&lock_wait->sem, 0, 0);
                YASSERT(ret == 0);
                lock_wait->schedule_type = LOCK_NO;
        }

        lock_wait->magic = 0;
        lock_wait->retval = 0;
        lock_wait->lock_type = type;
        list_add_tail(&lock_wait->hook, &rwlock->queue);
}

STATIC int __plock_wait(lock_wait_t *lock_wait, char type, int tmo)
{
        int ret;

        DBUG("lock_wait %c\n", type);

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

        return 0;
err_ret:
        return ret;
}

STATIC  int __plock_lock(plock_t *rwlock, char type, int tmo)
{
        int ret;
        lock_wait_t lock_wait;
        char *name = type == 'r' ? "rdlock" : "rwlock";

        __plock_check(rwlock);

        ANALYSIS_BEGIN(0);
        ret = sy_spin_lock(&rwlock->spin);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __plock_trylock(rwlock, type);
        if (unlikely(ret)) {
                if (ret == EBUSY || ret == EWOULDBLOCK) {
                        rwlock->count++;

                        __plock_register(rwlock, type, &lock_wait);

                        sy_spin_unlock(&rwlock->spin);

                        ret = __plock_wait(&lock_wait, type, tmo);
                        if (unlikely(ret)) {
                                if (ret == ESTALE) {//dangerous here
                                        YASSERT(lock_wait.retval == 0);
                                        YASSERT(schedule_running());
                                        plock_unlock(rwlock);
                                }

                                GOTO(err_ret, ret);
                        }

                        YASSERT(lock_wait.magic == (int)RET_MAGIC);
                        
                        goto out;
                } else
                        GOTO(err_lock, ret);
        } else {
                rwlock->count++;
                sy_spin_unlock(&rwlock->spin);
        }

        ANALYSIS_END(0, 1000 * 50, name);

out:
        return 0;
err_lock:
        sy_spin_unlock(&rwlock->spin);
err_ret:
        return ret;
}

int plock_rdlock(plock_t *rwlock)
{
        return __plock_lock(rwlock, 'r', -1);
}

inline int plock_wrlock(plock_t *rwlock)
{
        return __plock_lock(rwlock, 'w', -1);
}

int plock_timedwrlock(plock_t *rwlock, int sec)
{
        return __plock_lock(rwlock, 'w', sec);
}


STATIC   int __plock_trylock1(plock_t *rwlock, char type)
{
        int ret;
        char *name = type == 'r' ? "rdlock" : "rwlock";

        __plock_check(rwlock);

        ANALYSIS_BEGIN(0);
        ret = sy_spin_lock(&rwlock->spin);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __plock_trylock(rwlock, type);
        if (unlikely(ret)) {
                GOTO(err_lock, ret);
        }

        rwlock->count++;
        sy_spin_unlock(&rwlock->spin);

        ANALYSIS_END(0, 1000 * 50, name);

        return 0;
err_lock:
        sy_spin_unlock(&rwlock->spin);
err_ret:
        return ret;
}

inline int plock_tryrdlock(plock_t *rwlock)
{
        return __plock_trylock1(rwlock, 'r');
}


inline int plock_trywrlock(plock_t *rwlock)
{
        return __plock_trylock1(rwlock, 'w');
}

void plock_unlock(plock_t *rwlock)
{
        int ret;
        lock_wait_t *lock_wait = NULL;
        struct list_head list, *pos, *n;

        __plock_check(rwlock);

        INIT_LIST_HEAD(&list);
        ret = sy_spin_lock(&rwlock->spin);
        if (unlikely(ret))
                UNIMPLEMENTED(__WARN__);

        ret = pthread_rwlock_unlock(&rwlock->lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__WARN__);

        list_for_each_safe(pos, n, &rwlock->queue) {
                lock_wait = (void *)pos;
                ret = __plock_trylock__(rwlock, lock_wait->lock_type);
                if (unlikely(ret))
                        break;

                list_del(pos);
                list_add_tail(&lock_wait->hook, &list);
        }

#if LOCK_DEBUG
        rwlock->count--;
        DBUG("lock count %d\n", rwlock->count);

        YASSERT(rwlock->count >= 0);
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

#if LOCK_DEBUG
        rwlock->last_unlock = gettime();
#endif
        return;
}

#endif

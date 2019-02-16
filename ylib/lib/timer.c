

#include <stdint.h>
#include <sys/timerfd.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "ylib.h"
#include "skiplist.h"
#include "timer.h"
#include "mem_cache.h"
#include "variable.h"
#include "dbg.h"

#define TIMER_IDLE 1024 * 1024
#define TIMER_TYPE_MISC 0
#define TIMER_TYPE_SCHE 1

typedef struct {
        ytime_t time;
        func_t func;
        void *obj;
} entry_t;

typedef struct {
        struct skiplist *list;
        sy_spinlock_t lock;
        int count;
        int seq;
        sem_t sem;
} group_t;

typedef struct {
        ytime_t max;
        ytime_t min;
        int thread;
        int maxlevel;
        int chunksize;
        int private;
        int polling;
        group_t group;
} ytimer_t;

static ytimer_t *__timer__ = NULL;

int __timer_cmp(const void *key, const void *data)
{
        int ret;
        ytime_t *keyid, *dataid;

        keyid = (ytime_t *)key;
        dataid = (ytime_t *)data;

        if (*keyid < *dataid)
                ret = -1;
        else if (*keyid > *dataid)
                ret = 1;
        else
                ret = 0;

        return ret;
}

static void __timer_expire__(group_t *group)
{
        int ret;
        ytime_t now;
        void *first;
        entry_t *ent;

        now = ytime_gettime();
        while (1) {
                ret = skiplist_get1st(group->list, (void **)&first);
                if (unlikely(ret)) {
                        break;
                }

                ent = first;

                if (now >= ent->time) {
                        group->count--;
                        (void) skiplist_del(group->list, first, (void **)&first);

                        DBUG("func %p\n", ent->obj);

                        ANALYSIS_BEGIN(0);
                        ent->func(ent->obj);
                        ANALYSIS_END(0, 1000 * 100, NULL);

                        mem_cache_free(MEM_CACHE_64, ent);
                } else {
                        break;
                }
        }
}


static void *__timer_expire(void *_args)
{
        int ret;
        group_t *group;
        struct timespec ts;
        ytime_t now;
        void *first;
        entry_t *ent;

        group = _args;

        ret = ytime_getntime(&ts);
        if (unlikely(ret)) {
                YASSERT(0);
        }

        ts.tv_sec += TIMER_IDLE;

        while (srv_running) {
                ret = _sem_timedwait(&group->sem, &ts);
                if (unlikely(ret)) {
                        if (ret != ETIMEDOUT)
                                YASSERT(0);
                }

                while (srv_running) {
                        ret = sy_spin_lock(&group->lock);
                        if (unlikely(ret))
                                YASSERT(0);

                        ret = skiplist_get1st(group->list, (void **)&first);
                        if (unlikely(ret)) {
                                if (ret == ENOENT) {
                                        ret = sy_spin_unlock(&group->lock);
                                        if (unlikely(ret))
                                                YASSERT(0);

                                        ts.tv_sec += TIMER_IDLE;

                                        break;
                                } else
                                        YASSERT(0);
                        }

                        ret = sy_spin_unlock(&group->lock);
                        if (unlikely(ret))
                                YASSERT(0);

                        now = ytime_gettime();

                        ent = first;

                        if (now >= ent->time) {
                                ret = sy_spin_lock(&group->lock);
                                if (unlikely(ret))
                                        YASSERT(0);

                                group->count--;
                                (void) skiplist_del(group->list, first, (void **)&first);

                                ret = sy_spin_unlock(&group->lock);
                                if (unlikely(ret))
                                        YASSERT(0);

                                DBUG("func %p\n", ent->obj);

                                ANALYSIS_BEGIN(0);
                                ent->func(ent->obj);
                                ANALYSIS_END(0, 1000 * 100, NULL);

                                mem_cache_free(MEM_CACHE_64, ent);
                        } else {
                                ytime_2ntime(ent->time, &ts);
                                break;
                        }
                }
        }

	return NULL;
}


int timer_init(int private, int polling)
{
        int ret, len;
        void *ptr;
        ytimer_t *_timer;
        group_t *group;
        pthread_t th;
        pthread_attr_t ta;

        /* group [ sche , misc ] */
        len = sizeof(ytimer_t);

        ret = ymalloc(&ptr, len);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        _timer = ptr;
        _timer->min = 0;
        _timer->max = (unsigned long long)-1;
        _timer->maxlevel = SKIPLIST_MAX_LEVEL;
        _timer->chunksize = SKIPLIST_CHKSIZE_DEF;
        _timer->private = private;
        _timer->polling = polling;

        group = &_timer->group;
        ret = skiplist_create(__timer_cmp, _timer->maxlevel, _timer->chunksize,
                              (void *)&_timer->min, (void *)&_timer->max,
                              &group->list);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        group->count = 0;

        if (private) {
                _timer->thread = variable_thread();
                variable_set(VARIABLE_TIMER, _timer);
        } else {
                YASSERT(__timer__ == NULL);
                _timer->thread = -1;
                __timer__ = _timer;
        }                

        if (!polling) {
                ret = sy_spin_init(&group->lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ret = sem_init(&group->sem, 0, 0);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                (void) pthread_attr_init(&ta);
                (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);
                //pthread_attr_setstacksize(&ta, 1<<19);
                
                ret = pthread_create(&th, &ta, __timer_expire, (void *)group);
                if (unlikely(ret))
                        GOTO(err_ret, ret); 
        }

        return 0;
err_ret:
        return ret;
}

static int __timer_destroy(void *arg)
{
        entry_t *ent = arg;

        ent->func(ent->obj);
        mem_cache_free(MEM_CACHE_64, ent);

        return 0;
}

void timer_destroy()
{
        ytimer_t *timer;
        group_t *group;
        
        timer = variable_get(VARIABLE_TIMER);
        group = &timer->group;

        skiplist_iterate_del(group->list, __timer_destroy);

        variable_unset(VARIABLE_TIMER);
}

static int __timer_insert(group_t *group, suseconds_t usec, void *obj, func_t func)
{
        int ret, retry = 0;
        entry_t *ent;
        uint64_t tmo;

        tmo = ytime_gettime();
        tmo += usec;

        ent = mem_cache_calloc(MEM_CACHE_64, 1);
        ent->time = tmo;
        ent->obj = obj;
        ent->func = func;

retry:
        ret = skiplist_put(group->list, (void *)&ent->time, (void *)ent);
        if (unlikely(ret)) {
                if (ret == EEXIST) {
                        if (retry > 1024) {
                                YASSERT(0);
                        }

                        if (retry > 256)
                                DINFO("retry %u, count %u\n", retry, group->count);

                        ent->time = ent->time + (group->seq ++) % 1024;
                        retry ++;
                        goto retry;
                }

                GOTO(err_ret, ret);
        }

        group->count++;

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_64, ent);
        return ret;
}

int timer_insert(const char *name, void *ctx, func_t func, suseconds_t usec)
{
        int ret;
        group_t *group;
        ytimer_t *timer;

        timer = variable_get(VARIABLE_TIMER);
        if (likely(timer)) {
                DBUG("timer insert %s %ju\n", name, usec);
                YASSERT(timer->thread == variable_thread());
        } else {
                DBUG("timer insert %s %ju\n", name, usec);
                timer = __timer__;
        }

        group = &timer->group;
        if (unlikely(!timer->private)) {
                ret = sy_spin_lock(&group->lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }
        
        ret = __timer_insert(group, usec, ctx, func);
        if (unlikely(ret))
                GOTO(err_lock, ret);

        if (unlikely(!timer->private)) {
                sy_spin_unlock(&group->lock);
        }

        if (unlikely(!timer->polling)) {
                sem_post(&group->sem);
        }
                
        return 0;
err_lock:
        if (unlikely(!timer->private)) {
                sy_spin_unlock(&group->lock);
        }
err_ret:
        return ret;
}

void timer_expire(void *ctx)
{
        ytimer_t *timer;
        timer = variable_get_byctx(ctx, VARIABLE_TIMER);
        YASSERT(timer);

        if (unlikely(!timer->polling))
                return;

        __timer_expire__(&timer->group);
}

#define MAX_QUEUED_LEN (1024 * 10)
#define QUEUE_EXTERN 32

typedef struct {
        timer_exec_t exec;
        void *ctx;
} timer_ctx_t;


inline static int __timer1_worker_exec(void *_ctx)
{
        timer_ctx_t *ctx;

        ctx = _ctx;

        ctx->exec(ctx->ctx);

        return 0;
}

int timer1_settime(const worker_handler_t *handler, uint64_t nsec)
{
        int ret;

        ret = worker_settime(handler, nsec);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int timer1_settime_retry(const worker_handler_t *handler, uint64_t nsec, int n) {
        int ret = 0, retry = 0;

        while (retry < n) {
                ret = timer1_settime(handler, nsec);
                if (unlikely(ret)) {
                        DWARN("ret %d\n", ret);
                        retry++;
                } else {
                        break;
                }
        }

        return ret;
}

int timer1_create(worker_handler_t *handler, const char *name, timer_exec_t exec, void *_ctx)
{
        int ret;
        timer_ctx_t *ctx;

        ret = ymalloc((void *)&ctx, sizeof(*ctx));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ctx->exec = exec;
        ctx->ctx = _ctx;

        ret = worker_create(handler, name, __timer1_worker_exec, NULL, ctx,
                            WORKER_TYPE_TIMER, 0);
        if (unlikely(ret))
                GOTO(err_free, ret);

        return 0;
err_free:
        yfree((void **)&ctx);
err_ret:
        return ret;
}

#if 0
// -- easy timer
int __do_easy_timer(void *arg) {
        int ret;
        easy_timer_t *timer = arg;

        if (timer->func) {
                ret = timer->func(timer->arg);
                if (timer->free_arg) {
                        yfree(&timer->arg);
                }
                DINFO("run timer %s nsec %llu count %d ret %d\n", timer->name, (LLU)timer->nsec,
                      timer->count, ret);
        }

        if (timer->count == -1 || timer->count > 0) {
                if (timer->count > 0)
                        timer->count--;

                ret = timer1_settime(&timer->handler, timer->nsec);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int easy_timer_init(easy_timer_t *timer, const char *name, timer_exec_t func, void *arg,
                    uint64_t nsec, int count, int free_arg) {
        int ret;

        DINFO("create timer %s nsec %llu count %d\n", name, (LLU)nsec, count);

        strcpy(timer->name, name);
        timer->func = func;
        timer->arg = arg;
        timer->nsec = nsec;
        timer->count = count;
        timer->free_arg = free_arg;

        ret = timer1_create(&timer->handler, name, __do_easy_timer, timer);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = timer1_settime(&timer->handler, nsec);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}
#endif

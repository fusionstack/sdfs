#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <signal.h>

#define DBG_SUBSYS S_LIBYLIB

#include "ylib.h"
#include "configure.h"
#include "ylock.h"
#include "tpool.h"
#include "dbg.h"

static int __tpool_new(tpool_t *tpool)
{
        int ret;
        pthread_t th;
        pthread_attr_t ta;
	sigset_t set, oldset;

	(void) pthread_attr_init(&ta);
	(void) pthread_attr_setdetachstate(&ta,PTHREAD_CREATE_DETACHED);

	if (sigfillset(&set))
                YASSERT(0);

	if (sigprocmask(SIG_SETMASK, &set, &oldset))
                YASSERT(0);

	ret = pthread_create(&th, &ta, tpool->worker,  (void *)tpool->context);
	if (ret == -1) {
		ret = errno;
		GOTO(err_ret, ret);
	}

	if (sigprocmask(SIG_SETMASK, &oldset, NULL)) YASSERT(0);

        tpool->last_threads = gettime();
        tpool->total++;

        DINFO("tpool %s create new threads, total %u, left %u idle %u\n", tpool->name,
              tpool->total, tpool->left, tpool->total - tpool->left);

        return 0;
err_ret:
        return ret;
}

int tpool_init(tpool_t *tpool, tpool_worker worker, void *context, const char *name, int idle_thread)
{
        int ret;

        ret = sy_spin_init(&tpool->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sem_init(&tpool->sem, 0, 1);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        tpool->left = 0;
        tpool->total = 0;
        tpool->idle = idle_thread;
        tpool->worker = worker;
        tpool->context = context;
        tpool->last_left = 0;

        strcpy(tpool->name, name);

        ret = __tpool_new(tpool);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int tpool_left(tpool_t *tpool)
{
        int ret;

        ret = sy_spin_lock(&tpool->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DBUG("total %u left %u idle %u\n", tpool->total, tpool->left, tpool->idle);

        YASSERT(tpool->total > tpool->left);
        if (tpool->total == tpool->left + 1) {
                if (tpool->total >= tpool->idle) {
                        ret = EBUSY;
                        //DWARN("total %u %u\n", tpool->total, IDLE_THREADS);
                        goto err_lock;
                } else {
                        ret = __tpool_new(tpool);
                        if (unlikely(ret))
                                GOTO(err_lock, ret);
                }
        }

        tpool->left++;

        sy_spin_unlock(&tpool->lock);

        tpool->last_left = gettime();
        sem_post(&tpool->sem);

        return 0;
err_lock:
        sy_spin_unlock(&tpool->lock);
err_ret:
        return ret;
}

void tpool_increase(tpool_t *tpool)
{
        int ret;

        ret = sy_spin_lock(&tpool->lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        tpool->idle++;

        sy_spin_unlock(&tpool->lock);

}

void tpool_return(tpool_t *tpool)
{
        int ret;

        ret = sy_spin_lock(&tpool->lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        YASSERT(tpool->left > 0);
        tpool->left--;

        YASSERT(tpool->total - tpool->left <= tpool->idle);
        sy_spin_unlock(&tpool->lock);

        return;
}

int tpool_wait(tpool_t *tpool)
{
        int ret;

        ret = _sem_wait(&tpool->sem);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

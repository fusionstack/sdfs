#include "coroutine.h"
#include "functools.h"


int co_worker_init(co_worker_t **worker, squeue_cmp_func cmp_func)
{
        int ret;
        co_worker_t *_worker;

        *worker = NULL;

        ret = ymalloc((void **)&_worker, sizeof(co_worker_t));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sy_spin_init(&_worker->lock);
        if (unlikely(ret))
                GOTO(err_free, ret);

        ret = squeue_init(&_worker->queue, 1024, cmp_func, chkid_hash_func);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        *worker = _worker;
        return 0;
err_free:
        yfree((void **)&worker);
err_ret:
        return ret;

}

#if 0
int co_worker_destroy(co_worker_t *worker)
{
        // TODO
        return 0;
}
#endif

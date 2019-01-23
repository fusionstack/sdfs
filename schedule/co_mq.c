

#include <time.h>

#define DBG_SUBSYS S_LIBSCHEDULE

#include "coroutine.h"

// sync stream

int co_mq_init_batch(co_mq_t *mq, const char *name, int batch) {
        count_list_init(&mq->queue);
        strcpy(mq->name, name);
        mq->batch = batch;
        mq->task_count = 0;
        //
        mq->swf = NULL;
        mq->swf_arg = NULL;
        return 0;
}

int co_mq_init(co_mq_t *mq, const char *name) {
        return co_mq_init_batch(mq, name, 1);
}

int co_mq_destroy(co_mq_t *mq) {
        (void) mq;
        return 0;
}

void __do_task(void *arg) {
        int ret;
        co_mq_ctx_t *ctx = arg;

        // DINFO("enter task %s\n", ctx->name);

        if (ctx->func) {
                ret = ctx->func(ctx->arg);
        } else {
                ret = 0;
        }
        if (unlikely(ret)) {
                DWARN("exit task %s, ret %d\n", ctx->name, ret);
        } else {
                DINFO_NOP("exit task %s, ret %d\n", ctx->name, ret);
        }

        // schedule_resume(&ctx->task, ret, NULL);
}

void __async_task(void *arg) {
        co_mq_t *mq = arg;
        struct list_head *first;
        co_mq_ctx_t *ctx;

        // do first n tasks
        for (int i=0; i < mq->batch; i++) {
                if (list_empty_careful(&mq->queue.list)) {
                        break;
                }

                mq->task_count++;

                first = mq->queue.list.next;
                ctx = (co_mq_ctx_t *) first;
                // ctx->task = schedule_task_get();

                DINFO_NOP("name %s batch %d/%d depth %u total %llu\n", ctx->name, i, mq->batch,
                      mq->queue.count, (LLU)mq->task_count);

                __do_task(ctx);

                // task done, release resource
                count_list_del_init(first, &mq->queue);
                if (ctx->need_free && ctx->arg != NULL) {
                        yfree(&ctx->arg);
                }
                yfree((void **)&ctx);

        }

        // wakeup next task
        if (!list_empty_careful(&mq->queue.list)) {
                schedule_task_new(mq->name, __async_task, mq, -1);
        } else {
                if (mq->swf) {
                        mq->swf(mq->swf_arg);
                }
        }

#if 0
        while(!list_empty_careful(&mq->queue)) {
                ctx = (co_mq_ctx_t *)mq->queue.next;

                ctx->task = schedule_task_get();

                schedule_task_new("__do_task", __do_task, ctx, -1);

                DINFO("yield task %u\n", ctx->task.taskid);
                ret = schedule_yield("__async_task", NULL, NULL);
                if (unlikely(ret)) {
                        DERROR("ret: %d\n", ret);
                }

                // task done, release resource
                list_del_init(mq->queue.next);
                if (ctx->need_free && ctx->arg != NULL) {
                        yfree((void **)&ctx->arg);
                }
                yfree((void **)&ctx);
        }
#endif

}

/**
 *
 * @param mq
 * @param task_name
 * @param func
 * @param arg
 * @param need_free
 * @return
 */
int co_mq_offer(co_mq_t *mq, const char *task_name, co_func_t func, void *arg, int need_free) {
        int ret;
        co_mq_ctx_t *ctx;

        if(list_empty_careful(&mq->queue.list)) {
                schedule_task_new(mq->name, __async_task, mq, -1);
        }

        ret = ymalloc((void **)&ctx, sizeof(co_mq_ctx_t));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ctx->mq = mq;

        // TODO ?
        strcpy(ctx->name, task_name);
        ctx->func = func;
        ctx->arg = arg;
        ctx->need_free = need_free;

        count_list_add_tail(&ctx->hook, &mq->queue);

        return 0;
err_ret:
        return ret;
}

#include <stdio.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "ylib.h"
#include "redis.h"
#include "redis_util.h"
#include "redis_conn.h"
#include "schedule.h"
#include "dbg.h"

typedef struct {
        struct list_head hook;
        fileid_t fileid;
        const char *format;
        va_list ap;
        redisReply *reply;

        task_t task;
} redis_pipline_ctx_t;

static schedule_t *__schedule__ = NULL;
static sy_spinlock_t __lock__;
static struct list_head __list__;
extern __thread int __redis_workerid__;
//static redis_ctx_array_t *__redis_ctx_array__;

static int __redis_pipline_run();

typedef struct {
        task_t task;
        sem_t sem;
        func_va_t exec;
        va_list ap;
        int type;
        int retval;
} arg1_t;

#define REQUEST_SEM 1
#define REQUEST_TASK 2

static void *__redis_schedule(void *arg);

int redis_pipeline_init()
{
        int ret;

        ret = sy_spin_init(&__lock__);
        if (ret)
                GOTO(err_ret, ret);

        INIT_LIST_HEAD(&__list__);
        
        ret = sy_thread_create2(__redis_schedule, NULL, "redis_schedule");
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}


static void __redis_pipeline_request__(void *_ctx)
{
        arg1_t *ctx = _ctx;

        ctx->retval = ctx->exec(ctx->ap);

        if (ctx->type == REQUEST_SEM) {
                sem_post(&ctx->sem);
        } else {
                schedule_resume(&ctx->task, 0, NULL);
        }
}

static int __redis_pipeline_request(const char *name, func_va_t exec, ...)
{
        int ret;
        schedule_t *schedule = __schedule__;
        arg1_t ctx;

        ctx.exec = exec;
        va_start(ctx.ap, exec);

        if (schedule_running()) {
                ctx.type = REQUEST_TASK;
                ctx.task = schedule_task_get();
        } else {
                ctx.type = REQUEST_SEM;
                ret = sem_init(&ctx.sem, 0, 0);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);
        }

        ret = schedule_request(schedule, -1, __redis_pipeline_request__, &ctx, name);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (schedule_running()) {
                ret = schedule_yield1(name, NULL, NULL, NULL, -1);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        } else {
                ret = _sem_wait(&ctx.sem);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        }

        return ctx.retval;
err_ret:
        return ret;
}

static void *__redis_schedule(void *arg)
{
        int ret, interrupt_eventfd;
        char name[MAX_NAME_LEN];
        struct pollfd pfd;
        uint64_t e;

        (void) arg;

        snprintf(name, sizeof(name), "redis");
        ret = schedule_create(&interrupt_eventfd, name, NULL, &__schedule__, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        pfd.fd = interrupt_eventfd;
        pfd.events = POLLIN;

        while (1) { 
                ret = poll(&pfd, 1, 1000 * 1000);
                if (ret  < 0)  {
                        ret = errno;
                        if (ret == EINTR) {
                                DWARN("poll EINTR\n");
                                continue;
                        } else
                                GOTO(err_ret, ret);
                }

                ret = read(interrupt_eventfd, &e, sizeof(e));
                if (ret < 0)  {
                        ret = errno;
                        if (ret == EAGAIN) {
                        } else {
                                GOTO(err_ret, ret);
                        }
                }

                DINFO("poll return\n");

                schedule_run(__schedule__);

                __redis_pipline_run();

                schedule_scan(NULL);
        }

        pthread_exit(NULL);
err_ret:
        UNIMPLEMENTED(__DUMP__);
        pthread_exit(NULL);
}

static int __redis_pipeline(va_list ap)
{
        int ret;
        redis_pipline_ctx_t *ctx = va_arg(ap, redis_pipline_ctx_t *);

        va_end(ap);

        DINFO("%s\n", ctx->format);
        YASSERT(ctx->fileid.type);
        
        ctx->task = schedule_task_get();
        
        ret = sy_spin_lock(&__lock__);
        if (ret)
                GOTO(err_ret, ret);

        list_add_tail(&ctx->hook, &__list__);
        
        sy_spin_unlock(&__lock__);

        ret = schedule_yield1("redis_pipline", NULL, NULL, NULL, -1);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int redis_pipeline(const fileid_t *fileid, redisReply **reply, const char *format, ...)
{
        int ret;
        redis_pipline_ctx_t ctx;

        ctx.format = format;
        ctx.fileid = *fileid;
        va_start(ctx.ap, format);

        DINFO("%s\n", format);
        
        ret = __redis_pipeline_request("pipline", __redis_pipeline, &ctx);
        if (ret)
                GOTO(err_ret, ret);

        *reply = ctx.reply;

        return 0;
err_ret:
        return ret;
}

static int __redis_utils_pipeline(redis_conn_t *conn, struct list_head *list)
{
        int ret;
        struct list_head *pos, *n;
        redis_pipline_ctx_t *ctx;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

        list_for_each_safe(pos, n, list) {
                ctx = (redis_pipline_ctx_t *)pos;

#if 0
                const char *hash = va_arg(ctx->ap, const char *);
                const char *key = va_arg(ctx->ap, const char *);
                DINFO("%s, %s, %s\n", ctx->format, hash, key);
                YASSERT(ctx->format);
                ret = redisAppendCommand(conn->ctx, ctx->format, hash, key);
#else
                ret = redisvAppendCommand(conn->ctx, ctx->format, ctx->ap);
#endif
                
                if ((unlikely(ret)))
                        GOTO(err_lock, ret);
        }

        list_for_each_safe(pos, n, list) {
                ctx = (redis_pipline_ctx_t *)pos;
                list_del(pos);
                
                ret = redisGetReply(conn->ctx, (void **)&ctx->reply);
                if ((unlikely(ret)))
                        GOTO(err_lock, ret);

                schedule_resume(&ctx->task, 0, NULL);
        }

        sy_rwlock_unlock(&conn->rwlock);

        return 0;
err_lock:
        sy_rwlock_unlock(&conn->rwlock);
err_ret:
        return ret;
}

typedef struct {
        struct list_head list;
        fileid_t fileid;
} arg2_t;

static int __redis_exec(va_list ap)
{
        arg2_t *arg2 = va_arg(ap, arg2_t *);

        va_end(ap);

        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;
        fileid_t *fileid = &arg2->fileid;

        id2key(ftype(fileid), fileid, key);

        DINFO(CHKID_FORMAT"\n", CHKID_ARG(fileid));
        
retry:
        ret = redis_conn_get(fileid->volid, fileid->sharding, __redis_workerid__, &handler);
        if(ret)
                GOTO(err_ret, ret);
        
        ret = __redis_utils_pipeline(handler.conn, &arg2->list);
        if(ret) {
                if (ret == ECONNRESET) {
                        redis_conn_close(&handler);
                        redis_conn_release(&handler);
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (100 * 1000));
                }
                
                GOTO(err_release, ret);
        }

        redis_conn_release(&handler);
        
        return 0;
err_release:
        redis_conn_release(&handler);
err_ret:
        return ret;
}

static int __redis_pipline_run()
{
        int ret;
        struct list_head list, *pos, *n;
        redis_pipline_ctx_t *ctx;
        arg2_t arg;

        INIT_LIST_HEAD(&list);
        ret = sy_spin_lock(&__lock__);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        list_splice_init(&__list__, &list);
        
        sy_spin_unlock(&__lock__);

        while (!list_empty(&list)) {
                pos = (void *)list.next;
                ctx = (redis_pipline_ctx_t *)pos;
                arg.fileid = ctx->fileid;
                INIT_LIST_HEAD(&arg.list);
                list_del(pos);
                list_add_tail(pos, &arg.list);
                YASSERT(ctx->fileid.type);
                
                list_for_each_safe(pos, n, &list) {
                        ctx = (redis_pipline_ctx_t *)pos;

                        if (ctx->fileid.sharding == arg.fileid.sharding
                            && ctx->fileid.volid == arg.fileid.volid) {
                                list_del(pos);
                                list_add_tail(pos, &arg.list);
                        }

                        YASSERT(ctx->fileid.type);
                }

                DINFO("%s\n", ctx->format);
                ret = redis_exec(&ctx->fileid, __redis_exec, &arg);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int pipeline_hget(const fileid_t *fileid, const char *key, void *buf, size_t *len)
{
        int ret;
        redisReply *reply;
        char hash[MAX_NAME_LEN];

        id2key(ftype(fileid), fileid, hash);

        DINFO("%s, %s, %s\n", "HGET %s %s", hash, key);
        
        ret = redis_pipeline(fileid, &reply, "HGET %s %s", hash, key);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DWARN("reply %p\n", reply);
        
        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset, hash %s, key %s\n", hash, key);
                GOTO(err_ret, ret);
        }
        
        if (reply->type == REDIS_REPLY_NIL) {
                ret = ENOENT;
                GOTO(err_free, ret);
        }
                
        if (reply->type != REDIS_REPLY_STRING) {
                DWARN("redis reply->type: %d\n", reply->type);
                ret = redis_error(__FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        *len = reply->len;
        memcpy(buf, reply->str, reply->len);

        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int pipeline_hset(const fileid_t *fileid, const char *key, const void *value, uint32_t size, int flag)
{
        int ret;
        redisReply *reply;
        char hash[MAX_NAME_LEN];

        id2key(ftype(fileid), fileid, hash);

        if (flag & O_EXCL) {
                ret = redis_pipeline(fileid, &reply, "HSETNX %s %s %b", hash, key, value, size);
        } else {
                ret = redis_pipeline(fileid, &reply,  "HSET %s %s %b", hash, key, value, size);
        }
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }

        if (reply->type != REDIS_REPLY_INTEGER) {
                ret = redis_error(__FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        //DINFO("reply->integer  %u\n", reply->integer);
        if (flag & O_EXCL && reply->integer == 0) {
                ret = EEXIST;
                GOTO(err_free, ret);
        }
        
        freeReplyObject(reply);

        return 0;
err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int pipeline_hdel(const fileid_t *fileid, const char *key)
{
        int ret;
        redisReply *reply;
        char hash[MAX_NAME_LEN];

        id2key(ftype(fileid), fileid, hash);

        ret = redis_pipeline(fileid, &reply, "HDEL %s %s", hash, key);
        if (unlikely(ret))
                GOTO(err_ret, ret);        

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }
        
        if (reply->type != REDIS_REPLY_INTEGER) {
                ret = redis_error(__FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        if (reply->integer == 0) {
                ret = ENOENT;
                GOTO(err_free, ret);
        }
        
        freeReplyObject(reply);

        return 0;
err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int pipeline_kget(const fileid_t *fileid, const char *key, void *buf, size_t *len)
{
        int ret;
        redisReply *reply;
        
        ret = redis_pipeline(fileid, &reply,"GET %s", key);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }
        
        if (reply->type == REDIS_REPLY_NIL) {
                ret = ENOENT;
                GOTO(err_free, ret);
        }
                
        if (reply->type != REDIS_REPLY_STRING) {
                DWARN("redis reply->type: %d\n", reply->type);
                ret = redis_error(__FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        *len = reply->len;
        memcpy(buf, reply->str, reply->len);

        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int pipeline_kset(const fileid_t *fileid, const char *key, const void *value,
                  size_t size, int flag, int _ttl)
{
        int ret;
        redisReply *reply;

        if (_ttl != -1) {
                size_t ttl = _ttl;
                if (flag & O_EXCL) {
                        ret = redis_pipeline(fileid, &reply, "SET %s %b EX %d NX", key, value, size, ttl);
                } else {
                        ret = redis_pipeline(fileid, &reply, "SET %s %b EX %d", key, value, size, ttl);
                }
        } else {
                if (flag & O_EXCL) {
                        ret = redis_pipeline(fileid, &reply, "SET %s %b NX", key, value, size);
                } else {
                        ret = redis_pipeline(fileid, &reply, "SET %s %b", key, value, size);
                }
        }

        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }

        if (flag & O_EXCL && reply->type == REDIS_REPLY_NIL) {
                ret = EEXIST;
                GOTO(err_free, ret);
        }
        
        if (reply->type != REDIS_REPLY_STATUS || strcmp(reply->str, "OK") != 0) {
                ret = redis_error(__FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        freeReplyObject(reply);

        return 0;
err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

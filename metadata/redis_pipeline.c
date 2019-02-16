#include <stdio.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "ylib.h"
#include "net_global.h"
#include "redis.h"
#include "redis_util.h"
#include "redis_conn.h"
#include "schedule.h"
#include "dbg.h"

#define PIPELINE_PARALLEL 0

typedef struct {
        struct list_head hook;
        fileid_t fileid;
        volid_t volid;
        const char *format;
        va_list ap;
        redisReply *reply;

        task_t task;

        void *pipeline;
} redis_pipline_ctx_t;

#if 0
static schedule_t *__schedule__ = NULL;
static sy_spinlock_t __lock__;
static struct list_head __list__;
#endif
extern __thread int __redis_workerid__;
extern int __redis_conn_pool__;
//static redis_ctx_array_t *__redis_ctx_array__;

typedef struct {
        schedule_t *schedule;
        sy_spinlock_t lock;
        struct list_head list;
        sem_t sem;
} pipeline_t;

static pipeline_t *__pipeline_array__ = NULL;
static int __count__ = 0;
extern int __use_pipeline__;

STATIC int __redis_pipline_run(pipeline_t *pipeline, int interrupt_eventfd);

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

STATIC void *__redis_schedule(void *arg);

int redis_pipeline_init()
{
        int ret, count;
        pipeline_t *pipeline;

#if 0
        count = ng.daemon ? gloconf.polling_core : 1;
#else
        count = __redis_conn_pool__;
#endif
        ret = ymalloc((void **)&__pipeline_array__, sizeof(*__pipeline_array__) * count);
        if (ret)
                GOTO(err_ret, ret);

        memset(__pipeline_array__, 0x0, sizeof(*__pipeline_array__) * count);

        for (int i = 0; i < count; i++) {
                pipeline = &__pipeline_array__[i];
                
                ret = sy_spin_init(&pipeline->lock);
                if (ret)
                        GOTO(err_ret, ret);

                INIT_LIST_HEAD(&pipeline->list);

                ret = sem_init(&pipeline->sem, 0, 0);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
                
                ret = sy_thread_create2(__redis_schedule, pipeline, "redis_schedule");
                if(ret)
                        GOTO(err_ret, ret);

                ret = _sem_wait(&pipeline->sem);
                if(ret) {
                        GOTO(err_ret, ret);
                }
        }

        __count__ = count;
        __use_pipeline__ = 1;
        
        return 0;
err_ret:
        return ret;
}


STATIC void __redis_pipeline_request__(void *_ctx)
{
        arg1_t *ctx = _ctx;

        ctx->retval = ctx->exec(ctx->ap);

        if (ctx->type == REQUEST_SEM) {
                sem_post(&ctx->sem);
        } else {
                schedule_resume(&ctx->task, 0, NULL);
        }
}

STATIC int __redis_pipeline_request(pipeline_t *pipeline, func_va_t exec, ...)
{
        int ret;
        schedule_t *schedule = pipeline->schedule;
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

        ret = schedule_request(schedule, -1, __redis_pipeline_request__, &ctx, "redis_request");
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (schedule_running()) {
                ret = schedule_yield1("redis_request", NULL, NULL, NULL, -1);
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

STATIC void *__redis_schedule(void *arg)
{
        int ret, interrupt_eventfd, idx;
        char name[MAX_NAME_LEN];
        pipeline_t *pipeline = arg;

        snprintf(name, sizeof(name), "redis");
        ret = schedule_create(&interrupt_eventfd, name, &idx, &pipeline->schedule, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        snprintf(name, sizeof(name), "redis[%u]", idx);
        ret = analysis_private_create(name);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }
        
        sem_post(&pipeline->sem);
        
        while (1) {
                ret = eventfd_poll(interrupt_eventfd, 1, NULL);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                DBUG("poll return\n");

#if PIPELINE_PARALLEL
                while (!list_empty(&pipeline->list)) {
                        schedule_run(pipeline->schedule);
                        __redis_pipline_run(pipeline, interrupt_eventfd);
                        schedule_run(pipeline->schedule);
                }
#else
                schedule_run(pipeline->schedule);
                __redis_pipline_run(pipeline, interrupt_eventfd);
                schedule_run(pipeline->schedule);
#endif

                analysis_merge();
                schedule_scan(pipeline->schedule);
        }

        pthread_exit(NULL);
err_ret:
        UNIMPLEMENTED(__DUMP__);
        pthread_exit(NULL);
}

STATIC int __redis_pipeline(va_list ap)
{
        int ret;
        redis_pipline_ctx_t *ctx = va_arg(ap, redis_pipline_ctx_t *);
        pipeline_t *pipeline = ctx->pipeline;

        va_end(ap);

        DBUG("%s\n", ctx->format);
        YASSERT(ctx->fileid.type);
        
        ctx->task = schedule_task_get();
        
        ret = sy_spin_lock(&pipeline->lock);
        if (ret)
                GOTO(err_ret, ret);

        list_add_tail(&ctx->hook, &pipeline->list);
        
        sy_spin_unlock(&pipeline->lock);

        ret = schedule_yield1("redis_pipline", NULL, NULL, NULL, -1);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int redis_pipeline(const volid_t *volid, const fileid_t *fileid, redisReply **reply,
                   const char *format, ...)
{
        int ret;
        redis_pipline_ctx_t ctx;
        pipeline_t *pipeline = &__pipeline_array__[fileid->sharding % __count__];

        ctx.format = format;
        ctx.fileid = *fileid;
        ctx.volid = *volid;
        ctx.pipeline = pipeline;
        va_start(ctx.ap, format);

        DBUG("%s\n", format);
        
        ret = __redis_pipeline_request(pipeline, __redis_pipeline, &ctx);
        if (ret)
                GOTO(err_ret, ret);

        *reply = ctx.reply;

        return 0;
err_ret:
        return ret;
}

STATIC int __redis_utils_pipeline(redis_conn_t *conn, struct list_head *list)
{
        int ret;
        struct list_head *pos, *n;
        redis_pipline_ctx_t *ctx;

        ret = pthread_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

        ANALYSIS_BEGIN(0);
        
        list_for_each_safe(pos, n, list) {
                ctx = (redis_pipline_ctx_t *)pos;
                ret = redisvAppendCommand(conn->ctx, ctx->format, ctx->ap);
                if ((unlikely(ret)))
                        GOTO(err_lock, ret);
        }

        list_for_each_safe(pos, n, list) {
                ctx = (redis_pipline_ctx_t *)pos;
                list_del(pos);
                
                ret = redisGetReply(conn->ctx, (void **)&ctx->reply);

                schedule_resume(&ctx->task, ret, NULL);
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        pthread_rwlock_unlock(&conn->rwlock);

        return 0;
err_lock:
        pthread_rwlock_unlock(&conn->rwlock);
err_ret:
        return ret;
}

typedef struct {
        struct list_head list;
        fileid_t fileid;
        volid_t volid;
        int finished;
} arg2_t;

STATIC int __redis_exec(va_list ap)
{
        arg2_t *arg2 = va_arg(ap, arg2_t *);

        va_end(ap);

        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;
        fileid_t *fileid = &arg2->fileid;

        id2key(ftype(fileid), fileid, key);

        DBUG(CHKID_FORMAT"\n", CHKID_ARG(fileid));

retry:
        ret = redis_conn_get(&arg2->volid, fileid->sharding, __redis_workerid__, &handler);
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

inline STATIC void __redis_pipline_run__(void *_arg)
{
        int ret;
        arg2_t *arg = _arg;

        ret = redis_exec(&arg->fileid, __redis_exec, arg);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        DBUG("-------------return-------------\n");

        arg->finished = 1;
        
        //yfree((void **)&arg);
}

STATIC int __redis_pipline_run(pipeline_t *pipeline, int interrupt_eventfd)
{
        int ret, count = 0;
        struct list_head list, *pos, *n;
        redis_pipline_ctx_t *ctx;
        arg2_t *arg, array[512];

        INIT_LIST_HEAD(&list);
        ret = sy_spin_lock(&pipeline->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        list_splice_init(&pipeline->list, &list);

        sy_spin_unlock(&pipeline->lock);

        count = 0;
        while (!list_empty(&list)) {
                arg = &array[count];
                count++;
                YASSERT(count < 512);

                pos = (void *)list.next;
                ctx = (redis_pipline_ctx_t *)pos;
                arg->fileid = ctx->fileid;
                arg->volid = ctx->volid;
                arg->finished = 0;
                INIT_LIST_HEAD(&arg->list);
                list_del(pos);
                list_add_tail(pos, &arg->list);
                YASSERT(ctx->fileid.type);

                int submit = 1;
                list_for_each_safe(pos, n, &list) {
                        ctx = (redis_pipline_ctx_t *)pos;

                        if (ctx->fileid.sharding == arg->fileid.sharding
                            && ctx->fileid.volid == arg->fileid.volid
                            && ctx->volid.snapvers == arg->volid.snapvers) {
                                list_del(pos);
                                list_add_tail(pos, &arg->list);
                                submit++;
                        }

                        YASSERT(ctx->fileid.type);
                }

                DBUG("submit %u\n", submit);

#if PIPELINE_PARALLEL
                schedule_task_new("pipeline_run", __redis_pipline_run__, arg, -1);
#else
                (void) interrupt_eventfd;
                
                ret = redis_exec(&arg->fileid, __redis_exec, arg);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
#endif
        }

#if PIPELINE_PARALLEL
        while (1) {
                schedule_run(pipeline->schedule);
                
                ret = eventfd_poll(interrupt_eventfd, 1, NULL);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                schedule_run(pipeline->schedule);

                int finished = 0;
                for (int i = 0; i < count; i++) {
                        arg = &array[i];

                        if (arg->finished) {
                                finished++;
                        }
                }

                DBUG("finished %u count %u\n", finished, count);
                if (finished == count) {
                        break;
                }
        }
#endif

        return 0;
err_ret:
        return ret;
}

int pipeline_hget(const volid_t *volid, const fileid_t *fileid, const char *key, void *buf, size_t *len)
{
        int ret;
        redisReply *reply;
        char hash[MAX_NAME_LEN];

        id2key(ftype(fileid), fileid, hash);

        DBUG("%s %s\n", hash, key);
        
        ret = redis_pipeline(volid, fileid, &reply, "HGET %s %s", hash, key);
        if (unlikely(ret))
                GOTO(err_ret, ret);

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

int pipeline_hset(const volid_t *volid, const fileid_t *fileid, const char *key, const void *value, size_t size, int flag)
{
        int ret;
        redisReply *reply;
        char hash[MAX_NAME_LEN];

        id2key(ftype(fileid), fileid, hash);

        DBUG("%s %s, flag 0x%o\n", hash, key, flag);
        
        if (flag & O_EXCL) {
                ret = redis_pipeline(volid, fileid, &reply, "HSETNX %s %s %b", hash, key, value, size);
        } else {
                ret = redis_pipeline(volid, fileid, &reply, "HSET %s %s %b", hash, key, value, size);
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

int pipeline_hdel(const volid_t *volid, const fileid_t *fileid, const char *key)
{
        int ret;
        redisReply *reply;
        char hash[MAX_NAME_LEN];
 
        id2key(ftype(fileid), fileid, hash);

        DBUG("%s %s\n", hash, key);
        
        ret = redis_pipeline(volid, fileid, &reply, "HDEL %s %s", hash, key);
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

STATIC int __pipeline_kget(const volid_t *volid, const fileid_t *fileid, const char *key, void *buf, size_t *len)
{
        int ret;
        redisReply *reply;

        DBUG("%s\n", key);
        
        ret = redis_pipeline(volid, fileid, &reply,"GET %s", key);
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

STATIC int __pipeline_kset(const volid_t *volid, const fileid_t *fileid, const char *key, const void *value,
                  size_t size, int flag, int _ttl)
{
        int ret;
        redisReply *reply;

        DBUG("%s, flag %u, size %ju ttl %d\n", key, flag, size, _ttl);
        
        if (_ttl != -1) {
                size_t ttl = _ttl;
                if (flag & O_EXCL) {
                        ret = redis_pipeline(volid, fileid, &reply, "SET %s %b EX %d NX",
                                             key, value, size, ttl);
                } else {
                        ret = redis_pipeline(volid, fileid, &reply, "SET %s %b EX %d",
                                             key, value, size, ttl);
                }
        } else {
                if (flag & O_EXCL) {
                        ret = redis_pipeline(volid, fileid, &reply, "SET %s %b NX",
                                             key, value, size);
                } else {
                        ret = redis_pipeline(volid, fileid, &reply, "SET %s %b",
                                             key, value, size);
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

STATIC int __pipeline_kdel(const volid_t *volid, const fileid_t *fileid, const char *key)
{
        int ret;
        redisReply *reply;

        DBUG("%s\n", key);
        
        ret = redis_pipeline(volid, fileid, &reply, "DEL %s", key);
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

int pipeline_hlen(const volid_t *volid, const fileid_t *fileid, uint64_t *count)
{
        int ret;
        redisReply *reply;
        char hash[MAX_NAME_LEN];

        id2key(ftype(fileid), fileid, hash);

        DBUG("%s\n", hash);

        ret = redis_pipeline(volid, fileid, &reply, "HLEN %s", hash);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (reply != NULL && reply->type == REDIS_REPLY_INTEGER) {
                *count = reply->integer;
        } else {
                ret = EIO;
                GOTO(err_free, ret);
        }

        freeReplyObject(reply);

        return 0;
err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

STATIC int __pipeline_klock(const volid_t *volid, const fileid_t *fileid, int ttl)
{
        int ret;
        char key[MAX_PATH_LEN], value[MAX_BUF_LEN];

        snprintf(key, MAX_NAME_LEN, "lock:"CHKID_FORMAT, CHKID_ARG(fileid));
        snprintf(value, MAX_NAME_LEN, "%u", ng.local_nid.id);
        ret = __pipeline_kset(volid, fileid, key, value, strlen(value) + 1, O_EXCL, ttl);
        if(ret) {
                GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}

int pipeline_klock(const volid_t *volid, const fileid_t *fileid, int ttl, int block)
{
        int ret, retry = 0;

retry:
        ret = __pipeline_klock(volid, fileid, ttl);
        if(ret) {
                if (ret == EEXIST && block) {
                        if (retry > 500 && retry % 100 == 0 ) {
                                DWARN("lock "CHKID_FORMAT", retry %u\n", CHKID_ARG(fileid), retry);
                        }
                        USLEEP_RETRY(err_ret, ret, retry, retry, 1000, (1 * 1000));
                } else {
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        ret = ret == EEXIST ? EAGAIN : ret;
        return ret;
}

int pipeline_kunlock(const volid_t *volid, const fileid_t *fileid)
{
        int ret;
        char key[MAX_PATH_LEN];

        snprintf(key, MAX_NAME_LEN, "lock:"CHKID_FORMAT, CHKID_ARG(fileid));
        ret = __pipeline_kdel(volid, fileid, key);
        if(ret) {
                GOTO(err_ret, ret);
        }
        
        return 0;
err_ret: 
        ret = ret == ENOENT ? EAGAIN : ret;
        return ret;
}

int pipeline_kget(const volid_t *volid, const fileid_t *fileid, void *buf, size_t *len)
{
        char key[MAX_NAME_LEN];

        id2key(ftype(fileid), fileid, key);
        
        return __pipeline_kget(volid, fileid, key, buf, len);
}

int pipeline_kset(const volid_t *volid, const fileid_t *fileid, const void *value,
                  size_t size, int flag, int _ttl)
{
        char key[MAX_NAME_LEN];
 
        id2key(ftype(fileid), fileid, key);
        
        return __pipeline_kset(volid, fileid, key, value, size, flag, _ttl);
}

int pipeline_kdel(const volid_t *volid, const fileid_t *fileid)
{
        char key[MAX_NAME_LEN];

        id2key(ftype(fileid), fileid, key);
        
        return __pipeline_kdel(volid, fileid, key);
}

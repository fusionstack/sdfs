#include <stdio.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <stdarg.h>
#include <sys/eventfd.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "ylib.h"
#include "net_global.h"
#include "redis.h"
#include "redis_util.h"
#include "redis_conn.h"
#include "schedule.h"
#include "dbg.h"

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

typedef struct {
        struct list_head list;
        fileid_t fileid;
        volid_t volid;
        int finished;
} arg2_t;

typedef struct {
        sy_spinlock_t lock;
        int eventfd;
        int running;
        struct list_head list;
} pipeline_t;

static __thread pipeline_t *__pipeline__ = NULL;
extern __thread int __use_pipline__;

#define REDIS_PIPLINE_THREAD 1

static int __redis_pipline_run(struct list_head *list);

static void *__redis_pipeline_worker(void *arg)
{
        int ret;
        pipeline_t *pipeline = arg;
        struct list_head list;

        ret = redis_vol_private_init();
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        DINFO("pipeline worker start %u\n", pipeline->running);

        while (pipeline->running) {
                eventfd_poll(pipeline->eventfd, 1, NULL);

                if (pipeline->running == 0) {
                        DINFO("destroyed\n");
                        break;
                }

                INIT_LIST_HEAD(&list);

                ret = sy_spin_lock(&pipeline->lock);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);
                
                list_splice_init(&pipeline->list, &list);

                sy_spin_unlock(&pipeline->lock);
        
                __redis_pipline_run(&list);
        }

        redis_vol_private_destroy(redis_conn_vol_close);
        
        pthread_exit(NULL);
}
        

int redis_pipeline_init()
{
        int ret;
        pipeline_t *pipeline;

        DINFO("pipeline start\n");
        
        ret = ymalloc((void **)&pipeline, sizeof(*pipeline));
        if (ret)
                GOTO(err_ret, ret);

        memset(pipeline, 0x0, sizeof(*pipeline));

        INIT_LIST_HEAD(&pipeline->list);
        pipeline->running = 1;

        ret = sy_spin_init(&pipeline->lock);
        if (ret)
                UNIMPLEMENTED(__WARN__);

#if !REDIS_PIPLINE_THREAD
        ret = redis_vol_private_init();
        if (ret)
                GOTO(err_ret, ret);
#endif

        int fd = eventfd(0, EFD_CLOEXEC);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        pipeline->eventfd = fd;
        
        ret = sy_thread_create2(__redis_pipeline_worker, pipeline, "__core_worker");
        if (ret)
                GOTO(err_ret, ret);

        __pipeline__ = pipeline;
        __use_pipline__ = 1;
        
        return 0;
err_ret:
        return ret;
}

void redis_pipeline_destroy()
{
        int ret;
        uint64_t e;

        UNIMPLEMENTED(__WARN__);

#if !REDIS_PIPLINE_THREAD
        redis_vol_private_destroy(redis_conn_vol_close);
#endif
        
        pipeline_t *pipeline = __pipeline__;
        pipeline->running = 0;

        ret = write(pipeline->eventfd, &e, sizeof(e));
        if (ret < 0) {
                ret = errno;
                UNIMPLEMENTED(__WARN__);
        }
}

int redis_pipeline(const volid_t *volid, const fileid_t *fileid, redisReply **reply,
                   const char *format, ...)
{
        int ret;
        redis_pipline_ctx_t ctx;
        pipeline_t *pipeline = __pipeline__;

        YASSERT(fileid->type);
        
        ctx.format = format;
        ctx.fileid = *fileid;
        ctx.volid = *volid;
        ctx.pipeline = pipeline;
        ctx.task = schedule_task_get();
        va_start(ctx.ap, format);

        ret = sy_spin_lock(&pipeline->lock);
        if (ret)
                UNIMPLEMENTED(__WARN__);

        list_add_tail(&ctx.hook, &pipeline->list);

        sy_spin_unlock(&pipeline->lock);

        DBUG("%s\n", format);

        ret = schedule_yield1("redis_pipline", NULL, NULL, NULL, -1);
        if (ret)
                GOTO(err_ret, ret);

        *reply = ctx.reply;

        return 0;
err_ret:
        return ret;
}

STATIC int __redis_utils_pipeline1(const arg2_t *arg2, redis_handler_t *handler, struct list_head *list)
{
        int ret;
        struct list_head *pos, *n;
        redis_pipline_ctx_t *ctx;
        redis_conn_t *conn;

        ret = redis_conn_get(&arg2->volid, arg2->fileid.sharding, 0, handler);
        if(ret)
                UNIMPLEMENTED(__DUMP__);

        conn = handler->conn;
        
        ret = pthread_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                UNIMPLEMENTED(__DUMP__);

        ANALYSIS_BEGIN(0);
        
        list_for_each_safe(pos, n, list) {
                ctx = (redis_pipline_ctx_t *)pos;
 
                ret = redisvAppendCommand(conn->ctx, ctx->format, ctx->ap);
                if ((unlikely(ret)))
                        UNIMPLEMENTED(__DUMP__);
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
}

STATIC void __redis_utils_pipeline2(redis_handler_t *handler, struct list_head *list)
{
        int ret;
        struct list_head *pos, *n;
        redis_pipline_ctx_t *ctx;
        redis_conn_t *conn;

        ANALYSIS_BEGIN(0);

        conn = handler->conn;
        
        list_for_each_safe(pos, n, list) {
                ctx = (redis_pipline_ctx_t *)pos;
                list_del(pos);
                
                ret = redisGetReply(conn->ctx, (void **)&ctx->reply);

                schedule_resume(&ctx->task, ret, NULL);
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        pthread_rwlock_unlock(&conn->rwlock);
        redis_conn_release(handler);
}

STATIC int __redis_exec(arg2_t *array, int count)
{
        int ret, i;
        redis_handler_t handler_array[512], *handler;
        arg2_t *arg2;

        ANALYSIS_BEGIN(0);
        
        for (i = 0; i < count; i++) {
                handler = &handler_array[i];
                arg2 = &array[i];

                ret = __redis_utils_pipeline1(arg2, handler, &arg2->list);
                if ((unlikely(ret)))
                        UNIMPLEMENTED(__DUMP__);
       }

        
        for (i = 0; i < count; i++) {
                handler = &handler_array[i];
                arg2 = &array[i];

                __redis_utils_pipeline2(handler, &arg2->list);
        }        

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
}

static int __redis_pipline_run(struct list_head *list)
{
        int ret, count = 0;
        struct list_head *pos, *n;
        redis_pipline_ctx_t *ctx;
        arg2_t *arg, array[512];

        count = 0;
        while (!list_empty(list)) {
                arg = &array[count];
                count++;
                YASSERT(count < 512);

                pos = (void *)list->next;
                ctx = (redis_pipline_ctx_t *)pos;
                arg->fileid = ctx->fileid;
                arg->volid = ctx->volid;
                arg->finished = 0;
                INIT_LIST_HEAD(&arg->list);
                list_del(pos);
                list_add_tail(pos, &arg->list);
                YASSERT(ctx->fileid.type);

                int submit = 1;
                list_for_each_safe(pos, n, list) {
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
        }

        ret = __redis_exec(array, count);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

#if REDIS_PIPLINE_THREAD
int redis_pipline_run()
{
        int ret;
        uint64_t e;
        pipeline_t *pipeline = __pipeline__;

        if (pipeline && !list_empty(&pipeline->list)) {
                ret = write(pipeline->eventfd, &e, sizeof(e));
                if (ret < 0) {
                        ret = errno;
                        UNIMPLEMENTED(__WARN__);
                }
        }

        return 0;
}

#else

int redis_pipline_run()
{
        int ret;
        struct list_head list;
        pipeline_t *pipeline = __pipeline__;    

        if (pipeline == NULL) {
                return 0;
        }
        
        INIT_LIST_HEAD(&list);

        ret = sy_spin_lock(&pipeline->lock);
        if (ret)
                UNIMPLEMENTED(__DUMP__);
        
        list_splice_init(&pipeline->list, &list);

        sy_spin_unlock(&pipeline->lock);
        
        ret = __redis_pipline_run(&list);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}
#endif

int pipeline_hget(const volid_t *volid, const fileid_t *fileid, const char *key,
                  void *buf, size_t *len)
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

int pipeline_hset(const volid_t *volid, const fileid_t *fileid, const char *key,
                  const void *value, size_t size, int flag)
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

STATIC int __pipeline_kget(const volid_t *volid, const fileid_t *fileid,
                           const char *key, void *buf, size_t *len)
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

STATIC int __pipeline_kset(const volid_t *volid, const fileid_t *fileid,
                           const char *key, const void *value,
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
                                DWARN("lock "CHKID_FORMAT", retry %u\n",
                                      CHKID_ARG(fileid), retry);
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

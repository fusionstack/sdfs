#include <stdio.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <stdarg.h>
#include <sys/eventfd.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "ylib.h"
#include "plock.h"
#include "net_global.h"
#include "redis.h"
#include "redis_util.h"
#include "redis_conn.h"
#include "schedule.h"
#include "variable.h"
#include "dbg.h"

#define REDIS_CO_THREAD 0

#if ENABLE_LOCK_FREE_LIST
#include <ll.h>
#endif

typedef struct redis_co_ctx {
        struct list_head hook;
        fileid_t fileid;
        volid_t volid;
        const char *format;
        va_list ap;
        redisReply *reply;
        task_t task;
        void *co;
#if ENABLE_LOCK_FREE_LIST
        uint32_t magic;
        LL_ENTRY(redis_co_ctx)entry;
#endif
} redis_co_ctx_t;

#if ENABLE_LOCK_FREE_LIST
LL_HEAD(co_list, redis_co_ctx);
LL_GENERATE(co_list, redis_co_ctx, entry);
#endif

typedef struct {
        struct list_head list;
        fileid_t fileid;
        volid_t volid;
        int finished;
} arg2_t;

typedef struct {
        struct list_head hook;
        redis_handler_t *handler_array;
        arg2_t *array;
        int count;
        task_t task;
} arg1_t;

typedef struct {
        plock_t plock;
#if REDIS_CO_THREAD
        int eventfd;
#if ENABLE_LOCK_FREE_LIST
        struct co_list list;
#else
        sy_spinlock_t lock;
        struct list_head queue2;
#endif
#endif
        int running;
        struct list_head queue1;
} co_t;

__thread int __use_co__ = 0;

static int __redis_co_run(void *ctx, struct list_head *list);
static void __redis_co_recv(arg2_t *array, redis_handler_t *handler_array, int count);

#if REDIS_CO_THREAD
static void *__redis_co_worker(void *arg)
{
        co_t *co = arg;
        struct list_head list, *pos, *n;
        arg1_t *arg1;

        DINFO("co worker start %u\n", co->running);

        __use_co__ = 1;        
        while (co->running) {
                eventfd_poll(co->eventfd, 1, NULL);

                if (co->running == 0) {
                        DINFO("destroyed\n");
                        break;
                }

#if ENABLE_LOCK_FREE_LIST
                INIT_LIST_HEAD(&list);

                redis_co_ctx_t *ctx = NULL;

                while (1) {
                        ctx = LL_FIRST(co_list, &co->list);
                        if (ctx == NULL)
                                break;
                        
                        //assert(o->satelite == i);
                        LL_UNLINK(co_list, &co->list, ctx);
                        DBUG("------------release 0x%o-------------\n", ctx->magic);
                        list_add_tail(&ctx->hook, &list);
                }

#else
                int ret;
                if (list_empty(&co->queue2)) {
                        continue;
                }

                INIT_LIST_HEAD(&list);

                ret = sy_spin_lock(&co->lock);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);

                list_splice_init(&co->queue2, &list);

                sy_spin_unlock(&co->lock);
#endif

                list_for_each_safe(pos, n, &list) {
                        arg1 = (void *)pos;
                        __redis_co_recv(arg1->array, arg1->handler_array, arg1->count);
                }

                schedule_resume(&arg1->task, 0, NULL);
        }

        pthread_exit(NULL);
}
#endif        

int redis_co_init()
{
        int ret;
        co_t *co;

        DINFO("co start\n");
        
        ret = ymalloc((void **)&co, sizeof(*co));
        if (ret)
                GOTO(err_ret, ret);

        memset(co, 0x0, sizeof(*co));

        INIT_LIST_HEAD(&co->queue1);
        co->running = 1;

        ret = plock_init(&co->plock, "redis_co");
        if (ret)
                GOTO(err_ret, ret);
        
#if REDIS_CO_THREAD
#if ENABLE_LOCK_FREE_LIST
        LL_INIT(&(co->list));
#else
        INIT_LIST_HEAD(&co->queue2);

        ret = sy_spin_init(&co->lock);
        if (ret)
                UNIMPLEMENTED(__WARN__);
#endif
        int fd = eventfd(0, EFD_CLOEXEC);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        co->eventfd = fd;
        
        ret = sy_thread_create2(__redis_co_worker, co, "__core_worker");
        if (ret)
                GOTO(err_ret, ret);
#endif
        ret = redis_vol_private_init();
        if (ret)
                GOTO(err_ret, ret);

        variable_set(VARIABLE_REDIS, co);
        __use_co__ = 1;
        
        return 0;
err_ret:
        return ret;
}

int redis_co_destroy()
{
        UNIMPLEMENTED(__WARN__);

        co_t *co = variable_get(VARIABLE_REDIS);
        co->running = 0;
        
#if REDIS_CO_THREAD
#if 1
        int ret;
        uint64_t e = 1;

        ret = write(co->eventfd, &e, sizeof(e));
        if (ret < 0) {
                ret = errno;
                UNIMPLEMENTED(__DUMP__);
        }
#endif

        close(co->eventfd);
#endif
        redis_vol_private_destroy(redis_conn_vol_close);
        variable_unset(VARIABLE_REDIS);

        return 0;
}

int redis_co(const volid_t *volid, const fileid_t *fileid, redisReply **reply,
                   const char *format, ...)
{
        int ret;
        redis_co_ctx_t ctx;
        co_t *co = variable_get(VARIABLE_REDIS);

        ANALYSIS_BEGIN(0);
        
        YASSERT(fileid->type);
        
        ctx.format = format;
        ctx.fileid = *fileid;
        ctx.volid = *volid;
        ctx.co = co;
        ctx.task = schedule_task_get();
        va_start(ctx.ap, format);

        list_add_tail(&ctx.hook, &co->queue1);

        DBUG("%s\n", format);

        ret = schedule_yield1("redis_co", NULL, NULL, NULL, -1);
        if (ret)
                GOTO(err_ret, ret);

        *reply = ctx.reply;

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

STATIC int __redis_utils_co1(const arg2_t *arg2, redis_handler_t *handler,
                                   struct list_head *list)
{
        int ret;
        struct list_head *pos, *n;
        redis_co_ctx_t *ctx;
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
                ctx = (redis_co_ctx_t *)pos;
 
                ret = redisvAppendCommand(conn->ctx, ctx->format, ctx->ap);
                if ((unlikely(ret)))
                        UNIMPLEMENTED(__DUMP__);
        }

        int done;
        ret = redisBufferWrite(conn->ctx, &done);
        DBUG("ret %d %d\n", ret, done);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
}

#if 1
STATIC int __redis_utils_co2(redis_handler_t *handler, struct list_head *list, int retry)
{
        int ret;
        struct list_head *pos, *n;
        redis_co_ctx_t *ctx;
        redis_conn_t *conn;

#if !REDIS_THREAD
        ANALYSIS_BEGIN(0);
#endif

        if (list_empty(list)) {
                return 0;
        }
        
        conn = handler->conn;

        ret = redisBufferRead(conn->ctx);
        if (ret) {
                DBUG("-----ret %u--\n", ret, retry);
                return 1;
        }
        
        list_for_each_safe(pos, n, list) {
                ctx = (redis_co_ctx_t *)pos;

                ret = redisGetReply(conn->ctx, (void **)&ctx->reply);
                if (ret || ctx->reply == NULL) {
                        DBUG("-----ret %u reply %p, retry %u--\n", ret, ctx->reply, retry);

                        return 1;
                } else {
                        DBUG("-----ret %u reply %p, retry %u--\n", ret, ctx->reply, retry);
                }

                list_del(pos);
                schedule_resume(&ctx->task, ret, NULL);
        }

#if !REDIS_THREAD
        ANALYSIS_QUEUE(0, IO_WARN, NULL);
#endif

        DBUG("----get reply success, retry %u----\n", retry);
        
        return 0;
}

static void __redis_co_recv(arg2_t *array, redis_handler_t *handler_array, int count)
{
        arg2_t *arg2;
        redis_handler_t *handler;

        int retry = 0;

        ANALYSIS_BEGIN(0);

        while (1) {
                int fail = 0;
                for (int i = 0; i < count; i++) {
                        handler = &handler_array[i];
                        arg2 = &array[i];

                        fail += __redis_utils_co2(handler, &arg2->list, retry);
                }

                DBUG("------------fail %u------------\n", fail);
                
                if (fail == 0) {
                        break;
                } else {
                        retry++;
                }
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
}
#else
STATIC void __redis_utils_co2(redis_handler_t *handler, struct list_head *list)
{
        int ret;
        struct list_head *pos, *n;
        redis_co_ctx_t *ctx;
        redis_conn_t *conn;

        ANALYSIS_BEGIN(0);

        conn = handler->conn;
        
        list_for_each_safe(pos, n, list) {
                ctx = (redis_co_ctx_t *)pos;
                list_del(pos);
                
                ret = redisGetReply(conn->ctx, (void **)&ctx->reply);

                schedule_resume(&ctx->task, ret, NULL);
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
}

static void __redis_co_recv(arg2_t *array, redis_handler_t *handler_array, int count)
{
        arg2_t *arg2;
        redis_handler_t *handler;

        for (int i = 0; i < count; i++) {
                handler = &handler_array[i];
                arg2 = &array[i];

                __redis_utils_co2(handler, &arg2->list);
        }
}
#endif

static void __redis_co_release(redis_handler_t *handler_array, int count)
{
        redis_handler_t *handler;
        redis_conn_t *conn;

        for (int i = 0; i < count; i++) {
                handler = &handler_array[i];
                conn = handler->conn;
                pthread_rwlock_unlock(&conn->rwlock);
                redis_conn_release(handler);
        }
}

STATIC int __redis_co_exec(void *core_ctx, arg2_t *array, int count)
{
        int ret, i;
        redis_handler_t handler_array[512], *handler;
        arg2_t *arg2;

        ANALYSIS_BEGIN(0);
        
        for (i = 0; i < count; i++) {
                handler = &handler_array[i];
                arg2 = &array[i];

                ret = __redis_utils_co1(arg2, handler, &arg2->list);
                if ((unlikely(ret)))
                        UNIMPLEMENTED(__DUMP__);
        }

#if REDIS_CO_THREAD
        arg1_t arg1;
        arg1.array = array;
        arg1.handler_array = handler_array;
        arg1.count = count;
        arg1.task = schedule_task_get();
        co_t *co = variable_get_byctx(core_ctx, VARIABLE_REDIS);
        
        ret = sy_spin_lock(&co->lock);
        if(ret)
                UNIMPLEMENTED(__DUMP__);

        list_add_tail(&arg1.hook, &co->queue2);
        
        sy_spin_unlock(&co->lock);

        uint64_t e = 1;
        ret = write(co->eventfd, &e, sizeof(e));
        if (ret < 0) {
                ret = errno;
                UNIMPLEMENTED(__DUMP__);
        }

        ret = schedule_yield1("redis_co", NULL, NULL, NULL, -1);
        if(ret)
                UNIMPLEMENTED(__DUMP__);
#else
        (void) core_ctx;
        __redis_co_recv(array, handler_array, count);
#endif
        
        __redis_co_release(handler_array, count);
        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
}

static int __redis_co_run(void *core_ctx, struct list_head *list)
{
        int ret, count = 0;
        struct list_head *pos, *n;
        redis_co_ctx_t *ctx;
        arg2_t *arg, array[512];

        ANALYSIS_BEGIN(0);
        count = 0;
        while (!list_empty(list)) {
                arg = &array[count];
                count++;
                YASSERT(count < 512);

                pos = (void *)list->next;
                ctx = (redis_co_ctx_t *)pos;
                arg->fileid = ctx->fileid;
                arg->volid = ctx->volid;
                arg->finished = 0;
                INIT_LIST_HEAD(&arg->list);
                list_del(pos);
                list_add_tail(pos, &arg->list);
                YASSERT(ctx->fileid.type);

                int submit = 1;
                list_for_each_safe(pos, n, list) {
                        ctx = (redis_co_ctx_t *)pos;

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

        ret = __redis_co_exec(core_ctx, array, count);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

static void __redis_co_run_task(void *ctx)
{
        int ret;
        struct list_head list;
        co_t *co = variable_get_byctx(ctx, VARIABLE_REDIS);

        if (list_empty(&co->queue1)) {
                return;
        }

        INIT_LIST_HEAD(&list);

        list_splice_init(&co->queue1, &list);
        
        ret = plock_wrlock(&co->plock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        __redis_co_run(ctx, &list);

        plock_unlock(&co->plock);
}

int redis_co_run(void *ctx)
{
        co_t *co = variable_get_byctx(ctx, VARIABLE_REDIS);

        if (co == NULL) {
                return 0;
        }

        if (list_empty(&co->queue1)) {
                return 0;
        }

        schedule_task_new("redis_co_run", __redis_co_run_task, ctx, -1);
        schedule_run(variable_get_byctx(ctx, VARIABLE_SCHEDULE));
        
#if 0        
        
#if ENABLE_LOCK_FREE_LIST

        redis_co_ctx_t *co_ctx;
        struct list_head *pos, *n;

        list_for_each_safe(pos, n, &co->queue1) {
                list_del(pos);
                co_ctx = (redis_co_ctx_t *)pos;
                LL_INIT_ENTRY(&co_ctx->entry);
                co_ctx->magic = _random();
                LL_PUSH_BACK(co_list, &co->list, co_ctx);
                //LL_RELEASE(co_list, &co->list, co_ctx);
                DBUG("------------push 0x%o-------------\n", co_ctx->magic);
        }
        
#else
        ret = sy_spin_lock(&co->lock);
        if ((unlikely(ret)))
                UNIMPLEMENTED(__DUMP__);
                
        list_splice_init(&co->queue1, &co->queue2);
                
        sy_spin_unlock(&co->lock);
#endif

        uint64_t e = 1;
        ret = write(co->eventfd, &e, sizeof(e));
        if (ret < 0) {
                ret = errno;
                UNIMPLEMENTED(__DUMP__);
        }

#endif
        
        return 0;
}


int co_hget(const volid_t *volid, const fileid_t *fileid, const char *key,
                  void *buf, size_t *len)
{
        int ret;
        redisReply *reply;
        char hash[MAX_NAME_LEN];

        ANALYSIS_BEGIN(0);
        
        id2key(ftype(fileid), fileid, hash);

        DBUG("%s %s\n", hash, key);
        
        ret = redis_co(volid, fileid, &reply, "HGET %s %s", hash, key);
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
        
        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        return 0;

err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int co_hset(const volid_t *volid, const fileid_t *fileid, const char *key,
                  const void *value, size_t size, int flag)
{
        int ret;
        redisReply *reply;
        char hash[MAX_NAME_LEN];

        ANALYSIS_BEGIN(0);
        
        id2key(ftype(fileid), fileid, hash);

        DBUG("%s %s, flag 0x%o\n", hash, key, flag);
        
        if (flag & O_EXCL) {
                ret = redis_co(volid, fileid, &reply, "HSETNX %s %s %b", hash, key, value, size);
        } else {
                ret = redis_co(volid, fileid, &reply, "HSET %s %s %b", hash, key, value, size);
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

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int co_hdel(const volid_t *volid, const fileid_t *fileid, const char *key)
{
        int ret;
        redisReply *reply;
        char hash[MAX_NAME_LEN];

        ANALYSIS_BEGIN(0);

        id2key(ftype(fileid), fileid, hash);

        DBUG("%s %s\n", hash, key);
        
        ret = redis_co(volid, fileid, &reply, "HDEL %s %s", hash, key);
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
        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        return 0;
err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

STATIC int __co_kget(const volid_t *volid, const fileid_t *fileid,
                           const char *key, void *buf, size_t *len)
{
        int ret;
        redisReply *reply;

        DBUG("%s\n", key);
        
        ret = redis_co(volid, fileid, &reply,"GET %s", key);
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

STATIC int __co_kset(const volid_t *volid, const fileid_t *fileid,
                           const char *key, const void *value,
                           size_t size, int flag, int _ttl)
{
        int ret;
        redisReply *reply;

        DBUG("%s, flag %u, size %ju ttl %d\n", key, flag, size, _ttl);
        
        if (_ttl != -1) {
                size_t ttl = _ttl;
                if (flag & O_EXCL) {
                        ret = redis_co(volid, fileid, &reply, "SET %s %b EX %d NX",
                                             key, value, size, ttl);
                } else {
                        ret = redis_co(volid, fileid, &reply, "SET %s %b EX %d",
                                             key, value, size, ttl);
                }
        } else {
                if (flag & O_EXCL) {
                        ret = redis_co(volid, fileid, &reply, "SET %s %b NX",
                                             key, value, size);
                } else {
                        ret = redis_co(volid, fileid, &reply, "SET %s %b",
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
                DBUG("exist\n");
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

STATIC int __co_kdel(const volid_t *volid, const fileid_t *fileid, const char *key)
{
        int ret;
        redisReply *reply;

        DBUG("%s\n", key);
        
        ret = redis_co(volid, fileid, &reply, "DEL %s", key);
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

int co_hlen(const volid_t *volid, const fileid_t *fileid, uint64_t *count)
{
        int ret;
        redisReply *reply;
        char hash[MAX_NAME_LEN];

        ANALYSIS_BEGIN(0);
        id2key(ftype(fileid), fileid, hash);

        DBUG("%s\n", hash);

        ret = redis_co(volid, fileid, &reply, "HLEN %s", hash);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (reply != NULL && reply->type == REDIS_REPLY_INTEGER) {
                *count = reply->integer;
        } else {
                ret = EIO;
                GOTO(err_free, ret);
        }

        freeReplyObject(reply);
        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        return 0;
err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

STATIC int __co_klock(const volid_t *volid, const fileid_t *fileid, int ttl)
{
        int ret;
        char key[MAX_PATH_LEN], value[MAX_BUF_LEN];

        snprintf(key, MAX_NAME_LEN, "lock:"CHKID_FORMAT, CHKID_ARG(fileid));
        snprintf(value, MAX_NAME_LEN, "%u", ng.local_nid.id);
        ret = __co_kset(volid, fileid, key, value, strlen(value) + 1, O_EXCL, ttl);
        if(ret) {
                GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}

int co_klock(const volid_t *volid, const fileid_t *fileid, int ttl, int block)
{
        int ret, retry = 0;

        ANALYSIS_BEGIN(0);
retry:
        ret = __co_klock(volid, fileid, ttl);
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

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        return 0;
err_ret:
        ret = ret == EEXIST ? EAGAIN : ret;
        return ret;
}

int co_kunlock(const volid_t *volid, const fileid_t *fileid)
{
        int ret;
        char key[MAX_PATH_LEN];
        
        ANALYSIS_BEGIN(0);
        snprintf(key, MAX_NAME_LEN, "lock:"CHKID_FORMAT, CHKID_ARG(fileid));
        ret = __co_kdel(volid, fileid, key);
        if(ret) {
                GOTO(err_ret, ret);
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        return 0;
err_ret: 
        ret = ret == ENOENT ? EAGAIN : ret;
        return ret;
}

int co_kget(const volid_t *volid, const fileid_t *fileid, void *buf, size_t *len)
{
        int ret;
        char key[MAX_NAME_LEN];

        ANALYSIS_BEGIN(0);
        id2key(ftype(fileid), fileid, key);
        
        ret = __co_kget(volid, fileid, key, buf, len);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        return ret;
}

int co_kset(const volid_t *volid, const fileid_t *fileid, const void *value,
                  size_t size, int flag, int _ttl)
{
        int ret;
        char key[MAX_NAME_LEN];

        ANALYSIS_BEGIN(0);
        id2key(ftype(fileid), fileid, key);
        
        ret = __co_kset(volid, fileid, key, value, size, flag, _ttl);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        return ret;
}

int co_kdel(const volid_t *volid, const fileid_t *fileid)
{
        int ret;
        char key[MAX_NAME_LEN];

        ANALYSIS_BEGIN(0);
        id2key(ftype(fileid), fileid, key);
        
        ret = __co_kdel(volid, fileid, key);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        return ret;
}

int co_newsharing(const volid_t *volid, uint8_t *idx)
{
        int ret, seq;
        redis_vol_t *vol;

        ANALYSIS_BEGIN(0);

retry:
        ret = redis_vol_get(volid, (void **)&vol);
        if(ret) {
                if (ret == ENOENT) {
                        ret = redis_conn_vol(volid);
                        if(ret)
                                GOTO(err_ret, ret);

                        goto retry;
                } else
                        GOTO(err_ret, ret);
        }
        
        if (ng.daemon) {
                seq = ++ vol->sequence;
        } else {
                seq = _random();
        }

        *idx = seq % vol->sharding;

#if 0
        DINFO("sharding %u %u %u\n", *idx, vol->sequence, vol->sharding);
#endif

        redis_vol_release(volid);

        ANALYSIS_END(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

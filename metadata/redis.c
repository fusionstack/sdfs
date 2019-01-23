
//#include <hircluster.h>

#define DBG_SUBSYS S_YFSLIB

#include "chk_proto.h"
#include "redis_util.h"
#include "redis.h"
#include "redis_conn.h"
#include "configure.h"
#include "net_global.h"
#include "dbg.h"
#include "adt.h"
#include "cJSON.h"
#include "network.h"
#include "sdfs_lib.h"
#include "schedule.h"
#include "md_lib.h"
#include "math.h"

static int __seq__ = 0;

#define ASYNC 1

int init_redis()
{
        int ret;

        ret = redis_conn_init();
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __hget__(const fileid_t *fileid, const char *name, char *value, size_t *size)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        ANALYSIS_BEGIN(0);
        
        id2key(ftype(fileid), fileid, key);

retry:
        ret = redis_conn_get(fileid->volid, fileid->sharding, &handler);
        if(ret)
                GOTO(err_ret, ret);
        
        ret = redis_hget(handler.conn, key, name, value, size);
        if(ret) {
                if (ret == ECONNRESET) {
                        redis_conn_close(&handler);
                        redis_conn_release(&handler);
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (100 * 1000));
                }
                
                GOTO(err_release, ret);
        }

        redis_conn_release(&handler);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_release:
        redis_conn_release(&handler);
err_ret:
        return ret;
}

static int __hget(va_list ap)
{
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        const char *name = va_arg(ap, const char *);
        char *value = va_arg(ap, char *);
        size_t *size = va_arg(ap, size_t *);

        va_end(ap);

        return __hget__(fileid, name, value, size);
}

int hget(const fileid_t *fileid, const char *name, char *value, size_t *size)
{
        YASSERT(fileid->type);

        if (likely(schedule_running() && ASYNC)) {
                return schedule_newthread(SCHE_THREAD_REDIS, ++__seq__, FALSE,
                                          "hget", -1, __hget,
                                          fileid, name, value, size);
        } else {
                return __hget__(fileid, name, value, size);
        }
}


static int __hset__(const fileid_t *fileid, const char *name, const void *value, uint32_t size, int flag)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        ANALYSIS_BEGIN(0);
        
        id2key(ftype(fileid), fileid, key);

retry:
        ret = redis_conn_get(fileid->volid, fileid->sharding, &handler);
        if(ret)
                GOTO(err_ret, ret);

#if 0
        if (flag) {
                DINFO("create "CHKID_FORMAT" @ redis[%u]\n",
                      CHKID_ARG(fileid), handler.sharding);
        }
#endif
        
        ret = redis_hset(handler.conn, key, name, value, size, flag);
        if(ret) {
                if (ret == ECONNRESET) {
                        redis_conn_close(&handler);
                        redis_conn_release(&handler);
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (100 * 1000));
                }
                
                GOTO(err_release, ret);
        }

        redis_conn_release(&handler);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_release:
        redis_conn_release(&handler);
err_ret:
        return ret;
}

static int __hset(va_list ap)
{
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        const char *name = va_arg(ap, const char *);
        const char *value = va_arg(ap, const char *);
        uint32_t size = va_arg(ap, uint32_t);
        int flag = va_arg(ap, int);

        va_end(ap);

        return __hset__(fileid, name, value, size, flag);
}


int hset(const fileid_t *fileid, const char *name, const void *value, uint32_t size, int flag)
{
        if (likely(schedule_running() && ASYNC)) {
                return schedule_newthread(SCHE_THREAD_REDIS, ++__seq__, FALSE,
                                          "hset", -1, __hset,
                                          fileid, name, value, size, flag);
        } else {
                return __hset__(fileid, name, value, size, flag);
        }
}

static int __hlen__(const fileid_t *fileid, uint64_t *count)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        id2key(ftype(fileid), fileid, key);

retry:
        ret = redis_conn_get(fileid->volid, fileid->sharding, &handler);
        if(ret)
                GOTO(err_ret, ret);
        
        ret = redis_hlen(handler.conn, key, count);
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

static int __hlen(va_list ap)
{
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        uint64_t *count = va_arg(ap, uint64_t *);

        va_end(ap);

        return __hlen__(fileid, count);
}


int hlen(const fileid_t *fileid, uint64_t *count)
{
        if (likely(schedule_running() && ASYNC)) {
                return schedule_newthread(SCHE_THREAD_REDIS, ++__seq__, FALSE,
                                          "hlen", -1, __hlen,
                                          fileid, count);
        } else {
                return __hlen__(fileid, count);
        }
}

redisReply *__hscan__(const fileid_t *fileid, const char *match, uint64_t cursor, uint64_t count)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;
        redisReply *reply;

        id2key(ftype(fileid), fileid, key);

retry:
        ret = redis_conn_get(fileid->volid, fileid->sharding, &handler);
        if(ret)
                GOTO(err_ret, ret);
        
        reply = redis_hscan(handler.conn, key, match, cursor, count);
        if (reply == NULL) {
                redis_conn_close(&handler);
                redis_conn_release(&handler);
                USLEEP_RETRY(err_ret, ret, retry, retry, 100, (100 * 1000));
        }

        redis_conn_release(&handler);
        
        return reply;
err_ret:
        return NULL;
}

static int __hscan(va_list ap)
{
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        const char *match = va_arg(ap, const char *);
        uint64_t cursor = va_arg(ap, uint64_t);
        uint64_t count = va_arg(ap, uint64_t);
        redisReply **_redisReply = va_arg(ap, redisReply **);

        va_end(ap);

        *_redisReply = __hscan__(fileid, match, cursor, count);
        return 0;
}


redisReply *hscan(const fileid_t *fileid, const char *match, uint64_t cursor, uint64_t count)
{
        if (likely(schedule_running() && ASYNC)) {
                redisReply *reply;
                schedule_newthread(SCHE_THREAD_REDIS, ++__seq__, FALSE,
                                   "hscan", -1, __hscan,
                                   fileid, match, cursor, count, &reply);

                return reply;
        } else {
                return __hscan__(fileid, match, cursor, count);
        }
}


static int __hdel__(const fileid_t *fileid, const char *name)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        id2key(ftype(fileid), fileid, key);

retry:
        ret = redis_conn_get(fileid->volid, fileid->sharding, &handler);
        if(ret)
                GOTO(err_ret, ret);
        
        ret = redis_hdel(handler.conn, key, name);
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

static int __hdel(va_list ap)
{
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        const char *name = va_arg(ap, const char *);

        va_end(ap);

        return __hdel__(fileid, name);
}


int hdel(const fileid_t *fileid, const char *name)
{
        if (likely(schedule_running() && ASYNC)) {
                return schedule_newthread(SCHE_THREAD_REDIS, ++__seq__, FALSE,
                                          "hdel", -1, __hdel,
                                          fileid, name);
        } else {
                return __hdel__(fileid, name);
        }
}

static int __kget__(const fileid_t *fileid, void *value, size_t *size)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        id2key(ftype(fileid), fileid, key);

retry:
        ret = redis_conn_get(fileid->volid, fileid->sharding, &handler);
        if(ret)
                GOTO(err_ret, ret);
        

        ret = redis_kget(handler.conn, key, value, size);
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

static int __kget(va_list ap)
{
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        void *value = va_arg(ap, void *);
        size_t *size = va_arg(ap, size_t *);

        va_end(ap);

        return __kget__(fileid, value, size);
}


int kget(const fileid_t *fileid, void *value, size_t *size)
{
        if (likely(schedule_running() && ASYNC)) {
                return schedule_newthread(SCHE_THREAD_REDIS, ++__seq__, FALSE,
                                          "kget", -1, __kget,
                                          fileid, value, size);
        } else {
                return __kget__(fileid, value, size);
        }
}

static int __kset__(const fileid_t *fileid, const void *value, size_t size, int flag)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        id2key(ftype(fileid), fileid, key);

retry:
        ret = redis_conn_get(fileid->volid, fileid->sharding, &handler);
        if(ret)
                GOTO(err_ret, ret);
        

        ret = redis_kset(handler.conn, key, value, size, flag, -1);
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

static int __kset(va_list ap)
{
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        const void *value = va_arg(ap, const void *);
        size_t size = va_arg(ap, size_t);
        int flag = va_arg(ap, int);

        va_end(ap);

        return __kset__(fileid, value, size, flag);
}


int kset(const fileid_t *fileid, const void *value, size_t size, int flag)
{
        if (likely(schedule_running() && ASYNC)) {
                return schedule_newthread(SCHE_THREAD_REDIS, ++__seq__, FALSE,
                                          "kset", -1, __kset,
                                          fileid, value, size, flag);
        } else {
                return __kset__(fileid, value, size, flag);
        }
}

static int __kdel__(const fileid_t *fileid)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        id2key(ftype(fileid), fileid, key);

retry:
        ret = redis_conn_get(fileid->volid, fileid->sharding, &handler);
        if(ret)
                GOTO(err_ret, ret);
        
        ret = redis_kdel(handler.conn, key);
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

static int __kdel(va_list ap)
{
        const fileid_t *fileid = va_arg(ap, const fileid_t *);

        va_end(ap);

        return __kdel__(fileid);
}


int kdel(const fileid_t *fileid)
{
        if (likely(schedule_running() && ASYNC)) {
                return schedule_newthread(SCHE_THREAD_REDIS, ++__seq__, FALSE,
                                          "kdel", -1, __kdel,
                                          fileid);
        } else {
                return __kdel__(fileid);
        }
}

static int __klock1(const fileid_t *fileid, int ttl)
{
        int ret, retry = 0;
        redis_handler_t handler;
        char key[MAX_PATH_LEN], value[MAX_BUF_LEN];

retry:
        ret = redis_conn_get(fileid->volid, fileid->sharding, &handler);
        if(ret)
                GOTO(err_ret, ret);

        snprintf(key, MAX_NAME_LEN, "lock:"CHKID_FORMAT, CHKID_ARG(fileid));
        snprintf(value, MAX_NAME_LEN, "%u", ng.local_nid.id);
        ret = redis_kset(handler.conn, key, value, strlen(value) + 1, O_EXCL, ttl);
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

static int __klock__(const fileid_t *fileid, int ttl, int block)
{
        int ret, retry = 0;

retry:
        ret = __klock1(fileid, ttl);
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

inline static int __klock(va_list ap)
{
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        int ttl = va_arg(ap, int);
        int block = va_arg(ap, int);

        va_end(ap);

        return __klock__(fileid, ttl, block);
}

int klock(const fileid_t *fileid, int ttl, int block)
{
#if ENABLE_KLOCK
        if (likely(schedule_running() && ASYNC)) {
                return schedule_newthread(SCHE_THREAD_REDIS, ++__seq__, FALSE,
                                          "klock", -1, __klock,
                                          fileid, ttl, block);
        } else {
                return __klock__(fileid, ttl, block);
        }
#else
        (void) fileid;
        (void) ttl;
        (void) block;
        return 0;
#endif
}

static int __kunlock__(const fileid_t *fileid)
{
        int ret, retry = 0;
        redis_handler_t handler;
        char key[MAX_PATH_LEN];

retry:
        ret = redis_conn_get(fileid->volid, fileid->sharding, &handler);
        if(ret)
                GOTO(err_ret, ret);

        snprintf(key, MAX_NAME_LEN, "lock:"CHKID_FORMAT, CHKID_ARG(fileid));
        ret = redis_del(handler.conn, key);
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
        ret = ret == ENOENT ? EAGAIN : ret;
        return ret;
}

inline static int __kunlock(va_list ap)
{
        const fileid_t *fileid = va_arg(ap, const fileid_t *);

        va_end(ap);

        return __kunlock__(fileid);
}


int kunlock(const fileid_t *fileid)
{
#if ENABLE_KLOCK
        if (likely(schedule_running() && ASYNC)) {
                return schedule_newthread(SCHE_THREAD_REDIS, ++__seq__, FALSE,
                                          "kunlock", -1, __kunlock,
                                          fileid);
        } else {
                return __kunlock__(fileid);
        }
#else
        (void) fileid;
        return 0;
#endif
}

static int __hiter__(const fileid_t *fileid, const char *match, func2_t func, void *ctx)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        id2key(ftype(fileid), fileid, key);

retry:
        ret = redis_conn_get(fileid->volid, fileid->sharding, &handler);
        if(ret)
                GOTO(err_ret, ret);

        ret = redis_hiterator(handler.conn, key, match, func, ctx);
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

static int __hiter(va_list ap)
{
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        const char *match = va_arg(ap, const char *);
        func2_t func = va_arg(ap, func2_t);
        void *ctx = va_arg(ap, void *);

        va_end(ap);

        return __hiter__(fileid, match, func, ctx);
}


int hiter(const fileid_t *fileid, const char *match, func2_t func, void *ctx)
{
        if (likely(schedule_running() && ASYNC)) {
                return schedule_newthread(SCHE_THREAD_REDIS, ++__seq__, FALSE,
                                          "hiter", -1, __hiter,
                                          fileid, match, func, ctx);
        } else {
                return __hiter__(fileid, match, func, ctx);
        }
}

static int __rm_push__(const nid_t *nid, int _hash, const chkid_t *chkid)
{
        int ret, hash, retry = 0;
        redis_handler_t handler;
        char key[MAX_PATH_LEN], value[MAX_BUF_LEN];
        uint64_t volid;

        hash = _hash == -1 ? (int)nid->id : _hash;

        ret = md_system_volid(&volid);
        if(ret)
                GOTO(err_ret, ret);
        
retry:
        ret = redis_conn_get(volid, hash, &handler);
        if(ret)
                GOTO(err_ret, ret);

        snprintf(key, MAX_NAME_LEN, "cds[%d]", nid->id);
        base64_encode((void *)chkid, sizeof(*chkid), value);
        ret = redis_sset(handler.conn, key, value);
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

static int __rm_push(va_list ap)
{
        const nid_t *nid = va_arg(ap, const nid_t *);
        int _hash = va_arg(ap, int);
        const chkid_t *chkid = va_arg(ap, const chkid_t *);

        va_end(ap);

        return __rm_push__(nid, _hash, chkid);
}


int rm_push(const nid_t *nid, int _hash, const chkid_t *chkid)
{

        DBUG("remove "CHKID_FORMAT" @ %s\n", CHKID_ARG(chkid), network_rname(nid));
        
        if (likely(schedule_running() && ASYNC)) {
                return schedule_newthread(SCHE_THREAD_REDIS, ++__seq__, FALSE,
                                          "rm_push", -1, __rm_push,
                                          nid, _hash, chkid);
        } else {
                return __rm_push__(nid, _hash, chkid);
        }
}



typedef struct {
        const nid_t *nid;
        int count;
        int idx;
        chkid_t *array;
        redis_conn_t *conn;
        const char *key;
} rm_pop_ctx_t;

static void __rm_pop_itor(void *key, void *_ctx)
{
        int ret;
        rm_pop_ctx_t *ctx = _ctx;
        char buf[MAX_BUF_LEN];
        chkid_t *chkid;
        int valuelen;

        if (ctx->idx + 1 > ctx->count) {
                return;
        }

        ret = redis_sdel(ctx->conn, ctx->key, key);
        if (ret) {
                DWARN("remove %s %s fail", ctx->key, key);
                return;
        }
        
        chkid = (void *)buf;
        valuelen = MAX_BUF_LEN;
        base64_decode((char *)key, &valuelen, (void *)chkid);

        DINFO("remove "CHKID_FORMAT" @ %s\n", CHKID_ARG(chkid), network_rname(ctx->nid));
        
        YASSERT(valuelen == sizeof(*chkid));

        ctx->array[ctx->idx] = *chkid;
        ctx->idx++;
}

static int __rm_pop__(const nid_t *nid, int _hash, chkid_t *array, int *count)
{
        int ret, hash, retry = 0;
        redis_handler_t handler;
        char key[MAX_PATH_LEN];
        rm_pop_ctx_t ctx;
        uint64_t volid;

        hash = _hash == -1 ? (int)nid->id : _hash;
 
        ret = md_system_volid(&volid);
        if(ret)
                GOTO(err_ret, ret);

retry:
        ret = redis_conn_get(volid, hash, &handler);
        if(ret)
                GOTO(err_ret, ret);

        ctx.count = *count;
        ctx.idx = 0;
        ctx.array = array;
        ctx.conn = handler.conn;
        ctx.key = key;
        ctx.nid = nid;
        
        snprintf(key, MAX_NAME_LEN, "cds[%d]", nid->id);
        ret = redis_siterator(handler.conn, key, __rm_pop_itor, &ctx);
        if(ret) {
                if (ret == ECONNRESET) {
                        redis_conn_close(&handler);
                        redis_conn_release(&handler);
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (100 * 1000));
                }

                GOTO(err_release, ret);
        }

        redis_conn_release(&handler);

        *count = ctx.idx;
        
        return 0;
err_release:
        redis_conn_release(&handler);
err_ret:
        return ret;
}

static int __rm_pop(va_list ap)
{
        const nid_t *nid = va_arg(ap, const nid_t *);
        int _hash = va_arg(ap, int);
        chkid_t *array = va_arg(ap, chkid_t *);
        int *count = va_arg(ap, int *);

        va_end(ap);

        return __rm_pop__(nid, _hash, array, count);
}


int rm_pop(const nid_t *nid, int _hash, chkid_t *array, int *count)
{
        if (likely(schedule_running() && ASYNC)) {
                return schedule_newthread(SCHE_THREAD_REDIS, ++__seq__, FALSE,
                                          "rm_pop", -1, __rm_pop,
                                          nid, _hash, array, count);
        } else {
                return __rm_pop__(nid, _hash, array, count);
        }
}

struct sche_thread_ops redis_ops = {
        .type           = SCHE_THREAD_REDIS,
        .begin_trans    = NULL,
        .commit_trans   = NULL,
};

static int __redis_ops_register()
{
        int size = ng.daemon ? 16 : 2;
        return sche_thread_ops_register(&redis_ops, redis_ops.type, size);
}

int redis_init()
{
        int ret;

        ret = __redis_ops_register();
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

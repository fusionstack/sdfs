#include <sys/eventfd.h>

#define DBG_SUBSYS S_YFSLIB

#include "chk_proto.h"
#include "redis_util.h"
#include "redis.h"
#include "redis_conn.h"
#include "redis_pipeline.h"
#include "redis_co.h"
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

typedef enum {
        __type_sche__,
        __type_sem__,
} type_t;

typedef struct {
        struct list_head hook;
        func_va_t exec;
        va_list ap;
        int type;
        task_t task;
        int retval;
        sem_t sem;
} entry_t;

typedef struct {
        int idx;
        sy_spinlock_t lock;
        struct list_head list;
        sem_t sem;
        int eventfd;
} redis_worker_t;

static int __seq__ = 0;
 __thread int __redis_workerid__ = -1;
static redis_worker_t *__redis_worker__;
static int __worker_count__ = 0;
int __redis_conn_pool__ = -1;
__thread int __redis_conn_pool_private__ = -1;
int __use_pipeline__ = 0;
extern __thread int __use_co__;

static int __redis_request(const int hash, const char *name, func_va_t exec, ...);

static int __hget__(const volid_t *volid, const fileid_t *fileid, const char *name,
                    char *value, size_t *size)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        ANALYSIS_BEGIN(0);
        
        id2key(ftype(fileid), fileid, key);
        
retry:
        ret = redis_conn_get(volid, fileid->sharding, __redis_workerid__, &handler);
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
        const volid_t *volid = va_arg(ap, const volid_t *);
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        const char *name = va_arg(ap, const char *);
        char *value = va_arg(ap, char *);
        size_t *size = va_arg(ap, size_t *);

        va_end(ap);

        return __hget__(volid, fileid, name, value, size);
}

int hget(const volid_t *volid, const fileid_t *fileid, const char *name,
         char *value, size_t *size)
{
        int ret;

        YASSERT(fileid->type);
        volid_t _volid = {fileid->volid, 0};
        if (unlikely(volid == NULL)) {
                volid = &_volid;
        }

        YASSERT(volid);

        ANALYSIS_BEGIN(0);

        if (__use_co__) { 
                ret = co_hget(volid, fileid, name, value, size);
        } else if (__use_pipeline__) {
                ret = pipeline_hget(volid, fileid, name, value, size);
        } else  {
                ret =  __redis_request(fileid_hash(fileid), "hget", __hget,
                                       volid, fileid, name, value, size);
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return ret;
}

static int __hset__(const volid_t *volid, const fileid_t *fileid,
                    const char *name, const void *value, uint32_t size, int flag)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        ANALYSIS_BEGIN(0);
        
        id2key(ftype(fileid), fileid, key);

retry:
        ret = redis_conn_get(volid, fileid->sharding, __redis_workerid__, &handler);
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
        const volid_t *volid = va_arg(ap, const volid_t *);
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        const char *name = va_arg(ap, const char *);
        const char *value = va_arg(ap, const char *);
        uint32_t size = va_arg(ap, uint32_t);
        int flag = va_arg(ap, int);

        va_end(ap);

        return __hset__(volid, fileid, name, value, size, flag);
}

int hset(const volid_t *volid, const fileid_t *fileid, const char *name,
         const void *value, uint32_t size, int flag)
{
        int ret;
        
        ANALYSIS_BEGIN(0);

        volid_t _volid = {fileid->volid, 0};
        if (unlikely(volid == NULL)) {
                volid = &_volid;
        }

        YASSERT(volid->volid);
        if (__use_co__) { 
                ret = co_hset(volid, fileid, name, value, size, flag);
        } else if (__use_pipeline__) {
                ret = pipeline_hset(volid, fileid, name, value, size, flag);
        } else {
                ret = __redis_request(fileid_hash(fileid), "hset", __hset,
                                      volid, fileid, name, value, size, flag);
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return ret;
}

static int __hlen__(const volid_t *volid, const fileid_t *fileid, uint64_t *count)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        id2key(ftype(fileid), fileid, key);

retry:
        ret = redis_conn_get(volid, fileid->sharding, __redis_workerid__, &handler);
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
        const volid_t *volid = va_arg(ap, const volid_t *);
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        uint64_t *count = va_arg(ap, uint64_t *);

        va_end(ap);

        return __hlen__(volid, fileid, count);
}

int hlen(const volid_t *volid, const fileid_t *fileid, uint64_t *count)
{
        int ret;

        ANALYSIS_BEGIN(0);
        volid_t _volid = {fileid->volid, 0};
        if (unlikely(volid == NULL)) {
                volid = &_volid;
        }

        if (__use_co__) { 
                ret = co_hlen(volid, fileid, count);
        } else if (__use_pipeline__) {
                ret = pipeline_hlen(volid, fileid, count);
        } else {
                ret = __redis_request(fileid_hash(fileid), "hlen", __hlen,
                                      volid, fileid, count);

        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        return ret;
}

redisReply *__hscan__(const volid_t *volid, const fileid_t *fileid,
                      const char *match, uint64_t cursor, uint64_t count)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;
        redisReply *reply;

        id2key(ftype(fileid), fileid, key);
retry:
        ret = redis_conn_get(volid, fileid->sharding, __redis_workerid__, &handler);
        if(ret) {
                DWARN("retry %u\n", retry);
                USLEEP_RETRY(err_ret, ret, retry, retry, 100, (100 * 1000));
                GOTO(err_ret, ret);
        }
        
        reply = redis_hscan(handler.conn, key, match, cursor, count);
        if (reply == NULL) {
                redis_conn_close(&handler);
                redis_conn_release(&handler);
                DWARN("retry %u\n", retry);
                USLEEP_RETRY(err_ret, ret, retry, retry, 100, (100 * 1000));
        }

        redis_conn_release(&handler);
        
        return reply;
err_ret:
        UNIMPLEMENTED(__DUMP__);
        return NULL;
}

static int __hscan(va_list ap)
{
        const volid_t *volid = va_arg(ap, const volid_t *);
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        const char *match = va_arg(ap, const char *);
        uint64_t cursor = va_arg(ap, uint64_t);
        uint64_t count = va_arg(ap, uint64_t);
        redisReply **_redisReply = va_arg(ap, redisReply **);

        va_end(ap);

        *_redisReply = __hscan__(volid, fileid, match, cursor, count);
        return 0;
}


redisReply *hscan(const volid_t *volid, const fileid_t *fileid, const char *match,
                  uint64_t cursor, uint64_t count)
{
        redisReply *reply;
        volid_t _volid = {fileid->volid, 0};
        if (unlikely(volid == NULL)) {
                volid = &_volid;
        }

        __redis_request(fileid_hash(fileid), "hscan", __hscan,
                        volid, fileid, match, cursor, count, &reply);

        return reply;
}


static int __hdel__(const volid_t *volid, const fileid_t *fileid, const char *name)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        id2key(ftype(fileid), fileid, key);
retry:
        ret = redis_conn_get(volid, fileid->sharding, __redis_workerid__, &handler);
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
        const volid_t *volid = va_arg(ap, const volid_t *);
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        const char *name = va_arg(ap, const char *);

        va_end(ap);

        return __hdel__(volid, fileid, name);
}


int hdel(const volid_t *volid, const fileid_t *fileid, const char *name)
{
        volid_t _volid = {fileid->volid, 0};
        if (unlikely(volid == NULL)) {
                volid = &_volid;
        }

        if (__use_co__) { 
                return co_hdel(volid, fileid, name);
        } else if (__use_pipeline__) {
                return pipeline_hdel(volid, fileid, name);
        } else {
                return __redis_request(fileid_hash(fileid), "hdel", __hdel,
                                       volid, fileid, name);
        }
}

static int __kget__(const volid_t *volid, const fileid_t *fileid, void *value, size_t *size)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        id2key(ftype(fileid), fileid, key);
retry:
        ret = redis_conn_get(volid, fileid->sharding, __redis_workerid__, &handler);
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
        const volid_t *volid = va_arg(ap, const volid_t *);
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        void *value = va_arg(ap, void *);
        size_t *size = va_arg(ap, size_t *);

        va_end(ap);

        return __kget__(volid, fileid, value, size);
}

int kget(const volid_t *volid, const fileid_t *fileid, void *value, size_t *size)
{
        int ret;

        volid_t _volid = {fileid->volid, 0};
        if (unlikely(volid == NULL)) {
                volid = &_volid;
        }

        if (__use_co__) { 
                ret = co_kget(volid, fileid, value, size);
        } else if (__use_pipeline__) {
                ret = pipeline_kget(volid, fileid, value, size);
        } else {
                ret = __redis_request(fileid_hash(fileid), "kget", __kget,
                                      volid, fileid, value, size);
        }

        return ret;
}

static int __kset__(const volid_t *volid, const fileid_t *fileid,
                    const void *value, size_t size, int flag)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        id2key(ftype(fileid), fileid, key);
retry:
        ret = redis_conn_get(volid, fileid->sharding, __redis_workerid__, &handler);
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
        const volid_t *volid = va_arg(ap, const volid_t *);
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        const void *value = va_arg(ap, const void *);
        size_t size = va_arg(ap, size_t);
        int flag = va_arg(ap, int);

        va_end(ap);

        return __kset__(volid, fileid, value, size, flag);
}

int kset(const volid_t *volid, const fileid_t *fileid, const void *value, size_t size, int flag)
{
        int ret;
        
        volid_t _volid = {fileid->volid, 0};
        if (unlikely(volid == NULL)) {
                volid = &_volid;
        }

        if (__use_co__) { 
                ret = co_kset(volid, fileid, value, size, flag, -1);
        } else if (__use_pipeline__) {
                ret = pipeline_kset(volid, fileid, value, size, flag, -1);
        } else {
                ret = __redis_request(fileid_hash(fileid), "kset", __kset,
                                      volid, fileid, value, size, flag);
        }

        return ret;
}

static int __kdel__(const volid_t *volid, const fileid_t *fileid)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        id2key(ftype(fileid), fileid, key);
retry:
        ret = redis_conn_get(volid, fileid->sharding, __redis_workerid__, &handler);
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
        const volid_t *volid = va_arg(ap, const volid_t *);
        const fileid_t *fileid = va_arg(ap, const fileid_t *);

        va_end(ap);

        return __kdel__(volid, fileid);
}


int kdel(const volid_t *volid, const fileid_t *fileid)
{
        volid_t _volid = {fileid->volid, 0};
        if (unlikely(volid == NULL)) {
                volid = &_volid;
        }

        if (__use_co__) { 
                return co_kdel(volid, fileid);
        } else if (__use_pipeline__) {
                return pipeline_kdel(volid, fileid);
        } else {
                return __redis_request(fileid_hash(fileid), "kdel", __kdel,
                                       volid, fileid);
        }
}

static int __klock1(const volid_t *volid, const fileid_t *fileid, int ttl)
{
        int ret, retry = 0;
        redis_handler_t handler;
        char key[MAX_PATH_LEN], value[MAX_BUF_LEN];

retry:
        ret = redis_conn_get(volid, fileid->sharding, __redis_workerid__, &handler);
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

static int __klock__(const volid_t *volid, const fileid_t *fileid, int ttl, int block)
{
        int ret, retry = 0;

retry:
        ret = __klock1(volid, fileid, ttl);
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
        const volid_t *volid = va_arg(ap, const volid_t *);
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        int ttl = va_arg(ap, int);
        int block = va_arg(ap, int);

        va_end(ap);

        return __klock__(volid, fileid, ttl, block);
}

int klock(const volid_t *volid, const fileid_t *fileid, int ttl, int block)
{
        volid_t _volid = {fileid->volid, 0};
        if (unlikely(volid == NULL)) {
                volid = &_volid;
        }

#if ENABLE_KLOCK
        int ret;
        
        ANALYSIS_BEGIN(0);

        if (__use_co__) { 
                ret = co_klock(volid, fileid, ttl, block);
        } else if (__use_pipeline__) {
                ret = pipeline_klock(volid, fileid, ttl, block);
        } else {
                ret = __redis_request(fileid_hash(fileid), "klock", __klock,
                                      volid, fileid, ttl, block);
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        return ret;
#else
        (void) fileid;
        (void) ttl;
        (void) block;
        return 0;
#endif
}

static int __kunlock__(const volid_t *volid, const fileid_t *fileid)
{
        int ret, retry = 0;
        redis_handler_t handler;
        char key[MAX_PATH_LEN];

retry:
        ret = redis_conn_get(volid, fileid->sharding, __redis_workerid__, &handler);
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
        const volid_t *volid = va_arg(ap, const volid_t *);
        const fileid_t *fileid = va_arg(ap, const fileid_t *);

        va_end(ap);

        return __kunlock__(volid, fileid);
}

int kunlock(const volid_t *volid, const fileid_t *fileid)
{
        volid_t _volid = {fileid->volid, 0};
        if (unlikely(volid == NULL)) {
                volid = &_volid;
        }
#if ENABLE_KLOCK
        int ret;

        ANALYSIS_BEGIN(0);

        if (__use_co__) { 
                ret = co_kunlock(volid, fileid);
        } else if (__use_pipeline__) {
                ret = pipeline_kunlock(volid, fileid);
        } else {
                ret = __redis_request(fileid_hash(fileid), "kunlock", __kunlock,
                                      volid, fileid);
        }

        ANALYSIS_END(0, IO_WARN, NULL);
        return ret;
#else
        (void) fileid;
        return 0;
#endif
}

static int __hiter__(const volid_t *volid, const fileid_t *fileid,
                     const char *match, func2_t func, void *ctx)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];
        redis_handler_t handler;

        id2key(ftype(fileid), fileid, key);
retry:
        ret = redis_conn_get(volid, fileid->sharding, __redis_workerid__, &handler);
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
        const volid_t *volid = va_arg(ap, const volid_t *);
        const fileid_t *fileid = va_arg(ap, const fileid_t *);
        const char *match = va_arg(ap, const char *);
        func2_t func = va_arg(ap, func2_t);
        void *ctx = va_arg(ap, void *);

        va_end(ap);

        return __hiter__(volid, fileid, match, func, ctx);
}


int hiter(const volid_t *volid, const fileid_t *fileid, const char *match,
          func2_t func, void *ctx)
{
        volid_t _volid = {fileid->volid, 0};
        if (unlikely(volid == NULL)) {
                volid = &_volid;
        }

        return __redis_request(fileid_hash(fileid), "hiter", __hiter,
                               volid, fileid, match, func, ctx);
}

static int __rm_push__(const nid_t *nid, int _hash, const chkid_t *chkid)
{
        int ret, hash, retry = 0;
        redis_handler_t handler;
        char key[MAX_PATH_LEN], value[MAX_BUF_LEN];
        uint64_t sysvolid;

        hash = _hash == -1 ? (int)nid->id : _hash;

        ret = md_system_volid(&sysvolid);
        if(ret)
                GOTO(err_ret, ret);

        volid_t volid = {sysvolid, 0};
retry:
        ret = redis_conn_get(&volid, hash, ++__seq__, &handler);
        if(ret) {
                if (ret == EBUSY) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 1000, (10 * 1000));
                } else 
                        GOTO(err_ret, ret);
        }

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
        
        return __redis_request(++__seq__, "rm_push", __rm_push,
                               nid, _hash, chkid);
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
        uint64_t sysvolid;;

        hash = _hash == -1 ? (int)nid->id : _hash;
 
        ret = md_system_volid(&sysvolid);
        if(ret)
                GOTO(err_ret, ret);

        volid_t volid = {sysvolid, 0};
retry:
        ret = redis_conn_get(&volid, hash, ++__seq__, &handler);
        if(ret) {
                if (ret == EBUSY) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 1000, (10 * 1000));
                } else 
                        GOTO(err_ret, ret);
        }

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
        return __redis_request(++__seq__, "rm_pop", __rm_pop,
                               nid, _hash, array, count);
}

static int __redis_worker_run(redis_worker_t *worker)
{
        int ret;
        struct list_head list, *pos, *n;
        entry_t *ent;

        while (1) {
                INIT_LIST_HEAD(&list);

                ret = sy_spin_lock(&worker->lock);
                if(ret)
                        GOTO(err_ret, ret);

                list_splice_init(&worker->list, &list);
                
                sy_spin_unlock(&worker->lock);

                list_for_each_safe(pos, n, &list) {
                        list_del(pos);
                        ent = (void *)pos;

                        ent->retval = ent->exec(ent->ap);

                        if (ent->type == __type_sem__) {
                                sem_post(&ent->sem);
                        } else {
                                schedule_resume(&ent->task, ent->retval, NULL);
                        }
                }

                if (list_empty(&worker->list)) {
                        break;
                }
        }

        return 0;
err_ret:
        return ret;
}

static void *__redis_worker(void *arg)
{
        int ret;
        redis_worker_t *worker = arg;

        __redis_workerid__ = worker->idx;

        sem_post(&worker->sem);
        
        while (1) {
                eventfd_poll(worker->eventfd, 1, NULL);

                ret = __redis_worker_run(worker);
                if(ret)
                        UNIMPLEMENTED(__DUMP__);
        }

        pthread_exit(NULL);
}

static int __redis_request__(const int hash, entry_t *ent)
{
        int ret;
        uint64_t e = 1;
        redis_worker_t *worker = &__redis_worker__[hash % __worker_count__];

        ret = sy_spin_lock(&worker->lock);
        if(ret)
                GOTO(err_ret, ret);
        
        list_add_tail(&ent->hook, &worker->list);

        sy_spin_unlock(&worker->lock);

        ret = write(worker->eventfd, &e, sizeof(e));
        if (ret < 0) {
                ret = errno;
                YASSERT(0);
        }

        return 0;
err_ret:
        return ret;
}


static int __redis_request(const int hash, const char *name, func_va_t exec, ...)
{
        int ret;
        entry_t ctx;

        ctx.exec = exec;
        va_start(ctx.ap, exec);

        ANALYSIS_BEGIN(0);

        if (likely(schedule_running())) {
                ctx.task = schedule_task_get();
                ctx.type = __type_sche__;

                ret = __redis_request__(hash, &ctx);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ret = schedule_yield1(name, NULL, NULL, NULL, -1);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        } else {
                ctx.type = __type_sem__;

                ret = sem_init(&ctx.sem, 0, 0);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ret = __redis_request__(hash, &ctx);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
                
                ret = sem_wait(&ctx.sem);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ret = ctx.retval;
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        ANALYSIS_END(0, IO_WARN, NULL);

        return 0;
err_ret:
        ANALYSIS_END(0, IO_WARN, NULL);
        return ret;
}

int redis_init(int _worker_count)
{
        int ret;
        redis_worker_t *worker;
        int worker_count = _worker_count * 4;

        ret = ymalloc((void **)&__redis_worker__, sizeof(*__redis_worker__) * worker_count);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (int i = 0; i < worker_count; i++) {
                worker = &__redis_worker__[i];
                worker->idx = i;
                ret = sy_spin_init(&worker->lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ret = sem_init(&worker->sem, 0, 0);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ret = eventfd(0, EFD_CLOEXEC);
                if (unlikely(ret < 0)) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                worker->eventfd = ret;
                
                INIT_LIST_HEAD(&worker->list);

                ret = sy_thread_create2(__redis_worker, worker, "redis_worker");
                if(ret)
                        GOTO(err_ret, ret);

                ret = sem_wait(&worker->sem);
                if(ret)
                        GOTO(err_ret, ret);
        }

        __worker_count__ = worker_count;
        __redis_conn_pool__ = worker_count;

        ret = redis_conn_init();
        if(ret)
                GOTO(err_ret, ret);
        
#if ENABLE_REDIS_PIPELINE
        ret = redis_pipeline_init();
        if(ret)
                GOTO(err_ret, ret);
#endif

        return 0;
err_ret:
        return ret;
}

int redis_exec(const fileid_t *fileid, func_va_t exec, void *arg)
{
        return __redis_request(fileid_hash(fileid), "exec", exec, arg);
}

int redis_new_sharding(const volid_t *volid, uint8_t *idx)
{
        if (__use_co__) {
                return co_newsharing(volid, idx);
        } else {
                return redis_conn_new(volid, idx);
        }
}

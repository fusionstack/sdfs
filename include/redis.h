#ifndef REDIS_UTIL_H
#define REDIS_UTIL_H

#include <hiredis/hiredis.h>
//#include <hircluster.h>
#include "sdfs_id.h"
#include "cJSON.h"
#include "zlib.h"

#define REDIS_THREAD_MAX 256
#define REDIS_INSTANCE_MAX 1000
#define SELF_NAME "@@"

typedef struct{
        redisContext *ctx;
        pthread_mutex_t lock;
} redis_context_t;

static inline int fid2key(const fileid_t *fid, char *key)
{
        sprintf(key, "%llu", (LLU)fid->id);
        return 0;
}

static inline int id2key(const char *prefix, const fileid_t *fid, char *key)
{
        YASSERT(fid->type);
        sprintf(key, "%s:%llu/%llu", prefix, (LLU)fid->volid, (LLU)fid->id);
        return 0;
}

extern int init_redis();

extern int kdel(const fileid_t *fid);
extern int kset(const fileid_t *fid, const void *buf, size_t size, int flag);
extern int kget(const fileid_t *fid, void *buf, size_t *size);

extern int klock(const fileid_t *fileid, int ttl, int block);
extern int kunlock(const fileid_t *fileid);
extern int hiter(const fileid_t *fid, const char *match, func2_t func, void *ctx);
extern int rm_push(const nid_t *nid, int _hash, const chkid_t *chkid);
extern int rm_pop(const nid_t *nid, int _hash, chkid_t *array, int *count);
extern int redis_exec(const fileid_t *fileid, func_va_t exec, void *arg);
extern int redis_init();

extern int hset(const fileid_t *fid, const char *name, const void *buf, uint32_t size, int flag);
extern int hget(const fileid_t *fid, const char *name, char *buf, size_t *len);
extern int hdel(const fileid_t *fid, const char *name);
extern int hlen(const fileid_t *fid, uint64_t *count);
extern redisReply *hscan(const fileid_t *fid, const char *match, uint64_t cursor, uint64_t count);
extern redisReply *scan(int redis_id, uint32_t cursor);

#endif

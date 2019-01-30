#ifndef ___REDIS_UTILS__
#define ___REDIS_UTILS__

#include "sdfs_list.h"
#include <hiredis/hiredis.h>

typedef  redisContext redis_ctx_t;

typedef struct {
        struct list_head hook;
        char *key;
        char *value;
        char __key__[MAX_NAME_LEN];
        char __value__[MAX_NAME_LEN];
        size_t size;
} mseg_t;

typedef struct {
        int segcount;
        struct list_head kvlist;
        //redisReply *reply;
} mctx_t;

typedef struct {
#if 0
        sy_spinlock_t lock;
        struct list_head list;
#endif
        sy_rwlock_t rwlock;
        char key[MAX_PATH_LEN];
        redis_ctx_t *ctx;
} redis_conn_t;

int connect_redis(const char *ip, short port, redis_ctx_t **ctx);
int connect_redis_unix(const char *path, redis_ctx_t **ctx);
int disconnect_redis(redis_ctx_t **ctx);

int redis_kget(redis_conn_t *conn, const char *key, void *buf, size_t *len);
int redis_kset(redis_conn_t *conn, const char *key, const void *value, size_t size,
              int flag, int ttl);
int redis_hget(redis_conn_t *conn, const char *hash, const char *key, void *buf, size_t *len);
int redis_hset(redis_conn_t *conn, const char *hash, const char *key,
               const void *value, size_t size, int flag);
int redis_hdel(redis_conn_t *conn, const char *hash, const char *key);
int redis_hiterator(redis_conn_t *conn, const char *hash, const char *match, func2_t func2, void *arg);
int redis_keys(redis_conn_t *conn, func1_t func1, void *arg);
int redis_connect(redis_conn_t **_conn, const char *addr, const int *port, const char *key);
int redis_sset(redis_conn_t *conn, const char *set, const char *key);
int redis_sdel(redis_conn_t *conn, const char *set, const char *key);
int redis_scount(redis_conn_t *conn, const char *set, uint64_t *count);
int redis_siterator(redis_conn_t *conn, const char *set, func1_t func, void *arg);
int redis_hlen(redis_conn_t *conn, const char *key, uint64_t *count);
int redis_iterator(redis_conn_t *conn, const char *match, func1_t func, void *arg);

#if 0
int redis_exec(redis_conn_t *conn, const char *buf);
int redis_multi(redis_conn_t *conn, ...);
int redis_exec_array(redis_conn_t *conn, const char **array, int count);

void redis_trans_sset(redis_conn_t *conn, const char *set, const char *key);
void redis_trans_hset(redis_conn_t *conn, const char *hash, const char *key, const void *value, size_t size);
void redis_trans_sdel(redis_conn_t *conn, const char *set, const char *key);
void redis_trans_hdel(redis_conn_t *conn, const char *hash, const char *key);
void redis_trans_hget(redis_conn_t *conn, const char *hash, const char *key);
void redis_trans_begin(redis_conn_t *conn);
void redis_trans_end(redis_conn_t *conn);
void redis_trans_end1(redis_conn_t *conn, func1_t func, void *arg, int count);
#endif

void redis_multi_init(mctx_t *ctx);
void redis_multi_append(mctx_t *ctx, char *key,  void *value, uint32_t len);
int redis_multi_exec(redis_conn_t *conn, const char *op, const char *tab , mctx_t *ctx, func1_t func, void *arg);
int redis_multi_destory(mctx_t *ctx);
redisReply *redis_hscan(redis_conn_t *conn, const char *key, const char *match, uint64_t cursor, uint64_t _count);
int redis_kdel(redis_conn_t *conn, const char *key);
int redis_del(redis_conn_t *conn, const char *key);
int redis_disconnect(redis_conn_t *conn);


#endif




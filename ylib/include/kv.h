#ifndef __KV_H__
#define __KV_H__

#include "hash_table.h"
#include "sdfs_lib.h"

typedef struct {
        hashtable_t tab;
        struct list_head list;
} kv_ctx_t;

void kv_expire(kv_ctx_t *ctx);
int kv_create(kv_ctx_t **_ctx);
int kv_get(kv_ctx_t *ctx, const char *key, void *value, int *valuelen);
int kv_set(kv_ctx_t *ctx, const char *key, const void *value, int valuelen, int flag, int ttl);


#endif


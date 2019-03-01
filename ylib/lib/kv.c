#include <errno.h>
#include <stdint.h>

#define DBG_SUBSYS S_LIBYLIB

#include "sysutil.h"
#include "kv.h"
#include "ylib.h"
#include "dbg.h"


typedef struct {
        struct list_head hook;
        int valuelen;
        time_t timeout;
        char key[MAX_NAME_LEN];
        char value[0];
} entry_t;

static int __kv_remove(kv_ctx_t *ctx, const char *key)
{
        int ret;
        entry_t *ent;
        
        ret = hash_table_remove(ctx->tab, (void *)key, (void **)&ent);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        list_del(&ent->hook);
        yfree((void **)&ent);

        return 0;
err_ret:
        return ret;
}

static int __kv_create(kv_ctx_t *ctx, const char *key, const void *value, int valuelen, int ttl)
{
        int ret, keylen;
        entry_t *ent;

        keylen = strlen(key);
        if (keylen >= MAX_NAME_LEN) {
                ret = ENAMETOOLONG;
                GOTO(err_ret, ret);
        }
        
        ret = ymalloc((void **)&ent, sizeof(*ent) + valuelen);
        if (ret)
                GOTO(err_ret, ret);

        memcpy(ent->key, key, keylen + 1);
        memcpy(ent->value, value, valuelen);
        ent->valuelen = valuelen;
        ent->timeout = gettime() + ttl;
        
        ret = hash_table_insert(ctx->tab, (void *)ent, ent->key, 0);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        list_add_tail(&ent->hook, &ctx->list);
        
        return 0;
err_ret:
        return ret;
}

static int __kv_update(kv_ctx_t *ctx, entry_t *ent, const char *key, const void *value, int valuelen, int ttl)
{
        int ret;

        if (unlikely(ent->valuelen !=  valuelen)) {
                DWARN("valuelen %u %u\n", ent->valuelen, valuelen);

                ret = __kv_remove(ctx, key);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
                
                ret = __kv_create(ctx, key, value, valuelen, ttl);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        } else {
                list_del(&ent->hook);
                memcpy(ent->value, value, valuelen);
                ent->valuelen = valuelen;
                ent->timeout = gettime() + ttl;

                list_add_tail(&ent->hook, &ctx->list);
        }

        return 0;
err_ret:
        return ret;
}

int kv_set(kv_ctx_t *ctx, const char *key, const void *value, int valuelen, int flag, int ttl)
{
        int ret;
        entry_t *ent;

        ent = hash_table_find(ctx->tab, (void *)key);
        if (ent) {
                if (flag & O_EXCL) {
                        ret = EEXIST;
                        GOTO(err_ret, ret);
                } else {
                        ret = __kv_update(ctx, ent, key, value, valuelen, ttl);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);
                }
        } else {
                ret = __kv_create(ctx, key, value, valuelen, ttl);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int kv_get(kv_ctx_t *ctx, const char *key, void *value, int *valuelen)
{
        int ret;
        entry_t *ent;

        ent = hash_table_find(ctx->tab, (void *)key);
        if (ent == NULL) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        time_t now = gettime();
        if (now > ent->timeout) {
                ret = __kv_remove(ctx, key);
                YASSERT(ret == 0);
                ret = ENOENT;
                GOTO(err_ret, ret);
        }
        
        if (*valuelen < ent->valuelen) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        DINFO("hit %s\n", key);
        
        memcpy(value, ent->value, ent->valuelen);

        *valuelen = ent->valuelen;

        return 0;
err_ret:
        return ret;
}

static uint32_t __kv_hash(const void *args)
{
        return hash_str(((entry_t *)args)->key);
}

static int __kv_cmp(const void *v1, const void *v2)
{
        const entry_t *ent = (entry_t *)v1;
        const char *key = v2;

        DBUG("cmp %s %s\n", ent->key, key);
        
        return strcmp(ent->key, key);
}

int kv_create(kv_ctx_t **_ctx)
{
        int ret;
        kv_ctx_t *ctx;

        ret = ymalloc((void **)&ctx, sizeof(*ctx));
        if (ret)
                GOTO(err_ret, ret);

        ctx->tab = hash_create_table(__kv_cmp, __kv_hash, "kv");
        if (ctx->tab == NULL) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        INIT_LIST_HEAD(&ctx->list);

        *_ctx = ctx;
        
        return 0;
err_ret:
        return ret;
}

void kv_expire(kv_ctx_t *ctx)
{
        int ret;
        struct list_head *pos, *n;
        entry_t *ent;
        time_t now = gettime();

        list_for_each_safe(pos, n, &ctx->list) {
                ent = (void *)pos;

                if (ent->timeout >= now) {
                        DINFO("%s expired\n", ent->key);
                        ret = __kv_remove(ctx, ent->key);
                        YASSERT(ret == 0);
                } else {
                        break;
                }
        }
}

#ifndef __FILE_TREE_H__
#define __FILE_TREE_H__

#include "skiplist_row.h"
#include "sdfs_conf.h"
#include "sdfs_id.h"

typedef struct {                                                
        verid64_t parentid;                                      
        char name[MAX_NAME_LEN];                                
} dirkey_t;                                           

static inline int get_name(const char *path, const char **name, int *len)
{
        const char *loc, *loc1, *loc2;

        loc = path;
        if (loc[0] == 0)
                return ENOENT;

        while (loc[0] == '/') {
                loc++;
                if (loc[0] == 0) {
                        return ENOENT;  
                }
        }

        loc1 = strstr(loc, "/");
        if (loc1 == NULL)
                return ENOENT;

        loc2 = loc1;

        while (srv_running) {
                loc2++;
                if (*loc2 == '/' || *loc2 == 0)
                        break;
        }

        *name = loc1;
        *len = loc2 - loc1 - 1;

        return 0;
}


#define DIR_CREATE(__type__)                                            \
        typedef struct {                                                \
                dirkey_t key;                                           \
                __type__ stat;                                          \
        } dent_##__type__;                                              \
                                                                        \
        int __type__##_cmp(const dirkey_t *key, const dent_##__type__ *data) \
        {                                                               \
                int ret;                                                \
                const verid64_t *keyid, *dataid;                        \
                                                                        \
                keyid = &key->parentid;                                 \
                dataid = &data->key.parentid;                           \
                                                                        \
                if (keyid->id < dataid->id)                             \
                        ret = -1;                                       \
                else if (keyid->id > dataid->id)                        \
                        ret = 1;                                        \
                else {                                                  \
                        if (keyid->version < dataid->version)           \
                                ret = -1;                               \
                        else if (keyid->version > dataid->version)      \
                                ret = 1;                                \
                        else                                            \
                                ret = 0;                                \
                }                                                       \
                                                                        \
                if (ret == 0)                                           \
                        return strcmp(key->name, data->key.name);       \
                else                                                    \
                        return ret;                                     \
        }                                                               \
                                                                        \
        SKIPLIST_CREATE(dirkey_t, dent_##__type__, __type__##_cmp,      \
                        SLIST_LEVEL)                                    \
                                                                        \
        typedef struct {                                                \
                sy_rwlock_t rwlock[SLIST_GROUP];                        \
                slist_dent_##__type__ slist[SLIST_GROUP];               \
        } dir_##__type__;                                               \
                                                                        \
        int dir_##__type__##_init(dir_##__type__ *tree)                 \
        {                                                               \
                int ret, i;                                             \
                dent_##__type__ max, min;                               \
                                                                        \
                min.key.parentid.id = 0;                                \
                min.key.parentid.version = 0;                           \
                max.key.parentid.id = UINT64_MAX;                       \
                max.key.parentid.version = UINT32_MAX;                  \
                memset(min.key.name, 0x0, MAX_NAME_LEN);                \
                memset(max.key.name, 0xFF, MAX_NAME_LEN);               \
                                                                        \
                for (i = 0; i < SLIST_GROUP; i++) {                     \
                        sy_rwlock_init(&tree->rwlock[i], NULL);               \
                                                                        \
                        ret = slist_dent_##__type__##_init(&tree->slist[i], \
                                                           SLIST_CHUNK_SIZE, \
                                                           &min, &max); \
                        if (ret)                                        \
                                GOTO(err_ret, ret);                     \
                }                                                       \
                return 0;                                               \
        err_ret:                                                        \
                return ret;                                             \
        }                                                               \
                                                                        \
        int dir_##__type__##_create(dir_##__type__ *tree,               \
                                    const verid64_t *parent,            \
                                    const char *name,                   \
                                    const __type__ *ent)                \
        {                                                               \
                int ret, hash;                                          \
                dent_##__type__ dent;                                   \
                                                                        \
                hash = parent->id / SLIST_GROUP;                        \
                dent.key.parentid = *parent;                            \
                strcpy(dent.key.name, name);                            \
                                                                        \
                ret = sy_rwlock_wrlock(&tree->rwlock[hash]);            \
                if (ret)                                                \
                        GOTO(err_ret, ret);                             \
                                                                        \
                ret = slist_dent_##__type__##_get(&tree->slist[hash],   \
                                                  &dent.key, &dent);    \
                if (ret) {                                              \
                        if (ret == ENOENT) {                            \
                                memcpy(&dent.stat, ent, sizeof(__type__)); \
                                ret = slist_dent_##__type__##_put(&tree->slist[hash], \
                                                                  &dent.key, &dent); \
                        } else                                          \
                                GOTO(err_lock, ret);                    \
                } else {                                                \
                        ret = EEXIST;                                   \
                        goto err_lock;                                  \
                }                                                       \
                sy_rwlock_unlock(&tree->rwlock[hash]);                  \
                                                                        \
                return 0;                                               \
        err_lock:                                                       \
                sy_rwlock_unlock(&tree->rwlock[hash]);                  \
        err_ret:                                                        \
                return ret;                                             \
        }                                                               \
                                                                        \
        int dir_##__type__##_lookup(dir_##__type__ *tree,               \
                                    const verid64_t *parent,            \
                                    const char *name,                   \
                                    __type__ *ent)                      \
        {                                                               \
                int ret, hash;                                          \
                dent_##__type__ dent;                                   \
                                                                        \
                hash = parent->id / SLIST_GROUP;                        \
                dent.key.parentid = *parent;                            \
                strcpy(dent.key.name, name);                            \
                                                                        \
                ret = sy_rwlock_rdlock(&tree->rwlock[hash]);            \
                if (ret)                                                \
                        GOTO(err_ret, ret);                             \
                                                                        \
                ret = slist_dent_##__type__##_get(&tree->slist[hash],   \
                                                  &dent.key, &dent);    \
                if (ret)                                                \
                        goto err_lock;                                  \
                                                                        \
                sy_rwlock_unlock(&tree->rwlock[hash]);            \
                                                                        \
                memcpy(ent, &dent.stat, sizeof(__type__));              \
                                                                        \
                return 0;                                               \
        err_lock:                                                       \
                sy_rwlock_unlock(&tree->rwlock[hash]);                  \
        err_ret:                                                        \
                return ret;                                             \
        }                                                               \
                                                                        \
        int dir_##__type__##_getsub(dir_##__type__ *tree,               \
                                    const verid64_t *parent,            \
                                    uint64_t offset, uint32_t *count,   \
                                    dent_##__type__ *dent)              \
        {                                                               \
                int ret, hash;                                          \
                dirkey_t max, min;                                      \
                                                                        \
                hash = parent->id / SLIST_GROUP;                        \
                max.parentid = *parent;                                 \
                min.parentid = *parent;                                 \
                memset(min.name, 0x0, MAX_NAME_LEN);                    \
                memset(max.name, 0xFF, MAX_NAME_LEN);                   \
                                                                        \
                ret = sy_rwlock_rdlock(&tree->rwlock[hash]);            \
                if (ret)                                                \
                        GOTO(err_ret, ret);                             \
                                                                        \
                ret = slist_dent_##__type__##_interval(&tree->slist[hash], \
                                                       &min, &max, offset, \
                                                       count, dent);    \
                if (ret)                                                \
                        goto err_lock;                                  \
                                                                        \
                ret = sy_rwlock_unlock(&tree->rwlock[hash]);            \
                if (ret)                                                \
                        GOTO(err_ret, ret);                             \
                                                                        \
                return 0;                                               \
        err_lock:                                                       \
                sy_rwlock_unlock(&tree->rwlock[hash]);                  \
        err_ret:                                                        \
                return ret;                                             \
        }

#endif

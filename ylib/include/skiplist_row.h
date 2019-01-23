#ifndef __SKIPLIST_ROW_H__
#define __SKIPLIST_ROW_H__

#include "snaprow.h"

#define SKIPLIST_ARRAY_FIX 1024
#define SLIST_LEVEL 128

static inline int randbits(int chunksize, int *randnum, int *randfbits)
{
        int rand;

        if (*randfbits < chunksize) {
                *randnum = random();
                *randfbits = 31;
        }

        *randfbits -= chunksize;

        rand = *randnum % (1 << chunksize);

        *randnum >>= chunksize;

        return rand;
}


static inline int genlevel(int chunksize, int maxlevel, int *randnum, int *randfbits, int *_level)
{
        int level, rand;

        for (level = 0; (rand = randbits(chunksize, randnum, randfbits))
                     && level < maxlevel; level++)
                ;

        if (level == maxlevel)
                level--;

        *_level = level;

        return 0;
}

#define SKIPLIST_CREATE(__key__, __type__, __cmp__, __level__)          \
                                                                        \
        typedef struct{                                                 \
                __type__ entry;                                         \
                uint64_t next[__level__];                               \
        } snode_##__type__;                                             \
                                                                        \
        typedef struct {                                                \
                uint64_t preidx;                                        \
                uint64_t curidx;                                        \
                snode_##__type__ *prenode;                              \
                snode_##__type__ *curnode;                              \
        } btnode_##__type__;                                            \
                                                                        \
        SNAPROW_CREATE(snode_##__type__,                                \
                       SKIPLIST_ARRAY_FIX, NULL);                       \
                                                                        \
        typedef struct {                                                \
                int curlevel;                                           \
                int maxlevel;                                           \
                int chunksize;                                          \
                int randnum;                                            \
                int randfbits;                                          \
                uint32_t total_node_num;                                \
                uint64_t nil;                                           \
                uint64_t head;                                          \
                int (*cmp)(const __key__ *, const __type__ *);          \
                snaprow_t snaprow;                                      \
        } slist_##__type__;                                             \
                                                                        \
        int findplace_##__type__(slist_##__type__ *slist,               \
                                 const __key__ *key,                    \
                                 btnode_##__type__ *btlist,             \
                                 int *found)                            \
        {                                                               \
                int ret, i;                                             \
                snode_##__type__ *cur, *pre, *head;                     \
                uint64_t *begin, pre_idx;                               \
                                                                        \
                if (found)                                              \
                        *found = 0;                                     \
                                                                        \
                head = snaprow_##snode_##__type__##_get(&slist->snaprow, slist->head); \
                DBUG("maxlevel %u, curlevel %d\n", slist->maxlevel, slist->curlevel); \
                for (i = slist->maxlevel - 1; i > slist->curlevel; i--) { \
                        btlist[i].preidx = slist->head;                 \
                        btlist[i].prenode = head;                       \
                        btlist[i].curnode = NULL;                       \
                }                                                       \
                                                                        \
                pre_idx = slist->head;                                  \
                begin = head->next;                                     \
                ret = ENOENT;                                           \
                                                                        \
                while (i >= 0) {                                        \
                        cur = snaprow_##snode_##__type__##_get(&slist->snaprow, begin[i]); \
                                                                        \
                        while ((ret = slist->cmp(key, &cur->entry)) > 0) { \
                                pre_idx = begin[i];                     \
                                pre = cur;                              \
                                begin = &cur->next[0];                  \
                                cur = snaprow_##snode_##__type__##_get(&slist->snaprow, begin[i]); \
                        }                                               \
                                                                        \
                        btlist[i].preidx = pre_idx;                     \
                        btlist[i].prenode =  pre;                       \
                        btlist[i].curnode = cur;                        \
                        btlist[i].curidx = begin[i];                    \
                        i--;                                            \
                }                                                       \
                                                                        \
                DBUG("ret %d\n", ret);                                  \
                if (found && ret == 0)                                  \
                        *found = 1;                                     \
                                                                        \
                return 0;                                               \
        }                                                               \
                                                                        \
        int slist_##__type__##_init(slist_##__type__ *list,             \
                                    int chunksize,                      \
                                    const __type__ *min,                \
                                    const __type__ *max)                \
        {                                                               \
                int ret, i;                                             \
                snode_##__type__ *nil, *head;                           \
                                                                        \
                ret = snaprow_##snode_##__type__##_init(&list->snaprow); \
                if (ret)                                                \
                        GOTO(err_ret, ret);                             \
                                                                        \
                list->curlevel = -1;                                    \
                list->maxlevel = __level__;                             \
                list->chunksize = chunksize;                            \
                list->total_node_num = 0;                               \
                list->cmp = __cmp__;                                    \
                                                                        \
                ret = snaprow_##snode_##__type__##_new(&list->snaprow, &list->nil); \
                if (ret)                                                \
                        GOTO(err_ret, ret);                             \
                ret = snaprow_##snode_##__type__##_new(&list->snaprow, &list->head); \
                if (ret)                                                \
                        GOTO(err_ret, ret);                             \
                                                                        \
                nil = snaprow_##snode_##__type__##_get(&list->snaprow, list->nil); \
                head = snaprow_##snode_##__type__##_get(&list->snaprow, list->head); \
                                                                        \
                memcpy(&nil->entry, max, sizeof(__type__));             \
                memcpy(&head->entry, min, sizeof(__type__));            \
                                                                        \
                for (i = 0; i < __level__; i++) {                       \
                        nil->next[i] = -1;                              \
                        head->next[i] = list->nil;                      \
                }                                                       \
                                                                        \
                list->randnum = 0;                                      \
                list->randfbits = 0;                                    \
                                                                        \
                return 0;                                               \
        err_ret:                                                        \
                return ret;                                             \
        }                                                               \
                                                                        \
        int slist_##__type__##_put(slist_##__type__ *slist,           \
                                   const __key__ *key,                  \
                                   const __type__ *entry)               \
        {                                                               \
                int found, ret, level = 0;                              \
                uint64_t new;                                           \
                btnode_##__type__ btlist[__level__];                    \
                snode_##__type__ *node, *btnode;                        \
                                                                        \
                YASSERT(entry != NULL);                                 \
                                                                        \
                findplace_##__type__(slist, key, btlist, &found);       \
                                                                        \
                if (found == 1)                                         \
                        return EEXIST;                                  \
                                                                        \
                genlevel(slist->chunksize, slist->maxlevel,             \
                         &slist->randnum, &slist->randfbits, &level);   \
                                                                        \
                ret = snaprow_##snode_##__type__##_new(&slist->snaprow, &new); \
                if (ret)                                                \
                        GOTO(err_ret, ret);                             \
                                                                        \
                node = snaprow_##snode_##__type__##_get(&slist->snaprow, new); \
                                                                        \
                if (slist->curlevel < level)                            \
                        slist->curlevel = level;                        \
                                                                        \
                for (; level >= 0; level--) {                           \
                        btnode = btlist[level].prenode;                 \
                                                                        \
                        node->next[level] = btnode->next[level];        \
                        btnode->next[level] = new;                      \
                }                                                       \
                                                                        \
                slist->total_node_num++;                                \
                                                                        \
                return 0;                                               \
        err_ret:                                                        \
                return ret;                                             \
        }                                                               \
                                                                        \
        int slist_##__type__##_get(slist_##__type__ *slist,             \
                                   const __key__ *key,                  \
                                   __type__ *entry)                     \
        {                                                               \
                int found;                                              \
                btnode_##__type__ btlist[__level__];                    \
                snode_##__type__ *node;                                 \
                                                                        \
                findplace_##__type__(slist, key, btlist, &found);       \
                                                                        \
                if (found == 1) {                                       \
                        YASSERT(btlist[0].curnode);                     \
                        node = btlist[0].curnode;                       \
                        memcpy(entry, &node->entry, sizeof(__type__));  \
                } else                                                  \
                        return ENOENT;                                  \
                                                                        \
                return 0;                                               \
        }                                                               \
                                                                        \
        int slist_##__type__##_get1st(slist_##__type__ *slist,          \
                                      __type__ *entry)                  \
        {                                                               \
                int ret;                                                \
                snode_##__type__ *node;                                 \
                                                                        \
                if (slist->curlevel >= 0) {                             \
                        node = snaprow_##snode_##__type__##_get(&slist->snaprow, \
                                                                slist->head); \
                                                                        \
                        if (node && node->next[0] != (uint64_t)-1) {    \
                                memcpy(entry, &node->entry,             \
                                       sizeof(__type__));               \
                                goto found;                             \
                        }                                               \
                }                                                       \
                                                                        \
                ret = ENOENT;                                           \
                goto err_ret;                                           \
                                                                        \
        found:                                                          \
                return 0;                                               \
        err_ret:                                                        \
                return ret;                                             \
        }                                                               \
                                                                        \
        int slist_##__type__##_del(slist_##__type__ *slist,             \
                                   const __key__ *key,                  \
                                   __type__ *data)                      \
        {                                                               \
                int ret, found, level, cur_level;                       \
                snode_##__type__ *node, *tnode, *head;                  \
                btnode_##__type__ btlist[__level__];                    \
                                                                        \
                findplace_##__type__(slist, key, btlist, &found);       \
                                                                        \
                if (found != 1) {                                       \
                        ret = ENOENT;                                   \
                        GOTO(err_ret, ret);                             \
                }                                                       \
                                                                        \
                node = btlist[0].curnode;                               \
                level = 0;                                              \
                while (level < slist->maxlevel) {                       \
                        tnode = btlist[level].curnode;                  \
                                                                        \
                        if (tnode != node)                              \
                                break;                                  \
                        level++;                                        \
                }                                                       \
                                                                        \
                cur_level = level - 1;                                  \
                                                                        \
                head = snaprow_##snode_##__type__##_get(&slist->snaprow, slist->head); \
                for (level--; (head->next[level] == btlist[0].curidx)   \
                             && (node->next[level] == slist->nil); level--) \
                        slist->curlevel--;                              \
                                                                        \
                for (level = cur_level; level >= 0; level--)            \
                        btlist[level].prenode->next[level] = node->next[level]; \
                                                                        \
                if (data)                                               \
                        memcpy(data, &node->entry, sizeof(__type__));   \
                                                                        \
                ret = snaprow_##snode_##__type__##_free(&slist->snaprow, btlist[0].curidx); \
                if (ret)                                                \
                        GOTO(err_ret, ret);                             \
                                                                        \
                slist->total_node_num--;                                \
                                                                        \
                return 0;                                               \
        err_ret:                                                        \
                return ret;                                             \
        }                                                               \
                                                                        \
        int slist_##__type__##_interval(slist_##__type__ *slist,        \
                                        const __key__ *min, const __key__ *max, \
                                        uint64_t offset, uint32_t *count, \
                                        __type__ *rs)                   \
        {                                                               \
                int found;                                              \
                uint64_t i;                                             \
                uint32_t _count;                                        \
                btnode_##__type__ btlist[__level__];                    \
                snode_##__type__ *begin, *end, *pos;                    \
                                                                        \
                findplace_##__type__(slist, min, btlist, &found);       \
                                                                        \
                begin = btlist[0].curnode;                              \
                                                                        \
                findplace_##__type__(slist, max, btlist, &found);       \
                                                                        \
                end = btlist[0].curnode;                                \
                                                                        \
                if (begin == end) {                                     \
                        *count = 0;                                     \
                        return 0;                                       \
                }                                                       \
                                                                        \
                _count = *count;                                        \
                *count = 0;                                             \
                for (i = 0, pos = begin; i < offset + _count && pos != end; i++) { \
                        if (i >= offset) {                              \
                                memcpy(&rs[*count], &pos->entry, sizeof(__type__)); \
                                (*count)++;                             \
                        }                                               \
                        if (pos[0].next[0] != (uint64_t)-1) {           \
                                pos = snaprow_##snode_##__type__##_get(&slist->snaprow, \
                                                                       pos[0].next[0]); \
                        } else                                          \
                                break;                                  \
                }                                                       \
                                                                        \
                return 0;                                               \
        }


#endif

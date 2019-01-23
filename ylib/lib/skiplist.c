

#include <string.h>
#include <stdlib.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIBSKIPLIST

#include "skiplist.h"
#include "ylib.h"
#include "sdfs_id.h"
#include "dbg.h"

#define MAX_LEVEL 48

int findplace(struct skiplist *slist, const void *key, struct skiplist_node **btlist,
              int *found)
{
        int ret, i;
        struct skiplist_node *cur, *pre;
        struct skiplist_node **list;

        if (found)
                *found = 0;

        DBUG("maxlevel %u, curlevel %d\n", slist->maxlevel, slist->curlevel);
        for (i = slist->maxlevel - 1; i > slist->curlevel; i--)
                btlist[i] = slist->head;

        pre = slist->head;
        list = &slist->head->list[0];
        ret = ENOENT;

        while (i >= 0) {

                cur = list[i];
                if (cur == NULL || cur->key == NULL) {
#ifdef SKIPLIST_BUG
                        ret = ENOENT;
                        goto out;
#else
                        skiplist_print(slist);

                        YASSERT(cur != NULL);
#endif
                }
                while ((ret = slist->cmp(key, cur->key)) > 0) {
                        pre = cur;
                        list = &cur->list[0];
                        cur = list[i];
                        if (cur == NULL || cur->key == NULL) {
#ifdef SKIPLIST_BUG
                                ret = ENOENT;
                                goto out;
#else
                                skiplist_print(slist);
                                YASSERT(cur != NULL);
#endif
                        }
                }

                btlist[i] = pre;
                i--;
        }

#ifdef SKIPLIST_BUG
out:
#endif
        DBUG("ret %d\n", ret);
        if (found && ret == 0)
                *found = 1;

        return 0;
}

int randbits(struct skiplist *slist)
{
        int rand;

        if (slist->randfbits < slist->chunksize) {
                slist->randnum = random();
                slist->randfbits = 31;
        }

        slist->randfbits -= slist->chunksize;

        rand = slist->randnum % (1 << slist->chunksize);

        slist->randnum >>= slist->chunksize;

        return rand;
}


int genlevel(struct skiplist *slist, int *_level)
{
        int level, rand;

        for (level = 0; (rand = randbits(slist))
                        && level < slist->maxlevel; level++)
                ;

        if (level == slist->maxlevel)
                level--;

        *_level = level;

        return 0;
}

int skiplist_create(slist_cmp cmp, int maxlevel, int chunksize,
                    void *min, void *max, struct skiplist **slist)
{
        int ret, i;
        uint32_t len;
        void *ptr;
        struct skiplist *list;

        YASSERT(maxlevel < MAX_LEVEL);

        len = sizeof(struct skiplist);

        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_ret, ret);

        list = ptr;

        list->curlevel = -1;
        list->maxlevel = maxlevel;
        list->chunksize = chunksize;
        list->total_node_num = 0;
        list->cmp = cmp;

        len = sizeof(struct skiplist_node)
              + sizeof(struct skiplist_node *) * maxlevel;

        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_list, ret);

        list->nil = ptr;
        list->nil->key = max;

        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_nil, ret);

        list->head = ptr;
        list->head->key = min;

        for (i = 0; i < maxlevel; i++) {
                list->nil->list[i] = NULL;
                list->head->list[i] = list->nil;
        }

#if 0
        len = sizeof(struct skiplist_node *) * maxlevel;

        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_head, ret);

        list->btlist = ptr;
#endif

        list->randnum = 0;
        list->randfbits = 0;

        *slist = list;

        return 0;

#if 0
err_head:
        yfree((void **)&list->head);
#endif
err_nil:
        yfree((void **)&list->nil);
err_list:
        yfree((void **)&list);
err_ret:
        return ret;
}

int skiplist_destroy(struct skiplist *slist)
{
        if (slist != NULL) {
#if 0
                if (slist->btlist != NULL)
                        yfree((void **)&slist->btlist);
#endif
                if (slist->head != NULL)
                        yfree((void **)&slist->head);
                if (slist->nil != NULL)
                        yfree((void **)&slist->nil);
                yfree((void **)&slist);
        }

        return 0;
}

int skiplist_put(struct skiplist *slist, const void *key, void *data)
{
        int found, ret, level = 0;
        uint32_t len;
        void *ptr;
        struct skiplist_node *btlist[MAX_LEVEL], *node, **list;

        YASSERT(data != NULL);

//        btlist = slist->btlist;

        findplace(slist, key, btlist, &found);

        if (found == 1)
                return EEXIST;

        (void) genlevel(slist, &level);

        len = sizeof(struct skiplist_node)
              + sizeof(struct skiplist_node *) * level;

        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_ret, ret);

        node = ptr;

        node->key = data;

        list = &node->list[0];

        if (slist->curlevel < level)
                slist->curlevel = level;

        for (; level >= 0; level--) {
                list[level] = btlist[level]->list[level];
                btlist[level]->list[level] = node;
        }

        slist->total_node_num++;

        return 0;
err_ret:
        return ret;
}

int skiplist_get(struct skiplist *slist, const void *key, void **data)
{
        int found;
        struct skiplist_node *btlist[MAX_LEVEL];

//        btlist = slist->btlist;

        findplace(slist, key, btlist, &found);

        if (found == 1) {
                if (data)
                        *data = btlist[0]->list[0]->key;
        } else
                return ENOENT;

        return 0;
}

void skiplist_print(struct skiplist *slist)
{
        int i;
        struct skiplist_node *node;

        DBUG("curlvl: %d, maxlvl: %d, chunksize: %d\n",
             slist->curlevel, slist->maxlevel, slist->chunksize);

        for (i = slist->curlevel; i >= 0; i--) {
                DBUG("level %d: ", i);
                node = slist->head->list[i];
                while (node != NULL && node->list[0] != NULL) {
                        DBUG_RAW("%p -> ", node->key);
                        YASSERT(node->key != NULL);
                        node = node->list[i];
                }
                DBUG_RAW("NULL\n");
        }
}

int skiplist_get1st(struct skiplist *slist, void **data)
{
        int ret;
        struct skiplist_node *node;

        if (slist->curlevel >= 0) {
                node = slist->head->list[0];

                if (node && node->list[0]) {
                        *data = node->key;
                        goto found;
                }
        }

        ret = ENOENT;
        goto err_ret;

found:
        return 0;
err_ret:
        return ret;
}

int skiplist_getlast(const struct skiplist *slist, void **data)
{
        struct skiplist_node* node;
        void *_data = NULL;

        *data = NULL;

        YASSERT(slist != NULL);

        node = slist->head->list[0];
        while(node != NULL && node->list[0] != NULL) {
                _data = node->key;
                node = node->list[0];
        }

        *data = _data;
        return 0;
}

int skiplist_del(struct skiplist *slist, const void *key, void **data)
{
        int ret, found, level, cur_level;
        struct skiplist_node *btlist[MAX_LEVEL], *node;

        /* @init */
        if (data) *data = NULL;

        findplace(slist, key, btlist, &found);

        if (found != 1) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        node = btlist[0]->list[0];
        level = 0;
        while ((level < slist->maxlevel) && btlist[level]->list[level] == node)
                level++;

        cur_level = level - 1;

        for (level--; (slist->head->list[level] == node)
                      && (node->list[level] == slist->nil); level--)
                slist->curlevel--;

        for (level = cur_level; level >= 0; level--)
                btlist[level]->list[level] = node->list[level];

        if (data)
                *data = node->key;
        else
                yfree((void **)&node->key);

        yfree((void **)&node);

        slist->total_node_num--;

        return 0;
err_ret:
        return ret;
}

int skiplist_get_size(struct skiplist* slist)
{
#if 0
        return slist->total_node_num;
#else
        int size = 0;
        struct skiplist_node* node;

        YASSERT(slist != NULL);

        node = slist->head->list[0];
        while(node != NULL && node->list[0] != NULL) {
                size++;
                node = node->list[0];
        }

        return size;
#endif
}

void skiplist_iterate(struct skiplist* slist, void (*func)(void*))
{
        struct skiplist_node* node;

        YASSERT(slist != NULL);

        node = slist->head->list[0];
        while(node != NULL && node->list[0] != NULL) {
                func(node->key);
                node = node->list[0];
        }
}

/*key must be the first struct of data*/
void skiplist_iterate_del(struct skiplist* slist, int (*func)(void*))
{
        int ret;
        struct skiplist_node *node, *node1;
        void *ptr;

        YASSERT(slist != NULL);

        node = slist->head->list[0];
        while(node != NULL && node->list[0] != NULL) {
                ret = func(node->key);

                node1 = node->list[0];

                if (ret == 1) {
                        ret = skiplist_del(slist, node->key, &ptr);
                        if (ret)
                                DERROR("%d - %s\n", ret, strerror(ret));
                }

                node = node1;
        }
}

int skiplist_lower_bound(struct skiplist *slist, void *key, void **_node)
{
        struct skiplist_node* node;

        YASSERT(slist != NULL);

        *_node = NULL;
        /************************************************/
        node = slist->head->list[0];
        while(node != NULL && node->list[0] != NULL) {

                if (slist->cmp(key, node->key) <= 0) {
                        *_node = node;
                        return 0;
                }

                node = node->list[0];
        }
        /************************************************/

        return ENOENT;
}

int skiplist_upper_bound(struct skiplist *slist, void *key, void **_node)
{
        struct skiplist_node* node;

        YASSERT(slist != NULL);

        *_node = 0;
        /************************************************/
        node = slist->head->list[0];
        while(node != NULL && node->list[0] != NULL) {

                if (slist->cmp(key, node->key) < 0) {
                        *_node = node;
                        return 0;
                }

                node = node->list[0];
        }
        /************************************************/

        return ENOENT; 
}

inline struct skiplist_node *skiplist_begin(struct skiplist *slist)
{
        return slist->head->list[0];
}

inline bool skiplist_end(struct skiplist_node *node)
{
        if (node == NULL || node->list[0] == NULL)
                return true;
        return false;
}

inline struct skiplist_node *skiplist_next(struct skiplist_node *node)
{
        if (skiplist_end(node))
                return NULL;
        return node->list[0];
}

int skiplist_clear(struct skiplist *slist, int free)
{
        struct skiplist_node *node;
        void *data;

        while (srv_running) {
                node = skiplist_begin(slist);
                if (skiplist_end(node))
                        break;
                data = NULL;
                skiplist_del(slist, node->key, &data);
                if (free && data)
                        yfree((void **)&data);
        }

        YASSERT(skiplist_get_size(slist) == 0);

        return 0;
}

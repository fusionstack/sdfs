/**
 * http://resnet.uoregon.edu/~gurney_j/jmpc/skiplist.html
 * http://www.csee.umbc.edu/courses/undergraduate/341/fall01/Lectures/SkipLists/skip_lists/skip_lists.html
 *
 * \li \c in skiplist_create, set min and max correctly.
 * \li \c if key is a pointer to sth, we must put a valid value.
 * \li \c if key is a pointer to sth, which must be comparable type.
 * \li \c who is responsible for the memory?
 * \li \c key can be a integer type. in this case, the value can be any integer, including 0.
 */

#ifndef __SKIPLIST_H__
#define __SKIPLIST_H__

#include <stdint.h>
#include <stdbool.h>

struct charkey {
        uint32_t len;
        char *buf;
};

#define charkeyclear(key)         \
do {                              \
        (key)->len = 0;           \
        (key)->buf = NULL;        \
} while (0)

struct skiplist_node {
        void *key;
        struct skiplist_node *list[1];
};

typedef int (*slist_cmp)(const void *, const void *);

struct skiplist {
        int curlevel;
        int maxlevel;
        int chunksize;
        uint32_t total_node_num;
        slist_cmp cmp;
        struct skiplist_node *nil;
        struct skiplist_node *head;
//        struct skiplist_node **btlist; /**< back trace list */

        int randnum;
        int randfbits;

};

extern int skiplist_create(slist_cmp cmp, int maxlevel,
                           int chunksize, void *min, void *max,
                           struct skiplist **);
extern int skiplist_destroy(struct skiplist *);

extern int skiplist_put(struct skiplist *, const void *key, void *data);
extern int skiplist_get(struct skiplist *, const void *key, void **data);
extern void skiplist_print(struct skiplist *);
extern int skiplist_get1st(struct skiplist *, void **data);
extern int skiplist_getlast(const struct skiplist *, void **data);
extern int skiplist_del(struct skiplist *, const void *key, void **data);

extern int skiplist_get_size(struct skiplist* slist);
extern void skiplist_iterate(struct skiplist* slist, void (*func)(void *));
extern void skiplist_iterate_del(struct skiplist* slist, int (*func)(void*));

/** @brief find the first element whose key is not less than key
 *
 * @param key
 * @param data
 * @retval 0 found
 * @retval ENOENT not found
 */
extern int skiplist_lower_bound(struct skiplist *slist, void *key, void **node);

/** @brief find the first element whose key is greater than key
 *
 * @param key
 * @param data
 * @retval 0 found
 * @retval ENOENT not found
 */
extern int skiplist_upper_bound(struct skiplist *slist, void *key, void **data);

/**
 * get first node.
 */
extern struct skiplist_node *skiplist_begin(struct skiplist *slist);

/**
 * get next node
 */
extern struct skiplist_node *skiplist_next(struct skiplist_node *node);

/**
 * node is the END?
 */
extern bool skiplist_end(struct skiplist_node *node);

extern int skiplist_clear(struct skiplist *slist, int free);

#endif

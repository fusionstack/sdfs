

#include <errno.h>
#include <stdint.h>

#define DBG_SUBSYS S_LIBYLIB

#include "sysutil.h"
#include "ylib.h"
#include "dbg.h"
#include "htable.h"

typedef struct {
        struct list_head hook;
        void *entry;
} entry_t;

static entry_t *list_find(struct list_head *head,
                          const void *key, int (*cmp)(const void *,const void *))
{
        struct list_head *pos;

        entry_t *entry, *ret = 0;

        list_for_each(pos, head) {
                entry = (entry_t *)pos;

                if (cmp(key, entry->entry)) {
                        ret = entry;
                        break;
                }
        }

        return ret;
}

int htable_create(htable_t **htable, uint32_t group,
                  int (*cmp_func)(const void *,const void *),
                  uint32_t (*hash_func)(const void *))
{
        int ret;
        uint32_t len, i;
        void *ptr;
        htable_t *_htable;
        htable_head_t *htable_head;

        len = sizeof(htable_t) + sizeof(htable_head_t) * group;

        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_ret, ret);

        _htable = ptr;

        _htable->hash = hash_func;
        _htable->cmp = cmp_func;
        _htable->group = group;

        for (i = 0; i < group; i++) {
                htable_head = &_htable->array[i];

                htable_head->curlen = 0;
                sy_rwlock_init(&htable_head->rwlock, NULL);
                INIT_LIST_HEAD(&htable_head->head);
        }

        *htable = _htable;

        return 0;
err_ret:
        return ret;
}

int htable_insert(htable_t *htable, const void *key, void *value)
{
        int ret;
        htable_head_t *head;
        entry_t *entry;
        uint32_t hash;

        hash = htable->hash(key);

        head = &htable->array[hash % htable->group];

        sy_rwlock_wrlock(&head->rwlock);

        DBUG("****htable insert, head %p\n", head);

        entry = list_find(&head->head, key, htable->cmp);
        if (entry) {
                ret = EEXIST;

                goto err_ret;
                //GOTO(err_ret, ret);
        }

        ret = ymalloc((void **)&entry, sizeof(entry_t));
        if (ret)
                GOTO(err_ret, ret);

        entry->entry = value;

        list_add(&entry->hook, &head->head);
        head->curlen ++;

        sy_rwlock_unlock(&head->rwlock);

        return 0;
err_ret:
        sy_rwlock_unlock(&head->rwlock);
        return ret;
}

int htable_get(htable_t *htable, const void *key, void **buf)
{
        int ret;
        htable_head_t *head;
        entry_t *entry;
        uint32_t hash;

        hash = htable->hash(key);

        head = &htable->array[hash % htable->group];

        sy_rwlock_rdlock(&head->rwlock);

        DBUG("****htable get, head %p len %u\n", head, head->curlen);

        if (head->curlen == 0) {
                ret = ENOENT;

                //GOTO(err_ret, ret);
                goto err_ret;
        }

        entry = list_find(&head->head, key, htable->cmp);
        if (!entry) {
                ret = ENOENT;

                goto err_ret;
        }

        *buf = entry->entry;

        sy_rwlock_unlock(&head->rwlock);

        return 0;
err_ret:
        sy_rwlock_unlock(&head->rwlock);
        return ret;
}

int htable_drop(htable_t *htable, const void *key, void **value)
{
        int ret;
        htable_head_t *head;
        entry_t *entry;
        uint32_t hash;

        hash = htable->hash(key);

        head = &htable->array[hash % htable->group];

        sy_rwlock_wrlock(&head->rwlock);

        entry = list_find(&head->head, key, htable->cmp);
        if (!entry) {
                ret = ENOENT;
                goto err_ret;
        }

        list_del(&entry->hook);

        *value = entry->entry;

        head->curlen --;

        yfree((void **)&entry);

        sy_rwlock_unlock(&head->rwlock);

        return 0;
err_ret:
        sy_rwlock_unlock(&head->rwlock);
        return ret;
}

int htable_iterator_remove(htable_t *htable, int (*remove)(void *, void *),
                                void *arg)
{
        htable_head_t *head;
        struct list_head *pos, *n;
        entry_t *entry;
        uint32_t i;

        for (i = 0; i < htable->group; i++) {
                head = &htable->array[i];

                sy_rwlock_wrlock(&head->rwlock);

                list_for_each_safe(pos, n, &head->head) {
                        entry = (entry_t *)pos;

                        if (remove(arg, entry->entry)) {
                                list_del(&entry->hook);

                                head->curlen --;

                                yfree((void **)&entry);
                        }
                }

                sy_rwlock_unlock(&head->rwlock);
        }

        return 0;
}

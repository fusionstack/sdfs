

#include <errno.h>
#include <stdint.h>

#define DBG_SUBSYS S_LIBYLIB

#include "sysutil.h"
#include "ylib.h"
#include "dbg.h"
#include "cache.h"

static cache_entry_t *__list_find(struct list_head *head, const void *key, cmp_func cmp)
{
        struct list_head *pos;

        cache_entry_t *entry, *ret = 0;

        list_for_each(pos, head) {
                entry = (cache_entry_t *)pos;

                if (cmp(key, entry->value)) {
                        ret = entry;
                        break;
                }
        }

        return ret;
}

void __cache_newsize(cache_t *cache, int size, int count)
{
        int ret;

        ret = sy_spin_lock(&cache->size_lock);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        DBUG("cache %s mem %llu:%llu entry %llu:%llu size %d count %d\n", cache->name,
             (LLU)cache->mem, (LLU)cache->max_mem, (LLU)cache->entry,
             (LLU)cache->max_entry, size, count);

        if (size < 0) {
                YASSERT(cache->mem > (unsigned)(0 -size));
        }

        cache->mem += size;
        cache->entry += count;

        sy_spin_unlock(&cache->size_lock);
}

static void *__collect(void *_cache)
{
        int ret, done, ref, slp = 1;
        cache_t *cache = _cache;
        struct list_head *pos;
        cache_entry_t *ent;
        uint32_t i, max, dentry, dmem;
        cache_head_t *head;

        while (srv_running) {
                ret = sleep(slp);
                if (ret) {
                        ret = errno;

                        if (ret == EINTR)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }

                ret = sy_spin_lock(&cache->size_lock);
                if (ret)
                        GOTO(err_ret, ret);

                if (cache->mem < cache->max_mem
                    && cache->entry < cache->max_entry) {
                        sy_spin_unlock(&cache->size_lock);
                        slp = 1;
                        continue;
                }

                DBUG("begin swap %s %llu:%llu %llu:%llu\n",
                     cache->name, (LLU)cache->mem, (LLU)cache->max_mem,
                     (LLU)cache->entry, (LLU)cache->max_entry);

                max = cache->max_entry;

                sy_spin_unlock(&cache->size_lock);

                ret = sy_spin_lock(&cache->lru_lock);
                if (ret)
                        GOTO(err_ret, ret);

                pos = &cache->lru;
                pos = pos->prev;

                sy_spin_unlock(&cache->lru_lock);

                done = 0;
                ref = 0;
                dentry = 0;
                dmem = 0;

                for (i = 0; i < max / 10 && done == 0; i++) {
                        ret = sy_spin_lock(&cache->lru_lock);
                        if (ret)
                                GOTO(err_ret, ret);

                        YASSERT(list_empty(&cache->lru) == 0);

                        if (pos == &cache->lru) {
                                sy_spin_unlock(&cache->lru_lock);
                                done = 1;
                                break;
                        }

                        sy_spin_unlock(&cache->lru_lock);

                        ent = (void *)pos - sizeof(struct list_head);

                        head = ent->head;

                        ret = sy_rwlock_wrlock(&head->rwlock);
                        if (ret)
                                GOTO(err_ret, ret);

                        ret = sy_spin_lock(&head->ref_lock);
                        if (ret)
                                GOTO(err_lock, ret);

                        if (ent->ref) {
                                YASSERT(ent->ref > 0);
                                DBUG("ref %u\n", ent->ref);

                                ref++;
                                pos = pos->prev;
                                sy_spin_unlock(&head->ref_lock);
                                sy_rwlock_unlock(&head->rwlock);
                                continue;
                        }

                        sy_spin_unlock(&head->ref_lock);

                        ret = sy_spin_lock(&cache->lru_lock);
                        if (ret)
                                GOTO(err_lock, ret);

                        YASSERT(&ent->lru_hook != &cache->lru);

                        list_del(&ent->lru_hook);
#if 1
                        pos = &cache->lru;
#endif
                        pos = pos->prev;

                        sy_spin_unlock(&cache->lru_lock);

                        list_del(&ent->hook);

                        head->curlen --;

                        if (cache->drop)
                                cache->drop(ent->value, ent);
                        else
                                yfree((void **)&ent->value);

                        ret = sy_spin_lock(&cache->size_lock);
                        if (ret)
                                UNIMPLEMENTED(__DUMP__);

                        cache->mem -= (ent->size);
                        cache->entry -= 1;
                        dmem += (ent->size);
                        dentry += 1;

                        if (cache->entry == 0) {
                                done = 2;
                        } else if (cache->max_mem > cache->mem
                                   && cache->max_entry > cache->entry) {
                                if ((int64_t)(cache->max_mem - cache->mem) >
                                    (int64_t)((cache->mem / cache->entry) * cache->decrease)
                                    && (int)(cache->max_entry - cache->entry) > (int)cache->decrease) {
                                        DBUG("finish... %llu:%llu %llu:%llu ____ %llu %d %d %d\n",
                                             (LLU)cache->mem, (LLU)cache->max_mem,
                                             (LLU)cache->entry, (LLU)cache->max_entry,
                                             (LLU)cache->max_mem - cache->mem,
                                             (int)(cache->mem / cache->entry) * cache->decrease,
                                             (int)cache->max_entry - cache->entry,
                                             (int)cache->decrease);

                                        done = 3;
                                } else
                                        done = 0;
                        } else
                                done = 0;

                        DBUG("swap... %llu:%llu %llu:%llu\n",
                             (LLU)cache->mem, (LLU)cache->max_mem,
                             (LLU)cache->entry, (LLU)cache->max_entry);

                        sy_spin_unlock(&cache->size_lock);

                        yfree((void **)&ent);

                        sy_rwlock_unlock(&head->rwlock);
                }

                if (cache->max_mem < cache->mem
                        || cache->max_entry < cache->entry) {
                        DWARN("cache %s busy current %llu max %llu current"
                              " %llu max %llu i %u max %u ref %u dentry %u dmem %u done %u\n",
                              cache->name, (LLU)cache->mem, (LLU)cache->max_mem,
                              (LLU)cache->entry, (LLU)cache->max_entry, i, max,
                              ref, dentry, dmem, done);
                        slp = 0;
                } else
                        slp = 1;
        }

        return NULL;
err_lock:
err_ret:
        UNIMPLEMENTED(__DUMP__);
        return NULL;
}

int cache_init(cache_t **cache, uint32_t max_entry, uint64_t max_mem,
               cmp_func cmp, hash_func hash, drop_func drop,
               int decrease, const char *name)
{
        int ret;
        uint32_t array_len, len, i;
        void *ptr;
        cache_t *_cache;
        cache_head_t *cache_head;
        pthread_attr_t ta;
        pthread_t th;

        YASSERT(max_entry);

/* #ifdef __CYGWIN__ */
	/* max_entry = 10; */
/* #endif */

        array_len = max_entry / 2;

        len = sizeof(cache_t) + sizeof(cache_head_t) * array_len;

        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_ret, ret);

        DINFO("cache %s malloc %u\n", name, len);

        _cache = ptr;
        ptr += sizeof(cache_t);

        _cache->array_len = array_len;
        _cache->drop = drop;
        _cache->hash = hash;
        _cache->cmp = cmp;
        _cache->mem = len;
        _cache->entry = 0;
        _cache->max_mem = max_mem + len + sizeof(cache_entry_t) * max_entry;
        _cache->max_entry = max_entry;

        if ((unsigned)decrease > max_entry / 2)
                _cache->decrease = max_entry / 2;
        else
                _cache->decrease = decrease;

        for (i = 0; i < array_len; i++) {
                cache_head = &_cache->array[i];

                cache_head->curlen = 0;
                sy_rwlock_init(&cache_head->rwlock, NULL);
                sy_spin_init(&cache_head->ref_lock);
                INIT_LIST_HEAD(&cache_head->head);
        }

        pthread_attr_init(&ta);
        pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        sy_spin_init(&_cache->lru_lock);
        sy_spin_init(&_cache->size_lock);

        INIT_LIST_HEAD(&_cache->lru);

        if (strlen(name) > MAX_NAME_LEN) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        strcpy(_cache->name, name);

        ret = pthread_create(&th, &ta, __collect, _cache);
        if (ret)
                GOTO(err_ret, ret);

        *cache = _cache;

        DBUG("cache %p name %s\n", _cache, _cache->name);

        return 0;
err_ret:
        return ret;
}

int cache_insert(cache_t *cache, const void *key, void *value, int size)
{
        int ret;
        cache_head_t *head;
        cache_entry_t *ent;
        uint32_t hash;
        int ent_size;

        YASSERT(size > 0);

        ent_size = sizeof(cache_entry_t) + size;

        hash = cache->hash(key);

        head = &cache->array[hash % cache->array_len];

        ret = sy_rwlock_wrlock(&head->rwlock);
        if (ret)
                GOTO(err_ret, ret);

        ent = __list_find(&head->head, key, cache->cmp);
        if (ent) {
                if (ent->erase) {
                        DWARN("ent erased\n");
                        ent->erase = 0;
                }

                ret = EEXIST;
                goto err_lock;
        }

        ret = ymalloc((void **)&ent, sizeof(cache_entry_t));
        if (ret)
                GOTO(err_lock, ret);

        ent->value = value;
        ent->size = ent_size;
        ent->ref = 0;
        ent->erase = 0;
        ent->cache = cache;
        list_add(&ent->hook, &head->head);
        head->curlen ++;
        ent->head = head;

        ret = sy_spin_lock(&cache->lru_lock);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        list_add(&ent->lru_hook, &cache->lru);

        sy_spin_unlock(&cache->lru_lock);

        __cache_newsize(cache, ent_size, 1);

        ent->time = time(NULL);
        sy_rwlock_unlock(&head->rwlock);

        return 0;

err_lock:
        sy_rwlock_unlock(&head->rwlock);
err_ret:
        return ret;
}

int cache_get(cache_t *cache, const void *key, cache_entry_t **_ent)
{
        int ret;
        cache_head_t *head;
        cache_entry_t *ent;
        uint32_t hash;

        hash = cache->hash(key);

        head = &cache->array[hash % cache->array_len];

        ret = sy_rwlock_rdlock(&head->rwlock);
        if (ret)
                GOTO(err_ret, ret);

        if (head->curlen == 0) {
                ret = ENOENT;
                goto err_lock;
        }

        ent = __list_find(&head->head, key, cache->cmp);
        if (!ent) {
                ret = ENOENT;
                goto err_lock;
        }

        if (ent->erase) {
                DWARN("ent erased\n");
                sy_rwlock_unlock(&head->rwlock);
                sleep(1);
                ret = ENOENT;
                goto err_ret;
        }

        ret = sy_spin_lock(&cache->lru_lock);
        if (ret)
                GOTO(err_lock, ret);

        list_del(&ent->lru_hook);
        list_add(&ent->lru_hook, &cache->lru);

        sy_spin_unlock(&cache->lru_lock);

        ret = sy_spin_lock(&head->ref_lock);
        if (ret)
                GOTO(err_lock, ret);

        ent->ref++;

        sy_spin_unlock(&head->ref_lock);

        *_ent = ent;
        sy_rwlock_unlock(&head->rwlock);

        DBUG("cache get\n");

        return 0;
err_lock:
        sy_rwlock_unlock(&head->rwlock);
err_ret:
        return ret;
}

int cache_drop_nolock(cache_entry_t *ent)
{
        int ret;
        cache_head_t *head;
        cache_t *cache;

        head = ent->head;
        cache = ent->cache;

        YASSERT(head->curlen);

        ret = sy_spin_lock(&head->ref_lock);
        if (ret)
                GOTO(err_ret, ret);

        YASSERT(ent->ref);
        ent->erase = 1;

        sy_spin_unlock(&head->ref_lock);

        ret = sy_spin_lock(&cache->lru_lock);
        if (ret)
                GOTO(err_ret, ret);

        list_del(&ent->lru_hook);

        sy_spin_unlock(&cache->lru_lock);

        list_del(&ent->hook);

        return 0;
err_ret:
        return ret;
}

static int __cache_drop_free(cache_entry_t *ent)
{
        int ret;
        cache_head_t *head;
        cache_t *cache;

        head = ent->head;
        cache = ent->cache;

        ret = sy_rwlock_wrlock(&head->rwlock);
        if (ret)
                GOTO(err_ret, ret);

        YASSERT(head->curlen);

        ret = sy_spin_lock(&head->ref_lock);
        if (ret)
                GOTO(err_lock, ret);

        if (ent->ref) {
                ret = EBUSY;
                sy_spin_unlock(&head->ref_lock);
                GOTO(err_lock, ret);
        }

        sy_spin_unlock(&head->ref_lock);

        __cache_newsize(cache, -ent->size, -1);

        if (cache->drop)
                cache->drop(ent->value, ent);
        else
                yfree((void **)&ent->value);

        sy_rwlock_unlock(&head->rwlock);

        yfree((void **)&ent);

        return 0;
err_lock:
        sy_rwlock_unlock(&head->rwlock);
err_ret:
        return ret;
}

int cache_release(cache_entry_t *ent)
{
        int ret;
        cache_head_t *head;

        DBUG("cache release\n");

        if (ent->erase == 0)
                YASSERT(ent->value);

        head = ent->head;

        ret = sy_spin_lock(&head->ref_lock);
        if (ret)
                GOTO(err_ret, ret);

        YASSERT(ent->ref > 0);

        ent->ref--;

        DBUG("ref %u\n", ent->ref);

        if (ent->ref == 0 && ent->erase) {
                sy_spin_unlock(&head->ref_lock);

                ret = __cache_drop_free(ent);
                if (ret)
                        GOTO(err_ret, ret);
        }

        sy_spin_unlock(&head->ref_lock);


        return 0;
err_ret:
        return ret;
}

int cache_reference(cache_entry_t *ent)
{
        int ret;
        cache_head_t *head;

        DBUG("cache release\n");

        head = ent->head;

        ret = sy_spin_lock(&head->ref_lock);
        if (ret)
                GOTO(err_ret, ret);

        YASSERT(ent->ref > 0);

        ent->ref++;

        sy_spin_unlock(&head->ref_lock);

        return 0;
err_ret:
        return ret;
}

int cache_wrlock(cache_entry_t *ent)
{
        int ret;
        cache_head_t *head;

        head = ent->head;

        ret = sy_rwlock_wrlock(&head->rwlock);
        if (ret)
                GOTO(err_ret, ret);

#if 0
        if (ent->erase) {
                ret = ENOENT;
                sy_rwlock_unlock(&head->rwlock);
                GOTO(err_ret, ret);
        }
#endif

        return 0;
err_ret:
        return ret;
}

int cache_rdlock(cache_entry_t *ent)
{
        int ret;
        cache_head_t *head;

        head = ent->head;

        ret = sy_rwlock_rdlock(&head->rwlock);
        if (ret)
                GOTO(err_ret, ret);

#if 0
        if (ent->erase) {
                ret = ENOENT;
                sy_rwlock_unlock(&head->rwlock);
                GOTO(err_ret, ret);
        }
#endif

        return 0;
err_ret:
        return ret;
}

int cache_unlock(cache_entry_t *ent)
{
        cache_head_t *head;

        head = ent->head;

        sy_rwlock_unlock(&head->rwlock);

        return 0;
}

int cache_create_lock(cache_t *cache, const void *key, cache_entry_t **_ent)
{
        int ret;
        cache_head_t *head;
        cache_entry_t *ent;
        uint32_t hash;

        hash = cache->hash(key);

        head = &cache->array[hash % cache->array_len];

        ret = sy_rwlock_wrlock(&head->rwlock);
        if (ret)
                GOTO(err_ret, ret);

        ent = __list_find(&head->head, key, cache->cmp);
        if (ent) {
                if (ent->erase) {
                        DWARN("ent erased\n");
                        ent->erase = 0;
                }

                ret = EEXIST;
                goto err_lock;
        }

        ret = ymalloc((void **)&ent, sizeof(cache_entry_t));
        if (ret)
                GOTO(err_lock, ret);

        ent->value = NULL;
        ent->size = sizeof(cache_entry_t);
        ent->ref = 1;
        ent->erase = 0;
        ent->cache = cache;
        list_add(&ent->hook, &head->head);
        head->curlen ++;
        ent->head = head;

        ret = sy_spin_lock(&cache->lru_lock);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        list_add(&ent->lru_hook, &cache->lru);

        sy_spin_unlock(&cache->lru_lock);

        __cache_newsize(cache, ent->size, 1);

        ent->time = time(NULL);

        *_ent = ent;
        /*return with lock*/

        return 0;

err_lock:
        sy_rwlock_unlock(&head->rwlock);
err_ret:
        return ret;
}

int cache_newsize(cache_t *cache, cache_entry_t *ent, uint32_t size)
{
        int ret;
        cache_head_t *head;
        int ent_size;

        head = ent->head;

        ret = sy_rwlock_tryrdlock(&head->rwlock);
        if (ret) {
                if (ret == EBUSY) {
                        ent_size = sizeof(cache_entry_t) + size;

                        if (ent->size != (unsigned)ent_size) {
                                __cache_newsize(cache, ent_size - ent->size, 0);
                                ent->size = ent_size;
                        }
                } else
                        GOTO(err_ret, ret);
        } else {
                DERROR("lock ent first!!!!!!!!!!\n");
                YASSERT(0);
        }

        return 0;
err_ret:
        return ret;
}

int cache_increase(cache_t *cache, cache_entry_t *ent, int size)
{
        int ret;
        cache_head_t *head;

        if (size == 0)
                return 0;

        head = ent->head;

        ret = sy_rwlock_tryrdlock(&head->rwlock);
        if (ret) {
                if (ret == EBUSY) {
                        ent->size += size;
                        __cache_newsize(cache, size, 0);
                } else
                        GOTO(err_ret, ret);
        } else {
                DERROR("lock ent first!!!!!!!!!!\n");
                YASSERT(0);
        }

        return 0;
err_ret:
        return ret;
}

void cache_iterator(cache_t *cache, void (*callback)(void *, void *), void *arg)
{
        int ret;
        struct list_head *pos;
        cache_entry_t *ent;

        DBUG("cache %p name %s\n", cache, cache->name);

        ret = sy_spin_lock(&cache->lru_lock);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        list_for_each(pos, &cache->lru) {
                ent = (void *)pos - sizeof(struct list_head);

                /*it can't have lock in callback, or it could happen dead lock with cache_get/cache_insert*/
                callback(arg, ent);
        }

        sy_spin_unlock(&cache->lru_lock);
}

void cache_destroy(cache_t *cache, void (*callback)(void *))
{
        int ret, count = 0;
        struct list_head *pos, *n;
        cache_entry_t *ent;
        cache_head_t *head;

        ret = sy_spin_lock(&cache->lru_lock);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        list_for_each_safe(pos, n, &cache->lru) {
                ent = (void *)pos - sizeof(struct list_head);
                list_del(&ent->lru_hook);
                list_del(&ent->hook);
                head = ent->head;
                head->curlen --;
                callback(ent->value);
                __cache_newsize(cache, -ent->size, -1);
                yfree((void **)&ent);
                count ++;
        }

        sy_spin_unlock(&cache->lru_lock);

        DINFO("cache %p name %s count %u %u\n", cache, cache->name, count, cache->entry);
}

int cache_drop(cache_t *cache, const void *key)
{
        int ret;
        cache_entry_t *ent;

        ret = cache_get(cache, key, &ent);
        if (ret) {
                if (ret == ENOENT)
                        goto out;
                else
                        GOTO(err_ret, ret);
        }

        ret = cache_wrlock(ent);
        if (ret)
                GOTO(err_ret, ret);

        cache_drop_nolock(ent);

        cache_unlock(ent);
        cache_release(ent);

out:
        return 0;
err_ret:
        return ret;
}

int cache_trywrlock(cache_entry_t *ent)
{
        int ret;
        cache_head_t *head;

        head = ent->head;

        ret = sy_rwlock_trywrlock(&head->rwlock);
        if (ret) {
                if (ret == EBUSY)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int cache_tryrdlock(cache_entry_t *ent)
{
        int ret;
        cache_head_t *head;

        head = ent->head;

        ret = sy_rwlock_tryrdlock(&head->rwlock);
        if (ret) {
                if (ret == EBUSY)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

#ifndef __NCACHE_H__
#define __NCACHE_H__

#include <semaphore.h>

#include "sdfs_list.h"
#include "ylock.h"

typedef struct {
        uint32_t curlen;
        sy_spinlock_t ref_lock;
        sy_rwlock_t rwlock;
        struct list_head head;
} cache_head_t;

typedef struct {
        struct list_head hook;
        struct list_head lru_hook; /*careful here*/
        uint32_t size;
        int16_t ref; /*ref count*/
        int16_t erase;
        time_t time;
        cache_head_t *head;
        void *cache;
        void *value;
} cache_entry_t;

//typedef int (*replace_func)(void **old, void **new);
typedef int (*drop_func)(void *, cache_entry_t *cent);
typedef uint32_t (*hash_func)(const void *);
typedef int (*cmp_func)(const void *,const void *);
typedef void (*exec_func)(void *, void *);
typedef int (*exec_func_r)(void *, void *);

typedef struct {
        char name[MAX_NAME_LEN];
        drop_func drop;
        hash_func hash;
        cmp_func cmp;
        sy_spinlock_t size_lock;
        sy_spinlock_t lru_lock;
        struct list_head lru;
        uint64_t mem;
        uint64_t max_mem;
        uint32_t max_entry; /*how many entrys can this cache hold*/
        uint32_t entry;
        uint32_t decrease;
        uint32_t array_len; /*length of the array below*/
        cache_head_t array[0];
} cache_t;

int cache_init(cache_t **cache, uint32_t max_entry, uint64_t max_mem,
               cmp_func cmp, hash_func hash, drop_func drop,
               int decrease, const char *name);

int cache_get(cache_t *cache, const void *key, cache_entry_t **ent);
int cache_insert(cache_t *cache, const void *key, void *value, int size);
int cache_release(cache_entry_t *ent);
int cache_reference(cache_entry_t *ent);
int cache_rdlock(cache_entry_t *ent);
int cache_wrlock(cache_entry_t *ent);
int cache_unlock(cache_entry_t *ent);
int cache_create_lock(cache_t *cache, const void *key, cache_entry_t **ent);
int cache_newsize(cache_t *cache, cache_entry_t *ent, uint32_t size);
void cache_iterator(cache_t *cache, void (*callback)(void *, void *), void *arg);
int cache_increase(cache_t *cache, cache_entry_t *ent, int size);
void cache_destroy(cache_t *cache, void (*callback)(void *));
int cache_drop(cache_t *cache, const void *key);
int cache_drop_nolock(cache_entry_t *ent);
int cache_tryrdlock(cache_entry_t *ent);
int cache_trywrlock(cache_entry_t *ent);

#endif

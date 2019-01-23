#ifndef __HTABLE_R_H__
#define __HTABLE_R_H__

#include "sdfs_list.h"
#include "ylock.h"

typedef struct {
        uint32_t curlen;
        sy_rwlock_t rwlock;
        struct list_head head;
} htable_head_t;

typedef struct {
        int (*cmp)(const void *,const void *);
        uint32_t (*hash)(const void *);
        uint32_t group;
        htable_head_t array[0];
} htable_t;

int htable_insert(htable_t *htable, const void *key, void *value);
int htable_create(htable_t **htable, uint32_t group,
                  int (*cmp_func)(const void *,const void *),
                  uint32_t (*hash_func)(const void *));
int htable_drop(htable_t *htable, const void *key, void **value);
int htable_iterator_remove(htable_t *htable, int (*remove)(void *, void *),
                           void *arg);
int htable_get(htable_t *htable, const void *key, void **buf);

#endif

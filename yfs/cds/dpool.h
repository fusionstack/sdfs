#ifndef __DPOOL_H__
#define __DPOOL_H__

#include <stdint.h>
#include <semaphore.h>

#include "sdfs_conf.h"

typedef struct {
        char home[MAX_PATH_LEN];
        sy_spinlock_t lock;
        struct list_head list;
        int len;
} dpool_level_t;

typedef struct {
        int inited;
        int max;
        int level_count;
        size_t size;
        dpool_level_t array[0];
} dpool_t;

int dpool_init(dpool_t **dpool, size_t size, int max, int level_count);
int dpool_get(dpool_t *dpool, char *path, int level);
int dpool_put(dpool_t *dpool, const char *path, int level);
uint64_t dpool_size(dpool_t *dpool, int levelid);

#endif

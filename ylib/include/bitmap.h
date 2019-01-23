#ifndef __BITMAP_H__
#define __BITMAP_H__

#include "array.h"
#include "sdfs_conf.h"
#include "dbg.h"

typedef struct {
        sy_spinlock_t lock;
        uint32_t count;
        uint32_t cached;
        array_t array;
} bitmap_t;

int bitmap_init(bitmap_t *bm);
int bitmap_set_unuse(bitmap_t *bm, uint64_t idx);
int bitmap_set_used(bitmap_t *bm, uint64_t idx);
int bitmap_get_empty(bitmap_t *bm, uint64_t *idx);

#endif

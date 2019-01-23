#ifndef __BMAP_H
#define __BMAP_H

#include <stdlib.h>
#include <sys/types.h>
#include <stdint.h>

typedef struct {
        uint32_t nr_one;
        uint32_t size;
        uint32_t len;
        char *bits;
} bmap_t;

int bmap_create(bmap_t *bmap, uint32_t size);
int bmap_destroy(bmap_t *bmap);
int bmap_set(bmap_t *bmap, uint32_t off);
int bmap_get(const bmap_t *bmap, uint32_t off);
int bmap_del(bmap_t *bmap, uint32_t off);
int bmap_full(const bmap_t *bmap);
void bmap_load(bmap_t *bmap, char *opaque, int len);
int bmap_get_empty(bmap_t *bmap);
void bmap_clean(bmap_t *bmap);

#endif


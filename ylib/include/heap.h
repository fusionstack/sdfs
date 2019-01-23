#ifndef __HEAP_H__
#define __HEAP_H__

#include "ylock.h"

#define HEAP_MAX_SIZE 4096

typedef int (*gt_func)(const void *, const void *);
typedef int (*heap_drop_func)(const void *);
typedef void (*heap_print_func)(const void *);

struct heap_head_t {
        void *data;
};

struct heap_t {
        gt_func gt;
        heap_drop_func drop;
        heap_print_func print;

        uint32_t capacity;

        uint32_t size;

        sy_rwlock_t size_rwlock;
        struct heap_head_t *element[HEAP_MAX_SIZE];
};

int heap_init(struct heap_t *heap, gt_func gt, heap_drop_func drop,
                heap_print_func print, uint32_t max_element, void *min);
int heap_insert(struct heap_t *heap, void *data);
int heap_pop(struct heap_t *heap, void **data);
void heap_print(struct heap_t *heap);
uint32_t heap_len(struct heap_t *heap);

#endif

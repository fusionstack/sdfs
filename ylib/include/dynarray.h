#ifndef YT_DYNARRAY_H
#define YT_DYNARRAY_H

#include <stdint.h>
#include <stdbool.h>
#include <sys/cdefs.h>

__BEGIN_DECLS

typedef struct dynarray_t {
    void     *buffer;
    uint32_t  size;
    uint32_t  max_size;
    uint32_t  alloc_inc;
    uint32_t  element_len;
} dynarray_t;

int  dynarray_init(dynarray_t *array, uint32_t element_len,
        uint32_t init_size, uint32_t alloc_inc);

void dynarray_destroy(dynarray_t *array);

int  dynarray_push(dynarray_t *array, void *element);
int  dynarray_pop(dynarray_t *array, void **element);
void dynarray_delete(dynarray_t *array, uint32_t idx);

int  dynarray_set(dynarray_t *array, uint32_t idx, void *element);
void dynarray_get(dynarray_t *array, uint32_t idx, void *element);

static inline uint32_t dynarray_size(dynarray_t *array)
{
    return array->size;
}

static inline bool dynarray_empty(dynarray_t *array)
{
    return array->size == 0;
}

__END_DECLS

#endif

/* Handling of arrays that can grow dynamicly. */

#include <stdbool.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#define DBG_SUBSYS S_LIBYLIB

#include "ylib.h"
#include "dynarray.h"
#include "dbg.h"

#define DEFAULT_INCREMENT 16

void dynarray_dump(dynarray_t *array)
{
        DBUG("size      : %u\n", array->size);
        DBUG("max_size  : %u\n", array->max_size);
        DBUG("element   : %u\n", array->element_len);
        DBUG("alloc_inc : %u\n", array->alloc_inc);
}

int __dynarray_resize(dynarray_t *array, uint32_t new_size)
{
        int       ret;
        uint32_t  old_len;
        uint32_t  new_len;

        if (new_size <= array->max_size)
                return 0;

        old_len = array->max_size * array->element_len;
        new_len = new_size        * array->element_len;

        ret = yrealloc((void **)&array->buffer, old_len, new_len);
        if (ret)
                GOTO(err_ret, ret);

        array->max_size = new_size;

        return 0;
err_ret:
        return ret;
}

int dynarray_init(dynarray_t *array, uint32_t element_len,
                uint32_t init_size, uint32_t alloc_inc)
{
        int ret;

        if (!alloc_inc) {
                alloc_inc = DEFAULT_INCREMENT;
        }

        if (!init_size)
                init_size = alloc_inc;

        array->size        = 0;
        array->max_size    = init_size;
        array->alloc_inc   = alloc_inc;
        array->element_len = element_len;

        ret = ymalloc((void **)&array->buffer, init_size * element_len);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

/**
 * Empty array by freeing all memory
 */
void dynarray_destroy(dynarray_t *array)
{
        if (array->buffer) {
                yfree((void **)&array->buffer);
                array->buffer = 0;
                array->size = array->max_size = 0;
        }
}

/**
 * Insert element at the end of array. Allocate memory if needed.
 */
int dynarray_push(dynarray_t *array, void *element)
{
        int       ret;
        uint32_t  new_size;
        void     *buffer;

        if (array->size == array->max_size) {
                new_size = array->max_size + array->alloc_inc;
                ret = __dynarray_resize(array, new_size);
                if (ret)
                        GOTO(err_ret, ret);
        }

        buffer = array->buffer + (array->size * array->element_len);
        array->size++;
        _memcpy(buffer, (const void *)element, (size_t) array->element_len);

        return 0;
err_ret:
        return ret;
}


int dynarray_set(dynarray_t *array, uint32_t idx, void *element)
{
        int       ret;
        uint32_t  new_size;

        if (idx >= array->size) {
                if (idx >= array->max_size) {
                        new_size  = (idx + array->alloc_inc) / array->alloc_inc;
                        new_size *= array->alloc_inc;

                        ret = __dynarray_resize(array, new_size);
                        if (ret)
                                GOTO(err_ret, ret);
                }

                bzero((void *) (array->buffer+array->size*array->element_len),
                                (idx - array->size)*array->element_len);
                array->size = idx + 1;
        }
        _memcpy(array->buffer + (idx * array->element_len),
                        element, (size_t)array->element_len);

        return 0;
err_ret:
        return ret;
}

void dynarray_get(dynarray_t *array, uint32_t idx, void *element)
{
        if (idx >= array->size) {
                bzero(element,array->element_len);
                return;
        }
        _memcpy(element, array->buffer+idx*array->element_len,
                        (size_t) array->element_len);
}

/**
 * Pop last element from array.
 */
int dynarray_pop(dynarray_t *array, void **element)
{
        if (array->size == 0) {
                *element = NULL;
                return EINVAL;
        }

        array->size--;
        *element = array->buffer + array->size * array->element_len;
        return 0;
}

/**
 * Delete element by given index
 */
void dynarray_delete(dynarray_t *array, uint32_t idx)
{
        void *ptr;

        ptr = array->buffer + array->element_len * idx;
        _memmove(ptr, ptr + array->element_len,
                        (array->size-idx)*array->element_len);
        array->size--;
}

// #define DYNARRAY_TEST
#ifdef DYNARRAY_TEST
int main()
{
        dynarray_t arr;

        dynarray_init(&arr, sizeof(int), 4, 0);
        dynarray_dump(&arr);

        assert(arr.size == 0);
        assert(arr.max_size == 4);

        uint32_t i ;
        for (i = 0; i < 10; ++i) {
                dynarray_push(&arr, &i);
        }
        dynarray_dump(&arr);

        dynarray_delete(&arr, 2);
        dynarray_delete(&arr, 1);
        dynarray_delete(&arr, 0);

        int k;
        for (i = 0; i < arr.size; ++i) {
                dynarray_get(&arr, i, &k);
                DBUG("%d : %d\n", i, k);
        }

        dynarray_destroy(&arr);
        return 0;
}
#endif

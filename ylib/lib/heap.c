
#include <stdint.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "heap.h"
#include "ylib.h"
#include "dbg.h"


static int is_full(struct heap_t *heap) 
{
        if (heap->capacity == heap->size) 
                return 1;
        else
                return 0;
}

static int is_empty(struct heap_t *heap)
{
        if (heap->size == 0) 
                return 1;
        else
                return 0;
}

int heap_init(struct heap_t *heap, gt_func gt, heap_drop_func drop,
                heap_print_func print, uint32_t max_element, void *min)
{
        int ret;
#if 0
        int len;
#endif
        struct heap_t *_heap;
        struct heap_head_t *_element;

        _heap = heap;

        sy_rwlock_init(&_heap->size_rwlock, NULL);

#if 0
        len = sizeof(struct heap_head_t *) * max_element + 1;
        ret = ymalloc((void **)(heap->element), len);
        if (ret) 
                GOTO(err_ret, ret);
#endif

        ret = ymalloc((void **)&_element, sizeof(struct heap_head_t));
        if (ret) 
                GOTO(err_ret, ret);

        _element->data = min;

        if (max_element + 1 > HEAP_MAX_SIZE)
                _heap->capacity = HEAP_MAX_SIZE - 1;
        else
                _heap->capacity = max_element;

        _heap->gt = gt;
        _heap->drop = drop;
        _heap->print = print;
        _heap->size = 0;
        _heap->element[0] = _element;

        return 0;
err_ret:
        return ret;
}

int heap_insert(struct heap_t *heap, void *data)
{
        int ret;
        int i;
        struct heap_head_t* element;

        ret = ymalloc((void **)&element, sizeof(struct heap_head_t)); 
        element->data = data;

        if (is_full(heap)) {
                ret = ENODATA;
                GOTO(err_ret, ret);
        }

        for (i = ++heap->size; 
             heap->gt(heap->element[i/2]->data, element->data) == 1;
             i /= 2) {
                heap->element[i] = heap->element[i/2];
        }
        heap->element[i] = element;

        return 0;
err_ret:
        return ret;
}

int heap_pop(struct heap_t *heap, void **data)
{
        int ret;
        uint32_t i, child;
        struct heap_head_t *min, *last;

        if (is_empty(heap)) {
                ret = ENOKEY;
                *data = NULL;
                GOTO(err_ret, ret);
        }

        min = heap->element[1];
        last = heap->element[heap->size];
        heap->size--;

        for (i = 1; i*2 <= heap->size; i = child) {
                child = i * 2;

                if (child != heap->size &&
                    heap->gt(heap->element[child]->data, 
                            heap->element[child+1]->data) == 1)
                    child++;

                if (heap->gt(last->data, heap->element[child]->data) == 1)
                        heap->element[i] = heap->element[child];
                else
                        break;
        }

        heap->element[i] = last;
        *data = min->data;
        yfree((void **)&min);

        return 0;
err_ret:
        return ret;
}

void heap_print(struct heap_t *heap) 
{
        uint32_t i;

        printf("-------------------\n");
        for (i = 1; i <= heap->size; i++) {
                heap->print((void *) heap->element[i]->data);
        }
        printf("\n-------------------\n");
}

uint32_t heap_len(struct heap_t *heap)
{
        return heap->size;
}

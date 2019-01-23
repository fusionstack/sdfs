#ifndef __ARRAY_H__
#define __ARRAY_H__

#include "sdfs_list.h"
#include "ylib.h"

#define ARRAY_DEFAUT_LEN 5
#define ARRAY_ROW_DEFAULT 1

/**
int array_<type>_init(array, init_len)
<type> array_<type>_idx(array, idx)
<type> array_<type>_insert(array, idx)
int array_<type>_copy(array, src, off, len)
int array_<type>_resize(array, newsize);
int array_<type>_destroy(array);
 */

typedef int (*arrayent_init_func)(void *);

typedef struct {
        uint32_t capacity;
        uint32_t rows;
        void *array;
} array_t;

#define ARRAY_CREATE(__ent_type__, __fix_len__, __init_func__)          \
        typedef struct {                                                \
                __ent_type__ seg[__fix_len__];                          \
        } array_##__ent_type__##_t;                                     \
                                                                        \
        static inline int array_##__ent_type__##_extern(array_t *array, \
                                                        uint64_t len)   \
        {                                                               \
                int ret, new;                                           \
                void *ptr;                                              \
                array_##__ent_type__##_t **seg;                         \
                uint32_t i, j;                                          \
                arrayent_init_func init_func;                           \
                                                                        \
                YASSERT(len != 0);                                      \
                                                                        \
                init_func = __init_func__;                              \
                                                                        \
                                                                        \
                if (array->capacity < len) {                            \
                        new = len / __fix_len__ + (len % __fix_len__)   \
                                ? 1 : 0 - array->rows;                  \
                                                                        \
                        if (array->capacity > len) {                    \
                                return 0;                               \
                        }                                               \
                                                                        \
                        if (array->rows == 0) {                         \
                                ret = ymalloc(&array->array, new        \
                                              * sizeof(array_##__ent_type__##_t *));   \
                        } else {                                        \
                                ret = yrealloc(&array->array,           \
                                               array->rows * sizeof(array_##__ent_type__##_t *), \
                                               (array->rows + new)      \
                                               * sizeof(array_##__ent_type__##_t *)); \
                        }                                               \
                        if (ret)                                        \
                                GOTO(err_ret, ret);                     \
                                                                        \
                        seg = array->array;                             \
                                                                        \
                        for (i = array->rows; i < array->rows + new; i++) { \
                                ret = ymalloc(&ptr, __fix_len__         \
                                              * sizeof(array_##__ent_type__##_t));   \
                                if (ret)                                \
                                        GOTO(err_ret, ret);            \
                                                                        \
                                seg[i] = ptr;                           \
                                                                        \
                                if (init_func) {                        \
                                        for (j = 0; j < __fix_len__; j++) { \
                                                ret = (init_func)(&(seg[i]->seg[j])); \
                                                if (ret)                \
                                                        GOTO(err_ret, ret); \
                                        }                               \
                                } else                                  \
                                        memset(ptr, 0x0, __fix_len__    \
                                               * sizeof(array_##__ent_type__##_t));  \
                        }                                               \
                                                                        \
                        array->rows += new;                             \
                        array->capacity += __fix_len__ * new;           \
                }                                                       \
                                                                        \
                return 0;                                               \
        err_ret:                                                        \
                return ret;                                             \
        }                                                               \
                                                                        \
        static inline int array_##__ent_type__##_init(array_t *array,   \
                                                        uint64_t len)   \
        {                                                               \
                memset(array, 0x0, sizeof(array_t));                    \
                                                                        \
                if (len == 0)                                           \
                        len = ARRAY_DEFAUT_LEN;                         \
                                                                        \
                return array_##__ent_type__##_extern(array, len);       \
        }                                                               \
                                                                        \
        static inline __ent_type__ *array_##__ent_type__##_idx(array_t *array, \
                                                               uint64_t idx) \
        {                                                               \
                int ret;                                                \
                array_##__ent_type__##_t **seg, *_seg;                  \
                if (array->capacity <= idx) {                           \
                        ret = array_##__ent_type__##_extern(array,      \
                                                            idx + 1);   \
                        if (ret)                                        \
                                YASSERT(0);                             \
                }                                                       \
                                                                        \
                seg = array->array;                                     \
                _seg = seg[idx / __fix_len__];                          \
                return &(_seg->seg[idx % __fix_len__]);                 \
        }                                                               \
                                                                        \
        static inline void array_##__ent_type__##_destroy(array_t *array) \
        {                                                               \
                uint32_t i;                                             \
                array_##__ent_type__##_t **seg;                         \
                                                                        \
                seg = array->array;                                     \
                for (i = 0; i < array->rows; i++) {                     \
                        yfree((void **)&(seg[i]));                      \
                }                                                       \
        }                                                               \
                                                                        \
        static inline int array_##__ent_type__##_copy(array_t *array,   \
                                                      __ent_type__ *mem, \
                                                      uint32_t off,     \
                                                      uint32_t count)   \
        {                                                               \
                int ret, seg_idx;                                       \
                uint32_t i, seg_off, mem_off, left, n;                  \
                array_##__ent_type__##_t **seg, *_seg;                               \
                                                                        \
                if (off + count >= array->capacity) {                   \
                        ret = array_##__ent_type__##_extern(array,      \
                                                            off + count + 1); \
                        if (ret)                                        \
                                GOTO(err_ret, ret);                     \
                }                                                       \
                                                                        \
                seg_idx = off / __fix_len__;                            \
                seg_off = off % __fix_len__;                            \
                left = count;                                           \
                mem_off = 0;                                            \
                for (i = seg_idx; i < array->rows; i++) {               \
                        n = left < (__fix_len__ - seg_off)              \
                                ? left : (__fix_len__ - seg_off);       \
                                                                        \
                        seg = array->array;                             \
                        _seg = seg[i];                                  \
                                                                        \
                        memcpy(&(_seg[seg_off]),                 \
                               mem + mem_off, n * sizeof(__ent_type__)); \
                                                                        \
                        if (n == left)                                  \
                                break;                                  \
                                                                        \
                        seg_off = 0;                                    \
                        mem_off += n;                                   \
                        left -= n;                                      \
                }                                                       \
                                                                        \
                return 0;                                               \
        err_ret:                                                        \
                return ret;                                             \
        }

#endif

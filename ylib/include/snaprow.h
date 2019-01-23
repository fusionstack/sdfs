#ifndef __SNAPROW_H__
#define __SNAPROW_H__

#include "array.h"
#include "bitmap.h"
#include "ylib.h"

#define SNAPROW_CREATE(__ent_type__, __fix_len__, __init_func__)        \
                                                                        \
        ARRAY_CREATE(__ent_type__, __fix_len__, __init_func__)          \
                                                                        \
        typedef struct {                                                \
                sy_spinlock_t lock;                                     \
                uint64_t count;                                         \
                bitmap_t bitmap;                                        \
                array_t array;                                          \
        } snaprow_t;                                                    \
                                                                        \
        int snaprow_##__ent_type__##_init(snaprow_t *snap)              \
        {                                                               \
                int ret;                                                \
                                                                        \
                ret = bitmap_init(&snap->bitmap);                       \
                if (ret)                                                \
                        GOTO(err_ret, ret);                             \
                                                                        \
                ret = array_##__ent_type__##_init(&snap->array, 0);   \
                if (ret)                                                \
                        GOTO(err_ret, ret);                             \
                                                                        \
                ret = sy_spin_init(&snap->lock);                        \
                if (ret)                                                \
                        GOTO(err_ret, ret);                             \
                                                                        \
                return 0;                                               \
        err_ret:                                                        \
                return ret;                                             \
        }                                                               \
                                                                        \
        int snaprow_##__ent_type__##_new(snaprow_t *snap, uint64_t *idx) \
        {                                                               \
                int ret;                                                \
                                                                        \
                if (snap->count == snap->array.capacity) {              \
                        *idx = snap->count;                             \
                                                                        \
                        ret = array_##__ent_type__##_extern(&snap->array, *idx); \
                        if (ret)                                        \
                                GOTO(err_ret, ret);                     \
                                                                        \
                        ret = bitmap_set_used(&snap->bitmap, *idx);     \
                        if (ret)                                        \
                                GOTO(err_ret, ret);                     \
                } else {                                                \
                        ret = bitmap_get_empty(&snap->bitmap, idx);     \
                        if (ret)                                        \
                                YASSERT(0);                             \
                                                                        \
                }                                                       \
                                                                        \
                ret = sy_spin_lock(&snap->lock);                        \
                if (ret)                                                \
                        YASSERT(0);                                     \
                snap->count ++;                                         \
                ret = sy_spin_unlock(&snap->lock);                      \
                if (ret)                                                \
                        YASSERT(0);                                     \
                                                                        \
                return 0;                                               \
        err_ret:                                                        \
                return ret;                                             \
        }                                                               \
                                                                        \
        __ent_type__ *snaprow_##__ent_type__##_get(snaprow_t *snap, uint64_t idx) \
        {                                                               \
                YASSERT(idx < snap->count);                             \
                                                                        \
                return array_##__ent_type__##_idx(&snap->array, idx);   \
        }                                                               \
                                                                        \
        int snaprow_##__ent_type__##_free(snaprow_t *snap, uint64_t idx) \
        {                                                               \
                int ret;                                                \
                YASSERT(idx < snap->count);                             \
                                                                        \
                ret = bitmap_set_unuse(&snap->bitmap, idx);             \
                if (ret)                                                \
                        GOTO(err_ret, ret);                             \
                                                                        \
                ret = sy_spin_lock(&snap->lock);                        \
                if (ret)                                                \
                        YASSERT(0);                                     \
                snap->count --;                                         \
                ret = sy_spin_unlock(&snap->lock);                      \
                if (ret)                                                \
                        YASSERT(0);                                     \
                                                                        \
                return 0;                                               \
        err_ret:                                                        \
                return ret;                                             \
        }
        

#endif
        

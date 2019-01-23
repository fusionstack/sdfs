

#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "sysutil.h"
#include "array.h"
#include "dbg.h"


int array_resize(array_t *array, int size)
{
        int ret;
        void *ptr;
        array_seg_t *seg;
        uint32_t seg_len_new, i;

        DBUG("resize  old %d , new %d\n", array->seg_len, size);

        sy_rwlock_wrlock(&array->rwlock);

        seg_len_new = size / ARRAY_DEFAUT_LEN + 1;

        for (i = array->seg_len; i < seg_len_new; i++) {
                ret = ymalloc((void **)&ptr, sizeof(array_seg_t)
                              + ARRAY_DEFAUT_LEN * array->entry_size);
                if (ret)
                        GOTO(err_ret, ret);

                seg = ptr;

                _memset(seg->seg, 0x0, ARRAY_DEFAUT_LEN * array->entry_size);

                seg->seq = i;

                list_add_tail(&seg->hook, &array->hook);
        }

        if (seg_len_new > array->seg_len)
                array->seg_len = seg_len_new;

        sy_rwlock_unlock(&array->rwlock);

        return 0;
err_ret:
        sy_rwlock_unlock(&array->rwlock);
        return ret;
}

int array_create(array_t *array, int entry_size, int size)
{
        int ret;

        INIT_LIST_HEAD(&array->hook);
        array->seg_len = 0;
        array->used = 0;
        array->entry_size = entry_size;

        sy_rwlock_init(&array->rwlock);

        ret = array_resize(array, size);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

void *array_idx(array_t *array, uint32_t idx)
{
        int seg_idx;
        struct list_head *pos;
        array_seg_t *seg = 0;

        if (idx >= array->seg_len * ARRAY_DEFAUT_LEN || (idx + 1) > array->used)
                return NULL;

        seg_idx = idx / ARRAY_DEFAUT_LEN;

        list_for_each(pos, &array->hook) {
                seg = (array_seg_t *)pos;
                if (seg->seq == seg_idx)
                        break;
        }

        return &seg->seg[(idx % ARRAY_DEFAUT_LEN) * array->entry_size];
}

void *array_insert(array_t *array, uint32_t idx)
{
        int ret;

        if (idx >= array->seg_len * ARRAY_DEFAUT_LEN) {
                ret = array_resize(array, idx + 1);
                if (ret) {
                        DERROR("resize error\n");
                        return 0;
                }
        }

        array->used = array->used > (idx + 1) ? array->used : (idx + 1);

        return array_idx(array, idx);
}

//mem length = size * array->entry_size
int array_memcpy(array_t *array, void *mem, uint32_t off, uint32_t size)
{
        int ret, seg_idx;
        struct list_head *pos;
        array_seg_t *seg;
        uint32_t seg_off, mem_off, left, n;

        if (off + size >= array->seg_len * ARRAY_DEFAUT_LEN) {
                ret = array_resize(array, off + size + 1);
                if (ret)
                        GOTO(err_ret, ret);
        }

        seg_idx = off / ARRAY_DEFAUT_LEN;
        seg_off = off % ARRAY_DEFAUT_LEN;
        left = size;
        mem_off = 0;

        list_for_each(pos, &array->hook) {
                seg = (array_seg_t *)pos;
                if (seg->seq == seg_idx) {
                        n = left < (ARRAY_DEFAUT_LEN - seg_off)
                                ? left : (ARRAY_DEFAUT_LEN - seg_off);
                        _memcpy(seg->seg + seg_off * array->entry_size,
                               mem + mem_off * array->entry_size,
                               n * array->entry_size);

                        if (n == left)
                                break;

                        seg_off = 0;
                        mem_off += n;
                        left -= n;
                        seg_idx ++;
                }
        }

        array->used = (off + size) > array->used ? (off + size) : array->used;

        return 0;
err_ret:
        return ret;
}

void array_destroy(array_t *array)
{
        struct list_head *head, *pos;

        head = &array->hook;

        while (!list_empty_careful(head)) {

                pos = head->next;

                list_del(pos);

                yfree((void **)&pos);
        }
}

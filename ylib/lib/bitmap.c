

#define DBG_SUBSYS S_LIBYLIB

#include "bitmap.h"
#include "errno.h"
#include "ylib.h"
#include "dbg.h"

#define BITS_BIND 64

typedef struct {
        uint64_t bit;
        sy_spinlock_t lock;
} bits;

int __bit_init(void *arg)
{
        int ret;
        bits *b;

        b = arg;

        b->bit = 0xFFFFFFFF;

        ret = sy_spin_init(&b->lock);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

ARRAY_CREATE(bits, 16, __bit_init);

int bitmap_init(bitmap_t *bm)
{
        int ret;

        memset(bm, 0x0, sizeof(bitmap_t));

        ret = array_bits_init(&bm->array, 0);
        if (ret)
                GOTO(err_ret, ret);

        ret = sy_spin_init(&bm->lock);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int bitmap_set_unuse(bitmap_t *bm, uint64_t idx)
{
        int ret;
        bits *b;

        b = array_bits_idx(&bm->array, idx / BITS_BIND);
        if (b == NULL) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        ret = sy_spin_lock(&b->lock);
        if (ret)
                GOTO(err_ret, ret);

        b->bit = b->bit & 1 << (idx % BITS_BIND);

        ret = sy_spin_unlock(&b->lock);
        if (ret)
                GOTO(err_ret, ret);

        bm->cached = idx / BITS_BIND;

        return 0;
err_ret:
        return ret;
}

int bitmap_set_used(bitmap_t *bm, uint64_t idx)
{
        int ret;
        bits *b;

        b = array_bits_idx(&bm->array, idx / BITS_BIND);
        if (b == NULL) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        ret = sy_spin_lock(&b->lock);
        if (ret)
                GOTO(err_ret, ret);

        b->bit = b->bit ^ 1 << (idx % BITS_BIND);

        ret = sy_spin_unlock(&b->lock);
        if (ret)
                GOTO(err_ret, ret);

        bm->cached = idx / BITS_BIND;

        return 0;
err_ret:
        return ret;
}

int bitmap_get_empty(bitmap_t *bm, uint64_t *idx)
{
        int ret;
        uint32_t cached, _idx, i, j;
        bits *b;

        cached = bm->cached;

        for (i = 0; i < bm->count; i++) {
                _idx = (cached + i) % bm->count;
                b = array_bits_idx(&bm->array, _idx);

                YASSERT(b);

                ret = sy_spin_lock(&b->lock);
                if (ret)
                        GOTO(err_ret, ret);

                if (b->bit) {
                        for (j = 0; j < BITS_BIND; j ++) {
                                if (b->bit & 1 << j) {
                                        *idx = i * BITS_BIND + j;
                                        b->bit = b->bit ^ 1 << j;

                                        if (b->bit)
                                                bm->cached = _idx;

                                        ret = sy_spin_unlock(&b->lock);
                                        if (ret)
                                                GOTO(err_ret, ret);

                                        goto out;
                                }
                        }
                } else {
                        ret = sy_spin_unlock(&b->lock);
                        if (ret)
                                GOTO(err_ret, ret);

                        continue;
                }
                
                ret = sy_spin_unlock(&b->lock);
                if (ret)
                        GOTO(err_ret, ret);
        }

        ret = ENOENT;

err_ret:
        return ret;
out:
        return 0;
}

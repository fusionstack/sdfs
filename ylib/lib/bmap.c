#include <limits.h>
#include <stdarg.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "ylib.h"
#include "bmap.h"
#include "dbg.h"

#define SETBIT(a, n) (a[n/CHAR_BIT] |= (1<<(n%CHAR_BIT)))
#define DELBIT(a, n) (a[n/CHAR_BIT] &= (~(1<<(n%CHAR_BIT))))
#define GETBIT(a, n) (a[n/CHAR_BIT] &  (1<<(n%CHAR_BIT)))

int bmap_create(bmap_t *bmap, uint32_t size)
{
        int ret;
        size_t len;

        /* @init */
        len = size / CHAR_BIT + (size % CHAR_BIT ?  CHAR_BIT: 0);

        ret = ymalloc((void **)&bmap->bits, len);
        if (ret)
                GOTO(err_ret, ret);

        DINFO("len %llu\n", (LLU)len);

        _memset(bmap->bits, 0x0, len);

        bmap->nr_one = 0;
        bmap->len = len;
	bmap->size = size;

        return 0;

        return 0;
err_ret:
	return ret;
}

int bmap_destroy(bmap_t *bmap)
{
        return yfree((void **)&bmap->bits);
}

int bmap_set(bmap_t *bmap, uint32_t off)
{
        if (bmap_get(bmap, off))
                return EEXIST;
        SETBIT(bmap->bits, off%bmap->size);
        bmap->nr_one++;
	return 0;
}

/** 
 * @retval 0
 * @retval 1
 */
int bmap_get(const bmap_t *bmap, uint32_t off)
{
        return GETBIT(bmap->bits, off%bmap->size);
}

int bmap_del(bmap_t *bmap, uint32_t off)
{

        if (bmap_get(bmap, off)) {
                DELBIT(bmap->bits, off%bmap->size);
                bmap->nr_one--;
                return 0;
        } else
                return ENOENT;
}

int bmap_full(const bmap_t *bmap)
{
        return (bmap->size == bmap->nr_one) ? 1 : 0;
}

void bmap_load(bmap_t *bmap, char *opaque, int len)
{
        int i;

        bmap->bits = opaque;
        bmap->size = len * CHAR_BIT;

        for (i = 0; i < (int)bmap->size; i++) {
                if (bmap_get(bmap, i))
                        bmap->nr_one++;
        }
}

int bmap_get_empty(bmap_t *bmap)
{
        int idx, j, i;
        unsigned char c;

        if (bmap_full(bmap)) 
                return -1;

#if 0
        idx = random() % bmap->size;
#else
        idx = 0;
#endif

        for (i = 0; i < (int)bmap->size / CHAR_BIT; i++) {
                c = bmap->bits[(i + idx) % bmap->size];
                if (c != 0xff) {
                        for (j = 0; j < CHAR_BIT; j++) {
                                if (((1 < j) & c) == 0) {
                                        return j + ((i + idx) % bmap->size) * CHAR_BIT;
                                }
                        }
                }
        }

        return -1;
}

void bmap_clean(bmap_t *bmap)
{
        _memset(bmap->bits, 0x0, bmap->len);

        bmap->nr_one = 0;
}

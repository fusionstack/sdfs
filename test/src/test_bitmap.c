#include <stdio.h>
#include <assert.h>

#include "bmap.h"

#define N 10

int main()
{
        int i;
        bmap_t *bmap;

        assert((1<<2) == 4);
        assert((1<<3) == 8);

        bmap_create(bmap, N);

        for (i = 0; i < N; i+=2) {
                bmap_set(bmap, i);
        }

        for (i = 0; i < N; ++i) {
                if (i % 2 == 0)
                        assert(bmap_get(bmap, i));
                else
                        assert(!bmap_get(bmap, i));
        }

        for (i = 0; i < N; ++i)
                bmap_del(bmap, i);

        for (i = 0; i < N; i++) {
                assert(!bmap_get(bmap, i));
        }

        bmap_destroy(bmap);

        return 0;
}

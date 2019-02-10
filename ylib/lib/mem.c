#define JEMALLOC_NO_DEMANGLE

#include <sys/types.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <jemalloc/jemalloc.h>

#define DBG_SUBSYS S_LIBYLIBMEM

#include "sysutil.h"
#include "ylib.h"
#include "mem_pool.h"
#include "mem_cache.h"
#include "hash_table.h"
#include "configure.h"
#include "dbg.h"

#define powerof2(x)     ((((x) - 1) & (x)) == 0)

static void *__malloc__(size_t size)
{
        return je_malloc(size);
}

static void *__calloc__(size_t n, size_t elem_size)
{
        return je_calloc(n, elem_size);
}

static void *__memalign__(size_t alignment, size_t bytes)
{
        int ret;
        void *ptr=NULL;

        ret = je_posix_memalign(&ptr, alignment, bytes);
        if (ret)
                return NULL;

        return ptr;
}

static void __free__(void *mem)
{
        return je_free(mem);
}

int ymalign(void **_ptr, size_t align, size_t size)
{
        int i;
        void *ptr=NULL;

        /* Test whether the SIZE argument is valid.  It must be a power of
        two multiple of sizeof (void *).  */
        if (align % sizeof (void *) != 0
                || !powerof2 (align / sizeof (void *))
                || align == 0)
                return EINVAL;

        for (i = 0; i < 3; i++) {
                ptr = __memalign__(align, size);
                if (ptr != NULL)
                        *_ptr = ptr;
                        return 0;
        }

        return ENOMEM;
}

int ymalloc(void **_ptr, size_t size)
{
        int ret, i;
        void *ptr = NULL;

        YASSERT(size != 0);

        if (unlikely(size == 0)) {
                *_ptr = NULL;
                return 0;
        }

        if (size > 4096)
                DBUG("big mem %u\n", (int)size);

	    if (size < sizeof(struct list_head))
                size = sizeof(struct list_head);
#if 0
        size_t memsize;
        memsize = 2;
        while (memsize < size)
                memsize *= 2;

        DBUG("malloc %llu %llu\n", (LLU)size, (LLU)memsize);

        size = memsize;
#endif

        for (i = 0; i < 3; i++) {
                ptr = __calloc__(1, size);
                if (ptr != NULL)
                        goto out;
        }

        ret = ENOMEM;

#if 0
        fprintf(stderr, "size (%lu) ret (%d) %s\n", (unsigned long)size, ret,
                strerror(ret));
#endif

        goto err_ret;

out:
        //calloc have init mem with 0
        /*memset(ptr, 0, size);*/
        *_ptr = ptr;
        //DBUG("calloc'ed mem %p, size %lu\n", ptr, (unsigned long)size);

        return 0;
err_ret:
        return ret;
}

int ymalloc2(void **_ptr, size_t size)
{
        int i, ret;
        void *ptr = NULL;

        for (i = 0; i < 3; i++) {
                ptr = __malloc__(size);
                if (ptr != NULL)
                        goto out;
        }

        ret = ENOMEM;
        return ret;

out:
        *_ptr = ptr;
        return 0;
}

inline int yrealloc(void **_ptr, size_t size, size_t newsize)
{
        int ret, i;
        void *ptr;

        if (*_ptr == NULL && size == 0) /*malloc*/ {
                ret = ymalloc(&ptr, newsize);
                if (ret)
                        GOTO(err_ret, ret);

                _memset(ptr, 0x0, newsize);

                *_ptr = ptr;
                return 0;
        }

        if (newsize == 0)
                return yfree(_ptr);

        if (newsize < size) {
                ptr = *_ptr;
                _memset(ptr + newsize, 0x0, size - newsize);
        }

        if (newsize < sizeof(struct list_head))
                newsize = sizeof(struct list_head);

        ret = ENOMEM;
        for (i = 0; i < 3; i++) {
                ptr = realloc(*_ptr, newsize);
                if (ptr != NULL)
                        goto out;
        }
        GOTO(err_ret, ret);
out:
        if (newsize > size)
                _memset(ptr + size, 0x0, newsize - size);

        *_ptr = ptr;

        return 0;
err_ret:
        return ret;
}

int yfree(void **ptr)
{
        if (*ptr != NULL) {
                __free__(*ptr);
        } else {
                YASSERT(0);
        }

        *ptr = NULL;

        return 0;
}


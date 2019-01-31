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

int mpool_init(mpool_t *mpool, size_t align, size_t size, int max)
{
        int ret;

        YASSERT(mpool->inited == 0);
        ret = sy_spin_init(&mpool->lock);
        if (ret)
                GOTO(err_ret, ret);

        INIT_LIST_HEAD(&mpool->list);
        mpool->len = 0;
        mpool->max = max;
        mpool->inited = 1;
        mpool->size = size;
        mpool->align = align;

        return 0;
err_ret:
        return ret;
}

int mpool_get(mpool_t *mpool, void **ptr)
{
        int ret;

        YASSERT(mpool->inited);

        if (mpool->align) {
                ret = ymalign(ptr, mpool->align, mpool->size);
                if (ret )
                        GOTO(err_ret, ret);
        } else {
                ret = ymalloc(ptr, mpool->size);
                if (ret )
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int mpool_put(mpool_t *mpool, void *ptr)
{
        (void)(*mpool);
        yfree((void **)&ptr);
        return 0;
}

void *mem_cache_alloc(mem_cache_type_t type, uint8_t flag)
{
        int ret;
        void *ptr = NULL;

        (void)flag;

        switch (type) {
                case MEM_CACHE_64:
                        ret = ymalloc((void **)&ptr, MEM_CACHE_SIZE64);
                        break;
                case MEM_CACHE_128:
                        ret = ymalloc((void **)&ptr, MEM_CACHE_SIZE128);
                        break;
                case MEM_CACHE_4K:
                        ret = ymalloc((void **)&ptr, MEM_CACHE_SIZE4K);
                        break;
                case MEM_CACHE_8K:
                        ret = ymalloc((void **)&ptr, MEM_CACHE_SIZE8K);
                        break;
                default:
                        YASSERT(0);
        }

        if (ret)
                GOTO(err_ret, ret);
                
        return ptr;
err_ret:
        return NULL;
}

void *mem_cache_calloc(mem_cache_type_t type, uint8_t flag)
{
        return mem_cache_alloc(type, flag);
}

int mem_cache_free(mem_cache_type_t type, void *ptr)
{
        (void)type;
        return yfree((void **)&ptr);
}

void *mem_cache_calloc1(mem_cache_type_t type, int size)
{
        (void) size;
        //YASSERT(size <= mem_cache_size(type));
        return mem_cache_calloc(type, 1);
}

int mem_cache_private_init()
{
        //not support
        YASSERT(0);
        return 0;
}

int mem_cache_private_destroy()
{
        //not support
        YASSERT(0);
        return 0;
}

#if 0
int mem_hugepage_private_init()
{
        //not support
        YASSERT(0);
        return 0;
}
#endif

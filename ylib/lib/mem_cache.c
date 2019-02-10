#include <time.h>
#include <stdlib.h>
#include <errno.h>

#define DBG_SUBSYS      S_LIBYLIB

#include "mem_cache.h"
#include "ylib.h"
#include "variable.h"
#include "dbg.h"

#if ENABLE_MEM_CACHE 

#define MEM_CACHE_POOL_ID_UNUSED        -1
#define MEM_CACHE_MAGIC  0x9dac358a

#if 0
#define ASYNC_FREE
#endif

/*
 * Attach a `mem_cache_info' to tail of each structure, used for quick
 * alloc and free.
 */
struct mem_cache_info {
        int64_t pool_id;
        uint32_t magic;
        uint32_t thread;
        mem_cache_type_t type;
};

static mem_cache_t **__mem_cache__ = NULL;
static mem_cache_t ***__mem_cache_array__ = NULL;
static sy_spinlock_t __mem_cache_array_lock__;

static void __mem_cache_free_async__(mem_cache_t *cachep);

static void *__mem_cache_private()
{
        return variable_get(VARIABLE_MEMCACHE);
}


#define mem_cache_info(cachep, obj) \
        ((struct mem_cache_info *)((obj) + (cachep)->unit_size))

/*
 * mem_cache_create -
 *
 * @name: the name of this memory pool
 * @size: size of unit
 * @base_nr: basic number of unit
 * @align: is the start of memory alloced need to align with page size
 *
 * @return the address of memory on success, otherwise NULL is returned.
 */

int mem_cache_inited()
{
        return (uint64_t )__mem_cache__;
}

mem_cache_t *mem_cache_create(char *name, uint32_t size, uint32_t base_nr,
                              uint8_t align, int thread, int type, int private)
{
        int ret, i = 0;
        void *obj;
        mem_cache_t *cachep;
        struct mem_cache_info *info;

        YASSERT(size >= sizeof(struct list_head));

        if (unlikely(!size || !base_nr)) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        cachep = calloc(1, sizeof(*cachep));
        if (unlikely(!cachep)) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        cachep->name = strdup(name);
        cachep->base_nr = base_nr;
        cachep->max_nr = base_nr;
        cachep->idx = 0;
        cachep->thread = thread;
        cachep->align = align;
        cachep->unit_size = size;
        cachep->type = type;
        cachep->real_size = size + sizeof(struct mem_cache_info);
        cachep->_private = private;

        cachep->pool = calloc(cachep->max_nr, sizeof(void *));
        if (unlikely(!cachep->pool)) {
                ret = ENOMEM;
                GOTO(cachep_free, ret);
        }

        for (i = 0; i < (int)cachep->max_nr; ++i) {
                /*
                 * NOTE ALIGN: if the @align is not zero, we need the alloced memory
                 * address be nultiple of the page size, this is usefull when use the
                 * O_DIRECT flag to open a file.
                 */
                obj = cachep->align ? valloc(cachep->real_size) : malloc(cachep->real_size);
                if (unlikely(!obj)) {
                        ret = ENOMEM;
                        GOTO(pool_free, ret);
                }

                if (align) {
                        YASSERT((LLU)obj % PAGE_SIZE == 0);
                }

                cachep->pool[i] = obj;

                info = mem_cache_info(cachep, obj);
                info->pool_id = MEM_CACHE_POOL_ID_UNUSED;
                info->magic = MEM_CACHE_MAGIC;
                info->type = type;
        }

        ret = pthread_spin_init(&cachep->lock, PTHREAD_PROCESS_PRIVATE);
        if (unlikely(ret))
                GOTO(pool_free, ret);

        ret = pthread_spin_init(&cachep->cross_lock, PTHREAD_PROCESS_PRIVATE);
        if (unlikely(ret))
                GOTO(pool_free, ret);

        INIT_LIST_HEAD(&cachep->cross_list);
        cachep->cross_free = 0;

        return cachep;
pool_free:
        for (--i; i >= 0; --i) {
                free(cachep->pool[i]);
        }
        free(cachep->pool);
cachep_free:
        free(cachep->name);
        free(cachep);
err_ret:
        return NULL;
}

/*
 * NOTE: Caller must hold the lock of cache.
 */
static int __mem_cache_expand(mem_cache_t *cachep, uint8_t flag)
{
        int ret;
        uint32_t new_nr, old_nr, i = 0;
        void **new, *obj;
        struct mem_cache_info *info;

        old_nr = cachep->max_nr;
        new_nr = old_nr * 2;

        DINFO("expand mem cache %s(%p) from %u to %u\n",
             cachep->name, cachep, old_nr, new_nr);

        while (!(new = calloc(new_nr, sizeof(void *)))) {
                if (flag & __MC_FLAG_NOFAIL__) {
                        sleep(1);
                        DWARN("no memory, wait success !!!\n");
                        continue;
                }
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        for (i = old_nr; i < new_nr; ++i) {
                while (1) {
                        /*
                         * NOTE ALIGN: if the @align is not zero, we need the alloced memory
                         * address be nultiple of the page size, this is usefull when use the
                         * O_DIRECT flag to open a file.
                         */
                        obj = cachep->align ? valloc(cachep->real_size) : malloc(cachep->real_size);
                        if (unlikely(!obj)) {
                                if (flag & __MC_FLAG_NOFAIL__) {
                                        sleep(1);
                                        DWARN("no memory, wait success !!!\n");
                                        continue;
                                }
                                ret = ENOMEM;
                                GOTO(pool_free, ret);
                        }

                        break;
                }

                new[i] = obj;

                info = mem_cache_info(cachep, obj);
                info->pool_id = MEM_CACHE_POOL_ID_UNUSED;
                info->magic = MEM_CACHE_MAGIC;
                info->type = cachep->type;
        }

        memcpy(new, cachep->pool, old_nr * sizeof(void *));
        free(cachep->pool);

        cachep->pool = new;
        cachep->max_nr = new_nr;
        cachep->last_expand = gettime();

        return 0;
pool_free:
        for (--i; i >= old_nr;  --i) {
                free(new[i]);
        }
        free(new);
err_ret:
        return ret;
}

/*
 * NOTE: Caller must hold the lock of cache.
 */
static int __mem_cache_shrink(mem_cache_t *cachep)
{
        int ret;
        uint32_t new_nr, old_nr, i = 0;
        void **new;

        if (gettime() - cachep->last_expand < 60) {
                return 0;
        }

        old_nr = cachep->max_nr;
        new_nr = old_nr / 2;

        DINFO("shrink mem cache %s(%p) from %u to %u\n",
              cachep->name, cachep, old_nr, new_nr);

        new = calloc(new_nr, sizeof(void *));
        if (unlikely(!new)) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        for (i = new_nr; i < old_nr; ++i)
                free(cachep->pool[i]);

        memcpy(new, cachep->pool, new_nr * sizeof(void *));
        free(cachep->pool);

        cachep->pool = new;
        cachep->max_nr = new_nr;

        return 0;
err_ret:
        return ret;
}

static int __mem_cache_lock(mem_cache_t *cachep)
{
        if (likely(!cachep->_private))
                return pthread_spin_lock(&cachep->lock);
        else {
                return 0;
        }
}

static int __mem_cache_unlock(mem_cache_t *cachep)
{
        if (likely(!cachep->_private))
                return pthread_spin_unlock(&cachep->lock);
        else
                return 0;
}

void *mem_cache_alloc(mem_cache_t *cachep, uint8_t flag)
{
        int ret;
        void *obj;
        struct mem_cache_info *info;

        __mem_cache_free_async__(cachep);

        __mem_cache_lock(cachep);

        if (unlikely(cachep->idx == cachep->max_nr)) {
                ret = __mem_cache_expand(cachep, flag);
                if (unlikely(ret))
                        GOTO(err_unlock, ret);
        }

        obj = cachep->pool[cachep->idx];
        info = mem_cache_info(cachep, obj);
        info->pool_id = cachep->idx;
        info->thread = cachep->thread;
        ++cachep->idx;

        __mem_cache_unlock(cachep);
        return obj;
err_unlock:
        __mem_cache_unlock(cachep);
        return NULL;
}

void *mem_cache_calloc(mem_cache_type_t type, uint8_t flag)
{
        mem_cache_t **__mem_cache_private__ = __mem_cache_private();
        mem_cache_t *cachep = NULL;

        if (likely(__mem_cache_private__)) {
                cachep = __mem_cache_private__[type];
        } else {
                cachep = __mem_cache__[type];
        }

        void *obj = mem_cache_alloc(cachep, flag);
        /*if (obj) {
                //memset(obj, 0x00, cachep->unit_size);
        }*/
        return obj;
}

static void __mem_cache_free__(mem_cache_t *cachep, struct mem_cache_info *del_info, void *del_obj)
{
        uint32_t del_idx, last_idx;
        void *last_obj;
        struct mem_cache_info *last_info;

        __mem_cache_lock(cachep);

        del_idx = del_info->pool_id;

        if (del_idx != cachep->idx - 1) {
                YASSERT(cachep->idx > 0);
                last_obj = cachep->pool[cachep->idx - 1];
                last_info = mem_cache_info(cachep, last_obj);
                last_idx = last_info->pool_id;

                cachep->pool[del_idx] = last_obj;
                last_info->pool_id = del_idx;
                cachep->pool[last_idx] = del_obj;
        }

        del_info->pool_id = MEM_CACHE_POOL_ID_UNUSED;

        --cachep->idx;

        if (unlikely(cachep->max_nr > cachep->base_nr &&
            cachep->idx < cachep->max_nr / 8))
                (void) __mem_cache_shrink(cachep);

        __mem_cache_unlock(cachep);
}

static void __mem_cache_queue_async__(mem_cache_t *cachep, void *del_obj)
{
        struct list_head *list;

        pthread_spin_lock(&cachep->cross_lock);

        list = del_obj;
        list_add_tail(list, &cachep->cross_list);
        cachep->cross_free = 1;

        pthread_spin_unlock(&cachep->cross_lock);
}

static void __mem_cache_free_async__(mem_cache_t *cachep)
{
        struct mem_cache_info *del_info;
        struct list_head *pos, *n;
        void *del_obj;

        if (unlikely(cachep->cross_free)) {
                pthread_spin_lock(&cachep->cross_lock);

                list_for_each_safe(pos, n, &cachep->cross_list) {
                        list_del(pos);
                        del_obj = pos;

                        del_info = mem_cache_info(cachep, del_obj);
                        __mem_cache_free__(cachep, del_info, del_obj);
                }

                cachep->cross_free = 0;
                pthread_spin_unlock(&cachep->cross_lock);
        }
}

void *mem_cache_calloc1(mem_cache_type_t type, int size)
{
        YASSERT(size <= mem_cache_size(type));
        return mem_cache_calloc(type, 1);
}

void IO_FUNC mem_cache_free(mem_cache_type_t type, void *del_obj)
{
        mem_cache_t **__mem_cache_private__ = __mem_cache_private();
        int async_free = 1;
        struct mem_cache_info *del_info;
        mem_cache_t *cachep = NULL;

        if (likely(__mem_cache_private__)) {
                cachep = __mem_cache_private__[type];
        } else {
                cachep = __mem_cache__[type];
        }

        del_info = mem_cache_info(cachep, del_obj);
        if (unlikely(del_info->thread != cachep->thread)) {
                DBUG("cross thread free %s, %u --> %u\n", cachep->name,
                     cachep->thread, del_info->thread);
                cachep = __mem_cache_array__[del_info->thread][type];

                YASSERT(del_info->magic == MEM_CACHE_MAGIC);
                YASSERT(del_info->type == type);

        }

#ifdef ASYNC_FREE
        if (!cachep->private) {
                async_free = 0;
        } else if (__mem_cache_private__
                   && __mem_cache_private__[type]->thread == del_info->thread) {
                async_free = 0;
        }
#else
        async_free = 0;
#endif


        YASSERT(del_info->magic == MEM_CACHE_MAGIC);
        YASSERT(del_info->type == type);


        if (unlikely(async_free))
                __mem_cache_queue_async__(cachep, del_obj);
        else
                __mem_cache_free__(cachep, del_info, del_obj);
}

void mem_cache_destroy(mem_cache_t *cachep)
{
        uint32_t i;

        pthread_spin_destroy(&cachep->lock);

        for (i = 0; i < cachep->max_nr; ++i)
                free(cachep->pool[i]);

        free(cachep->name);
        free(cachep->pool);
        free(cachep);
}

static mem_cache_t ** __mem_cache_init(int thread, int private)
{
        int ret, i;
        mem_cache_t **mem_cache;
        static struct mem_cache_param {
                char *name;
                uint32_t unit_size;
                uint32_t base_nr;
                uint16_t align;
        } mem_cache_params[MEM_CACHE_NR] = {
                /*    name       |          size          | base_nr | align */
                { "mem_cache_64", sizeof(mem_cache64_t),   1024, 0, },
                { "mem_cache_128", sizeof(mem_cache128_t),   1024, 0, },
                { "mem_cache_4k", MEM_CACHE_SIZE4K,   1024, 1, },
                { "mem_cache_8k", MEM_CACHE_SIZE8K,   128, 1, },
        };

        ret = ymalloc((void**)&mem_cache, sizeof(**mem_cache) * MEM_CACHE_NR);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (i = 0; i < MEM_CACHE_NR; ++i) {
                mem_cache[i] =
                        mem_cache_create(mem_cache_params[i].name,
                                         mem_cache_params[i].unit_size,
                                         mem_cache_params[i].base_nr,
                                         mem_cache_params[i].align,
                                         thread,
                                         i,
                                         private);
                if (unlikely(!mem_cache[i])) {
                        ret = ENOMEM;
                        GOTO(err_free, ret);
                }
        }

        return mem_cache;
err_free:
        for (--i; i >= 0; --i)
                mem_cache_destroy(mem_cache[i]);
err_ret:
        return NULL;
}

int mem_cache_init()
{
        int ret;

        ret = ymalloc((void**)&__mem_cache_array__, sizeof(mem_cache_t ***) * LICH_VM_MAX);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        memset(__mem_cache_array__, 0x0, sizeof(mem_cache_t ***) * LICH_VM_MAX);

        ret = sy_spin_init(&__mem_cache_array_lock__);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        __mem_cache__ = __mem_cache_init(LICH_VM_MAX - 1, 0);
        if (unlikely(!__mem_cache__)) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        __mem_cache_array__[LICH_VM_MAX - 1] = __mem_cache__;

        YASSERT(__mem_cache__[0]->_private == 0);

        return 0;
err_ret:
        return ret;
}

int mem_cache_private_init()
{
        int ret, i;
        void *__pad__ = (void *)0x1;
        mem_cache_t **__mem_cache_private__ = __mem_cache_private();

        YASSERT(!__mem_cache_private__);

        ret = sy_spin_lock(&__mem_cache_array_lock__);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (i = 0; i < LICH_VM_MAX; i++) {
                if (__mem_cache_array__[i] == NULL)
                        break;
        }

        if (i == LICH_VM_MAX) {
                ret = ENOMEM;
                GOTO(err_lock, ret);
        }

        __mem_cache_array__[i] = __pad__;

        sy_spin_unlock(&__mem_cache_array_lock__);

#ifdef ASYNC_FREE
        variable_set(VARIABLE_MEMCACHE, __mem_cache_init(i, 1));
#else
        variable_set(VARIABLE_MEMCACHE, __mem_cache_init(i, 0));
#endif

        __mem_cache_private__ = __mem_cache_private();
        if (!__mem_cache_private__) {
                ret = ENOMEM;
                UNIMPLEMENTED(__DUMP__);
        }

        __mem_cache_array__[i] = __mem_cache_private__;

        return 0;
err_lock:
        sy_spin_unlock(&__mem_cache_array_lock__);
err_ret:
        return ret;
}

int mem_cache_private_destroy()
{
        int ret, i;

        mem_cache_t **__mem_cache_private__ = __mem_cache_private();
        YASSERT(__mem_cache_private__);

        ret = sy_spin_lock(&__mem_cache_array_lock__);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (i = 0; i < LICH_VM_MAX; i++) {
                if (__mem_cache_array__[i] == __mem_cache_private__) {
                        __mem_cache_array__[i] = NULL;
                        break;
                }
        }

        YASSERT(i < LICH_VM_MAX);

        sy_spin_unlock(&__mem_cache_array_lock__);

        for (i = 0; i < MEM_CACHE_NR; ++i) {
                mem_cache_destroy(__mem_cache_private__[i]);
        }

        yfree((void**)&__mem_cache_private__);

        DINFO("mem cache destroy\n");

        return 0;
err_ret:
        return ret;
}

#else

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

void mem_cache_free(mem_cache_type_t type, void *ptr)
{
        (void)type;
        yfree((void **)&ptr);
}

void *mem_cache_calloc1(mem_cache_type_t type, int size)
{
        (void) size;
        //YASSERT(size <= mem_cache_size(type));
        return mem_cache_calloc(type, 1);
}

int mem_cache_private_init()
{
        return 0;
}

int mem_cache_private_destroy()
{
        return 0;
}

#endif

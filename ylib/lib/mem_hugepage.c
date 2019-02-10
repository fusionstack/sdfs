#include <time.h>
#include <string.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define DBG_SUBSYS S_LIBYLIB

#include "configure.h"
#include "mem_hugepage.h"
#include "ylock.h"
#include "ylib.h"
#include "variable.h"
#include "dbg.h"

typedef struct {
        int idx;
        int ref;
        void *addr;
        uint32_t size;
        uint32_t offset;
} entry_t;

typedef struct {
        int cur;
        sy_spinlock_t lock;
        entry_t array[0];
} mem_hugepage_t;

extern int use_memcache;

static mem_hugepage_t *__mem_hugepage__ = NULL;

mem_hugepage_t *mem_self()
{
        return variable_get(VARIABLE_HUGEPAGE);
}

static int __mem_hugepage_init(mem_hugepage_t **_mem)
{
        int ret, i;
        entry_t *ent;
        mem_hugepage_t *mem;
        void *ptr = NULL;

        if (use_memcache == 0) {
                DINFO("disable memcache\n");
                return 0;
        }
        
        YASSERT(gloconf.memcache_count < INT16_MAX);

        ret = ymalloc((void **)&mem, sizeof(mem_hugepage_t)
                      + sizeof(entry_t) * gloconf.memcache_count);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (i = 0; i < gloconf.memcache_count; i++) {
                ent = &mem->array[i];

                ret = posix_memalign((void **)&ptr, 4096, gloconf.memcache_seg);
                if (ret < 0) {
                        ret = errno;
                        UNIMPLEMENTED(__DUMP__);
                }

                ent->ref = 0;
                ent->idx = i;
                ent->addr = ptr;
                ent->size = gloconf.memcache_seg;
                //YASSERT((uint64_t)ptr > 1000);
        }

        ret = sy_spin_init(&mem->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        mem->cur = 0;
        *_mem = mem;

        static int __enter__ = 0;
        DINFO("enter %d memcache_seg %d memcache_count %d\n",
              __enter__, gloconf.memcache_seg, gloconf.memcache_count);
        __enter__++;
	return 0;
err_ret:
        return ret;
}

static int __mem_hugepage_ref(mem_hugepage_t *mem, int idx)
{
        int ret;
        entry_t *ent;

        ret = sy_spin_lock(&mem->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ent = &mem->array[idx];
        YASSERT(ent->ref > 0);

        ent->ref++;

        sy_spin_unlock(&mem->lock);

        return 0;
err_ret:
        return ret;
}

static int __mem_hugepage_deref(mem_hugepage_t *mem, int idx)
{
        int ret;
        entry_t *ent;

        ret = sy_spin_lock(&mem->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ent = &mem->array[idx];

        YASSERT(ent->ref > 0);

        ent->ref --;
        if (ent->ref == 0) {
                ent->offset = 0;
        }

        YASSERT(ent->ref >= 0);

        sy_spin_unlock(&mem->lock);

        return 0;
err_ret:
        return ret;
}

static int __mem_hugepage_new__(int32_t *idx, void **ptr, uint32_t _size, entry_t *ent)
{
        int ret, size;

        if (_size % PAGE_SIZE)
                size = (_size / PAGE_SIZE + 1) * PAGE_SIZE;
        else
                size = _size;

        if (ent->size == ent->offset) {
                DBUG("fd %d size %d offset %d\n", ent->idx, ent->size, ent->offset);
                ret = ENOMEM;
                goto err_ret;
        }

        if (ent->offset + size > ent->size) {
                DBUG("fd %d size %d offset %d size %u\n", ent->idx, ent->size, ent->offset, size);
                ent->offset = ent->size;
                ret = ENOMEM;
                goto err_ret;
        }

        YASSERT(ent->addr);
        *idx = ent->idx;
        *ptr = ent->addr + ent->offset;
        ent->offset += size;
        ent->ref ++;

        DBUG("fd %d size %d offset %d size %u\n", ent->idx, ent->size, ent->offset, size);

        return 0;
err_ret:
        return ret;
}

static int __mem_hugepage_next(mem_hugepage_t *mem)
{
        int ret, cur, i;
        entry_t *ent;

        cur = mem->cur;
        for (i = 0; i < gloconf.memcache_count; i++) {
                ent = &mem->array[(cur + i) % gloconf.memcache_count];
                if (ent->ref == 0) {
                        DBUG("current %u next %u\n", cur, (cur + i) % gloconf.memcache_count);
                        mem->cur = (cur + i) % gloconf.memcache_count;
                        break;
                }
        }

        if (i == gloconf.memcache_count) {
                ret = EBUSY;
                DERROR("ret %d\n", ret);
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __mem_hugepage_new(mem_hugepage_t *mem, uint32_t size, mem_handler_t *mem_handler)
{
        int ret, idx;
        entry_t *ent;
        void *ptr;

        DBUG("new %u\n", size);

        ret = sy_spin_lock(&mem->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        while (1) {
                ent = &mem->array[mem->cur];
                ret = __mem_hugepage_new__(&idx, &ptr, size, ent);
                if (unlikely(ret)) {
                        ret = __mem_hugepage_next(mem);
                        if (unlikely(ret)) {
                                GOTO(err_lock, ret);
                        }

                        continue;
                }

                DBUG("cur %u ptr %p\n", mem->cur, ptr);
                
                YASSERT(ptr);
                
                break;
        }

        sy_spin_unlock(&mem->lock);

        mem_handler->idx = idx;
        mem_handler->pool = mem;
        mem_handler->ptr = ptr;

        return 0;
err_lock:
        sy_spin_unlock(&mem->lock);
        DWARN("no memory, count %u, size %u\n", gloconf.memcache_count, gloconf.memcache_seg);
        //UNIMPLEMENTED(__DUMP__);
        EXIT(EAGAIN);
err_ret:
        return ret;
}

int mem_hugepage_new(uint32_t size, mem_handler_t *mem_handler)
{
        mem_hugepage_t *__mem_hugepage_private__ = mem_self();
        mem_hugepage_t *mem = __mem_hugepage_private__
                ? __mem_hugepage_private__ : __mem_hugepage__;
        return __mem_hugepage_new(mem, size, mem_handler);
}

int mem_hugepage_ref(mem_handler_t *mem_handler)
{
        mem_hugepage_t *mem = mem_handler->pool;

        return __mem_hugepage_ref(mem, mem_handler->idx);
}

int mem_hugepage_deref(mem_handler_t *mem_handler)
{
        mem_hugepage_t *mem = mem_handler->pool;

        return __mem_hugepage_deref(mem, mem_handler->idx);

}

int mem_hugepage_init()
{
        if (use_memcache == 0) {
                DINFO("disable memcache\n");
                return 0;
        }
        
        YASSERT(__mem_hugepage__ == NULL);
        return __mem_hugepage_init(&__mem_hugepage__);
}

int mem_hugepage_private_init()
{
        int ret;
        mem_hugepage_t *__mem_hugepage_private__ = mem_self();

        if (use_memcache == 0) {
                DINFO("disable memcache\n");
                return 0;
        }
        
        YASSERT(__mem_hugepage_private__ == NULL);
        ret = __mem_hugepage_init(&__mem_hugepage_private__);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        variable_set(VARIABLE_HUGEPAGE, __mem_hugepage_private__);

        return 0;
err_ret:
        return ret;
}

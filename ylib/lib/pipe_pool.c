

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "ylib.h"
#include "ylock.h"
#include "dbg.h"

typedef struct {
        int pipe[2];
        sy_spinlock_t used;
} ppool_ent_t;

typedef struct {
        int size; /*max size of the array*/
        int used; /*current used in the array*/
        uint64_t hit;
        uint64_t unhit;
        sy_spinlock_t lock;
        int idx;
        ppool_ent_t array[0];
} ppool_t;

static ppool_t *__ppool;

int ppool_init(int size)
{
        int ret, i;
        uint32_t len;
        void *ptr;

        len = sizeof(ppool_ent_t) * size + sizeof(ppool_t);

        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_ret, ret);

        __ppool = ptr;

        __ppool->size = size;
        __ppool->idx = 0;
        __ppool->hit = 0;
        __ppool->unhit = 0;

        (void) sy_spin_init(&__ppool->lock);

        for (i = 0; i < size; i++) {
                (void) sy_spin_init(&__ppool->array[i].used);

                ret = pipe(__ppool->array[i].pipe);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                ret = fcntl(__ppool->array[i].pipe[1], F_SETFL, O_NONBLOCK);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

void ppool_status()
{
        DINFO("pipe pool used %u left %u hit %llu unhit %llu\n",
              __ppool->used, __ppool->size - __ppool->used,
              (LLU)__ppool->hit, (LLU)__ppool->unhit);
}

int ppool_get()
{
        int ret, curno, no;

        if (__ppool->used == __ppool->size) {
                __ppool->unhit++;
                return -ENOENT;
        }
        
        __ppool->idx = (__ppool->idx + 1) % __ppool->size;

        curno = __ppool->idx;
        no = curno;
        while (srv_running) {
                ret = sy_spin_trylock(&__ppool->array[no].used);
                if (ret != 0) {
                        no = (no + 1) % __ppool->size;
                        if (no == curno)  /* not found */
                                break;

                        continue;
                } else  /* free table */
                        goto found;
        }

        DWARN("pipe used up\n");

        __ppool->unhit++;

        return -ENOENT;
found:
        sy_spin_lock(&__ppool->lock);
        __ppool->used++;
        __ppool->hit++;
        sy_spin_unlock(&__ppool->lock);

#if 0
        close(__ppool->array[no].pipe[0]);
        close(__ppool->array[no].pipe[1]);

        pipe(__ppool->array[no].pipe);
#endif

        return no + 1;
}

int ppool_put(int *_no, int len)
{
        int ret, no;

#if 0
        YASSERT(*_no > 0);
#else
        if (unlikely(*_no <= 0)) {
                DERROR("no %u\n", *_no);

                ret = EINVAL;

                GOTO(err_ret, ret);
        }
#endif

        no = *_no - 1;

        DBUG("put pipe %d\n", no);

        ret = sy_spin_trylock(&__ppool->array[no].used);
        if (unlikely(ret == 0)) {
                (void) sy_spin_unlock(&__ppool->array[no].used);

                ret = EINVAL;
                DERROR("table %d un-alloc'ed\n", no);

                GOTO(err_ret, ret);
        }

        sy_spin_lock(&__ppool->lock);
        __ppool->used--;
        sy_spin_unlock(&__ppool->lock);

        if (unlikely(len)) {
                DWARN("non empty pipe %u\n", len);
                close(__ppool->array[no].pipe[0]);
                close(__ppool->array[no].pipe[1]);

                ret = pipe(__ppool->array[no].pipe);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                ret = fcntl(__ppool->array[no].pipe[1], F_SETFL, O_NONBLOCK);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        ret = sy_spin_unlock(&__ppool->array[no].used);
        if (unlikely(ret))
                return ret;

        *_no = 0;

        return 0;
err_ret:
        return ret;
}

inline int ppool(int idx, int io)
{
        YASSERT(io == 0 || io == 1);
        YASSERT(idx <= __ppool->size);

        return __ppool->array[idx - 1].pipe[io];
}


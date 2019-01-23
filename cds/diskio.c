#include <sys/types.h>
#include <sys/stat.h>
#include <sys/poll.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <ctype.h>
#include <fcntl.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSCDS

#include "yfs_conf.h"
#include "yfscds_conf.h"
#include "chk_meta.h"
#include "md_proto.h"
#include "ylib.h"
#include "ynet_rpc.h"
#include "yfs_chunk.h"
#include "sdfs_lib.h"
#include "cd_proto.h"
#include "job_dock.h"
#include "job_tracker.h"
#include "md_lib.h"
#include "diskio.h"
#include "../yfs/cds/disk.h"
#include "net_global.h"
#include "dbg.h"
#include "adt.h"

typedef struct {
        sem_t sem;
        io_context_t  ctx;
        sy_spinlock_t lock;
        struct list_head list;
} diskio_mt_t;

typedef struct {
        struct list_head hook;
        struct iocb *iocb;
        func1_t func;
} iocb_mt_t;

static diskio_mt_t  *diskio;

static int __diskio_submit()
{
        int ret, retval;
        iocb_mt_t *iocb_mt;
        struct iocb *iocb;

        ret = sy_spin_lock(&diskio->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        YASSERT(!list_empty(&diskio->list));
        iocb_mt = (void *)diskio->list.next;
        list_del(&iocb_mt->hook);

        sy_spin_unlock(&diskio->lock);

        ANALYSIS_BEGIN(0);

        iocb = iocb_mt->iocb;
        
        if (iocb->aio_lio_opcode == IO_CMD_PWRITEV) {
                ret = pwritev(iocb->aio_fildes, iocb->u.c.buf, iocb->u.c.nbytes, iocb->u.c.offset);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        } else {
                YASSERT(iocb->aio_lio_opcode == IO_CMD_PREADV);
                ret = preadv(iocb->aio_fildes, iocb->u.c.buf, iocb->u.c.nbytes, iocb->u.c.offset);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        retval = ret;
        iocb_mt->func(iocb_mt->iocb, &retval);
        
        return 0;
err_ret:
        retval = -ret;
        iocb_mt->func(iocb_mt->iocb, &retval);
        return ret;
}

static void *__submit_mt_worker(void *_args)
{
        int ret;

        (void) _args;

        while (1) {
                ret = _sem_wait(&diskio->sem);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ret = __diskio_submit();
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return NULL;
err_ret:
        UNIMPLEMENTED(__DUMP__);
        return NULL;
}

static int __diskio_queue(iocb_mt_t *iocb)
{
        int ret;

        ret = sy_spin_lock(&diskio->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        list_add_tail(&iocb->hook, &diskio->list);

        sy_spin_unlock(&diskio->lock);

        sem_post(&diskio->sem);

        return 0;
err_ret:
        return ret;
}

int diskio_submit(struct iocb *_iocb, func1_t func)
{
        int ret;
        iocb_mt_t *iocb;

        ret = ymalloc((void **)&iocb, sizeof(iocb_mt_t));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        iocb->iocb = _iocb;
        iocb->func = func;

        ret = __diskio_queue(iocb);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        

        return 0;
err_ret:
        return ret;
}

int diskio_init()
{
        int ret, size, i, thread;
        pthread_t th;
        pthread_attr_t ta;

        size = sizeof(diskio_mt_t);
        ret = ymalloc((void **)&diskio, size);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        memset(diskio, 0x0, size);

        INIT_LIST_HEAD(&diskio->list);

        ret = io_setup(MAX_SUBMIT, &diskio->ctx);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        ret = sy_spin_init(&diskio->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sem_init(&diskio->sem, 0, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

#if 0
        if (cds_info.tier == TIER_HDD) {
                thread = gloconf.diskio_hdd;
        } else {
                thread = gloconf.diskio_ssd;
        }

        DINFO("init tier %d diskio thread %d\n", cds_info.tier, thread);
#else
        thread = 16;
#endif

        for (i = 0; i < thread; i++) {
                (void) pthread_attr_init(&ta);
                (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

                ret = pthread_create(&th, &ta, __submit_mt_worker, NULL);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

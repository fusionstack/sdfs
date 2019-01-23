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

#include "shm.h"
#include "yfs_conf.h"
#include "yfscds_conf.h"
#include "chk_meta.h"
#include "cds.h"
#include "cds_volume.h"
#include "disk.h"
#include "md_proto.h"
#include "ylib.h"
#include "ynet_rpc.h"
#include "yfs_chunk.h"
#include "sdfs_lib.h"
#include "cd_proto.h"
#include "job_dock.h"
#include "job_tracker.h"
#include "djnl_master.h"
#include "md_lib.h"
#include "bh.h"
#include "dpool.h"
#include "cds_hb.h"
#include "net_global.h"
#include "chkinfo.h"
#include "round_journal.h"
#include "dbg.h"
//#include "leveldb_util.h"
//#include "cds_leveldb.h"
#include "adt.h"

typedef struct {
        sem_t sem;
        io_context_t  ctx;
        sy_spinlock_t lock;
        struct list_head list;
} disk_mt_t;

typedef struct {
        struct list_head hook;
        int op;
        int fd;
        struct iovec *iov;
        int iov_count;
        off_t offset;
        int idx;
        job_t *job;
} iocb_mt_t;

static disk_mt_t  *disk_mt;

static int __disk_mt_submit()
{
        int ret;
        iocb_mt_t *iocb;

        ret = sy_spin_lock(&disk_mt->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        YASSERT(!list_empty(&disk_mt->list));
        iocb = (void *)disk_mt->list.next;
        list_del(&iocb->hook);

        sy_spin_unlock(&disk_mt->lock);

        ANALYSIS_BEGIN(0);

        if (iocb->op == DISKIO_OP_WRITE) {
                ret = pwritev(iocb->fd, iocb->iov, iocb->iov_count, iocb->offset);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        } else {
                YASSERT(iocb->op == DISKIO_OP_READ);
                ret = preadv(iocb->fd, iocb->iov, iocb->iov_count, iocb->offset);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        close(iocb->fd);
        job_resume1(iocb->job, 0, iocb->idx, 0);

        yfree((void **)&iocb);

        ANALYSIS_END(0, 1000 * 1000, NULL);

        return 0;
err_ret:
        UNIMPLEMENTED(__DUMP__);
        return ret;
}

static void *__submit_mt_worker(void *_args)
{
        int ret;

        (void) _args;

        while (1) {
                ret = _sem_wait(&disk_mt->sem);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ret = __disk_mt_submit();
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return NULL;
err_ret:
        UNIMPLEMENTED(__DUMP__);
        return NULL;
}

static int __disk_mt_queue(iocb_mt_t *iocb)
{
        int ret;

        ret = sy_spin_lock(&disk_mt->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        list_add_tail(&iocb->hook, &disk_mt->list);

        sy_spin_unlock(&disk_mt->lock);

        sem_post(&disk_mt->sem);

        return 0;
err_ret:
        return ret;
}

static int __disk_mt(const diskio_t *diskio)
{
        int ret, fd, level, max;
        iocb_mt_t *iocb;

        ret = disk_getlevel(&diskio->id, &level, &max);
        if (ret)
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&iocb, sizeof(iocb_mt_t));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = disk_get_syncfd(&diskio->id, level, &fd);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        iocb->op = diskio->op;
        iocb->iov = (struct iovec *)diskio->buf;
        iocb->fd = fd;
        iocb->iov_count = diskio->count;
        iocb->offset = diskio->offset;
        iocb->job = diskio->job;

        ret = __disk_mt_queue(iocb);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int disk_mt_io_submit(const diskio_t **_diskios, int count, int hash)
{
        int ret, i;
        const diskio_t *diskio;

        (void)hash;

        for (i = 0; i < count; i++) {
                diskio = _diskios[i];
                ret = __disk_mt(diskio);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int disk_mt_init()
{
        int ret, size, i, thread;
        pthread_t th;
        pthread_attr_t ta;

        size = sizeof(disk_mt_t);
        ret = ymalloc((void **)&disk_mt, size);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        memset(disk_mt, 0x0, size);

        INIT_LIST_HEAD(&disk_mt->list);

        ret = io_setup(MAX_SUBMIT, &disk_mt->ctx);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        ret = sy_spin_init(&disk_mt->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sem_init(&disk_mt->sem, 0, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (cds_info.tier == TIER_HDD) {
                thread = gloconf.disk_mt_hdd;
        } else {
                thread = gloconf.disk_mt_ssd;
        }

        DINFO("init tier %d disk_mt thread %d\n", cds_info.tier, thread);

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

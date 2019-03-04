#ifndef __LICH_AIO__
#define __LICH_AIO__

#include <sys/epoll.h>
#include <semaphore.h>
#include <linux/aio_abi.h> 
#include <pthread.h>

/**
 * 每个core线程,创建一个aio线程和队列
 *
 * @param name
 * @param cpu
 * @param _eventfd
 * @return
 */

#define IO_CMD_PWRITEV IOCB_CMD_PWRITEV
#define IO_CMD_PREADV IOCB_CMD_PREADV


static inline int io_getevents(aio_context_t ctx_id, long min_nr, long nr,
                               struct io_event *events, struct timespec *timeout)
{
        return syscall(SYS_io_getevents, ctx_id, min_nr, nr, events, timeout);
}

static inline int io_submit(aio_context_t ctx_id, long nr, struct iocb **iocbpp)
{
        return syscall(SYS_io_submit, ctx_id, nr, iocbpp);
}

static inline int io_setup(unsigned nr_events, aio_context_t *ctx_idp)
{
        return syscall(SYS_io_setup, nr_events, ctx_idp);
}

static inline int io_destroy(aio_context_t ctx_id)
{
        return syscall(SYS_io_destroy, ctx_id);
}

static inline void io_set_eventfd(struct iocb *iocb, int eventfd)
{
        iocb->aio_flags |= (1 << 0) /* IOCB_FLAG_RESFD */;
        iocb->aio_resfd = eventfd;
}

static inline void io_prep_preadv(struct iocb *iocb, int fd, const struct iovec *iov, int iovcnt, long long offset)
{
        memset(iocb, 0, sizeof(*iocb));
        iocb->aio_fildes = fd; 
        iocb->aio_lio_opcode = IO_CMD_PREADV;
        iocb->aio_reqprio = 0;
        iocb->aio_buf = (__u64)iov;
        iocb->aio_nbytes = iovcnt;
        iocb->aio_offset = offset;
}

static inline void io_prep_pwritev(struct iocb *iocb, int fd, const struct iovec *iov, int iovcnt, long long offset)
{
        memset(iocb, 0, sizeof(*iocb));
        iocb->aio_fildes = fd; 
        iocb->aio_lio_opcode = IO_CMD_PWRITEV;
        iocb->aio_reqprio = 0;
        iocb->aio_buf = (__u64)iov;
        iocb->aio_nbytes = iovcnt;
        iocb->aio_offset = offset;
}

#define AIO_THREAD 4

int aio_create(const char *name, int cpu, int polling);
void aio_destroy();
int aio_commit(struct iocb *iocb, int prio);
void aio_polling();

/**
 * @brief 相当于flush或sync操作,在一次调度器循环中,提交剩余的iocb
 *
 * @see aio_commit 配合使用
 */
void aio_submit();

/**
 * @brief iocb事件完成后,唤醒相应的task
 *
 * @return
 */

#endif

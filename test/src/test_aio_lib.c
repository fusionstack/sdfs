#include <limits.h>
#include <time.h>
#include <string.h>
#include <sys/epoll.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <linux/aio_abi.h>         /* Defines needed types */
#include <errno.h>

#define DBG_SUBSYS S_LIBSCHEDULE

#include "sysutil.h"
#include "ylib.h"
#include "variable.h"
#include "configure.h"
#include "schedule.h"
#include "aio.h"
#include "cpuset.h"
#include "core.h"
#include "dbg.h"

#define AIO_MODE_SYNC 0
#define AIO_MODE_DIRECT 1
#define AIO_MODE_SIZE 2


#define AIO_EVENT_MAX 1024
#define AIO_THREAD 1

typedef struct {
        /*aio cb*/
        char name[MAX_NAME_LEN];
        //sem_t sem;
        sem_t exit_sem;
        sy_spinlock_t lock;

        int running;
        int cpu;
        int prio_max;
        int idx;
        int mode;
        aio_context_t ioctx;
        int out_eventfd;
        int in_eventfd;

        int iocb_count;
        struct iocb *iocb[AIO_EVENT_MAX];
} aio_t;

int __aio_sche__ = 0;

#if 0
static aio_t **__aio_array__ = NULL;
static sy_spinlock_t __aio_array_lock__;

typedef struct {
} aio_worker_t;
#endif

#define AIO_SUBMIT_MAX 16

static __thread aio_t *__aio__ = NULL;

aio_t *test_aio_self()
{
        return __aio__;
}

static int __aio_getevent(aio_t *aio, uint64_t left)
{
        int ret, r, i, idx = 0, retval;
        struct io_event events[AIO_EVENT_MAX], *ev;

        DINFO("aio event %ju\n", left);
        YASSERT(left <= AIO_EVENT_MAX);

        (void) retval;
        //memset(events, 0x0, sizeof(events));
        
retry:
        r = io_getevents(aio->ioctx, left, left, events, NULL);
        if (likely(r > 0)) {
                idx = 0;
                for (i = 0; i < r; i++) {
                        ev = &events[i];

                        if (likely((long long)ev->res >= 0)) {
                                /*
                                  DBUG("resume sem %u, res %ld, res2 %lu\n",
                                      sem->semid, ev->res, ev->res2);
                                */
                                retval = 0;
                                YASSERT(ev->res2 == 0);
                        } else {
                                DERROR("resume res %ld, res2 %lu\n",
                                       ev->res, ev->res2);
                                retval = -ev->res;
                                YASSERT(ev->res != 0);
                        }

                        DINFO("resume res %ld, res2 %lu\n",
                               ev->res, ev->res2);
                        DINFO("retval %d\n", retval);

                        YASSERT(ev->data);
                        if (__aio_sche__) {
                                task_t *task;
                                task = (void *)ev->data;
                                schedule_resume(task, retval, NULL);
                        } else {
                                sem_t *sem;
                                sem = (void *)ev->data;
                                sem_post(sem);
                        }
                        idx++;
                }
        } else if (r == 0) {
                UNIMPLEMENTED(__DUMP__);
        } else {
                ret = -r;

                if (ret == EINTR)
                        goto retry;

                DERROR("getevent ret %d %s\n", ret, strerror(ret));
                if (ret == EIO) {
                        DERROR("disk IOError\n");
                        SERROR(0, "%s disk IOError\n", M_DISK_IO_ERROR);
                        EXIT(EAGAIN);
                } else {
                        UNIMPLEMENTED(__DUMP__);
                }
        }

        return 0;
}

void test_aio_submit()
{
        int ret, i;
        uint64_t e = 1;
        aio_t *__aio__ = test_aio_self();
        aio_t *aio;

        if (__aio__ == NULL)
                return;
        
        for (i = 0; i < AIO_THREAD * AIO_MODE_SIZE; i++) {
                aio = &__aio__[i];
                if (aio->iocb_count) {
                        ret = write(aio->in_eventfd, &e, sizeof(e));
                        if (ret < 0) {
                                ret = errno;
                                YASSERT(0);
                        }
                }
        }
}

static int __aio_submit__(aio_context_t ioctx, int total, struct iocb **iocb)
{
        int ret, count, offset, left, i;
        struct iocb **_iocb;

        offset = 0;
        while (offset < total) {
                left = total - offset;
                count = _min(left, AIO_SUBMIT_MAX);

#if 0
                struct iocb **p = iocb + offset;
                for (int i=0; i < count; i++) {
                        if (p[i]->aio_lio_opcode == IO_CMD_PWRITEV) {
                                long long int off = p[i]->u.c.offset;

                                for (int j=0; j < p[i]->u.c.nbytes; j++) {
                                        DERROR("pwritev %u/%u %u/%lu fd %d off %llu %llu size %lu\n",
                                               i,
                                               count,
                                               j,
                                               p[i]->u.c.nbytes,
                                               p[i]->aio_fildes,
                                               off / 1048576,
                                               (LLU)p[i]->u.c.offset,
                                               ((struct iovec *)p[i]->u.c.buf)[j].iov_len);
                                }
                        } else if (p[i]->aio_lio_opcode == IO_CMD_PWRITE) {
                                DERROR("pwrite %u/%u %d %llu %lu\n", i, count,
                                       p[i]->aio_fildes,
                                       (LLU)p[i]->u.c.offset,
                                       p[i]->u.c.nbytes);
                        }
                }
#endif

                ANALYSIS_BEGIN(0);

                ret = io_submit(ioctx, count, iocb + offset);
                if (unlikely(ret < 0)) {
                        // ret = (ret == -1) ? ((errno == 0) ? -ret : errno) : -ret;
                        ret = -ret;

                        DERROR("io submit count %d, errno %d ret %d\n", count, errno, ret);
                        SERROR(0, "io submit count %d, errno %d ret %d\n", M_DISK_IO_ERROR, count, errno, ret);

                        _iocb = iocb + offset;
                        for (i = 0; i < count; i++) {
                                schedule_resume((void *)_iocb[i]->aio_data, ret, NULL);
                        }
                } else if (unlikely(ret < count)){
                        /* sbumit successful iocbs, maybe less than expectation */
                        count = ret;
                }

                ANALYSIS_QUEUE(0, IO_WARN, "aio_submit");

                offset += count;
        }

        return 0;
}

static int __aio_submit(aio_t *aio)
{
        int ret, count;
        struct iocb *iocb[AIO_EVENT_MAX];

        if (likely(aio->iocb_count)) {
                ret = sy_spin_lock(&aio->lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                memcpy(iocb, aio->iocb, sizeof(struct iocb *) * aio->iocb_count);
                count = aio->iocb_count;
                aio->iocb_count = 0;

                sy_spin_unlock(&aio->lock);

                DBUG("vm submit %u\n", count);

                ANALYSIS_BEGIN(0);

                ret = __aio_submit__(aio->ioctx, count, iocb);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ANALYSIS_QUEUE(0, IO_WARN, "aio_submit_total");
        }

        return 0;
err_ret:
        return ret;
}

int test_aio_commit(struct iocb *iocb, size_t size, int prio, int mode)
{
        int ret;
        aio_t *__aio__ = test_aio_self(), *aio;

        (void) size;
        
        if (unlikely(!__aio__)) {
                ret = ENOSYS;
                GOTO(err_ret, ret);
        }

        YASSERT(mode == AIO_MODE_DIRECT || mode == AIO_MODE_SYNC);
        aio = &__aio__[iocb->aio_fildes % AIO_THREAD + mode * AIO_THREAD];
        YASSERT(aio->iocb_count < AIO_EVENT_MAX);
        YASSERT(aio->mode == mode);

        if (prio)
                iocb->aio_reqprio = aio->prio_max;

        io_set_eventfd(iocb, aio->out_eventfd);

        ret = sy_spin_lock(&aio->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        aio->iocb[aio->iocb_count] = iocb;
        aio->iocb_count++;

        sy_spin_unlock(&aio->lock);

        if (aio->iocb_count > AIO_SUBMIT_MAX) {
                test_aio_submit();
        }

        //CORE_ANALYSIS_BEGIN(1);

#if 1
        // 由调用者task向iocb.data注册自己,以便被唤醒
        ret = schedule_yield("aio_commit", NULL, iocb);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }
#endif

        //CORE_ANALYSIS_UPDATE(1, IO_WARN, "aio_commit");

        return 0;
err_ret:
        return ret;
}

#define POLL_SD 2

static int __aio_poll(aio_t *aio, struct pollfd *pfd)
{
        int ret;

        pfd[0].fd = aio->out_eventfd;
        pfd[0].events = POLLIN;
        pfd[0].revents = 0;
        pfd[1].fd = aio->in_eventfd;
        pfd[1].events = POLLIN;
        pfd[1].revents = 0;

        //DBUG("aio poll sd (%u, %u)\n", aio->sd, aio->interrupt_eventfd);
        while (1) {
                ret = poll(pfd, POLL_SD, 1000);
                if (ret  < 0)  {
                        ret = errno;
                        if (ret == EINTR) {
                                continue;
                        } else
                                GOTO(err_ret, ret);
                }

                break;
        }

        DBUG("got event %u\n", ret);

        return ret;
err_ret:
        return -ret;
}

static int __aio_event(aio_t *aio, const struct pollfd *_pfd, int _count)
{
        int ret, i, count = 0;
        const struct pollfd *pfd;
        uint64_t left;

        for (i = 0; i < POLL_SD; i++) {
                pfd = &_pfd[i];
                if (pfd->revents == 0)
                        continue;

                // left是完成的iocb事件数量
                ret = read(pfd->fd, &left, sizeof(left));
                if (unlikely(ret  < 0)) {
                        ret = errno;
                        YASSERT(ret == EAGAIN);
                        GOTO(err_ret, ret);
                }
                
                if (pfd->fd == aio->out_eventfd) {
                        ret = __aio_getevent(aio, left);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);
                } else if (pfd->fd == aio->in_eventfd) {
                        ret = __aio_submit(aio);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);
                } else {
                        UNIMPLEMENTED(__DUMP__);
                }

                count++;
        }

        YASSERT(count == _count);

        return 0;
err_ret:
        return ret;
}

static int __aio_worker_poll(aio_t *aio)
{
        int ret, event;
        struct pollfd pfd[POLL_SD];

        event = __aio_poll(aio, pfd);
        if (event < 0) {
                ret = -event;
                GOTO(err_ret, ret);
        }

        ret = __aio_event(aio, pfd, event);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}


static void *__aio_worker(void *arg)
{
        int ret;
        aio_t *aio = arg;

        DINFO("start aio %s[%u]...\n", aio->name, aio->idx);

#if 1
        if (aio->cpu != -1) {
                ret = cpuset(aio->name, aio->cpu);
                if (unlikely(ret)) {
                        DWARN("set cpu fail\n");
                }
        }
#endif

        while (aio->running) {
                ret = __aio_worker_poll(aio);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        sem_post(&aio->exit_sem);

        return NULL;
err_ret:
        UNIMPLEMENTED(__DUMP__);
        return NULL;
}

static int __aio_create(const char *name, int idx, int event_max,  aio_t *aio, int cpu, int mode)
{
        int ret, efd;

        (void) idx;
        
        ret = io_setup(event_max, &aio->ioctx);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (efd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        aio->out_eventfd = efd;

        efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (efd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        aio->in_eventfd = efd;

        strcpy(aio->name, name);
        aio->running = 1;
        aio->cpu = cpu;
        aio->mode = mode;
        aio->prio_max = sysconf(_SC_AIO_PRIO_DELTA_MAX);

        DINFO("name %s cpu %d aio fd %d prio max %u\n", name, cpu, efd, aio->prio_max);
        
        ret = sy_spin_init(&aio->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sem_init(&aio->exit_sem, 0, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sy_thread_create2(__aio_worker, aio, "__aio_worker");
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int test_aio_create(const char *name, int cpu)
{
        int ret, i;
        aio_t *aio;

        ret = ymalloc((void **)&aio, sizeof(*aio) * AIO_THREAD * AIO_MODE_SIZE);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (i = 0; i < AIO_THREAD; i++) {
                ret = __aio_create(name, i, AIO_EVENT_MAX / (AIO_THREAD * AIO_MODE_SIZE), &aio[i], cpu, AIO_MODE_SYNC);
                if (unlikely(ret))
                        GOTO(err_free, ret);
        }

        for (i = AIO_THREAD; i < AIO_THREAD * AIO_MODE_SIZE; i++) {
                ret = __aio_create(name, i, AIO_EVENT_MAX / (AIO_THREAD * AIO_MODE_SIZE), &aio[i], cpu, AIO_MODE_DIRECT);
                if (unlikely(ret))
                        GOTO(err_free, ret);
        }

        __aio__ = aio;

        return 0;
err_free:
        yfree((void **)&aio);
err_ret:
        return ret;
}

static void __aio_destroy(aio_t *aio)
{
        int ret;

        aio->running = 0;
        test_aio_submit();

        ret = _sem_timedwait1(&aio->exit_sem, 10);
        YASSERT(ret == 0);

        DINFO("aio[%u] destroy %d\n", aio->idx, aio->out_eventfd);

        io_destroy(aio->ioctx);
        close(aio->out_eventfd);
        close(aio->in_eventfd);
}

void test_aio_destroy()
{
        int i;
        aio_t *__aio__ = test_aio_self();

        YASSERT(__aio__);
        for (i = 0; i < AIO_THREAD * AIO_MODE_SIZE; i++) {
                __aio_destroy(&__aio__[i]);
        }
        
        yfree((void **)&__aio__);
        __aio__ = NULL;
}

#include <limits.h>
#include <time.h>
#include <string.h>
#include <linux/aio_abi.h> 
#include <sys/epoll.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBSCHEDULE

#include "ylib.h"
#include "aio.h"
#include "core.h"
#include "corenet.h"
#include "dbg.h"

#define AIO_EVENT_MAX 1024

typedef struct {
        /*aio cb*/
        char name[MAX_NAME_LEN];
        //sem_t sem;
        sem_t exit_sem;
        sy_spinlock_t lock;

        int polling;
        int running;
        int cpu;
        int prio_max;
        int idx;
        aio_context_t ioctx;
        int out_eventfd;
        int in_eventfd;

        int iocb_count;
        struct iocb *iocb[AIO_EVENT_MAX];
} aio_t;

#define AIO_SUBMIT_MAX 128

static int __aio_submit(aio_t *aio);

static aio_t *aio_self()
{
        return variable_get(VARIABLE_AIO);
}

static int __aio_getevent(aio_t *aio, uint64_t left)
{
        int ret, r, i, idx = 0, retval;
        struct io_event events[AIO_EVENT_MAX], *ev;
        task_t *task;

        DBUG("aio event %ju\n", left);
        YASSERT(left <= AIO_EVENT_MAX);

retry:
        r = io_getevents(aio->ioctx, left, left, events, NULL);
        if (likely(r > 0)) {
                idx = 0;
                for (i = 0; i < r; i++) {
                        ev = &events[i];
                        task = (void *)ev->data;

                        YASSERT(task);

                        if (likely((long long)ev->res >= 0)) {
                                DBUG("resume task %u, res %ld, res2 %lu\n",
                                     task->taskid, ev->res, ev->res2);
                                retval = ev->res;
                                YASSERT(ev->res2 == 0);
                        } else {
                                DERROR("resume task %u, res %ld, res2 %lu\n",
                                       task->taskid, ev->res, ev->res2);
                                retval = ev->res;
                                YASSERT(ev->res != 0);
                        }

                        schedule_resume(task, retval, NULL);
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
                        EXIT(EAGAIN);
                } else {
                        UNIMPLEMENTED(__DUMP__);
                }
        }

        return 0;
}

static void __aio_recv(void *arg)
{
        int ret;
        uint64_t left;
        aio_t *aio = arg;

        DBUG("aio return\n");
        ret = read(aio->out_eventfd, &left, sizeof(left));
        if (unlikely(ret < 0)) {
                ret = errno;
                UNIMPLEMENTED(__DUMP__);
        }

        ret = __aio_getevent(aio, left);
        if (unlikely(ret)) {
                UNIMPLEMENTED(__DUMP__);
        }
}


void aio_submit()
{
        int ret;
        aio_t *__aio__ = aio_self();
        aio_t *aio;

        for (int i = 0; i < AIO_MODE_SIZE; i++) {
                aio = &__aio__[i];
                if (aio->iocb_count) {
#if 1
                        ret = __aio_submit(aio);
                        if (unlikely(ret))
                                UNIMPLEMENTED(__DUMP__);
#else
                        uint64_t e = 1;
                        ret = write(aio->in_eventfd, &e, sizeof(e));
                        if (ret < 0) {
                                ret = errno;
                                YASSERT(0);
                        }
#endif
                }
        }
}

static int __aio_submit__(aio_context_t ioctx, int total, struct iocb **iocb)
{
        int ret, count, offset, left;
        struct iocb **_iocb;

        offset = 0;
        while (offset < total) {
                left = total - offset;
                count = _min(left, AIO_SUBMIT_MAX);

                //ANALYSIS_BEGIN(0);

                ret = io_submit(ioctx, count, iocb + offset);
                if (unlikely(ret < 0)) {
                        // ret = (ret == -1) ? ((errno == 0) ? -ret : errno) : -ret;
                        ret = -ret;

                        DERROR("io submit count %d, errno %d ret %d\n", count, errno, ret);

                        _iocb = iocb + offset;
                        for (int i = 0; i < count; i++) {
                                schedule_resume((void *)_iocb[i]->aio_data, ret, NULL);
                        }
                } else if (unlikely(ret < count)){
                        /* sbumit successful iocbs, maybe less than expectation */
                        count = ret;
                }

                //ANALYSIS_QUEUE(0, IO_WARN, "aio_submit");

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

                DBUG("submit %u\n", count);

                //ANALYSIS_BEGIN(0);

                ret = __aio_submit__(aio->ioctx, count, iocb);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                //ANALYSIS_QUEUE(0, IO_WARN, "aio_submit_total");
        }

        return 0;
err_ret:
        return ret;
}

int aio_commit(struct iocb *iocb, size_t size, int prio, int mode)
{
        int ret;
        aio_t *__aio__ = aio_self(), *aio;

        (void) size;
        
        if (unlikely(!__aio__)) {
                ret = ENOSYS;
                GOTO(err_ret, ret);
        }

        DBUG("aio\n");
        
        YASSERT(mode == AIO_MODE_DIRECT || mode == AIO_MODE_SYNC);
        aio = &__aio__[mode % AIO_MODE_SIZE];
        YASSERT(aio->iocb_count < AIO_EVENT_MAX);

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
                aio_submit();
        }

        CORE_ANALYSIS_BEGIN(1);

        // 由调用者task向iocb.data注册自己,以便被唤醒
        ret = schedule_yield("aio_commit", NULL, iocb);
        if (unlikely(ret < 0)) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

#if 0
        if ((size_t)ret != size) {
                ret = EIO;
                GOTO(err_ret, ret);
        }
#endif
        
        CORE_ANALYSIS_UPDATE(1, IO_WARN, "aio_commit");

        return ret;
err_ret:
        return -ret;
}

static int __aio_poll_in(aio_t *aio, struct pollfd *pfd)
{
        int ret, count;

        pfd[0].fd = aio->in_eventfd;
        pfd[0].events = POLLIN;
        pfd[0].revents = 0;
        count = 1;

        //DBUG("aio poll sd (%u, %u)\n", aio->sd, aio->interrupt_eventfd);
        while (1) {
                ret = poll(pfd, count, 1000);
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

static void __aio_event_out(aio_t *array, int fd, int left)
{
        int ret, i;
        aio_t *aio;

        DBUG("left %u\n", left);
        
        for (i = 0; i < AIO_MODE_SIZE; i++) {
                aio = &array[i];

                if (fd == aio->out_eventfd) {
                        ret = __aio_getevent(aio, left);
                        if (unlikely(ret)) {
                                UNIMPLEMENTED(__DUMP__);
                        }
                }

                break;
        }

        YASSERT(i != AIO_MODE_SIZE);
        //YASSERT(count == _count);
}

void aio_polling()
{
        int ret, count;
        struct pollfd pfds[2], *pfd;
        aio_t *__aio__ = aio_self(), *aio;
        uint64_t left;
        
        if (__aio__[0].polling == 0) {
                return;
        }

        for (int i = 0; i < AIO_MODE_SIZE; i++) {
                aio = &__aio__[i];
                pfds[i].fd = aio->out_eventfd;
                pfds[i].events = POLLIN;
                pfds[i].revents = 0;
        }

        while (1) {
                ret = poll(pfds, AIO_MODE_SIZE, 0);
                if (ret < 0)  {
                        ret = errno;
                        if (ret == EINTR) {
                                continue;
                        } else
                                GOTO(err_ret, ret);
                }

                break;
        }

        count = ret;
        DBUG("got event %u\n", count);
        for (int i = 0; i < count; i++) {
                pfd = &pfds[i];
                if (pfd->revents == 0)
                        continue;

                // left是完成的iocb事件数量
                ret = read(pfd->fd, &left, sizeof(left));
                if (unlikely(ret  < 0)) {
                        ret = errno;
                        YASSERT(ret == EAGAIN);
                        GOTO(err_ret, ret);
                }

                __aio_event_out(__aio__, pfd->fd, left);
        }

        return;
err_ret:
        return;
}

static int __aio_event_in(aio_t *aio, const struct pollfd *_pfd, int _count)
{
        int ret, i, count = 0;
        const struct pollfd *pfd;
        uint64_t left;

        for (i = 0; i < _count; i++) {
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
                
                if (pfd->fd == aio->in_eventfd) {
                        ret = __aio_submit(aio);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);
                } else {
                        UNIMPLEMENTED(__DUMP__);
                }

                count++;
        }

        //YASSERT(count == _count);

        return 0;
err_ret:
        return ret;
}

static int __aio_worker_pollin(aio_t *aio)
{
        int ret, event;
        struct pollfd pfd[2];

        event = __aio_poll_in(aio, pfd);
        if (event < 0) {
                ret = -event;
                GOTO(err_ret, ret);
        }

        ret = __aio_event_in(aio, pfd, event);
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
                ret = __aio_worker_pollin(aio);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        sem_post(&aio->exit_sem);

        return NULL;
err_ret:
        UNIMPLEMENTED(__DUMP__);
        return NULL;
}

static int __aio_create(const char *name, int event_max,  aio_t *aio, int cpu, int polling)
{
        int ret, efd;

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

        strcpy(aio->name, name);
        aio->running = 1;
        aio->cpu = cpu; 
        aio->prio_max = sysconf(_SC_AIO_PRIO_DELTA_MAX);

        DINFO("name %s cpu %d aio fd %d prio max %u\n", name, cpu, efd, aio->prio_max);

        ret = sy_spin_init(&aio->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (efd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        aio->in_eventfd = efd;
        aio->polling = polling;

        ret = sem_init(&aio->exit_sem, 0, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sy_thread_create2(__aio_worker, aio, "__aio_worker");
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (!polling) {
                sockid_t sockid;
                sockid.sd = aio->out_eventfd;
                sockid.seq = _random();
                sockid.type = SOCKID_CORENET;
                sockid.addr = 789;
                ret = corenet_tcp_add(NULL, &sockid, aio, NULL, NULL, NULL,
                                      __aio_recv, "aio_recv");
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int aio_create(const char *name, int cpu, int polling)
{
        int ret;
        aio_t *aio;

        DINFO("polling mode %s\n", polling ? "on" : "off");
        
        ret = ymalloc((void **)&aio, sizeof(*aio) * AIO_MODE_SIZE);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (int i = 0; i < AIO_MODE_SIZE; i++) {
                ret = __aio_create(name, AIO_EVENT_MAX / (AIO_MODE_SIZE), &aio[i],
                                   cpu, polling);
                if (unlikely(ret))
                        GOTO(err_free, ret);
        }

        variable_set(VARIABLE_AIO, aio);

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
        aio_submit();

        ret = _sem_timedwait1(&aio->exit_sem, 10);
        YASSERT(ret == 0);

        DINFO("aio[%u] destroy %d\n", aio->idx, aio->out_eventfd);

        io_destroy(aio->ioctx);
        close(aio->out_eventfd);
        close(aio->in_eventfd);
}

void aio_destroy()
{
        int i;
        aio_t *__aio__ = aio_self();

        YASSERT(__aio__);
        for (i = 0; i < AIO_MODE_SIZE; i++) {
                __aio_destroy(&__aio__[i]);
        }
        
        yfree((void **)&__aio__);
        variable_unset(VARIABLE_AIO);
}

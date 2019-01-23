#include <limits.h>
#include <time.h>
#include <string.h>
#include <sys/epoll.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "sysutil.h"
#include "net_proto.h"
#include "ylib.h"
#include "sdevent.h"
#include "../net/xnect.h"
#include "net_table.h"
#include "sdfs_aio.h"
#include "rpc_table.h"
#include "configure.h"
#include "main_loop.h"
#include "mem_cache.h"
#include "job_dock.h"
#include "schedule.h"
#include "bh.h"
#include "variable.h"
#include "net_global.h"
#include "timer.h"
#include "adt.h"
#include "dbg.h"

extern int nofile_max;

typedef struct {
        int epoll_rfd;   // 只监听EPOLL_IN事件

        int eventfd;     // schedule->eventfd

        int idx;
        int busy;
        pid_t tid;
        sem_t sem;

        uint64_t cutime_run;
        uint64_t cstime_run;
        uint64_t cutime_wait;
        uint64_t cstime_wait;
        uint64_t ctime_idle;

        schedule_t *schedule;
} worker_t;

#define EPOLL_TMO 30

static int __worker_count__;
static worker_t *__worker__;
//extern int nofile_max;
//static int __config_hz__ = 0;
static int __main_loop_request__ = 0;
int __main_loop_hold__ = 1;

int main_loop_check()
{
        return __main_loop_hold__ ? EAGAIN : 0;
}

static int __main_loop_worker_init(worker_t *worker)
{
        int ret, interrupt_eventfd;
        event_t ev;
        char name[MAX_NAME_LEN];

        snprintf(name, sizeof(name), "default");
        ret = schedule_create(&interrupt_eventfd, name, &worker->idx, &worker->schedule, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        worker->eventfd = interrupt_eventfd;

        ev.data.fd = worker->eventfd;
        ev.events = EPOLLIN;
        ret = epoll_ctl(worker->epoll_rfd, EPOLL_CTL_ADD, worker->eventfd, &ev);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

#if 0
        if (ng.daemon) {
                ret = aio_create(name, -1);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }
#endif

        worker->tid = _gettid();

        return 0;
err_ret:
        return ret;
}

/**
 * 读写分流
 * - 此处为读事件
 * - EPOLL_OUT在sdevent.c里处理
 *
 * @param _args
 * @return
 */
static void *__main_loop_worker(void *_args)
{
        int ret, nfds;
        event_t ev;
        worker_t *worker;
        char buf[MAX_BUF_LEN];

        worker = _args;
        ret = __main_loop_worker_init(worker);
        if (unlikely(ret)) {
                DERROR("main loop init fail, ret %u\n", ret);
                EXIT(ret);
        }

        sem_post(&worker->sem);

        main_loop_hold();        

        while (1) {
                DBUG("running thread %u, epoll_fd %u, eventfd %u\n",
                      worker->idx, worker->epoll_rfd, worker->eventfd);

                nfds = _epoll_wait(worker->epoll_rfd, &ev,  1, EPOLL_TMO * 1000);
                if (nfds < 0) {
                        ret = errno;
                        YASSERT(0);
                }

                if (nfds == 0) {
                        schedule_scan(NULL);
                        continue;
                }

                schedule_backtrace();

                DBUG("thread %u got new event, fd %u, epollfd %u\n",
                      worker->idx, ev.data.fd, worker->epoll_rfd);

                YASSERT(nfds == 1);
                YASSERT((ev.events & EPOLLOUT) == 0);

                if (ev.data.fd == worker->eventfd) {
                        DBUG("got schedule event\n");

                        ret = read(ev.data.fd, buf, MAX_BUF_LEN);
                        if (ret < 0) {
                                ret = errno;
                                UNIMPLEMENTED(__DUMP__);
                        }

                        DBUG("read %u\n", ret);
                } else {
                        if ((ev.events & EPOLLRDHUP) || (ev.events & EPOLLERR)
                            || (ev.events & EPOLLHUP))  {
                                //__main_loop_remove_event(worker->epoll_rfd, ev.data.fd);
                                sdevent_exit(ev.data.fd);
                        } else {
                                ret = sdevent_recv(ev.data.fd);
                                if (ret == ECONNRESET) {
                                        //__main_loop_remove_event(worker->epoll_rfd, ev.data.fd);
                                        sdevent_exit(ev.data.fd);
                                }
                        }
                }

                worker->busy = 1;

                schedule_run(NULL);
                schedule_scan(NULL);

                if (ng.daemon) {
                        aio_submit();
                }

                worker->busy = 0;
        }

        return NULL;
}

int main_loop_event(int sd, int event, int op)
{
        int ret;
        event_t ev;

        ev.events = event;
        ev.data.fd = sd;

        YASSERT(__worker_count__);

        ret = _epoll_ctl(__worker__[sd % __worker_count__].epoll_rfd, op, sd, &ev);
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int main_loop_request(void (*exec)(void *buf), void *buf, const char *name)
{
        int ret, rand;
        schedule_t *schedule;

        if (__worker__ == NULL || __worker_count__ == 0) {
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }

        rand = ++__main_loop_request__ % (__worker_count__);

        schedule = __worker__[rand].schedule;
        if (schedule == NULL) {
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }

        ret = schedule_request(schedule, -1, exec, buf, name);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

#if 0
inline static int __main_loop_gettime(worker_t *worker)
{
        int ret, i;
        char buf[MAX_BUF_LEN], path[MAX_PATH_LEN], *cur;

        snprintf(path, MAX_PATH_LEN, "/proc/%u/task/%u/stat", getpid(), worker->tid);
        ret = _get_value(path, buf, MAX_BUF_LEN);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        i = 0;
        cur = buf;
        while (1) {
                if (i == 13) {
                        ret = sscanf(cur, "%lu[^ ]", &worker->cutime_run);
                        YASSERT(ret == 1);
                } else if (i == 14) {
                        ret = sscanf(cur, "%lu[^ ]", &worker->cstime_run);
                        YASSERT(ret == 1);
                } else if (i == 15) {
                        ret = sscanf(cur, "%lu[^ ]", &worker->cstime_wait);
                        YASSERT(ret == 1);
                } else if (i == 16) {
                        ret = sscanf(cur, "%lu[^ ]", &worker->cstime_wait);
                        YASSERT(ret == 1);
                        break;
                }

                cur = strchr(cur, ' ');
                YASSERT(cur);
                cur = cur + 1;
                i++;
        }

        return 0;
err_ret:
        return ret;
}

inline static void __main_loop_get_hz()
{
        int ret;
        const char *path = SHM_ROOT"/config_hz";
        char tmp[MAX_BUF_LEN];

        if (ng.daemon) {
                ret = _get_text(path, tmp, MAX_NAME_LEN);
                if (ret < 0)
                        UNIMPLEMENTED(__DUMP__);

                DINFO("CONFIG_HZ=%s\n", tmp);

                ret = sscanf(tmp, "%u", &__config_hz__);
                if (ret != 1) {
                        DWARN("can not find CONFIG_HZ\n");
                        __config_hz__ = 0;
                }
        }

}
#endif

static int __main_loop_worker_create(int threads)
{
        int ret, i;
        worker_t *_worker, *worker;

        ret = ymalloc((void **)&_worker, sizeof(worker_t) * threads);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (i = 0; i < threads; i++) {
                worker = &_worker[i];
                worker->idx = i;
                worker->busy = 0;

                worker->epoll_rfd = epoll_create(nofile_max);
                if (worker->epoll_rfd == -1) {
                        ret = errno;
                        GOTO(err_free, ret);
                }

                ret = sem_init(&worker->sem, 0, 0);
                if (unlikely(ret))
                        GOTO(err_free, ret);

                ret = sy_thread_create2(__main_loop_worker, worker, "main_loop");
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_free, ret);
                }

                sem_wait(&worker->sem);
        }

        __worker_count__ = threads;
        __worker__ = _worker;

        return 0;
err_free:
        yfree((void **)&_worker);
err_ret:
        return ret;
}

int main_loop_create(int threads)
{
        int ret;

        __main_loop_hold__ = 1;
        
        DINFO("main loop create start, threads %u\n", threads);

        ret = __main_loop_worker_create(threads);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DINFO("main loop inited, threads %d\n", threads);

        return 0;
err_ret:
        return ret;
}

void main_loop_start()
{
        DINFO("main loop start\n");
        __main_loop_hold__ = 0;
}

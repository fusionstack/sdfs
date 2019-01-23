#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <stdint.h>
#include <sched.h>
#include <pthread.h>
#include <poll.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "ylib.h"
#include "ynet_sock.h"
#include "sdevent.h"
#include "schedule.h"
#include "job_dock.h"
#include "dbg.h"

static int __sock_poll_sd1(const int *sd, int sd_count, short event,
                           uint64_t usec, struct pollfd *pfd, int *retval)
{
        int ret, i;
        struct timeval oldtv, newtv;
        uint64_t left, used;

        YASSERT(usec < 100 * 1000 * 1000);

        if (usec < 1000 * 500) {
                DERROR("wait %llu\n", (LLU)usec);
        }

        ANALYSIS_BEGIN(0);

        for (i = 0; i < sd_count; i++) {
                pfd[i].fd = sd[i];
                pfd[i].events = event;
                pfd[i].revents = 0;
        }

        timerclear(&oldtv);
        ret = _gettimeofday(&oldtv, NULL);
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        left = usec;
        while (1) {
                ret = poll(pfd, sd_count, left / 1000);
                if (ret == -1) {
                        ret = errno;
                        if (ret == EINTR) {
                                timerclear(&newtv);
                                ret = _gettimeofday(&newtv, NULL);
                                if (ret == -1) {
                                        ret = errno;
                                        GOTO(err_ret, ret);
                                }

                                used = _time_used(&oldtv, &newtv);
                                if (used >= left) {
                                        ret = ETIME;
                                        GOTO(err_ret, ret);
                                } else {
                                        left -= used;
                                        continue;
                                }
                        }

                        GOTO(err_ret, ret);
                } else if (ret == 0) {
                        // TODO rep exception, or ARP cache?
                        ret = ETIME;
                        goto err_ret;
                } else {
                        *retval = ret;
                        break;
                }
        }

        ANALYSIS_END(0, IO_WARN, NULL);

        return 0;
err_ret:
        return ret;
}

static int __sock_poll__(va_list ap)
{
        int ret;
        const int *sd = va_arg(ap, const int *);
        int sd_count = va_arg(ap, int);
        int event = va_arg(ap, int);
        uint64_t usec = va_arg(ap, uint64_t);
        struct pollfd *pfd = va_arg(ap, struct pollfd *);
        int *retval = va_arg(ap, int *);

        va_end(ap);

        ret = __sock_poll_sd1(sd, sd_count, event, usec, pfd, retval);
        if (unlikely(ret))
                goto err_ret;

        return 0;
err_ret:
        return ret;
}


int sock_poll_sd1(const int *sd, int sd_count, short event, uint64_t usec,
                  struct pollfd *pfd, int *retval)
{
        int ret;

        YASSERT(!schedule_suspend());

        if (schedule_running()) {
                ret = schedule_newthread(SCHE_THREAD_MISC, _random(), FALSE,
                                         "poll", 100, __sock_poll__, sd, sd_count, 
                                         (int)event, usec, pfd, retval);
                if (unlikely(ret))
                        goto err_ret;
        } else {
                ret = __sock_poll_sd1(sd, sd_count, event, usec, pfd, retval);
                if (unlikely(ret))
                        goto err_ret;
        }

        return 0;
err_ret:
        return ret;
}

int sock_poll_sd(int sd, uint64_t usec, short event)
{
        int ret, pcount;
        struct pollfd pfds[1];

        pcount = 1;
        ret = sock_poll_sd1(&sd, 1, event, usec, pfds, &pcount);
        if (unlikely(ret))
                goto err_ret;

        return 0;
err_ret:
        return ret;
}

#include <errno.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#define DBG_SUBSYS S_LIBYNET

#include "sysutil.h"
#include "configure.h"
#include "sock_buffer.h"
#include "job_dock.h"
#include "pipe_pool.h"
#include "ylib.h"
#include "ynet_conf.h"
#include "net_global.h"
#include "ynet_net.h"
#include "sdevent.h"
#include "adt.h"
#include "dbg.h"

typedef enum {
        STATUS_HEAD,
        STATUS_MSG,
        STATUS_IO_0,
        STATUS_IO_1,
        STATUS_FINISH,
} SBUF_STATUS_T;

const char *netable_rname(const void *_nh);
const char *network_rname(const nid_t *nid);
//extern mpool_t page_pool;

#ifdef YFS_DEBUG
//#define SOCK_BUFFER_DEBUG
#endif

#ifdef SOCK_BUFFER_DEBUG
#define SOCK_WBUFFER_CHECK(__buf__)                                     \
        do {                                                            \
                struct list_head *__pos;                                \
                uint32_t __count = 0;                                   \
                job_t *__job;                                           \
                ret = sy_rwlock_wrlock(&__buf__->wlock);                    \
                if (unlikely(ret)) {                                    \
                        break;                                          \
                }                                                       \
                                                                        \
                if (__buf__->half_job) {                                \
                        __job = __buf__->half_job;                      \
                        __count += (__job->iocb.buf->len - __job->iocb.offset); \
                        BUFFER_CHECK(__job->iocb.buf);                  \
                }                                                       \
                                                                        \
                list_for_each(__pos, &__buf__->list[0]) {               \
                        __job = (job_t *)__pos;                         \
                        __count += (__job->iocb.buf->len - __job->iocb.offset); \
                        BUFFER_CHECK(__job->iocb.buf);                  \
                }                                                       \
                                                                        \
                list_for_each(__pos, &__buf__->list[1]) {               \
                        __job = (job_t *)__pos;                         \
                        __count += (__job->iocb.buf->len - __job->iocb.offset); \
                        BUFFER_CHECK(__job->iocb.buf);                  \
                }                                                       \
                                                                        \
                YASSERT(__count == __buf__->length);                    \
                sy_rwlock_unlock(&__buf__->wlock);                        \
        } while (0);

#define SOCK_RBUFFER_CHECK(__buf__)                                     \
        do {                                                            \
                int __i;                                                \
                rbuf_seg_t *__seg;                                      \
                                                                        \
                for (__i = 0; __i < 2; __i ++) {                        \
                        __seg = &__buf__->seg[__i];                     \
                        YASSERT(__seg->fd != 0);                        \
                        YASSERT(__seg->split_off <= __seg->recv_off);   \
                        YASSERT(__seg->recv_off <= SHM_MAX);            \
                        YASSERT(__seg->split_off < SHM_MAX);            \
                }                                                       \
        } while (0);

#else
#define SOCK_WBUFFER_CHECK(__buf__)
#define SOCK_RBUFFER_CHECK(__buf__)
#endif

volatile struct sockstate sock_state = {0, 0};
extern int __shutdown__;

int sock_rbuffer_create(sock_rbuffer_t *rbuf)
{
        int ret;

        ret = sy_spin_init(&rbuf->lock);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        ret = mbuffer_init(&rbuf->buf, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int sock_rbuffer_destroy(sock_rbuffer_t *rbuf)
{
        if (rbuf->buf.len) {
                DWARN("left %u\n", rbuf->buf.len);
        }

        mbuffer_free(&rbuf->buf);
        return 0;
}

static int __sock_rbuffer_recv(sock_rbuffer_t *rbuf, int sd, int toread)
{
        int ret, iov_count;
        struct msghdr msg;
        buffer_t buf;

        ret = mbuffer_init(&buf, toread);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        YASSERT((int)buf.len <= SOCK_IOV_MAX * BUFFER_SEG_SIZE);

        iov_count = SOCK_IOV_MAX;
        ret = mbuffer_trans(rbuf->iov, &iov_count,  &buf);
        YASSERT(ret == (int)buf.len);        
        memset(&msg, 0x0, sizeof(msg));
        msg.msg_iov = rbuf->iov; 
        msg.msg_iovlen = iov_count;

        ret = _recvmsg(sd, &msg, MSG_DONTWAIT);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_free, ret);
        }

        YASSERT(ret == (int)buf.len);
        DBUG("got data %u\n", ret);

        mbuffer_merge(&rbuf->buf, &buf);

        return 0;
err_free:
        mbuffer_free(&buf);
err_ret:
        return ret;
}

int sock_rbuffer_recv(sock_rbuffer_t *rbuf, int sd)
{
        int ret, toread, left, cp;

        DBUG("left %u, socket %u\n", rbuf->buf.len, sd);

        ret = ioctl(sd, FIONREAD, &toread);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        if (toread == 0) {
                ret = ECONNRESET;
                GOTO(err_ret, ret);
        }

        left = toread;
        while (left) {
                cp = left > BUFFER_SEG_SIZE * SOCK_IOV_MAX ? BUFFER_SEG_SIZE * SOCK_IOV_MAX : left;

                if (toread > BUFFER_SEG_SIZE * SOCK_IOV_MAX) {
                        DINFO("long msg, total %u, left %u, read %u\n", toread, left, cp);
                }

                ret = __sock_rbuffer_recv(rbuf, sd, cp);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                left -= cp;
        }

        DBUG("recv %u\n", rbuf->buf.len);

        return 0;
err_ret:
        return ret;
}

int sock_wbuffer_create(sock_wbuffer_t *wbuf)
{
        int ret;
        //_memset(buf, 0x0, sizeof(sock_wbuffer_t));

        wbuf->closed = 0;

        ret = sy_spin_init(&wbuf->lock);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        mbuffer_init(&wbuf->buf, 0);

        return 0;
err_ret:
        return ret;
}

static int __sock_wbuffer_send__(struct iovec *iov, buffer_t *buf, int sd)
{
        int ret, iov_count;
        struct msghdr msg;

        iov_count = SOCK_IOV_MAX;
        ret = mbuffer_trans(iov, &iov_count,  buf);
        YASSERT(iov_count <= SOCK_IOV_MAX);
        memset(&msg, 0x0, sizeof(msg));
        msg.msg_iov = iov;
        msg.msg_iovlen = iov_count;

        ret = sendmsg(sd, &msg, MSG_DONTWAIT);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        DBUG("send %u\n", ret);

        return ret;
err_ret:
        return -ret;
}

static int __sock_wbuffer_send(struct iovec *iov, buffer_t *buf, int sd)
{
        int ret, sent;

        sent = 0;
        while (buf->len) {
                ret = __sock_wbuffer_send__(iov, buf, sd);
                if (ret < 0) {
                        ret = -ret;
                        if (ret == EAGAIN || ret == EWOULDBLOCK) {
                                break;
                        } else
                                GOTO(err_ret, ret);
                }

                sent += ret;

                if ((int)buf->len == ret) {
                        mbuffer_free(buf);
                } else {
                        DBUG("pop %u from %u\n", ret, buf->len);
                        mbuffer_pop(buf, NULL, ret);
                }
        }

        return sent;
err_ret:
        return -ret;
}

static int sock_wbuffer_revert(sock_wbuffer_t *wbuf, buffer_t *buf)
{
        int ret;
        buffer_t tmp;

        DWARN("left %u\n", buf->len);

        ret = sy_spin_lock(&wbuf->lock);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        if (wbuf->buf.len) {
                DWARN("merge %u\n", wbuf->buf.len);

                mbuffer_init(&tmp, 0);
                mbuffer_merge(&tmp, &wbuf->buf);
                mbuffer_merge(&wbuf->buf, buf);
                mbuffer_merge(&wbuf->buf, &tmp);
        } else {
                mbuffer_merge(&wbuf->buf, buf);
        }

        sy_spin_unlock(&wbuf->lock);

        return 0;
err_ret:
        return ret;
}

//read form buf, write to sd
int sock_wbuffer_send(sock_wbuffer_t *wbuf, int sd)
{
        int ret, count = 0;
        buffer_t buf;

        DBUG("send by event\n");
        mbuffer_init(&buf, 0);

        while (1) {
                ret = sy_spin_lock(&wbuf->lock);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }

                mbuffer_merge(&buf, &wbuf->buf);

                sy_spin_unlock(&wbuf->lock);

                if (!buf.len) {
                        break;
                }

                ret = __sock_wbuffer_send(wbuf->iov, &buf, sd);
                if (ret < 0) {
                        ret = -ret;
                        //mbuffer_free(&buf);
                        if (sock_wbuffer_revert(wbuf, &buf)) {
                                UNIMPLEMENTED(__DUMP__);
                        }

                        GOTO(err_ret, ret);
                }

                count += ret;

                if (buf.len) {
                        ret = sock_wbuffer_revert(wbuf, &buf);
                        if (unlikely(ret)) {
                                UNIMPLEMENTED(__DUMP__);
                        }

                        break;
                }
        }

        DBUG("send %u\n", count);

        return 0;
err_ret:
        return ret;
}

static void __sock_wbuffer_queue(sock_wbuffer_t *wbuf, const buffer_t *buf, int flag)
{
        DBUG("queue msg %u\n", buf->len);

        if (flag & BUFFER_KEEP) {
                UNIMPLEMENTED(__DUMP__);
                mbuffer_reference(&wbuf->buf, buf);
        } else {
                mbuffer_merge(&wbuf->buf, (void *)buf);
        }
}

int sock_wbuffer_queue(int sd, sock_wbuffer_t *wbuf, const buffer_t *buf, int flag)
{
        int ret;

        (void) sd;

        YASSERT(buf->len);
        BUFFER_CHECK(buf);
        //SOCK_WBUFFER_CHECK(buf);

        ret = sy_spin_lock(&wbuf->lock);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        if (wbuf->closed) {
                ret = ECONNRESET;
                GOTO(err_lock, ret);
        }

        __sock_wbuffer_queue(wbuf, buf, flag);

        sy_spin_unlock(&wbuf->lock);

        return 0;
err_lock:
        sy_spin_unlock(&wbuf->lock);
err_ret:
        return ret;
}

int sock_wbuffer_isempty(sock_wbuffer_t *wbuf)
{
        int ret, empty;

        ret = sy_spin_lock(&wbuf->lock);
        if (unlikely(ret)) {
                UNIMPLEMENTED(__DUMP__);
        }

        empty = !wbuf->buf.len;

        sy_spin_unlock(&wbuf->lock);

        return empty;
}

int sock_wbuffer_destroy(sock_wbuffer_t *wbuf)
{
        int ret;

        ret = sy_spin_lock(&wbuf->lock);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        wbuf->closed = 1;

        if (wbuf->buf.len) {
                DWARN("left data %u\n", wbuf->buf.len);
        }

        mbuffer_free(&wbuf->buf);

        sy_spin_unlock(&wbuf->lock);

        return 0;
err_ret:
        return ret;
}

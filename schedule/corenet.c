
#include <limits.h>
#include <time.h>
#include <string.h>
#include <sys/epoll.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBSCHEDULE

#include "sysutil.h"
#include "net_proto.h"
#include "ylib.h"
#include "corenet.h"
#include "../net/xnect.h"
#include "net_table.h"
#include "rpc_table.h"
#include "core.h"
#include "corerpc.h"
#include "configure.h"
#include "net_global.h"
#include "job_dock.h"
#include "schedule.h"
#include "bh.h"
#include "timer.h"
#include "adt.h"
#include "variable.h"
#include "dbg.h"

static int __corenet_add(corenet_t *corenet, const sockid_t *sockid, void *ctx,
                         core_exec exec, func_t reset, func_t check, func_t recv);

static void *__corenet_get()
{
        return variable_get(VARIABLE_CORENET);
}

static void __corenet_set_out(corenet_node_t *node)
{
        int ret, event;
        event_t ev;
        corenet_t *__corenet__ = __corenet_get();

        if (node->ev & EPOLLOUT || node->send_buf.len == 0)
                return;
                
        _memset(&ev, 0x0, sizeof(struct epoll_event));

        event = node->ev | EPOLLOUT;
        ev.events = event;
        ev.data.fd = node->sockid.sd;

        DBUG("set sd %u epollfd %u\n", node->sockid.sd, __corenet__->epoll_fd);

        ret = _epoll_ctl(__corenet__->epoll_fd, EPOLL_CTL_MOD, node->sockid.sd, &ev);
        if (ret == -1) {
                ret = errno;
                YASSERT(0);
        }

        node->ev = event;
}

static void __corenet_unset_out(corenet_node_t *node)
{
        int ret, event;
        event_t ev;
        corenet_t *__corenet__ = __corenet_get();

        if (!(node->ev & EPOLLOUT) || node->send_buf.len) {
                return;
        }

        _memset(&ev, 0x0, sizeof(struct epoll_event));

        event = node->ev ^ EPOLLOUT;
        ev.data.fd = node->sockid.sd;
        ev.events = event;

        DBUG("unset sd %u epollfd %u\n", ev.data.fd, __corenet__->epoll_fd);

        ret = _epoll_ctl(__corenet__->epoll_fd, EPOLL_CTL_MOD, node->sockid.sd, &ev);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        node->ev = event;
}

static void __corenet_checklist_add(corenet_t *corenet, corenet_node_t *node)
{
        int ret;

        ret = sy_spin_lock(&corenet->lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        list_add_tail(&node->hook, &corenet->check_list);

        sy_spin_unlock(&corenet->lock);
}

static void __corenet_checklist_del(corenet_t *corenet, corenet_node_t *node)
{
        int ret;

        ret = sy_spin_lock(&corenet->lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        list_del(&node->hook);

        sy_spin_unlock(&corenet->lock);
}


static void __corenet_check_interval()
{
        int ret;
        time_t now;
        struct list_head *pos;
        corenet_node_t *node;
        corenet_t *__corenet__ = __corenet_get();

        now = gettime();
        if (now - __corenet__->last_check < 30) {
                return;
        }

        __corenet__->last_check  = now;

        DBUG("corenet check\n");

        ret = sy_spin_lock(&__corenet__->lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        list_for_each(pos, &__corenet__->check_list) {
                node = (void *)pos;
                node->check(node->ctx);
        }

        sy_spin_unlock(&__corenet__->lock);
}

static void __corenet_check_add()
{
        int ret;
        corenet_t *__corenet__ = __corenet_get();
        struct list_head *pos, *n, list;
        corenet_node_t *node;

        if (likely(list_empty(&__corenet__->add_list))) {
                return;
        }

        INIT_LIST_HEAD(&list);

        ret = sy_spin_lock(&__corenet__->lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        list_splice_init(&__corenet__->add_list, &list);

        sy_spin_unlock(&__corenet__->lock);


        list_for_each_safe(pos, n, &list) {
                node = (void *)pos;
                list_del(pos);

                DINFO("add sd %d\n", node->sockid.sd);
                
                ret = __corenet_add(__corenet__, &node->sockid, node->ctx, node->exec,
                                    node->reset, node->check, node->recv);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

                yfree((void **)&node);
        }
}

void corenet_check()
{
        __corenet_check_interval();
        __corenet_check_add();
}

static int __corenet_add(corenet_t *corenet, const sockid_t *sockid, void *ctx,
                core_exec exec, func_t reset, func_t check, func_t recv)
{
        int ret, event, sd;
        struct epoll_event ev;
        corenet_node_t *node;

        sd = sockid->sd;
        event = EPOLLIN;
        _memset(&ev, 0x0, sizeof(struct epoll_event));

        YASSERT(sd < 32768);
        node = &corenet->array[sd];

        if (node->ev & event) {
                ret = EEXIST;
                GOTO(err_ret, ret);
        }

        DINFO("add sd %d, ev %o:%o\n", sd, node->ev, event);

        YASSERT((event & EPOLLOUT) == 0);

        if (check) {
                __corenet_checklist_add(corenet, node);
        } else {
                INIT_LIST_HEAD(&node->hook);
        }

        node->ev = event;
        node->ctx = ctx;
        node->exec = exec;
        node->reset = reset;
        node->recv = recv;
        node->check = check;
        node->sockid = *sockid;

        ev.data.fd = sd;
        ev.events = event;
        ret = _epoll_ctl(corenet->epoll_fd, EPOLL_CTL_ADD, sd, &ev);
        if (ret == -1) {
                ret = errno;
                UNIMPLEMENTED(__DUMP__);//remove checklist
        }

        return 0;
err_ret:
        return ret;
}

int corenet_add(corenet_t *corenet, const sockid_t *sockid, void *ctx,
                core_exec exec, func_t reset, func_t check, func_t recv)
{
        int ret;
        corenet_node_t *node;

        YASSERT(sockid->addr);
        YASSERT(sockid->type == SOCKID_CORENET);

        if (corenet) {
                ret = ymalloc((void **)&node, sizeof(*node));
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                node->sockid = *sockid;
                node->ctx = ctx;
                node->exec = exec;
                node->reset = reset;
                node->check = check;
                node->recv = recv;

                ret = sy_spin_lock(&corenet->lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                list_add_tail(&node->hook, &corenet->add_list);

                sy_spin_unlock(&corenet->lock);
        } else {
                ret = __corenet_add(__corenet_get(), sockid, ctx, exec, reset, check, recv);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

static void __corenet_close(corenet_node_t *node)
{
        int ret, sd;
        event_t ev;
        corenet_t *__corenet__ = __corenet_get();

        sd = node->sockid.sd;

        DINFO("close %d\n", sd);

        if (node->ev) {
                ev.data.fd = sd;
                ev.events = node->ev;
                ret = _epoll_ctl(__corenet__->epoll_fd, EPOLL_CTL_DEL, sd, &ev);
                if (ret == -1) {
                        ret = errno;
                        DERROR("epoll del %d %d\n", sd, node->ev);
                }
        }

        if (node->reset)
                node->reset(node->ctx);

#if ENABLE_CORERPC
        corerpc_reset(&node->sockid);
#endif

        close(node->sockid.sd);
        mbuffer_free(&node->recv_buf);
        mbuffer_free(&node->send_buf);

        if (!list_empty(&node->hook)) {
                __corenet_checklist_del(__corenet__, node);
        }

        node->ev = 0;
        node->ctx = NULL;
        node->exec = NULL;
        node->reset = NULL;
        node->recv = NULL;
        node->sockid.sd = -1;
}

static void __corenet_exit(corenet_node_t *node)
{
        __corenet_close(node);
}

void corenet_close(const sockid_t *sockid)
{
        corenet_node_t *node;
        corenet_t *__corenet__ = __corenet_get();

        node = &__corenet__->array[sockid->sd];
        __corenet_close(node);
}


static int __core_recv__(corenet_node_t *node, int toread)
{
        int ret, iov_count;
        struct msghdr msg;
        buffer_t buf;
        corenet_t *__corenet__ = __corenet_get();

        ret = mbuffer_init(&buf, toread);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        YASSERT(buf.len <= CORE_IOV_MAX * PAGE_SIZE);

        iov_count = CORE_IOV_MAX;
        ret = mbuffer_trans(__corenet__->iov, &iov_count,  &buf);
        YASSERT(ret == (int)buf.len);
        memset(&msg, 0x0, sizeof(msg));
        msg.msg_iov = __corenet__->iov;
        msg.msg_iovlen = iov_count;

        DBUG("read data %u\n", toread);

        ret = _recvmsg(node->sockid.sd, &msg, MSG_DONTWAIT);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_free, ret);
        }

        YASSERT(ret == (int)buf.len);
        mbuffer_merge(&node->recv_buf, &buf);
        DBUG("new recv %u, left %u\n", buf.len, node->recv_buf.len);

        return 0;
err_free:
        mbuffer_free(&buf);
err_ret:
        return ret;
}

static int __corenet_recv(corenet_node_t *node, int *count)
{
        int ret, toread, left, cp;

        ret = ioctl(node->sockid.sd, FIONREAD, &toread);
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
                cp = left > PAGE_SIZE * CORE_IOV_MAX ? PAGE_SIZE * CORE_IOV_MAX : left;

                if (toread > PAGE_SIZE * CORE_IOV_MAX) {
                        DINFO("long msg, total %u, left %u, read %u\n", toread, left, cp);
                }

                ret = __core_recv__(node, cp);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                left -= cp;
        }

        ret = node->exec(node->ctx, &node->recv_buf, count);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __corenet_send(corenet_node_t *node)
{
        int ret, iov_count;
        struct msghdr msg;
        buffer_t *buf;
        corenet_t *__corenet__ = __corenet_get();

        buf = &node->send_buf;
        while (buf->len) {
                memset(&msg, 0x0, sizeof(msg));
                DBUG("send %u\n", buf->len);
                iov_count = CORE_IOV_MAX;
                ret = mbuffer_trans(__corenet__->iov, &iov_count,  buf);

                msg.msg_iov = __corenet__->iov;
                msg.msg_iovlen = iov_count;
                ret = sendmsg(node->sockid.sd, &msg, MSG_DONTWAIT);
                if (ret < 0) {
                        ret = errno;
                        mbuffer_free(buf);
                        DWARN("forward to %s @ %u fail\n",
                              _inet_ntoa(node->sockid.addr), node->sockid.sd);
                        GOTO(err_ret, ret);
                }

                //DBUG("%s send data %u\n", node->name, ret);
                YASSERT(ret <= (int)buf->len);
                mbuffer_pop(buf, NULL, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int  __corenet_exec(corenet_node_t *node, event_t *ev)
{
        int ret, recv_msg;

        YASSERT(node->ev);
        YASSERT(node->sockid.sd != -1);

        DBUG("ev %x\n", ev->events);

        if ((ev->events & EPOLLRDHUP) || (ev->events & EPOLLERR)
            || (ev->events & EPOLLHUP))  {
                ret = ECONNRESET;
                GOTO(err_ret, ret);
        }

        if (ev->events & EPOLLIN) {
                if (node->recv) {
                        node->recv(&node->sockid.sd);
                } else {
                        ret = __corenet_recv(node, &recv_msg);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);
                }
        }

        if (ev->events & EPOLLOUT) {
                ret = __corenet_send(node);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                __corenet_unset_out(node);
        }

        return 0;
err_ret:
        __corenet_exit(node);
        return ret;
}

int corenet_poll(int tmo)
{
        int ret, nfds, i;
        event_t events[512], *ev;
        corenet_node_t *node;
        corenet_t *__corenet__ = __corenet_get();

        nfds = _epoll_wait(__corenet__->epoll_fd, events, 512, tmo);
        if (nfds < 0) {
                ret = -nfds;
                GOTO(err_ret, ret);
        }

        for (i = 0; i < nfds; i++) {
                ANALYSIS_BEGIN(0);
                ev = &events[i];

                node = &__corenet__->array[ev->data.fd];
                __corenet_exec(node, ev);
                ANALYSIS_QUEUE(0, 1000 * 1000, "corenet_poll");
        }

        return 0;
err_ret:
        return ret;
}

typedef struct {
        struct list_head hook;
        sockid_t sockid;
        buffer_t buf;
} corenet_fwd_t;

static void __corenet_queue(corenet_t *__corenet__, const sockid_t *sockid, buffer_t *buf, int flag)
{
        int found = 0;
        corenet_fwd_t *corenet_fwd;
        struct list_head *pos;

        DBUG("corenet_fwd\n");

        list_for_each(pos, &__corenet__->forward_list) {
                corenet_fwd = (void *)pos;

                if (sockid_cmp(sockid, &corenet_fwd->sockid) == 0) {
                        DBUG("append forward to %s @ %u\n",
                              _inet_ntoa(sockid->addr), sockid->sd);

                        if (flag & BUFFER_KEEP) {
                                mbuffer_reference(&corenet_fwd->buf, buf);
                        } else {
                                mbuffer_merge(&corenet_fwd->buf, buf);
                        }

                        found = 1;
                        break;
                }
        }

        if (found == 0) {
                DBUG("new forward to %s @ %u\n",
                      _inet_ntoa(sockid->addr), sockid->sd);

#ifdef HAVE_STATIC_ASSERT
                static_assert(sizeof(*corenet_fwd)  < sizeof(mem_cache64_t), "corenet_fwd_t");
#endif

                corenet_fwd = mem_cache_calloc(MEM_CACHE_64, 0);
                YASSERT(corenet_fwd); 
                corenet_fwd->sockid = *sockid;
                mbuffer_init(&corenet_fwd->buf, 0);

                if (flag & BUFFER_KEEP) {
                        mbuffer_reference(&corenet_fwd->buf, buf);
                } else {
                        mbuffer_merge(&corenet_fwd->buf, buf);
                }

                list_add_tail(&corenet_fwd->hook, &__corenet__->forward_list);
        }
}

int corenet_send(const sockid_t *sockid, buffer_t *buf, int flag)
{
        int ret;
        corenet_node_t *node;
        corenet_t *__corenet__ = __corenet_get();

        YASSERT(sockid->type == SOCKID_CORENET);
        YASSERT(sockid->addr);

        node = &__corenet__->array[sockid->sd];
        if (node->sockid.seq != sockid->seq || node->sockid.sd == -1) {
                ret = ECONNRESET;
                GOTO(err_ret, ret);
        }

        __corenet_queue(__corenet__, sockid, buf, flag);

        return 0;
err_ret:
        return ret;
}


static int __corenet_commit(const sockid_t *sockid, buffer_t *buf)
{
        int ret;
        corenet_node_t *node;
        corenet_t *__corenet__ = __corenet_get();

        YASSERT(sockid->type == SOCKID_CORENET);
        YASSERT(sockid->addr);

        node = &__corenet__->array[sockid->sd];
        if (node->sockid.seq != sockid->seq || node->sockid.sd == -1) {
                ret = ECONNRESET;
                mbuffer_free(buf);
                GOTO(err_ret, ret);
        }

        mbuffer_merge(&node->send_buf, buf);

        (void) __corenet_send(node);

        if (!list_empty(&node->send_buf.list)) {
                __corenet_set_out(node);
        }

        return 0;
err_ret:
        return ret;
}

void corenet_commit()
{
        struct list_head *pos, *n;
        corenet_fwd_t *corenet_fwd;
        corenet_t *__corenet__ = __corenet_get();

        list_for_each_safe(pos, n, &__corenet__->forward_list) {
                corenet_fwd = (void *)pos;
                list_del(pos);

                DBUG("forward to %s @ %u\n",
                     _inet_ntoa(corenet_fwd->sockid.addr), corenet_fwd->sockid.sd);

                __corenet_commit(&corenet_fwd->sockid, &corenet_fwd->buf);

                mem_cache_free(MEM_CACHE_64, corenet_fwd);
        }
}

int corenet_connected(const sockid_t *sockid)
{
        int ret;
        corenet_node_t *node;
        corenet_t *__corenet__ = __corenet_get();

        node = &__corenet__->array[sockid->sd];
        if (node->sockid.seq != sockid->seq || node->sockid.sd == -1) {
                ret = ECONNRESET;
                GOTO(err_ret, ret);
        }

        return 1;
err_ret:
        return 0;
}

int corenet_init(int max, corenet_t **_corenet)
{
        int ret, len, i, size;
        corenet_t *corenet;
        corenet_node_t *node;

        size = max;

        DINFO("malloc %llu\n", (LLU)sizeof(corenet_node_t) * size);
        len = sizeof(corenet_t) + sizeof(corenet_node_t) * size;
        ret = ymalloc((void **)&corenet, len);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DBUG("corenet use %u\n", len);

        _memset(corenet, 0x0, len);
        corenet->size = size;

        for (i = 0; i < size; i++) {
                node = &corenet->array[i];

                mbuffer_init(&node->recv_buf, 0);
                mbuffer_init(&node->send_buf, 0);
                node->sockid.sd = -1;
        }

        corenet->epoll_fd = epoll_create(corenet->size);
        if (corenet->epoll_fd == -1) {
                ret = errno;
                GOTO(err_free, ret);
        }

        INIT_LIST_HEAD(&corenet->forward_list);
        INIT_LIST_HEAD(&corenet->check_list);
        INIT_LIST_HEAD(&corenet->add_list);

        ret = sy_spin_init(&corenet->lock);
        if (unlikely(ret))
                GOTO(err_free, ret);


        variable_set(VARIABLE_CORENET, corenet);
        if (_corenet)
                *_corenet = corenet;

        DBUG("corenet init done\n");

	return 0;
err_free:
        yfree((void **)&corenet);
err_ret:
        return ret;
}

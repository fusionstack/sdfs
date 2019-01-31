#include <limits.h>
#include <time.h>
#include <string.h>
#include <sys/epoll.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "sysutil.h"
#include "net_proto.h"
#include "ylib.h"
#include "sock_tcp.h"
#include "ynet_sock.h"
#include "sdevent.h"
#include "../net/xnect.h"
#include "net_table.h"
#include "rpc_table.h"
#include "main_loop.h"
#include "configure.h"
#include "job_dock.h"
#include "schedule.h"
#include "bh.h"
#include "timer.h"
#include "adt.h"
#include "dbg.h"

typedef struct {
        int ev;
        ynet_sock_conn_t *sock;
        uint64_t heartbeat_seq_send;
        uint64_t heartbeat_seq_reply;
        sy_rwlock_t lock;
        void *ctx;
        func_t reset;
        sy_spinlock_t evlock;
} event_node_t;

typedef struct {
        int epoll_wfd;     // 只监听EPOLL_OUT
	int size;
        event_node_t array[0];
} sdevent_t;

static uint32_t sock_seq = 1;
static sdevent_t *sdevent;

#define EPOLL_TMO 30

static void *__sdevent_worker_writer(void *_args);

static void __sdevent_set_out(event_node_t *node)
{
        int ret, sd;
        event_t ev;

        ret = sy_spin_lock(&node->evlock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        YASSERT(node->sock);

        if (!(node->ev & EPOLLOUT) && !sock_wbuffer_isempty(&node->sock->wbuf)) {
                _memset(&ev, 0x0, sizeof(struct epoll_event));

                sd = node->sock->nh.u.sd.sd;
                ev.events = EPOLLOUT;// | EPOLLONESHOT;
                ev.data.fd = sd;

                DBUG("set sd %u epollfd %u\n", sd, sdevent->epoll_wfd);

                ret = _epoll_ctl(sdevent->epoll_wfd,
                                 EPOLL_CTL_ADD, sd, &ev);
                if (ret == -1) {
                        ret = errno;
                        DWARN("%d\n", ret);
                        if (ret != EEXIST)
                                YASSERT(0);
                }
                
                node->ev = node->ev | EPOLLOUT;
        }

        sy_spin_unlock(&node->evlock);
}

static void __sdevent_unset_out(event_node_t *node)
{
        int ret;
        ynet_sock_conn_t *sock;
        event_t ev;

        ret = sy_spin_lock(&node->evlock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        YASSERT(node->sock);

        sock = node->sock;

        if (sock_wbuffer_isempty(&sock->wbuf)) {
                if (node->ev & EPOLLOUT) {
                        _memset(&ev, 0x0, sizeof(struct epoll_event));
                        ev.data.fd = sock->nh.u.sd.sd;
                        ev.events = 0;

                        DBUG("unset sd %u epollfd %u\n", ev.data.fd, sdevent->epoll_wfd);

                        ret = _epoll_ctl(sdevent->epoll_wfd,
                                         EPOLL_CTL_DEL, sock->nh.u.sd.sd, &ev);
                        if (unlikely(ret))
                                UNIMPLEMENTED(__DUMP__);

                        node->ev = node->ev ^ EPOLLOUT;
                } else
                        DERROR("epoll del %d\n", sock->nh.u.sd.sd);
        }

        sy_spin_unlock(&node->evlock);
}

static int __sdevent_rdlock(const net_handle_t *nh, event_node_t **_node)
{
        int ret;
        event_node_t *node;

        YASSERT(nh->u.sd.type == SOCKID_NORMAL);

        node = &sdevent->array[nh->u.sd.sd];

        ret = sy_rwlock_rdlock(&node->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (node->sock == NULL || !node->sock->used
            || node->sock->nh.u.sd.seq != nh->u.sd.seq) {
                ret = ECONNRESET;
                GOTO(err_lock, ret);
        }

        *_node = node;

        return 0;
err_lock:
        sy_rwlock_unlock(&node->lock);
err_ret:
        return ret;
}

static int __sdevent_tryrdlock(const net_handle_t *nh, event_node_t **_node)
{
        int ret;
        event_node_t *node;

        YASSERT(nh->u.sd.type == SOCKID_NORMAL);

        node = &sdevent->array[nh->u.sd.sd];

        ret = sy_rwlock_tryrdlock(&node->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (node->sock == NULL || !node->sock->used
            || node->sock->nh.u.sd.seq != nh->u.sd.seq) {
                ret = ECONNRESET;
                GOTO(err_lock, ret);
        }

        *_node = node;

        return 0;
err_lock:
        sy_rwlock_unlock(&node->lock);
err_ret:
        return ret;
}

static int __sdevent_wrlock(const net_handle_t *nh, event_node_t **_node)
{
        int ret;
        event_node_t *node;

        YASSERT(nh->u.sd.type == SOCKID_NORMAL);

        node = &sdevent->array[nh->u.sd.sd];

        ret = sy_rwlock_wrlock(&node->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (node->sock == NULL || !node->sock->used
            || node->sock->nh.u.sd.seq != nh->u.sd.seq) {
                ret = ECONNRESET;
                GOTO(err_lock, ret);
        }

        *_node = node;

        return 0;
err_lock:
        sy_rwlock_unlock(&node->lock);
err_ret:
        return ret;
}

static int __sdevent_rdlock1(event_node_t *node, int force)
{
        int ret;

        if (force) {
                ret = sy_rwlock_rdlock(&node->lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        } else {
                ret = sy_rwlock_tryrdlock(&node->lock);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        }

        if (node->sock == NULL || !node->sock->used) {
                ret = ECONNRESET;
                GOTO(err_lock, ret);
        }

        return 0;
err_lock:
        sy_rwlock_unlock(&node->lock);
err_ret:
        return ret;
}

static int __sdevent_wrlock1(event_node_t *node, int force)
{
        int ret;

        if (force) {
                ret = sy_rwlock_wrlock(&node->lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        } else {
                ret = sy_rwlock_trywrlock(&node->lock);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        }

        if (node->sock == NULL || !node->sock->used) {
                ret = ECONNRESET;
                GOTO(err_lock, ret);
        }

        return 0;
err_lock:
        sy_rwlock_unlock(&node->lock);
err_ret:
        return ret;
}


static int __sdevent_unlock(event_node_t *node)
{
        sy_rwlock_unlock(&node->lock);
        return 0;
}

static int __sdevent_close_nolock(event_node_t *node)
{
        int ret, sd, event;
        event_t ev;

        sd = node->sock->nh.u.sd.sd;

        if (node->ev & EPOLLOUT) {
                ev.data.fd = sd;
                ev.events = EPOLLOUT;
                ret = _epoll_ctl(sdevent->epoll_wfd, EPOLL_CTL_DEL, sd, &ev);
                if (ret == -1) {
                        ret = errno;
                        DERROR("epoll del %d %d\n", sd, node->ev);
                }
        }

        if (node->ev & EPOLLOUT)
                event = node->ev ^ EPOLLOUT;
        else
                event = node->ev;

        if (node->ev) {
                ret = main_loop_event(sd, event, EPOLL_CTL_DEL);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);
        }

        node->ev = 0;

        DBUG("close sock %u\n", node->sock->nh.u.sd.sd);

        ret = sock_close(node->sock);
        if (unlikely(ret)) {
                DERROR("close fail\n");
                GOTO(err_ret, ret);
        }

        if (node->ctx) {
                yfree((void **)&node->ctx);
        }

        yfree((void **)&node->sock);
        node->sock = NULL;

        return 0;
err_ret:
        return ret;
}

static void __sdevent_close(const net_handle_t *nh)
{
        int ret;
        event_node_t *node;

        YASSERT(nh->type == NET_HANDLE_TRANSIENT);

        ret = __sdevent_wrlock(nh, &node);
        if (unlikely(ret))
                return;

        __sdevent_close_nolock(node);

        __sdevent_unlock(node);
}

static void __sdevent_exit(event_node_t *node, int force)
{
        int ret;
        net_handle_t parent, socknh;
        char name[MAX_NAME_LEN];
        void *ctx;
        void (*callback)(void *);

        memset(&parent, 0x0, sizeof(net_handle_t));
        memset(&socknh, 0x0, sizeof(net_handle_t));

        ret = __sdevent_wrlock1(node, force);
        if (unlikely(ret))
                return;

        socknh = node->sock->nh;
        //parent = node->sock->parent;
        callback = node->reset;
        ctx = node->ctx;
        node->ctx = NULL;

        sprintf(name, "%s", _inet_ntoa(node->sock->nh.u.sd.addr));

        __sdevent_close_nolock(node);

        __sdevent_unlock(node);

        DBUG("close %u\n", socknh.u.sd.sd);

        if (ctx) {
                YASSERT(callback);

                if (schedule_self()) {
                        schedule_task_new("netable_close", callback, ctx, -1);
                } else {
                        callback(ctx);
                }
        } else {
                rpc_table_reset(__rpc_table__, &socknh.u.sd, NULL);
                //jobdock_setmo(&socknh.u.sd, name);
        }
}

static int __sdevent_read_recv(event_node_t *node)
{
        int ret;
        ynet_sock_conn_t *conn;

        YASSERT(!schedule_running());

        ret = __sdevent_rdlock1(node, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        conn = node->sock;
        ret = conn->proto.reader(conn, node->ctx);
        if (unlikely(ret)) {
                goto err_lock;
        }

        __sdevent_unlock(node);

        return 0;
err_lock:
        __sdevent_unlock(node);
err_ret:
        return ret;
}

static int __sdevent_exec_write(event_node_t *node, event_t *ev)
{
        int ret;
        ynet_sock_conn_t *sock;
        int64_t sent = 0;

        ret = __sdevent_rdlock1(node, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if ((ev->events & EPOLLRDHUP) || (ev->events & EPOLLERR)
            || (ev->events & EPOLLHUP))  {
                ret = ECONNRESET;
                GOTO(err_lock, ret);
        }

        YASSERT(ev->events == EPOLLOUT);

        DBUG("write event from sd %u\n", ev->data.fd);

        ANALYSIS_BEGIN(0);

        sock = node->sock;
        sent = sock->proto.writer(ev, sock);
        if (sent < 0) {
                ret = -sent;
                if (ret == EAGAIN) {
                }  else {
                        DWARN("send error %d\n", ret);
                        ret = ECONNRESET;
                }

                GOTO(err_lock, ret);
        }

        ANALYSIS_END(0, 1000 * 100, NULL);

        __sdevent_unset_out(node);

        __sdevent_unlock(node);

        return 0;
err_lock:
        __sdevent_unlock(node);
err_ret:
        return ret;
}

static void *__sdevent_worker_writer(void *_args)
{
        int ret, epollfd, nfds, i;
        void *ptr;
        event_t *ev, *events;
        event_node_t *node;
        sdevent_t *sdevent = _args;

        epollfd = sdevent->epoll_wfd;

        ret = ymalloc((void **)&ptr, sizeof(event_t) * sdevent->size);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        _memset(ptr, 0x0, sizeof(event_t) * sdevent->size);

        events = ptr;

        while (1) {
                DBUG("running epoll_fd %u\n", epollfd);

                nfds = _epoll_wait(epollfd, events, sdevent->size, EPOLL_TMO * 1000);
                if (nfds < 0) {
                        ret = -nfds;
                        YASSERT(0);
                }

                DBUG("new event %d\n", nfds);

                for (i = 0; i < nfds; i++) {
                        ev = &events[i];

                        node = &sdevent->array[ev->data.fd];

                        ret = __sdevent_exec_write(node, ev);
                        if (ret == ECONNRESET) {
                                __sdevent_exit(node, 1);
                        }
                }
        }

        yfree((void **)&ptr);

        return NULL;
err_ret:
        return NULL;
}

static int __sdevent_worker_init(sdevent_t *sdevent)
{
        int ret;

        sdevent->epoll_wfd = epoll_create(sdevent->size);
        if (sdevent->epoll_wfd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = sy_thread_create2(__sdevent_worker_writer, sdevent, "__sdevent_worker_writer");
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        DINFO("worker inited\n");

        return 0;
err_ret:
        return ret;
}

int sdevent_add(const net_handle_t *socknh, const nid_t *nid, int event,
                void *ctx, func_t reset)
{
        int ret, sd;
        struct epoll_event ev;
        event_node_t *node;

        YASSERT(socknh->type == NET_HANDLE_TRANSIENT);

        sd = socknh->u.sd.sd;

        DBUG("add sd %d\n", sd);

        _memset(&ev, 0x0, sizeof(struct epoll_event));

        ret = __sdevent_wrlock(socknh, &node);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (node->ev & event) {
                ret = EEXIST;
                GOTO(err_lock, ret);
        }

        if (nid) {
                node->sock->nid = &node->sock->__nid__;
                *node->sock->nid = *nid;
        } else {
                node->sock->nid = NULL;
        }
        
        DBUG("ev %o:%o\n", node->ev, event);

        YASSERT((event & EPOLLOUT) == 0);

        ret = main_loop_event(sd, event, EPOLL_CTL_ADD);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        node->ev |= event;
        node->ctx = ctx;
        node->reset = reset;

        __sdevent_unlock(node);

        DBUG("add sd %d done\n", sd);

        return 0;

err_lock:
        __sdevent_unlock(node);
err_ret:
        return ret;
}

int sdevent_open(net_handle_t *nh, const net_proto_t *proto)
{
        int ret;
        event_node_t *node;

        YASSERT(nh->u.sd.sd > 0 && nh->u.sd.sd < sdevent->size);

        node = &sdevent->array[nh->u.sd.sd];

        ret = sy_rwlock_wrlock(&node->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        YASSERT(node->sock == NULL);

        ret = ymalloc((void **)&node->sock, sizeof(*node->sock));
        if (unlikely(ret))
                GOTO(err_lock, ret);

        ret = sock_init(node->sock, NULL);
        if (unlikely(ret))
                GOTO(err_free, ret);

        ret = sy_spin_init(&node->evlock);
        if (unlikely(ret))
                GOTO(err_free, ret);

        node->sock->used = 1;
        node->sock->proto = *proto;
        node->sock->nh = *nh;
        node->sock->nh.u.sd.seq = sock_seq ++;
        node->sock->nh.u.sd.type = SOCKID_NORMAL;
        node->sock->nh.type = NET_HANDLE_TRANSIENT;
        *nh = node->sock->nh;
        node->heartbeat_seq_send = 0;
        node->heartbeat_seq_reply = 0;

        __sdevent_unlock(node);

        return 0;
err_free:
        yfree((void **)&node->sock);
        node->sock = NULL;
err_lock:
        __sdevent_unlock(node);
err_ret:
        return ret;
}

int sdevent_accept(int sd, net_handle_t *nh, const net_proto_t *proto, int nonblock)
{
        int ret;

        ret = sock_accept(nh, sd, YNET_RPC_TUNNING, nonblock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sdevent_open(nh, proto);
        if (unlikely(ret))
                GOTO(err_close, ret);

        return 0;
err_close:
        close(nh->u.sd.sd);
err_ret:
        return ret;
}

int sdevent_connect(const ynet_sock_info_t *info,
                    net_handle_t *nh, net_proto_t *proto, int nonblock, int timeout)
{
        int ret;

        ret = sock_info2sock(nh, info, nonblock, timeout);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        ret = sdevent_open(nh, proto);
        if (unlikely(ret))
                GOTO(err_close, ret);

        return 0;
err_close:
        close(nh->u.sd.sd);
err_ret:
        return ret;
}

void sdevent_close_force(const net_handle_t *nh)
{
        __sdevent_close(nh);
}

static int __sdevent_queue__(event_node_t *node, const net_handle_t *nh, const buffer_t *buf, int flag)
{
        int ret, sd, epollout = 0;

        sd = nh->u.sd.sd;

        if (node->ev == 0) {
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }

        ret = sock_wbuffer_queue(sd, &node->sock->wbuf, buf, flag);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        if (!(node->ev & EPOLLOUT)) {
                DBUG("add epollout %u %u %u\n", epollout,
                     node->ev & EPOLLOUT, sock_wbuffer_isempty(&node->sock->wbuf));

                __sdevent_set_out(node);
        }

        return 0;
err_ret:
        return ret;
}

STATIC int __sdevent_queue(const net_handle_t *nh, const buffer_t *buf, int flag)
{
        int ret;
        event_node_t *node;

        YASSERT(nh->type == NET_HANDLE_TRANSIENT);
        //YASSERT(nh->u.sd.addr);
        YASSERT(nh->u.sd.sd > 0 && nh->u.sd.sd < sdevent->size);

#if 0
        ynet_net_head_t *head;
        head = mbuffer_head(buf);
        YASSERT(head->magic == YNET_PROTO_TCP_MAGIC);
#endif
        
        ret = __sdevent_tryrdlock(nh, &node);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __sdevent_queue__(node, nh, buf, flag);
        if (unlikely(ret)) {
                GOTO(err_lock, ret);
        }

        __sdevent_unlock(node);

        return 0;
err_lock:
        __sdevent_unlock(node);
err_ret:
        return ret;
}

int sdevent_queue(const net_handle_t *nh, const buffer_t *buf, int flag)
{
        int ret;

        ret = __sdevent_queue(nh, buf, flag);
        if (unlikely(ret)) {
                if (ret == EAGAIN || ret == EBUSY) {
                        ret = ECONNRESET;
                        DWARN("send %u fail\n",nh->u.sd.sd);
                }

                GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}

void sdevent_destroy()
{
        UNIMPLEMENTED(__WARN__);
#if 0
        int ret, i;
        event_group_t *group;

        running = 0;

        for (i = 0; i < sdevent->threads; i++) {
                group = &sdevent->group[i];

                ret = _sem_wait(&group->destroy_sem);
                if (unlikely(ret))
                        YASSERT(0);
        }
        for (i = 0; i < sdevent->size; i++) {
                node = &sdevent->array[i];

                if (node->sock) {
                        DINFO("close %u\n", node->sock->nh.u.sd.sd);
                        sock_close(node->sock);
                }
        }
#endif
}

int sdevent_session(const sockid_t *id,  func1_t func, void *arg)
{
        int ret;
        event_node_t *node;
        net_handle_t nh;

        sock2nh(&nh, id);
        ret = __sdevent_rdlock(&nh, &node);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        func(node->ctx, arg);

        __sdevent_unlock(node);

        return 0;
err_ret:
        return ret;
}

#if 1
int sdevent_check(const sockid_t *id)
{
        int ret;
        event_node_t *node;
        net_handle_t nh;

        sock2nh(&nh, id);
        ret = __sdevent_rdlock(&nh, &node);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        __sdevent_unlock(node);

        return 0;
err_ret:
        return ret;
}
#endif

void sdevent_exit(int fd)
{
        event_node_t *node;
        node = &sdevent->array[fd];
        __sdevent_exit(node, 0);
}


int sdevent_recv(int fd)
{
        event_node_t *node;
        node = &sdevent->array[fd];
        return __sdevent_read_recv(node);
}

int sdevent_heartbeat_set(const sockid_t *id, const uint64_t *send, const uint64_t *reply)
{
        int ret;
        event_node_t *node;
        net_handle_t nh;

        sock2nh(&nh, id);
        ret = __sdevent_rdlock(&nh, &node);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (send) {
                node->heartbeat_seq_send = *send;
        }

        if (reply) {
                node->heartbeat_seq_reply = *reply;
        }

        __sdevent_unlock(node);

        return 0;
err_ret:
        return ret;
}

int sdevent_heartbeat_get(const sockid_t *id, uint64_t *send, uint64_t *reply)
{
        int ret;
        event_node_t *node;
        net_handle_t nh;

        sock2nh(&nh, id);
        ret = __sdevent_rdlock(&nh, &node);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (send) {
                *send = node->heartbeat_seq_send;
        }

        if (reply) {
                *reply = node->heartbeat_seq_reply;
        }

        __sdevent_unlock(node);

        return 0;
err_ret:
        return ret;
}

int sdevent_init(int max)
{
        int ret, len, i, size;
        sdevent_t *ev;

        size = max;

        DINFO("malloc %lu\n", sizeof(event_node_t) * size);

        len = sizeof(sdevent_t) + sizeof(event_node_t) * size;
        ret = ymalloc((void **)&ev, len);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DBUG("sdevent use %u\n", len);

        _memset(ev, 0x0, len);
        ev->size = size;

        for (i = 0; i < size; i++) {
                ret = sy_rwlock_init(&ev->array[i].lock, "sdevent.lock");
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ev->array[i].sock = NULL;
        }

        ret = __sdevent_worker_init(ev);
        if (unlikely(ret))
                GOTO(err_free, ret);

        sdevent = ev;

        DBUG("sdevent init done\n");

	return 0;
err_free:
        yfree((void **)&ev);
err_ret:
        return ret;
}

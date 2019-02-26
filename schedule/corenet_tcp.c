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
#include "timer.h"
#include "adt.h"
#include "mem_cache.h"
#include "variable.h"
#include "dbg.h"

extern int nofile_max;

#define __OP_SEND__ 1
#define __OP_RECV__ 2

typedef corenet_tcp_node_t corenet_node_t;

#if ENABLE_TCP_THREAD

static int __corenet_tcp_wrlock(corenet_node_t *node);
static int __corenet_tcp_rdlock(corenet_node_t *node);
static int __corenet_tcp_rwlock_init(corenet_node_t *node);
static void __corenet_tcp_unlock(corenet_node_t *node);
static int __corenet_tcp_remote(int fd, buffer_t *buf, int op);

#endif

static int __corenet_add(corenet_tcp_t *corenet, const sockid_t *sockid, void *ctx,
                         core_exec exec, func_t reset, func_t check, func_t recv, const char *name);

static void *__corenet_get()
{
        return variable_get(VARIABLE_CORENET_TCP);
}

static void *__corenet_get_byctx(void *ctx)
{
        return variable_get_byctx(ctx, VARIABLE_CORENET_TCP);
}

static void __corenet_set_out(corenet_node_t *node)
{
        int ret, event;
        event_t ev;
        corenet_tcp_t *__corenet__ = __corenet_get();

        if ((node->ev & EPOLLOUT || node->send_buf.len == 0)
            || !(node->ev & EPOLLIN))
                return;
                
        _memset(&ev, 0x0, sizeof(struct epoll_event));

        event = node->ev | EPOLLOUT;
        ev.events = event;
        ev.data.fd = node->sockid.sd;

        DBUG("set sd %u epollfd %u\n", node->sockid.sd, __corenet__->corenet.epoll_fd);

        ret = _epoll_ctl(__corenet__->corenet.epoll_fd, EPOLL_CTL_MOD, node->sockid.sd, &ev);
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
        corenet_tcp_t *__corenet__ = __corenet_get();

        if (!(node->ev & EPOLLOUT) || node->send_buf.len) {
                return;
        }

        _memset(&ev, 0x0, sizeof(struct epoll_event));

        event = node->ev ^ EPOLLOUT;
        ev.data.fd = node->sockid.sd;
        ev.events = event;

        DBUG("unset sd %u epollfd %u\n", ev.data.fd, __corenet__->corenet.epoll_fd);

        ret = _epoll_ctl(__corenet__->corenet.epoll_fd, EPOLL_CTL_MOD, node->sockid.sd, &ev);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        node->ev = event;
}

static void __corenet_checklist_add(corenet_tcp_t *corenet, corenet_node_t *node)
{
        int ret;

        ret = sy_spin_lock(&corenet->corenet.lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        list_add_tail(&node->hook, &corenet->corenet.check_list);

        sy_spin_unlock(&corenet->corenet.lock);
}

static void __corenet_checklist_del(corenet_tcp_t *corenet, corenet_node_t *node)
{
        int ret;

        ret = sy_spin_lock(&corenet->corenet.lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        list_del(&node->hook);

        sy_spin_unlock(&corenet->corenet.lock);
}


static void __corenet_check_interval()
{
        int ret;
        time_t now;
        struct list_head *pos;
        corenet_node_t *node;
        corenet_tcp_t *__corenet__ = __corenet_get();

        now = gettime();
        if (likely(now - __corenet__->corenet.last_check < 30)) {
                return;
        }

        __corenet__->corenet.last_check  = now;

        DBUG("corenet check\n");

        ret = sy_spin_lock(&__corenet__->corenet.lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        list_for_each(pos, &__corenet__->corenet.check_list) {
                node = (void *)pos;
                node->check(node->ctx);
        }

        sy_spin_unlock(&__corenet__->corenet.lock);
}

static void __corenet_check_add()
{
        int ret;
        corenet_tcp_t *__corenet__ = __corenet_get();
        struct list_head *pos, *n, list;
        corenet_node_t *node;

        if (likely(list_empty(&__corenet__->corenet.add_list))) {
                return;
        }

        INIT_LIST_HEAD(&list);

        ret = sy_spin_lock(&__corenet__->corenet.lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        list_splice_init(&__corenet__->corenet.add_list, &list);

        sy_spin_unlock(&__corenet__->corenet.lock);


        list_for_each_safe(pos, n, &list) {
                node = (void *)pos;
                list_del(pos);

                DINFO("add sd %d\n", node->sockid.sd);
                
                ret = __corenet_add(__corenet__, &node->sockid, node->ctx, node->exec,
                                    node->reset, node->check, node->recv, node->name);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

                yfree((void **)&node);
        }
}

void corenet_tcp_check()
{
        __corenet_check_interval();
        __corenet_check_add();
}

static int __corenet_add(corenet_tcp_t *corenet, const sockid_t *sockid, void *ctx,
                core_exec exec, func_t reset, func_t check, func_t recv, const char *name)
{
        int ret, event, sd;
        struct epoll_event ev;
        corenet_node_t *node;
        schedule_t *schedule = schedule_self();

        sd = sockid->sd;
        event = EPOLLIN;
        _memset(&ev, 0x0, sizeof(struct epoll_event));

        YASSERT(sd < 32768);
        node = &corenet->array[sd];

        if (node->ev & event) {
                ret = EEXIST;
                GOTO(err_ret, ret);
        }

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
        strcpy(node->name, name);

        ev.data.fd = sd;
        ev.events = event;
        ret = _epoll_ctl(corenet->corenet.epoll_fd, EPOLL_CTL_ADD, sd, &ev);
        if (ret == -1) {
                ret = errno;
                DERROR("%d, %d\n", ret, corenet->corenet.epoll_fd);
                UNIMPLEMENTED(__DUMP__);//remove checklist
        }

        DINFO("corenet_tcp connect %s[%u] %s sd %d, ev %o:%o\n", schedule->name,
              schedule->id, node->name, sd, node->ev, event);

        return 0;
err_ret:
        return ret;
}

int corenet_tcp_add(corenet_tcp_t *corenet, const sockid_t *sockid, void *ctx,
                    core_exec exec, func_t reset, func_t check, func_t recv, const char *name)
{
        int ret;
        corenet_node_t *node;

        //YASSERT(sockid->addr);
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
                strcpy(node->name, name);

                YASSERT(sockid->sd < nofile_max);
                ret = sy_spin_lock(&corenet->corenet.lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                list_add_tail(&node->hook, &corenet->corenet.add_list);

                sy_spin_unlock(&corenet->corenet.lock);
        } else {
                ret = __corenet_add(__corenet_get(), sockid, ctx, exec, reset, check, recv, name);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

static void __corenet_close__(const sockid_t *sockid)
{
        int ret, sd;
        event_t ev;
        corenet_tcp_t *__corenet__ = __corenet_get();
        corenet_node_t *node =  &__corenet__->array[sockid->sd];
        schedule_t *schedule = schedule_self();

        YASSERT(sockid->sd >= 0);
#if ENABLE_TCP_THREAD
        ret = __corenet_tcp_wrlock(node);
        if (unlikely(ret))
                GOTO(err_ret, ret);

#endif
        if (node->sockid.seq != sockid->seq
            || node->sockid.sd == -1) {
                goto out;
        }
        
        //DBUG("close %d\n", sd);

        sd = node->sockid.sd;
        DINFO("corenet_tcp close %s[%u] %s sd %d, ev %x\n", schedule->name,
              schedule->id, node->name, sd, node->ev);
        YASSERT(node->ev);
        
        if (node->ev) {
                ev.data.fd = sd;
                ev.events = node->ev;
                ret = _epoll_ctl(__corenet__->corenet.epoll_fd, EPOLL_CTL_DEL, sd, &ev);
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

out:
#if ENABLE_TCP_THREAD
        __corenet_tcp_unlock(node);
#endif
        return;
#if ENABLE_TCP_THREAD
err_ret:
        return;
#endif
}

static void __corenet_tcp_close_task(void *args)
{
        const sockid_t *sockid = args;
        __corenet_close__(sockid);

        mem_cache_free(MEM_CACHE_128, args);
}

void corenet_tcp_close(const sockid_t *_sockid)
{
        sockid_t *sockid;

        if (_sockid->sd == -1) {
                return;
        }
        
        if (schedule_running()) {
                __corenet_close__(_sockid);
        } else {
#ifdef HAVE_STATIC_ASSERT
                static_assert(sizeof(*sockid)  < sizeof(mem_cache128_t), "corenet_tcp_close");
#endif

                sockid = mem_cache_calloc(MEM_CACHE_128, 1);
                YASSERT(sockid);
                *sockid = *_sockid;
        
                schedule_task_new("corenet_tcp_close", __corenet_tcp_close_task, sockid, -1);
                schedule_run(NULL);
        }
}

#if !ENABLE_TCP_THREAD
static int __corenet_tcp_local(int fd, buffer_t *buf, int op)
{
        int ret, iov_count;
        struct msghdr msg;
        corenet_tcp_t *__corenet__ = __corenet_get();

        iov_count = CORE_IOV_MAX;
        ret = mbuffer_trans(__corenet__->iov, &iov_count,  buf);
        //YASSERT(ret == (int)buf->len);
        if(unlikely(ret != (int)buf->len)) {
                DBUG("for bug test tcp send %d, buf->len:%u\n", ret, buf->len);
        }

        memset(&msg, 0x0, sizeof(msg));
        msg.msg_iov = __corenet__->iov;
        msg.msg_iovlen = iov_count;

        if (op == __OP_SEND__) {
                ret = _sendmsg(fd, &msg, MSG_DONTWAIT);
        } else if (op == __OP_RECV__) {
                ret = _recvmsg(fd, &msg, MSG_DONTWAIT);
        } else {
                YASSERT(0);
        }
        
        if (ret < 0) {
                ret = -ret;
                DWARN("sd %u %u %s\n", fd, ret, strerror(ret));
                YASSERT(ret != EMSGSIZE);
                GOTO(err_ret, ret);
        }

        return ret;
err_ret:
        return -ret;
}
#endif

static int __corenet_tcp_recv__(corenet_node_t *node, int toread)
{
        int ret;
        buffer_t buf;

        ret = mbuffer_init(&buf, toread);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        YASSERT(buf.len <= CORE_IOV_MAX * BUFFER_SEG_SIZE);
        DBUG("read data %u\n", toread);

#if ENABLE_TCP_THREAD
        ret = __corenet_tcp_remote(node->sockid.sd, &buf, __OP_RECV__);
#else
        ret = __corenet_tcp_local(node->sockid.sd, &buf, __OP_RECV__);
#endif
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

static int __corenet_tcp_recv(corenet_node_t *node, int *count)
{
        int ret, toread;
        uint64_t left, cp;

        //ANALYSIS_BEGIN(0);
        
        ret = ioctl(node->sockid.sd, FIONREAD, &toread);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        DBUG("recv %u\n", toread);
        
        if (toread == 0) {
                ret = ECONNRESET;
                GOTO(err_ret, ret);
        }

        left = toread;
        while (left) {
                cp = _min(left, BUFFER_SEG_SIZE * CORE_IOV_MAX);

                if ((uint64_t)toread > (BUFFER_SEG_SIZE * CORE_IOV_MAX)) {
                        DINFO("long msg, total %u, left %u, read %u\n", toread, left, cp);
                }

                ret = __corenet_tcp_recv__(node, cp);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                left -= cp;
        }

        //ANALYSIS_QUEUE(0, IO_WARN, "recvmsg");

        // __iscsi_newtask_core
        // corerpc_recv
        ret = node->exec(node->ctx, &node->recv_buf, count);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        return 0;
err_ret:
        return ret;
}

static int __corenet_tcp_send(corenet_node_t *node)
{
        int ret;
        buffer_t *buf, tmp;
        
        //ANALYSIS_BEGIN(0);
        
        buf = &node->send_buf;
        if (likely(buf->len)) {
                DBUG("send %u\n", buf->len);
#if ENABLE_TCP_THREAD
                ret = __corenet_tcp_remote(node->sockid.sd, buf, __OP_SEND__);
#else
                ret = __corenet_tcp_local(node->sockid.sd, buf, __OP_SEND__);
#endif
                if (ret < 0) {
                        ret = -ret;
                        DWARN("forward to %s @ %u len %d fail ret %d\n",
                              _inet_ntoa(node->sockid.addr), node->sockid.sd, buf->len, ret);
                        GOTO(err_ret, ret);
                }

                //DBUG("%s send data %u\n", node->name, ret);
                //YASSERT(ret <= (int)buf->len);
                /* DWARN("for bug test tcp send %d, buf->len:%u\n", ret, buf->len);*/
                mbuffer_init(&tmp, 0);
                mbuffer_pop(buf, &tmp, ret);//maybe yield with null arg 2
                mbuffer_free(&tmp);
        }

        //ANALYSIS_QUEUE(0, IO_WARN, "sendmsg");
        
        return 0;
err_ret:
        //mbuffer_free(buf);
        return ret;
}

#if ENABLE_TCP_THREAD

typedef struct {
        int ev;
        corenet_node_t *node;
} args_t;

typedef struct {
        struct list_head hook;
        task_t task;
        buffer_t *buf;
        int fd;
        int op;
        int retval;
} sr_ctx_t;

typedef struct {
        sy_spinlock_t lock;
        sem_t sem;
        struct list_head list;
} corenet_tcp_worker_t;

#define __TCP_WORKER_COUNT__ 2

static __thread corenet_tcp_worker_t *__worker__;

static int __corenet_tcp_wrlock(corenet_node_t *node)
{
        int ret;

        YASSERT(schedule_running());
        ret = plock_wrlock(&node->rwlock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (node->sockid.sd == -1) {
                ret = ESTALE;
                GOTO(err_lock, ret);
        }
        
        return 0;
err_lock:
        plock_unlock(&node->rwlock);
err_ret:
        return ret;
}

inline static int __corenet_tcp_rdlock(corenet_node_t *node)
{
        int ret;

        YASSERT(schedule_running());
        ret = plock_rdlock(&node->rwlock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (node->sockid.sd == -1) {
                ret = ESTALE;
                GOTO(err_lock, ret);
        }
        
        return 0;
err_lock:
        plock_unlock(&node->rwlock);
err_ret:
        return ret;
}

static int __corenet_tcp_rwlock_init(corenet_node_t *node)
{
        int ret;

        ret = plock_init(&node->rwlock, "corenet_tcp");
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static void __corenet_tcp_unlock(corenet_node_t *node)
{
        plock_unlock(&node->rwlock);
}

static void __corenet_event_cleanup(corenet_node_t *node, int *_ev)
{
        int ret;
        event_t ev;
        corenet_tcp_t *__corenet__ = __corenet_get();

        YASSERT(node->ev != 0);
                
        _memset(&ev, 0x0, sizeof(struct epoll_event));

        *_ev = node->ev;
        ev.events = node->ev;
        ev.data.fd = node->sockid.sd;

        DBUG("set sd %u epollfd %u\n", node->sockid.sd, __corenet__->corenet.epoll_fd);

        ret = _epoll_ctl(__corenet__->corenet.epoll_fd, EPOLL_CTL_DEL, node->sockid.sd, &ev);
        if (ret == -1) {
                ret = errno;
                YASSERT(0);
        }

        node->ev = 0;
}

static void __corenet_event_set(corenet_node_t *node, int _ev)
{
        int ret;
        event_t ev;
        corenet_tcp_t *__corenet__ = __corenet_get();

        YASSERT(node->ev == 0);
                
        _memset(&ev, 0x0, sizeof(struct epoll_event));

        ev.events = _ev;
        ev.events = node->send_buf.len ? (ev.events | EPOLLOUT) : ev.events;
        ev.data.fd = node->sockid.sd;

        DBUG("set sd %u epollfd %u\n", node->sockid.sd, __corenet__->corenet.epoll_fd);

        ret = _epoll_ctl(__corenet__->corenet.epoll_fd, EPOLL_CTL_ADD, node->sockid.sd, &ev);
        if (ret == -1) {
                ret = errno;
                YASSERT(0);
        }

        node->ev = ev.events;
}


static int __corenet_tcp_thread_send(int fd, buffer_t *buf, struct iovec *iov, int iov_count)
{
        int ret;
        struct msghdr msg;

        iov_count = CORE_IOV_MAX / 10;
        ret = mbuffer_trans(iov, &iov_count, buf);
        //YASSERT(ret == (int)newbuf.len);
        memset(&msg, 0x0, sizeof(msg));
        msg.msg_iov = iov;
        msg.msg_iovlen = iov_count;

        ret = _sendmsg(fd, &msg, MSG_DONTWAIT);
        if (ret < 0) {
                ret = -ret;
                DWARN("sd %u %u %s\n", fd, ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        return ret;
err_ret:
        return -ret;
}

static int __corenet_tcp_thread_recv(int fd, buffer_t *buf, struct iovec *iov, int iov_count)
{
        int ret;
        struct msghdr msg;

        ret = mbuffer_trans(iov, &iov_count,  buf);
        YASSERT(ret == (int)buf->len);
        memset(&msg, 0x0, sizeof(msg));
        msg.msg_iov = iov;
        msg.msg_iovlen = iov_count;

        ret = _recvmsg(fd, &msg, MSG_DONTWAIT);
        if (ret < 0) {
                ret = -ret;
                DWARN("sd %u %u %s\n", fd, ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        return ret;
err_ret:
        return -ret;
}


static void *__corenet_tcp_thread(void *args)
{
        int ret, iov_count;
        corenet_tcp_worker_t *worker = args;
        struct list_head list, *pos, *n;
        sr_ctx_t *ctx;
        struct iovec *iov;

        INIT_LIST_HEAD(&list);

        ret = ymalloc((void **)&iov, sizeof(*iov) * CORE_IOV_MAX);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        iov_count = CORE_IOV_MAX;
        
        while (1) {
                ret = _sem_wait(&worker->sem);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ret = sy_spin_lock(&worker->lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                list_splice_init(&worker->list, &list);

                sy_spin_unlock(&worker->lock);

                list_for_each_safe(pos, n, &list) {
                        list_del(pos);
                        ctx = (void *)pos;
                        if (ctx->op == __OP_SEND__) {
                                ctx->retval = __corenet_tcp_thread_send(ctx->fd, ctx->buf,
                                                                        iov, iov_count);
                        } else {
                                ctx->retval = __corenet_tcp_thread_recv(ctx->fd, ctx->buf,
                                                                        iov, iov_count);
                        }
                        schedule_resume(&ctx->task, 0, NULL);
                }
        }

        return NULL;
err_ret:
        UNIMPLEMENTED(__DUMP__);
        return NULL;
}

static int __corenet_tcp_thread_init()
{
        int ret, i;
        corenet_tcp_worker_t *worker, *array;

        YASSERT(__worker__ == NULL);

        ret = ymalloc((void **)&array, sizeof(*array) * __TCP_WORKER_COUNT__);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (i = 0; i < __TCP_WORKER_COUNT__; i++) {
                worker = &array[i];
                ret = sy_spin_init(&worker->lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                INIT_LIST_HEAD(&worker->list);

                ret = sem_init(&worker->sem, 0, 0);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ret = sy_thread_create2(__corenet_tcp_thread, worker, "__tcp_thread_worker");
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        __worker__ = array;

        return 0;
err_ret:
        return ret;
}

static int __corenet_tcp_remote(int fd, buffer_t *buf, int op)
{
        int ret;
        sr_ctx_t ctx;
        corenet_tcp_worker_t *worker = &__worker__[fd % __TCP_WORKER_COUNT__];

        ctx.task = schedule_task_get();
        ctx.buf = buf;
        ctx.fd = fd;
        ctx.retval = 0;
        ctx.op = op;
        
        ret = sy_spin_lock(&worker->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        list_add_tail(&ctx.hook, &worker->list);

        sy_spin_unlock(&worker->lock);

        sem_post(&worker->sem);

        ret = schedule_yield1(op == __OP_SEND__ ? "send" : "recv", NULL, NULL, NULL, -1);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        ret = ctx.retval;
        if (unlikely(ret < 0)) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        return ret;
err_ret:
        return -ret;
}

static int __corenet_tcp_exec_request(void *_args)
{
        int ret, count;
        args_t *args = _args;
        corenet_node_t *node = args->node;

        if (args->ev & EPOLLOUT) {
                ret = __corenet_tcp_send(node);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                if (list_empty(&node->send_buf.list)) {
                        __corenet_unset_out(node);
                }
        }
        
        if (args->ev & EPOLLIN) {
                if (node->recv) {
                        // __core_interrupt_eventfd_func
                        // __core_aio_eventfd_func
                        node->recv(node->ctx);
                } else {
                        // connect sd
                        ret = __corenet_tcp_recv(node, &count);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}


static void __corenet_tcp_exec__(void *_args)
{
        int ret, ev;
        args_t *args = _args;
        corenet_node_t *node = args->node;
        sockid_t sockid = node->sockid;

        ret = __corenet_tcp_wrlock(node);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        __corenet_event_cleanup(node, &ev);
        
        ret = __corenet_tcp_exec_request(_args);
        if (unlikely(ret))
                GOTO(err_lock, ret);

        __corenet_event_set(node, ev);
        __corenet_tcp_unlock(node);

        mem_cache_free(MEM_CACHE_128, args);
        
        return;
err_lock:
        __corenet_event_set(node, ev);
        __corenet_tcp_unlock(node);
err_ret:
        mem_cache_free(MEM_CACHE_128, args);
        corenet_tcp_close(&sockid);
        return;
}

static int __corenet_tcp_exec(void *ctx, corenet_node_t *node, event_t *ev)
{
        int ret;
        args_t *args;
        sockid_t sockid = node->sockid;

        //YASSERT(node->ev);
        //YASSERT(node->sockid.sd != -1);

        DBUG("ev %x\n", ev->events);

        if (unlikely((ev->events & EPOLLRDHUP) || (ev->events & EPOLLERR))
            || (ev->events & EPOLLHUP))  {
                ret = ECONNRESET;
                GOTO(err_ret, ret);
        }

#ifdef HAVE_STATIC_ASSERT
        static_assert(sizeof(*args)  < sizeof(mem_cache128_t), "mcache_collect_ctx_t");
#endif
        args = mem_cache_calloc(MEM_CACHE_128, 0);
        args->ev = ev->events;
        args->node = node;
        
        schedule_task_new("corenet_tcp_exec", __corenet_tcp_exec__, args, -1);
        schedule_run(variable_get_byctx(ctx, VARIABLE_SCHEDULE));
        
        return 0;
err_ret:
        corenet_tcp_close(&sockid);
        return ret;
}

#else

#if 0
static int __corenet_tcp_exec(void *ctx, corenet_node_t *node, event_t *ev)
{
        int ret, recv_msg;

        YASSERT(node->ev);
        YASSERT(node->sockid.sd != -1);

        DBUG("ev %x\n", ev->events);

        if (unlikely((ev->events & EPOLLRDHUP) || (ev->events & EPOLLERR))
            || (ev->events & EPOLLHUP))  {
                ret = ECONNRESET;
                GOTO(err_ret, ret);
        }

        if (ev->events & EPOLLOUT) {
                ret = __corenet_tcp_send(node);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                if (list_empty(&node->send_buf.list)) {
                        __corenet_unset_out(node);
                }
        }
        
        if (ev->events & EPOLLIN) {
                if (node->recv) {
                        // __core_interrupt_eventfd_func
                        // __core_aio_eventfd_func
                        node->recv(node->ctx);
                } else {
                        // connect sd
                        ret = __corenet_tcp_recv(node, &recv_msg);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        corenet_tcp_close(&node->sockid);
        return ret;
}

#else

static void __corenet_tcp_exec_recv(void *_node)
{
        int ret, recv_msg;
        corenet_node_t *node = _node;
        sockid_t sockid = node->sockid;

        if (node->recv) {
                // __core_interrupt_eventfd_func
                // __core_aio_eventfd_func
                node->recv(node->ctx);
        } else {
                // connect sd
                ret = __corenet_tcp_recv(node, &recv_msg);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return;
err_ret:
        corenet_tcp_close(&sockid);
        return;
 
}

static void __corenet_tcp_exec_send(void *_node)
{
        int ret;
        corenet_node_t *node = _node;
        sockid_t sockid = node->sockid;

        ret = __corenet_tcp_send(node);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (list_empty(&node->send_buf.list)) {
                __corenet_unset_out(node);
        }

        return;
err_ret:
        corenet_tcp_close(&sockid);
        return;
}

static int __corenet_tcp_exec(void *ctx, corenet_node_t *node, event_t *ev)
{
        int ret;
        sockid_t sockid = node->sockid;

        if (node->ev == 0 || node->sockid.sd == -1) {
                 DERROR("node ev:%d, sockid sd:%d, ev->events:0x%x\n", node->ev, node->sockid.sd, ev->events);
                 ret = ESTALE;
                 return ret;
        }

        DBUG("ev %x\n", ev->events);

        if (unlikely((ev->events & EPOLLRDHUP) || (ev->events & EPOLLERR))
            || (ev->events & EPOLLHUP))  {
                ret = ECONNRESET;
                GOTO(err_ret, ret);
        }

        if (ev->events & EPOLLOUT) {
                schedule_task_new("corenet_tcp_send", __corenet_tcp_exec_send, node, -1);
        }

        if (ev->events & EPOLLIN) {
                schedule_task_new("corenet_tcp_recv", __corenet_tcp_exec_recv, node, -1);
        }

        schedule_run(variable_get_byctx(ctx, VARIABLE_SCHEDULE));

        return 0;
err_ret:
        corenet_tcp_close(&sockid);
        return ret;
}

#endif
#endif

int corenet_tcp_poll(void *ctx, int tmo)
{
        int nfds, i;
        event_t events[512], *ev;
        corenet_node_t *node;
        corenet_tcp_t *__corenet__ = __corenet_get_byctx(ctx);

        DBUG("polling %d begin\n", tmo);
        YASSERT(tmo >= 0 && tmo < gloconf.rpc_timeout * 2);
        nfds = _epoll_wait(__corenet__->corenet.epoll_fd, events, 512, tmo * 1000);
        if (unlikely(nfds < 0)) {
                UNIMPLEMENTED(__DUMP__);
        }

        DBUG("polling %d return\n", nfds);
        
        for (i = 0; i < nfds; i++) {
                //ANALYSIS_BEGIN(0);
                ev = &events[i];

                node = &__corenet__->array[ev->data.fd];
                __corenet_tcp_exec(ctx, node, ev);
                //ANALYSIS_QUEUE(0, IO_WARN, "corenet_poll");
        }

        return 0;
}

typedef struct {
        struct list_head hook;
        sockid_t sockid;
        buffer_t buf;
} corenet_fwd_t;

static void __corenet_tcp_queue(corenet_tcp_t *__corenet__, const sockid_t *sockid,
                                buffer_t *buf, int flag)
{
        int found = 0;
        corenet_fwd_t *corenet_fwd;
        struct list_head *pos;

        DBUG("corenet_fwd\n");

        list_for_each(pos, &__corenet__->corenet.forward_list) {
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
                static_assert(sizeof(*corenet_fwd)  < sizeof(mem_cache128_t), "corenet_fwd_t");
#endif

                corenet_fwd = mem_cache_calloc(MEM_CACHE_128, 0);
                YASSERT(corenet_fwd); 
                corenet_fwd->sockid = *sockid;
                mbuffer_init(&corenet_fwd->buf, 0);

                if (flag & BUFFER_KEEP) {
                        mbuffer_reference(&corenet_fwd->buf, buf);
                } else {
                        mbuffer_merge(&corenet_fwd->buf, buf);
                }

                list_add_tail(&corenet_fwd->hook, &__corenet__->corenet.forward_list);
        }
}

int corenet_tcp_send(void *ctx, const sockid_t *sockid, buffer_t *buf, int flag)
{
        int ret;
        corenet_node_t *node;
        corenet_tcp_t *__corenet__ = __corenet_get_byctx(ctx);

        YASSERT(sockid->type == SOCKID_CORENET);
        //YASSERT(sockid->addr);

        node = &__corenet__->array[sockid->sd];
        if (node->sockid.seq != sockid->seq || node->sockid.sd == -1) {
                ret = ECONNRESET;
                DWARN("seq %d %d, sd %d\n", node->sockid.seq, sockid->seq, node->sockid.sd);
                GOTO(err_ret, ret);
        }

        __corenet_tcp_queue(__corenet__, sockid, buf, flag);

        return 0;
err_ret:
        return ret;
}

#if ENABLE_TCP_THREAD
static int __corenet_tcp_commit(const sockid_t *sockid, buffer_t *buf)
{
        int ret;
        corenet_node_t *node;
        corenet_tcp_t *__corenet__ = __corenet_get();

        YASSERT(sockid->type == SOCKID_CORENET);
        //YASSERT(sockid->addr);

        node = &__corenet__->array[sockid->sd];
        ret = __corenet_tcp_wrlock(node);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        if (node->sockid.seq != sockid->seq || node->sockid.sd == -1) {
                ret = ECONNRESET;
                mbuffer_free(buf);
                GOTO(err_lock, ret);
        }

        mbuffer_merge(&node->send_buf, buf);

#if 1
        int ev;
        __corenet_event_cleanup(node, &ev);

        ret = __corenet_tcp_send(node);
        if (unlikely(ret)) {
                DWARN("send to %d fail\n", sockid->sd);
        }

        __corenet_event_set(node, ev);
#endif
        
        if (!list_empty(&node->send_buf.list)) {
                __corenet_set_out(node);
        }

        __corenet_tcp_unlock(node);
        
        return 0;
err_lock:
        __corenet_tcp_unlock(node);
err_ret:
        return ret;
}

static void __corenet_tcp_commit_task(void *ctx)
{
        struct list_head *pos, *n;
        corenet_fwd_t *corenet_fwd;
        corenet_tcp_t *__corenet__ = __corenet_get_byctx(ctx);
        struct list_head list;

        INIT_LIST_HEAD(&list);
        list_splice_init(&__corenet__->corenet.forward_list, &list);

        list_for_each_safe(pos, n, &list) {
                corenet_fwd = (void *)pos;
                list_del(pos);

                DBUG("forward to %s @ %u\n",
                     _inet_ntoa(corenet_fwd->sockid.addr), corenet_fwd->sockid.sd);

                __corenet_tcp_commit(&corenet_fwd->sockid, &corenet_fwd->buf);

                mem_cache_free(MEM_CACHE_128, corenet_fwd);
        }
}

void corenet_tcp_commit(void *ctx)
{
        corenet_tcp_t *__corenet__ = __corenet_get_byctx(ctx);

        if (list_empty(&__corenet__->corenet.forward_list))
                return;

        schedule_task_new("corenet_tcp_commit", __corenet_tcp_commit_task, ctx, -1);
        schedule_run(variable_get_byctx(ctx, VARIABLE_SCHEDULE));
}

#else

inline static void __corenet_tcp_exec_send_nowait(void *_node)
{
        int ret;
        corenet_node_t *node = _node;
        sockid_t sockid = node->sockid;

        ret =  __corenet_tcp_send(node);
        if (unlikely(ret)) {
                DWARN("send to %d fail\n", sockid.sd);
        }

        if (!list_empty(&node->send_buf.list)) {
                __corenet_set_out(node);
        }

        return;
}

static int __corenet_tcp_commit(void *ctx, const sockid_t *sockid, buffer_t *buf)
{
        int ret;
        corenet_node_t *node;
        corenet_tcp_t *__corenet__ = __corenet_get_byctx(ctx);

        YASSERT(sockid->type == SOCKID_CORENET);
        //YASSERT(sockid->addr);

        node = &__corenet__->array[sockid->sd];
        if (node->sockid.seq != sockid->seq || node->sockid.sd == -1) {
                ret = ECONNRESET;
                mbuffer_free(buf);
                GOTO(err_ret, ret);
        }

        mbuffer_merge(&node->send_buf, buf);

#if 1
        schedule_task_new("corenet_tcp_send", __corenet_tcp_exec_send_nowait, node, -1);
        schedule_run(variable_get_byctx(ctx, VARIABLE_SCHEDULE));
#else   
#if 0
        ret =  __corenet_tcp_send(node);
        if (unlikely(ret)) {
                DWARN("send to %d fail\n", sockid->sd);
        }
#endif

        if (!list_empty(&node->send_buf.list)) {
                __corenet_set_out(node);
        }
#endif

        return 0;
err_ret:
        return ret;
}

void corenet_tcp_commit(void *ctx)
{
        struct list_head *pos, *n;
        corenet_fwd_t *corenet_fwd;
        corenet_tcp_t *__corenet__ = __corenet_get_byctx(ctx);

        list_for_each_safe(pos, n, &__corenet__->corenet.forward_list) {
                corenet_fwd = (void *)pos;
                list_del(pos);

                DBUG("forward to %s @ %u\n",
                     _inet_ntoa(corenet_fwd->sockid.addr), corenet_fwd->sockid.sd);

                __corenet_tcp_commit(ctx, &corenet_fwd->sockid, &corenet_fwd->buf);

                mem_cache_free(MEM_CACHE_128, corenet_fwd);
        }
}

#endif

int corenet_tcp_connected(const sockid_t *sockid)
{
        int ret;
        corenet_node_t *node;
        corenet_tcp_t *__corenet__ = __corenet_get();

        if (sockid->sd == -1) {
                ret = ECONNRESET;
                GOTO(err_ret, ret);
        }
        
        node = &__corenet__->array[sockid->sd];
        if (node->sockid.seq != sockid->seq || node->sockid.sd == -1) {
                ret = ECONNRESET;
                GOTO(err_ret, ret);
        }

        return 1;
err_ret:
        return 0;
}

int corenet_tcp_init(int max, corenet_tcp_t **_corenet)
{
        int ret, len, i, size;
        corenet_tcp_t *corenet;
        corenet_node_t *node;

        size = max;

        DINFO("malloc %ju\n", sizeof(corenet_node_t) * size);

        len = sizeof(corenet_tcp_t) + sizeof(corenet_node_t) * size;
        ret = ymalloc((void **)&corenet, len);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DBUG("corenet use %u\n", len);

        _memset(corenet, 0x0, len);
        corenet->corenet.size = size;

        for (i = 0; i < size; i++) {
                node = &corenet->array[i];

#if ENABLE_TCP_THREAD
                __corenet_tcp_rwlock_init(node);
#endif
                
                mbuffer_init(&node->recv_buf, 0);
                mbuffer_init(&node->send_buf, 0);
                node->sockid.sd = -1;
        }

        corenet->corenet.epoll_fd = epoll_create(corenet->corenet.size);
        if (corenet->corenet.epoll_fd == -1) {
                ret = errno;
                GOTO(err_free, ret);
        }

        INIT_LIST_HEAD(&corenet->corenet.forward_list);
        INIT_LIST_HEAD(&corenet->corenet.check_list);
        INIT_LIST_HEAD(&corenet->corenet.add_list);

        ret = sy_spin_init(&corenet->corenet.lock);
        if (unlikely(ret))
                GOTO(err_free, ret);


        variable_set(VARIABLE_CORENET_TCP, corenet);
        if (_corenet)
                *_corenet = corenet;

#if ENABLE_TCP_THREAD
        ret = __corenet_tcp_thread_init();
        if (unlikely(ret))
                GOTO(err_free, ret);
#endif
        
        DINFO("corenet init done corenet:%p\n", corenet);

	return 0;
err_free:
        yfree((void **)&corenet);
err_ret:
        return ret;
}

void __corenet_tcp_destroy(void *_finished)
{
        corenet_tcp_t *corenet = __corenet_get();
        int *finished = _finished;

        struct list_head *pos, *n;
        corenet_fwd_t *corenet_fwd;
        list_for_each_safe(pos, n, &corenet->corenet.forward_list) {
                list_del(pos);
                corenet_fwd = (void *)pos;
                mbuffer_free(&corenet_fwd->buf);
                mem_cache_free(MEM_CACHE_128, corenet_fwd);
        }

        corenet_node_t *node;
        for (int i = 0; i < corenet->corenet.size; i++) {
                node = &corenet->array[i];
                
                if (node->sockid.sd != -1) {
                        __corenet_close__(&node->sockid);
                }
        }
        
        close(corenet->corenet.epoll_fd);
        corenet->corenet.epoll_fd = -1;
        
        yfree((void **)&corenet);

        variable_unset(VARIABLE_CORENET_TCP);

        *finished = 1;
}

void corenet_tcp_destroy(corenet_tcp_t **_corenet)
{
        int finished = 0;
        
        schedule_task_new("corenet_tcp_send", __corenet_tcp_destroy, &finished, -1);

        while (finished) {
                schedule_run(NULL);
        }

        DINFO("corenet tcp destroyed\n");
        
        *_corenet = NULL;
       
}

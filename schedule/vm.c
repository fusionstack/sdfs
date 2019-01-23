

#include <limits.h>
#include <time.h>
#include <string.h>
#include <sys/epoll.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBSCHEDULE

#include "sysutil.h"
#include "net_proto.h"
#include "ylib.h"
#include "sdevent.h"
#include "../net/xnect.h"
#include "net_table.h"
#include "mem_cache.h"
#include "rpc_table.h"
#include "configure.h"
#include "vm.h"
#include "sdfs_aio.h"
#include "schedule.h"
#include "bh.h"
#include "net_global.h"
#include "cpuset.h"
#include "adt.h"
#include "dbg.h"

#define POLL_SD 2

typedef struct {
        struct list_head hook;
        sockid_t sockid;
        buffer_t buf;
} vm_fwd_t;

static __thread vm_t *__vm__ = NULL;

vm_t *vm_self()
{
        return __vm__;
}

static int __vm_poll(vm_t *vm, struct pollfd *pfd, int pollout)
{
        int ret;

        pfd[0].fd = vm->sd;
        pfd[0].events = POLLIN
                | (pollout ? POLLOUT : 0);
        pfd[0].revents = 0;
        pfd[1].fd = vm->interrupt_eventfd;
        pfd[1].events = POLLIN;
        pfd[1].revents = 0;

        DBUG("vm poll sd (%u, %u)\n", vm->sd, vm->interrupt_eventfd);
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

static int __vm_send()
{
        int ret, iov_count;
        struct msghdr msg;
        buffer_t *buf;

        buf = &__vm__->send_buf;
        while (buf->len) {
                memset(&msg, 0x0, sizeof(msg));
                DBUG("send %u\n", __vm__->send_buf.len);
                iov_count = VM_IOV_MAX;
                ret = mbuffer_trans(__vm__->iov, &iov_count,  buf);
                if (ret != (int)buf->len) {
                        DBUG("left %d\n", (int)buf->len - ret);
                }

                msg.msg_iov = __vm__->iov;
                msg.msg_iovlen = iov_count;
                ret = sendmsg(__vm__->sd, &msg, MSG_DONTWAIT);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                DBUG("%s send data %u to %u\n", __vm__->name, ret, __vm__->sd);
                YASSERT(ret <= (int)__vm__->send_buf.len);
                mbuffer_pop(buf, NULL, ret);
        }

        return 0;
err_ret:
        mbuffer_free(buf);
        return ret;
}

int vm_forward(const sockid_t *sockid, buffer_t *buf, int flag)
{
        int ret, found = 0;
        vm_fwd_t *vm_fwd;
        struct list_head *pos;

        if (__vm__ == NULL) {
                ret = ENOSYS;
                goto err_ret;
        } else if ( __vm__->exiting) {
                ret = ESHUTDOWN;
                goto err_ret;
        }

#if 1
        ret = sdevent_check(sockid);
        if (unlikely(ret)) {
                DWARN("append forward to %s fail\n",
                      _inet_ntoa(sockid->addr));
                GOTO(err_ret, ret);
        }
#endif

        YASSERT(flag == 0);

        DBUG("vm_fwd\n");

        list_for_each(pos, &__vm__->forward_list) {
                vm_fwd = (void *)pos;

                if (sockid_cmp(sockid, &vm_fwd->sockid) == 0) {
                        DBUG("append forward to %s @ %u\n",
                             _inet_ntoa(sockid->addr), sockid->sd);
                        mbuffer_merge(&vm_fwd->buf, buf);
                        found = 1;
                        break;
                }
        }

        if (found == 0) {
                DBUG("new forward to %s @ %u\n",
                      _inet_ntoa(sockid->addr), sockid->sd);

#ifdef HAVE_STATIC_ASSERT
                static_assert(sizeof(*vm_fwd)  < sizeof(mem_cache128_t), "vm_fwd_t");
#endif

                vm_fwd = mem_cache_calloc(MEM_CACHE_128, 0);
                YASSERT(vm_fwd);
                vm_fwd->sockid = *sockid;
                mbuffer_init(&vm_fwd->buf, 0);
                mbuffer_merge(&vm_fwd->buf, buf);
                list_add_tail(&vm_fwd->hook, &__vm__->forward_list);
        }

        return 0;
err_ret:
        return ret;
}

static void  __vm_forward()
{
        int ret;
        net_handle_t nh;
        struct list_head *pos, *n;
        vm_fwd_t *vm_fwd;

        YASSERT(__vm__);
        list_for_each_safe(pos, n, &__vm__->forward_list) {
                vm_fwd = (void *)pos;
                list_del(pos);

                DBUG("forward to %s @ %u\n",
                     _inet_ntoa(vm_fwd->sockid.addr), vm_fwd->sockid.sd);

                sock2nh(&nh, &vm_fwd->sockid);
                ret = sdevent_queue(&nh, &vm_fwd->buf, 0);
                if (unlikely(ret)) {
                        DWARN("forward to %s @ %u fail\n",
                              _inet_ntoa(vm_fwd->sockid.addr), vm_fwd->sockid.sd);
                        mbuffer_free(&vm_fwd->buf);
                }

                mem_cache_free(MEM_CACHE_128, vm_fwd);
        }
}

static int __vm_recv__(vm_t *vm, int toread)
{
        int ret, iov_count;
        struct msghdr msg;
        buffer_t buf;

        ret = mbuffer_init(&buf, toread);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        YASSERT(buf.len <= VM_IOV_MAX * BUFFER_SEG_SIZE);

        iov_count = VM_IOV_MAX;
        ret = mbuffer_trans(__vm__->iov, &iov_count,  &buf);
        YASSERT(ret == (int)buf.len);
        memset(&msg, 0x0, sizeof(msg));
        msg.msg_iov = __vm__->iov;
        msg.msg_iovlen = iov_count;

        DBUG("read data %u\n", toread);

        ret = _recvmsg(vm->sd, &msg, MSG_DONTWAIT);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_free, ret);
        }

        YASSERT(ret == (int)buf.len);
        DBUG("new recv %u, left %u\n", buf.len, vm->recv_buf.len);
        mbuffer_merge(&vm->recv_buf, &buf);

        return 0;
err_free:
        mbuffer_free(&buf);
err_ret:
        return ret;
}

static int __vm_recv(vm_t *vm, int *count)
{
        int ret, toread, left, cp;

        ret = ioctl(vm->sd, FIONREAD, &toread);
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
                cp = left > BUFFER_SEG_SIZE * VM_IOV_MAX ? BUFFER_SEG_SIZE * VM_IOV_MAX : left;

                if (toread > BUFFER_SEG_SIZE * VM_IOV_MAX) {
                        DINFO("long msg, total %u, left %u, read %u\n", toread, left, cp);
                }

                ret = __vm_recv__(vm, cp);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                left -= cp;
        }

        ret = vm->exec(count);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __vm_event__(vm_t *vm, int event, int *count)
{
        int ret;

        YASSERT(event);

        if (event & POLLIN) {
                DBUG("recv\n");
                ret = __vm_recv(vm, count);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        if (event & POLLOUT) {
                DBUG("send\n");
                ret = __vm_send(vm);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        if (event & POLLOUT && event & POLLHUP) {
                ret = ECONNRESET;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}


static int __vm_event(vm_t *vm, const struct pollfd *_pfd, int _count, int *recv_msg)
{
        int ret, i, count = 0;
        char buf[MAX_BUF_LEN];
        const struct pollfd *pfd;

        for (i = 0; i < POLL_SD; i++) {
                pfd = &_pfd[i];
                if (pfd->revents == 0)
                        continue;

                if (pfd->fd == vm->sd) {
                        DBUG("network event from %u %x, idx %u\n", pfd->fd, pfd->revents, i);
                        ret = __vm_event__(vm, pfd->revents, recv_msg);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);
                } else if (pfd->fd == vm->interrupt_eventfd) {
                        DBUG("interrupted from %u %x, idx %u\n", pfd->fd, pfd->revents, i);
                        ret = read(pfd->fd, buf, MAX_BUF_LEN);
                        if (ret < 0) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }
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

static int __vm_worker_poll(vm_t *vm)
{
        int ret, event, recv_msg = 0;
        struct pollfd pfd[POLL_SD];

        DBUG("running thread %u\n", vm->idx);
        event = __vm_poll(vm, pfd, __vm__->send_buf.len);
        if (event < 0) {
                ret = -event;
                DWARN("vm[%u] exit, sd %u\n", vm->idx, vm->sd);
                GOTO(err_ret, ret);
        }

        schedule_backtrace();

        DBUG("thread %u got new event %u, sd %u, remote_event %u\n",
              vm->idx, event, vm->sd, vm->interrupt_eventfd);

        recv_msg = 0;
        ret = __vm_event(vm, pfd, event, &recv_msg);
        if (unlikely(ret)) {
                DWARN("vm[%u] exit, sd %u\n", vm->idx, vm->sd);
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __vm_worker_run(vm_t *vm)
{
        //int ret;

        schedule_run(vm->schedule);

        __vm_forward();

#if 1
        __vm_send();
#else
        ret = __vm_send();
        if (unlikely(ret)) {
                DWARN("vm[%u] exit, sd %u\n", vm->idx, vm->sd);
                GOTO(err_ret, ret);
        }
#endif

        if (vm->check) {
                vm->check();
        }

        schedule_scan(vm->schedule);

        return 0;
//err_ret:
//        return ret;
}

static int __vm_worker_init(vm_t *vm)
{
        int ret;
        char name[MAX_NAME_LEN];

        snprintf(name, sizeof(name), "vm");
        ret = schedule_create(&vm->interrupt_eventfd, name, &vm->idx, &vm->schedule, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        vm->schedule->suspendable = 1;
        DINFO("vm[%d] sd %u creating...\n", vm->idx, vm->sd);

#if ENABLE_VM_MEM_PRIVATE
        ret = mem_cache_private_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);
#else
        DINFO("vm private cache disabled\n");
#endif

        if (vm->init) {
                ret = vm->init();
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        UNIMPLEMENTED(__DUMP__);
        return ret;
}

static void __vm_exit(vm_t *vm)
{
        DINFO("vm[%d] sd %u exiting...\n", vm->idx, vm->sd);

#if 1
        vm->exiting = 1;
        __vm_forward();

        schedule_destroy();

        if (vm->exit) {
                vm->exit();
        }

        //aio_destroy();
#if 0
        rpc_table_private_destroy();
#endif
        mbuffer_free(&vm->send_buf);
        mbuffer_free(&vm->recv_buf);

#if ENABLE_VM_MEM_PRIVATE
        mem_cache_private_destroy();
#else
        DINFO("vm private cache disabled\n");
#endif

        YASSERT(list_empty(&vm->forward_list));

#else
        EXIT(EAGAIN);
#endif
}

static  int __vm_worker_connect(vm_t *vm)
{
        int ret, retry = 0;

retry:
        ret = vm->reconnect(&vm->sd, vm->ctx);
        if (unlikely(ret)) {
                DWARN("connect fail\n");
                __vm_worker_run(vm);

                USLEEP_RETRY(err_ret, ret, retry, retry, gloconf.rpc_timeout * 4, (1000 * 1000));
        }

        return 0;
err_ret:
        return ret;
}

static void *__vm_worker(void *_args)
{
        int ret;
        vm_t *vm;

        vm =_args;
        DINFO("vm sd %u start...\n", vm->sd);

        ret = __vm_worker_init(vm);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        __vm__ = vm;
        YASSERT(!vm->exiting);

        while (1) {
                ret = __vm_worker_poll(vm);
                if (unlikely(ret)) {
                        if (vm->reconnect) {
                                ret = __vm_worker_connect(vm);
                                if (unlikely(ret))
                                        EXIT(EAGAIN);
                        } else
                                GOTO(err_exit, ret);
                }

                __vm_worker_run(vm);
                
                if (vm->stop) {
                        ret = EPERM;
                        GOTO(err_exit, ret);
                }
        }

        __vm_exit(vm);
        
        sem_post(&vm->sem);
        
        return NULL;
err_exit:
        __vm_exit(vm);
err_ret:
        sem_post(&vm->sem);
        __vm__ = NULL;
        return NULL;
}

void vm_stop(vm_t *vm)
{
        int ret;
        schedule_t *schedule;

        vm->stop= 1;

        schedule = vm->schedule;
        YASSERT(vm->schedule);

        schedule_post(schedule);

        ret = sem_wait(&vm->sem);
        YASSERT(ret == 0);

        close(vm->sd);
        yfree((void **)&vm);
}

void vm_send(buffer_t *buf, int flag)
{
        if (flag & BUFFER_KEEP) {
                mbuffer_reference(&__vm__->send_buf, buf);
        } else {
                mbuffer_merge(&__vm__->send_buf, buf);
        }

        DBUG("send data %u\n", __vm__->send_buf.len);
}

int vm_request(vm_t *vm, void (*exec)(void *buf), void *buf, const char *name)
{
        int ret;
        schedule_t *schedule;

        schedule = vm->schedule;
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

int vm_create(const vm_op_t *vm_op, vm_t **_vm)
{
        int ret;
        vm_t *vm;

        DBUG("create vm to %u\n", vm_op->sd);

        ret = ymalloc((void **)&vm, sizeof(*vm));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        memset(vm, 0x0, sizeof(*vm));

        vm->idx = -1;
        vm->stop= 0;
        vm->exiting = 0;
        vm->sd = vm_op->sd;
        vm->exec = vm_op->exec;
        vm->exit = vm_op->exit;
        vm->ctx = vm_op->ctx;
        vm->check = vm_op->check;
        vm->reconnect = vm_op->reconnect;
        vm->init = vm_op->init;
        strcpy(vm->name, vm_op->name);

        mbuffer_init(&vm->send_buf, 0);
        mbuffer_init(&vm->recv_buf, 0);

        INIT_LIST_HEAD(&vm->forward_list);

        ret = sem_init(&vm->sem, 0, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sy_thread_create(__vm_worker, vm);
        if (ret == -1) {
                ret = errno;
                GOTO(err_free, ret);
        }

        if (_vm)
                *_vm = vm;

        return 0;
err_free:
        yfree((void **)&vm);
err_ret:
        return ret;
}

#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "ynet_net.h"
#include "adt.h"
#include "job.h"
#include "job_dock.h"
#include "ylib.h"
#include "ynet_rpc.h"
#include "configure.h"
#include "rpc_table.h"
#include "net_global.h"
#include "network.h"
#include "core.h"
#include "vm.h"
#include "dbg.h"

typedef struct {
        sem_t sem;
        task_t task;
        int retval;
        uint64_t latency;
        buffer_t buf;
} rpc_ctx_t;

int rpc_request_prep(buffer_t *buf, const msgid_t *msgid, const void *request,
                     int reqlen, const buffer_t *data, int prog, int merge, int priority)
{
        int ret;
        ynet_net_head_t *net_req;

        if (ng.master_magic == (uint32_t)-1) {
                ret = ENOSYS;
                GOTO(err_ret, ret);
        }
        
        if (unlikely(reqlen + sizeof(ynet_net_head_t) > BUFFER_SEG_SIZE)) {
                DERROR("why we get such a request? %u\n", reqlen);
                YASSERT(0);
        }

        ret = mbuffer_init(buf, sizeof(ynet_net_head_t) + reqlen);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        net_req = mbuffer_head(buf);
        net_req->magic = YNET_PROTO_TCP_MAGIC;
        net_req->len = sizeof(ynet_net_head_t) + reqlen;
        net_req->type = YNET_MSG_REQ;
        net_req->prog = prog;
        net_req->msgid = *msgid;
        net_req->crcode = 0;
        net_req->blocks = 0;
        net_req->master_magic = ng.master_magic;
        net_req->priority = (priority == -1) ? schedule_priority(NULL) : priority;
        net_req->load = core_latency_get();
        memcpy(net_req->buf, request, reqlen);

        
        if (data) {
                net_req->blocks = data->len;
                
                if (unlikely(merge)) {
                        net_req->len += data->len;
                        mbuffer_reference(buf, data);
                }
        }

        return 0;
err_ret:
        return ret;
}

static int __rpc_request_send(const sockid_t *sockid, const msgid_t *msgid, const void *request,
                              int reqlen, const buffer_t *data, int msg_type, int priority)
{
        int ret;
        buffer_t buf;
        net_handle_t nh;

        ret = rpc_request_prep(&buf, msgid, request, reqlen, data, msg_type, 1, priority);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DBUG("send msg to %s, id (%u, %x), len %u\n",
              _inet_ntoa(sockid->addr), msgid->idx,
              msgid->figerprint, buf.len);

#if 0
        ret = vm_forward(sockid, &buf, 0);
        if (unlikely(ret)) {
                if (ret == ENOSYS) {
                        // rpc here
                        sock2nh(&nh, sockid);
                        ret = sdevent_queue(&nh, &buf, 0);
                        if (unlikely(ret)) {
                                ret = _errno_net(ret);
                                GOTO(err_free, ret);
                        }
                } else {
                        GOTO(err_free, ret);
                }
        }
#else
        sock2nh(&nh, sockid);
        ret = sdevent_queue(&nh, &buf, 0);
        if (unlikely(ret)) {
                ret = _errno_net(ret);
                GOTO(err_free, ret);
        }
#endif

        //mbuffer_free(&buf);

        return 0;
err_free:
        mbuffer_free(&buf);
err_ret:
        return ret;
}

static void __rpc_request_post_sem(void *arg1, void *arg2, void *arg3, void *arg4)
{
        rpc_ctx_t *ctx = arg1;
        int retval = *(int *)arg2;
        buffer_t *buf = arg3;
        uint64_t latency = *(uint64_t *)arg4;

        ctx->latency = latency;
        ctx->retval = retval;
        if (buf) {
                mbuffer_merge(&ctx->buf, buf);
        } else {
                ctx->buf.len = 0;
        }

        sem_post(&ctx->sem);
}

static void __rpc_request_post_task(void *arg1, void *arg2, void *arg3, void *arg4)
{
        rpc_ctx_t *ctx = arg1;
        int retval = *(int *)arg2;
        buffer_t *buf = arg3;
        uint64_t latency = *(uint64_t *)arg4;

        ctx->latency = latency;
        ctx->retval = -1;
        ctx->buf.len = 0;

        schedule_resume(&ctx->task, retval, buf);
}

static void __rpc_request_reset(const msgid_t *msgid)
{
        if (schedule_running()) {
                schedule_task_reset();
        }
        
        rpc_table_free(__rpc_table__, msgid);
}

static int __rpc_request_getsolt(msgid_t *msgid, rpc_ctx_t *ctx, const char *name,
                                 const sockid_t *sockid, const nid_t *nid, int timeout)
{
        int ret;

        ret = rpc_table_getsolt(__rpc_table__, msgid, name);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        mbuffer_init(&ctx->buf, 0);

        YASSERT(sockid->type == SOCKID_NORMAL);

        if (schedule_running()) {
                ctx->task = schedule_task_get();

                ret = rpc_table_setsolt(__rpc_table__, msgid, __rpc_request_post_task, ctx, sockid, nid, timeout);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);
        } else {
                ret = sem_init(&ctx->sem, 0, 0);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

                ret = rpc_table_setsolt(__rpc_table__, msgid, __rpc_request_post_sem, ctx, sockid, nid, timeout);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);
        }

        return 0;
err_ret:
        return ret;
}

STATIC int __rpc_request_wait__(const net_handle_t *nh, const char *name, buffer_t *rbuf,
                                rpc_ctx_t *ctx, int timeout)
{
        int ret;
#if 0
        vm_t *vm = vm_self();
#endif

        (void) timeout;
        
        ANALYSIS_BEGIN(0);
        
        if (schedule_running()) {
                DBUG("%s yield wait\n", name);
                ret = schedule_yield(name, rbuf, ctx);
                if (unlikely(ret))
                        goto err_ret;

                DBUG("%s yield resume\n", name);
#if 0
                if (vm && vm->exiting) {
                        ret = ESHUTDOWN;
                        GOTO(err_ret, ret);
                }
#endif
        } else {
                DBUG("%s sem wait\n", name);
                ret = _sem_wait(&ctx->sem);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }

                ret = ctx->retval;
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                if (rbuf) {
                        mbuffer_merge(rbuf, &ctx->buf);
                } else {
                        YASSERT(ctx->buf.len == 0);
                }
                
                DBUG("%s sem resume\n", name);
        }


        if (nh->type == NET_HANDLE_PERSISTENT) {
                DBUG("%s latency %llu\n", netable_rname_nid(&nh->u.nid), (LLU)ctx->latency);
                netable_load_update(&nh->u.nid, ctx->latency);
        }

#ifdef RPC_ASSERT
        timeout = _max(timeout, _get_rpc_timeout());
        ANALYSIS_ASSERT(0, 1000 * 1000 * (timeout * 3), name);
#else
        ANALYSIS_END(0, IO_WARN, NULL);
#endif

        return 0;
err_ret:
#ifdef RPC_ASSERT
        timeout = _max(timeout, _get_rpc_timeout());
        ANALYSIS_ASSERT(0, 1000 * 1000 * (timeout * 3), name);
#else
        ANALYSIS_END(0, IO_WARN, NULL);
#endif
        return ret;
}

STATIC int __rpc_request_wait(const char *name, const net_handle_t *nh, const void *request,
                              int reqlen, const buffer_t *wbuf, buffer_t *rbuf, int msg_type,
                              int priority, int timeout)
{
        int ret;
        msgid_t msgid;
        rpc_ctx_t ctx;
        sockid_t sockid;
        const nid_t *nid;

        if (nh->type == NET_HANDLE_PERSISTENT) {
#if 0
                ret = network_connect(&nh->u.nid, NULL, 1, 0);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
#endif
                
                ret = netable_getsock(&nh->u.nid, &sockid);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                nid = &nh->u.nid;
        } else {
                sockid = nh->u.sd;
                nid = NULL;
        }

        ANALYSIS_BEGIN(0);
        ret = __rpc_request_getsolt(&msgid, &ctx, name, &sockid, nid, timeout);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __rpc_request_send(&sockid, &msgid, request, reqlen, wbuf, msg_type, priority);
        if (unlikely(ret)) {
                ret = _errno_net(ret);
                YASSERT(ret == ENONET || ret == ESHUTDOWN || ret == ESTALE || ret == ENOSYS);
                GOTO(err_free, ret);
        }

        DBUG("%s msgid (%u, %x) to %s\n", name, msgid.idx, msgid.figerprint,
             _inet_ntoa(sockid.addr));

        ANALYSIS_END(0, IO_WARN, NULL);
        
        ret = __rpc_request_wait__(nh, name, rbuf, &ctx, timeout);
        if (unlikely(ret)) {
                goto err_ret;
        }

        return 0;
err_free:
        __rpc_request_reset(&msgid);
err_ret:
        return ret;
}

int rpc_request_wait(const char *name, const nid_t *nid, const void *request, int reqlen,
                     void *reply, int *replen, int msg_type,
                     int priority, int timeout)
{
        int ret;
        buffer_t buf;
        net_handle_t nh;

        mbuffer_init(&buf, 0);
        id2nh(&nh, nid);


        ret = __rpc_request_wait(name, &nh, request, reqlen, NULL, &buf, msg_type, priority, timeout);
        if (unlikely(ret))
                goto err_ret;

        if (buf.len) {
                YASSERT(reply);
                if (replen) {
                        YASSERT((int)buf.len <= *replen);
                        *replen = buf.len;
                }

                mbuffer_get(&buf, reply, buf.len);
                mbuffer_free(&buf);
        } else {
                //YASSERT(reply == NULL);
                //YASSERT(replen == NULL);
        }

        return 0;
err_ret:
        return ret;
}


//for write
int rpc_request_wait1(const char *name, const nid_t *nid, const void *request,
                      int reqlen, const buffer_t *wbuf, int msg_type,
                      int priority, int timeout)
{
        int ret;
        net_handle_t nh;

        id2nh(&nh, nid);
        ret = __rpc_request_wait(name, &nh, request, reqlen, wbuf, NULL, msg_type, priority, timeout);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

//for read
int rpc_request_wait2(const char *name, const nid_t *nid, const void *request,
                      int reqlen, buffer_t *rbuf, int msg_type,
                     int priority, int timeout)
{
        int ret;
        net_handle_t nh;

        YASSERT(rbuf);

        id2nh(&nh, nid);
        ret = __rpc_request_wait(name, &nh, request, reqlen, NULL, rbuf, msg_type, priority, timeout);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int rpc_request_wait_sock(const char *name, const net_handle_t *nh, const void *request,
                          int reqlen, void *reply, int *replen, int msg_type,
                          int priority, int timeout)
{
        int ret;
        buffer_t buf;

        mbuffer_init(&buf, 0);
        ret = __rpc_request_wait(name, nh, request, reqlen, NULL, &buf, msg_type, priority, timeout);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (buf.len) {
                YASSERT(reply);
                if (replen) {
                        YASSERT((int)buf.len <= *replen);
                        *replen = buf.len;
                }

                mbuffer_get(&buf, reply, buf.len);
                mbuffer_free(&buf);
        } else {
                YASSERT(reply == NULL);
                YASSERT(replen == NULL);
        }

        return 0;
err_ret:
        return ret;
}

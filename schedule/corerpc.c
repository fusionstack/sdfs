#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBSCHEDULE

#include "ynet_net.h"
#include "adt.h"
#include "job.h"
#include "job_dock.h"
#include "ylib.h"
#include "ynet_rpc.h"
#include "configure.h"
#include "rpc_table.h"
#include "rpc_proto.h"
#include "net_global.h"
#include "corenet.h"
#include "corerpc.h"
#include "corenet_maping.h"
#include "network.h"
#include "variable.h"
#include "dbg.h"

typedef struct {
        sem_t sem;
        task_t task;
        uint64_t latency;
} rpc_ctx_t;

static net_prog_t net_prog[MSG_MAX];

rpc_table_t *corerpc_self()
{
        return variable_get(VARIABLE_CORERPC);
}

rpc_table_t *corerpc_self_byctx(void *ctx)
{
        return variable_get_byctx(ctx, VARIABLE_CORERPC);
}

static void __request_nosys(void *arg)
{
        sockid_t sockid;
        msgid_t msgid;
        buffer_t buf;
        nid_t nid;

        request_trans(arg, &nid, &sockid, &msgid, &buf, NULL);

        schedule_task_setname("nosys");
        mbuffer_free(&buf);
        corerpc_reply_error(&sockid, &msgid, ENOSYS);
        return;
}

static void __request_stale(void *arg)
{
        sockid_t sockid;
        msgid_t msgid;
        buffer_t buf;
        nid_t nid;

        request_trans(arg, &nid, &sockid, &msgid, &buf, NULL);

        DERROR("got stale msg\n");
        
        schedule_task_setname("stale");
        mbuffer_free(&buf);
        corerpc_reply_error(&sockid, &msgid, ESTALE);
        return;
}


static void __corerpc_post_task(void *arg1, void *arg2, void *arg3, void *arg4)
{
        rpc_ctx_t *ctx = arg1;
        int retval = *(int *)arg2;
        buffer_t *buf = arg3;
        uint64_t latency = *(uint64_t *)arg4;

        ctx->latency = latency;

        schedule_resume(&ctx->task, retval, buf);
}

static void __corerpc_request_reset(const msgid_t *msgid)
{
        rpc_table_t *__rpc_table_private__ = corerpc_self();
        schedule_task_reset();
        rpc_table_free(__rpc_table_private__, msgid);
}

STATIC int __corerpc_getsolt(void *_ctx, msgid_t *msgid, rpc_ctx_t *ctx, const char *name,
                             const sockid_t *sockid, const nid_t *nid, int timeout)
{
        int ret;
        rpc_table_t *__rpc_table_private__ = corerpc_self_byctx(_ctx);

        ret = rpc_table_getsolt(__rpc_table_private__, msgid, name);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ctx->task = schedule_task_get();

        ret = rpc_table_setsolt(__rpc_table_private__, msgid,
                                __corerpc_post_task, ctx, sockid, nid, timeout);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        return 0;
err_ret:
        return ret;
}

STATIC int __corerpc_wait__(const nid_t *nid, const char *name, buffer_t *rbuf, rpc_ctx_t *ctx, int timeout)
{
        int ret;

        (void) timeout;
        
        ANALYSIS_BEGIN(0);
        //ANALYSIS_BEGIN(1);

        DBUG("%s yield wait\n", name);
        ret = schedule_yield(name, rbuf, ctx);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        DBUG("%s latency %ju\n", netable_rname_nid(nid), ctx->latency);

        netable_load_update(nid, ctx->latency);
        
        //ANALYSIS_QUEUE(1, IO_WARN, "corenet_wait");
        
        DBUG("%s yield resume\n", name);

#ifdef RPC_ASSERT
        timeout = _max(timeout, _get_rpc_timeout());
        ANALYSIS_ASSERT(0, 1000 * 1000 * (timeout * 3), name);
#else
        ANALYSIS_END(0, IO_WARN, name);
#endif

        return 0;
err_ret:
#ifdef RPC_ASSERT
        timeout = _max(timeout, _get_rpc_timeout());
        ANALYSIS_ASSERT(0, 1000 * 1000 * (timeout * 3), name);
#else
        ANALYSIS_END(0, IO_WARN, name);
#endif
        return ret;
}

#if ENABLE_RDMA
static void __corerpc_msgid_prep(msgid_t *msgid, const buffer_t *wbuf, buffer_t *rbuf,
                                int msg_size, const rdma_conn_t *handler)
{
	YASSERT(msg_size <= (1024 * 1024));

	/*if (msg_size >= (512 * 1024)) {
		DWARN("big io size %d\n", msg_size);
		_backtrace("BIGIIO");
	} */

	if (wbuf != NULL) {
		msgid->data_prop.rkey = handler->mr->rkey;
		msgid->data_prop.remote_addr = (uintptr_t)mbuffer_head(wbuf);
		msgid->data_prop.size = msg_size;
		msgid->opcode = CORERPC_WRITE;
	} else {
		YASSERT(rbuf != NULL);
		mbuffer_init(rbuf, msg_size);

		msgid->data_prop.rkey = handler->mr->rkey;
		msgid->data_prop.remote_addr = (uintptr_t)mbuffer_head(rbuf);
		msgid->data_prop.size = msg_size;
		msgid->opcode = CORERPC_READ;
	}
}
#endif

STATIC int __corerpc_send(void *ctx, msgid_t *msgid, const sockid_t *sockid, const void *request,
                          int reqlen, const buffer_t *wbuf, buffer_t *rbuf, int msg_type, int msg_size)
{
        int ret;
        buffer_t buf;

#if ENABLE_RDMA
        if (likely(sockid->rdma_handler > 0)) {

                 rdma_conn_t *handler = (rdma_conn_t *)sockid->rdma_handler;
                 __corerpc_msgid_prep(msgid, wbuf, rbuf, msg_size, handler);

                 ret = rpc_request_prep(&buf, msgid, request, reqlen, wbuf, msg_type, 0, -1);
                 if (unlikely(ret))
                         GOTO(err_ret, ret);

                 ret = corenet_rdma_send(sockid, &buf, 0);
                 if (unlikely(ret)) {
                        GOTO(err_free, ret);
                 }
        } else {
                ret = rpc_request_prep(&buf, msgid, request, reqlen, wbuf, msg_type, 1, -1);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                DBUG("send msg to %s, id (%u, %x), len %u\n",
                                _inet_ntoa(sockid->addr), msgid->idx,
                                msgid->figerprint, buf.len);

                ret = corenet_tcp_send(ctx, sockid, &buf, 0);
                if (unlikely(ret)) {
                        GOTO(err_free, ret);
                }
        }
#else
        (void) rbuf;
        (void) msg_size;

        ret = rpc_request_prep(&buf, msgid, request, reqlen, wbuf, msg_type, 1, -1);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DBUG("send msg to %s, id (%u, %x), len %u\n",
             _inet_ntoa(sockid->addr), msgid->idx,
             msgid->figerprint, buf.len);

        ret = corenet_tcp_send(ctx, sockid, &buf, 0);
        if (unlikely(ret)) {
                GOTO(err_free, ret);
        }
#endif
        
        return 0;
err_free:
        mbuffer_free(&buf);
err_ret:
        return ret;
}

static inline int __get_buffer_seg_count(const buffer_t *buf)
{
        int count = 0;
        struct list_head *pos;

        list_for_each(pos, &buf->list) {
                count++;
        }

        return count;
}

int corerpc_send_and_wait(void *_ctx, const char *name, const sockid_t *sockid,
                          const nid_t *nid, const void *request,
                          int reqlen, const buffer_t *wbuf, buffer_t *rbuf,
                          int msg_type, int msg_size, int timeout)
{
        int ret;
        msgid_t msgid;
        rpc_ctx_t ctx;
        const buffer_t *tmp = wbuf;
        buffer_t cbuf;
        int wbuf_seg_count = 0;

        ret = __corerpc_getsolt(_ctx, &msgid, &ctx, name, sockid, nid, timeout);
        if (unlikely(ret))
                GOTO(err_ret, ret);

	if (wbuf) {
		wbuf_seg_count = __get_buffer_seg_count(wbuf);
		if (unlikely(wbuf_seg_count > 1)) {
			mbuffer_init(&cbuf, 0);
			mbuffer_clone(&cbuf, wbuf);
			tmp = &cbuf;
		}
	}

        ret = __corerpc_send(_ctx, &msgid, sockid, request, reqlen, tmp, rbuf, msg_type, msg_size);
        if (unlikely(ret)) {
                corenet_maping_close(nid, sockid);
		ret = _errno_net(ret);
		YASSERT(ret == ENONET || ret == ESHUTDOWN);
		if (wbuf_seg_count > 1)
			mbuffer_free(&cbuf);

#if ENABLE_RDMA
                // bugfix #11347
                if (likely(sockid->rdma_handler > 0 && rbuf && wbuf == NULL))
                        mbuffer_free(rbuf);
#else
                if (rbuf && wbuf == NULL)
                        mbuffer_free(rbuf);
#endif
		GOTO(err_free, ret);
	}

        DBUG("%s msgid (%u, %x) to %s\n", name, msgid.idx,
             msgid.figerprint, _inet_ntoa(sockid->addr));

        ret = __corerpc_wait__(nid, name, rbuf, &ctx, timeout);
        if (unlikely(ret)) {
#if ENABLE_RDMA
                if (likely(sockid->rdma_handler > 0 && rbuf && wbuf == NULL))
                        mbuffer_free(rbuf);
#else
                if (rbuf && wbuf == NULL)
                        mbuffer_free(rbuf);
#endif
                GOTO(err_ret, ret);
        }

	if (unlikely(wbuf_seg_count > 1))
		mbuffer_free(&cbuf);

        return 0;

err_free:
        __corerpc_request_reset(&msgid);
err_ret:
        return ret;
}

int IO_FUNC corerpc_postwait(const char *name, const nid_t *nid, const void *request,
                             int reqlen, const buffer_t *wbuf, buffer_t *rbuf,
                             int msg_type, int msg_size, int timeout)
{
        int ret;
        sockid_t sockid;
        void *ctx = variable_get_ctx();
        //rpc_table_t *__rpc_table_private__ = corerpc_self();

        ret = corenet_maping(ctx, nid, &sockid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = corerpc_send_and_wait(ctx, name, &sockid, nid, request, reqlen,
                                    wbuf, rbuf, msg_type, msg_size, timeout);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __corerpc_request_handler(corerpc_ctx_t *ctx, const ynet_net_head_t *head,
                                 buffer_t *buf)
{
        int ret;
        rpc_request_t *rpc_request;
        const msgid_t *msgid;
        net_prog_t *prog;
        sockid_t *sockid;
        net_request_handler handler;

        sockid = &ctx->sockid;
        YASSERT(sockid->addr);
        DBUG("new msg from %s/%u, id (%u, %x)\n",
              _inet_ntoa(sockid->addr), sockid->sd, head->msgid.idx,
              head->msgid.figerprint);

        msgid = &head->msgid;
        prog = &net_prog[head->prog];

#ifdef HAVE_STATIC_ASSERT
        static_assert(sizeof(*rpc_request)  < sizeof(mem_cache128_t), "rpc_request_t");
#endif

        rpc_request = mem_cache_calloc(MEM_CACHE_128, 0);
        if (!rpc_request) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        YASSERT(sockid->addr);
        rpc_request->sockid = *sockid;
        rpc_request->msgid = *msgid;
        rpc_request->ctx = ctx;
        mbuffer_init(&rpc_request->buf, 0);
        mbuffer_merge(&rpc_request->buf, buf);

        if (unlikely(head->master_magic != ng.master_magic)) {
                DERROR("got stale msg, master_magic %x:%x\n", head->master_magic, ng.master_magic);
                handler = __request_stale;
        } else {
                handler = prog->handler ? prog->handler : __request_nosys;
        }
        
        schedule_task_new("corenet", handler,
                          rpc_request, head->priority);

        if (!gloconf.rdma) {
                netable_load_update(&ctx->nid, head->load);
                DBUG("update %s latency %ju\n", network_rname(&ctx->nid), head->load);
        }

        return 0;
err_ret:
        return ret;
}

static void __corerpc_reply_handler(const ynet_net_head_t *head, buffer_t *buf)
{
        int ret, retval;
        rpc_table_t *__rpc_table_private__ = corerpc_self();

        retval = ynet_pack_err(buf);
        if (retval)
                mbuffer_free(buf);

        DBUG("reply msg id (%u, %x)\n", head->msgid.idx,
             head->msgid.figerprint);

        ret = rpc_table_post(__rpc_table_private__, &head->msgid, retval, buf, head->load);
        if (unlikely(ret)) {
                mbuffer_free(buf);
        }
}

static int __corerpc_handler(corerpc_ctx_t *ctx, buffer_t *buf)
{
        int ret;
        ynet_net_head_t head;

        ANALYSIS_BEGIN(0);

        ret = ynet_pack_crcverify(buf);
        if (unlikely(ret)) {
                mbuffer_free(buf);
                YASSERT(0);
        }

        DBUG("new msg %u\n", buf->len);

        ret = mbuffer_popmsg(buf, &head, sizeof(ynet_net_head_t));
        if (unlikely(ret))
                YASSERT(0);

        switch (head.type) {
        case YNET_MSG_REQ:
                __corerpc_request_handler(ctx, &head, buf);
                break;
        case YNET_MSG_REP:
                __corerpc_reply_handler(&head, buf);
                break;
        default:
                DERROR("bad msgtype\n");
        }

        ANALYSIS_END(0, 1000 * 100, NULL);

        return 0;
}

static int __corerpc_len(void *buf, uint32_t len)
{
        int msg_len, io_len;
        ynet_net_head_t *head;

        YASSERT(len >= sizeof(ynet_net_head_t));
        head = buf;

        YASSERT(head->magic == YNET_PROTO_TCP_MAGIC);

        DBUG("len %u %u\n", head->len, head->blocks);

        if (head->blocks) {
                msg_len =  head->len - head->blocks;
                io_len = head->blocks;
                YASSERT(io_len > 0);
        } else {
                msg_len =  head->len;
                io_len = 0;
        }

        return msg_len + io_len;
}

#if ENABLE_RDMA
static int __corerpc_rdma_handler(corerpc_ctx_t *ctx, buffer_t *data_buf, buffer_t *msg_buf)
{
        int ret;
        ynet_net_head_t *head;

        ANALYSIS_BEGIN(0);

        if (data_buf) {
                mbuffer_merge(msg_buf, data_buf);
                head = mbuffer_head(msg_buf);
                head->len += data_buf->len;
        }
        
    //    DWARN("for bug test before popmsg :%u\n", msg_buf->len);
        ret = mbuffer_rdma_popmsg(msg_buf, (void **)&head, sizeof(ynet_net_head_t));
        if (unlikely(ret))
                YASSERT(0);
     //   DWARN("for bug test after popmsg :%u\n", msg_buf->len);

        switch (head->type) {
                case YNET_MSG_REQ:
                case YNET_MSG_RECV:
                        __corerpc_request_handler(ctx, head, msg_buf);
                        break;
                case YNET_MSG_REP:
                        __corerpc_reply_handler(head, msg_buf);
                        break;
                default:
                        DERROR("bad msgtype:%d\n", head->type);
                        YASSERT(0);
        }

        ANALYSIS_END(0, 1000 * 100, NULL);

        return 0;
}


int corerpc_rdma_recv_data(void *_ctx, void *_data_buf, void *_msg_buf)
{
        corerpc_ctx_t *ctx = _ctx;
        buffer_t *data_buf = _data_buf;
        buffer_t *msg_buf  = _msg_buf;
        int ret;
        
        ret = __corerpc_rdma_handler(ctx, data_buf, msg_buf);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int corerpc_rdma_recv_msg(void *_ctx, void *iov, int *_count)
{
        int len = 0, left = 0;
        buffer_t _buf;
        void *msg = iov - 256;
        corerpc_ctx_t *ctx = _ctx;
        ynet_net_head_t *net_head = NULL;
        sockid_t *sockid;

        left = *_count;
        sockid = &ctx->sockid;

        while(left >= (int)sizeof(ynet_net_head_t)) {
                net_head = msg;
                len = __corerpc_len((void *)net_head, sizeof(ynet_net_head_t)); 
                mbuffer_initwith(&_buf, msg, len, iov, corenet_rdma_post_recv);
                
                if (net_head->blocks) {
                        net_head->type = YNET_MSG_RECV;
                        corenet_rdma_send(sockid, &_buf, 0);
                } else {
                        __corerpc_rdma_handler(ctx, NULL, &_buf);
                }
                
                YASSERT(len <= 256);
                break;
        }

        *_count = len;

        return 0;
}
#endif

int corerpc_recv(void *_ctx, void *buf, int *_count)
{
        int len, count = 0;
        char tmp[MAX_BUF_LEN];
        buffer_t _buf, *mbuf = buf;
        corerpc_ctx_t *ctx = _ctx;

        DBUG("recv %u\n", mbuf->len);

        while (mbuf->len >= sizeof(ynet_net_head_t)) {
                mbuffer_get(mbuf, tmp, sizeof(ynet_net_head_t));
                len = __corerpc_len(tmp, sizeof(ynet_net_head_t));

                DBUG("msg len %u\n", len);
                
                if (len > (int)mbuf->len) {
                        DBUG("wait %u %u\n", len, mbuf->len);
                        break;
                }

                mbuffer_init(&_buf, 0);
                mbuffer_pop(buf, &_buf, len);

                __corerpc_handler(ctx, &_buf);
                count++;
        }

        *_count = count;

        return 0;
}

void corerpc_register(int type, net_request_handler handler, void *context)
{
        net_prog_t *prog;

        prog = &net_prog[type];

        prog->handler = handler;
        prog->context = context;
}

void corerpc_close(void *_ctx)
{
        corerpc_ctx_t *ctx = _ctx;

        if (ctx->running) {
                DWARN("running %u\n", ctx->running);
                EXIT(EAGAIN);
        }

        yfree((void **)&ctx);
}


void IO_FUNC corerpc_reply1(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf)
{
        int ret;
        buffer_t reply_buf;

        DBUG("reply msgid (%d, %x) %s\n", msgid->idx, msgid->figerprint,
             _inet_ntoa(sockid->addr));

#if ENABLE_RDMA
        int seg_count = 0;
        buffer_t data_buf, tmp, *_tmp;
        if (sockid->rdma_handler > 0) {
                mbuffer_init(&data_buf, 0);
                if (_buf && _buf->len) {
			_tmp = _buf;
			seg_count = __get_buffer_seg_count(_buf);
			if (unlikely(seg_count > 2)) {
				_backtrace("corerpc_reply");
				YASSERT(0);
			}
			
			if (unlikely(seg_count > 1)) {
				mbuffer_clone(&tmp, _buf);
				mbuffer_free(_buf);
				_tmp = &tmp;
			} 
			
                        rpc_reply_prep1(msgid, &data_buf, _tmp);
                        ret = corenet_rdma_send(sockid, &data_buf, 0);
                        if (unlikely(ret)) {
                                DERROR("corenet rdma send data failed ret %d \n", ret);
                                mbuffer_free(&data_buf);
                                return;
                        }
                }

                rpc_reply_prep(msgid, &reply_buf, NULL, 0);
                ret = corenet_rdma_send(sockid, &reply_buf, 0);
                if (unlikely(ret)) {
                        DERROR("corenet rdma post send reply fail ret:%d\n", ret);
                        mbuffer_free(&reply_buf);
                        mbuffer_free(&data_buf);
                }

        } else {
                rpc_reply_prep(msgid, &reply_buf, _buf, 1);

                ret = corenet_tcp_send(NULL, sockid, &reply_buf, 0);
                if (unlikely(ret))
                        mbuffer_free(&reply_buf);
        }
#else
        rpc_reply_prep(msgid, &reply_buf, _buf, 1);

        ret = corenet_tcp_send(NULL, sockid, &reply_buf, 0);
        if (unlikely(ret))
                mbuffer_free(&reply_buf);
#endif
}

void corerpc_reply(const sockid_t *sockid, const msgid_t *msgid, const void *_buf, int len)
{
        buffer_t buf;

        mbuffer_init(&buf, 0);
        if (len)
                mbuffer_copy(&buf, _buf, len);

        corerpc_reply1(sockid, msgid, &buf);
}

void corerpc_reply_error(const sockid_t *sockid, const msgid_t *msgid, int _error)
{
        int ret;
        buffer_t buf;

#if ENABLE_RDMA
        rpc_reply_error_prep(msgid, &buf, _error);
        if (sockid->rdma_handler > 0) {
                ret = corenet_rdma_send(sockid, &buf, 0);
        } else {
                ret = corenet_tcp_send(NULL, sockid, &buf, 0);
        }
        if (unlikely(ret))
                mbuffer_free(&buf);
#else
        rpc_reply_error_prep(msgid, &buf, _error);
        ret = corenet_tcp_send(NULL, sockid, &buf, 0);
        if (unlikely(ret))
                mbuffer_free(&buf);
        
#endif
}

int corerpc_init(const char *name, core_t *core)
{
        int ret;
        rpc_table_t *__rpc_table_private__;

        ret = rpc_table_init(name, &__rpc_table_private__, 1);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        variable_set(VARIABLE_CORERPC, __rpc_table_private__);
        core->rpc_table = __rpc_table_private__;

        return 0;
err_ret:
        return ret;
}

void corerpc_scan(void *ctx)
{
        rpc_table_t *__rpc_table_private__ = corerpc_self_byctx(ctx);

        if (likely(__rpc_table_private__)) {
#if 1
                rpc_table_scan(__rpc_table_private__, _ceil(_get_timeout(), 10), 1);
                schedule_run(variable_get_byctx(ctx, VARIABLE_SCHEDULE));
#else
                rpc_table_scan(__rpc_table_private__, _ceil(_get_timeout(), 10), 0);
#endif
        }
}

#if ENABLE_RDMA
void corerpc_rdma_reset(const sockid_t *sockid)
{
        rpc_table_t *__rpc_table_private__ = NULL;
        rdma_conn_t *rdma_handler = NULL;

        rdma_handler = (rdma_conn_t *)sockid->rdma_handler;
        __rpc_table_private__ = rdma_handler->core->rpc_table;

        if (__rpc_table_private__) {
                DINFO("rpc table reset ... \n");
                rpc_table_reset(__rpc_table_private__, sockid, NULL);
        } else 
                YASSERT(0);
}
#endif

void corerpc_reset(const sockid_t *sockid)
{
        rpc_table_t *__rpc_table_private__ = corerpc_self();

        if (__rpc_table_private__) {
                rpc_table_reset(__rpc_table_private__, sockid, NULL);
        }
}

void corerpc_destroy(rpc_table_t **_rpc_table)
{
        rpc_table_t *__rpc_table_private__ = corerpc_self();
        
        rpc_table_destroy(&__rpc_table_private__);
        *_rpc_table = NULL;
        variable_unset(VARIABLE_CORERPC);
}

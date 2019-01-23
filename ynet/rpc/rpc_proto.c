#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "net_global.h"
#include "job_dock.h"
#include "rpc_proto.h"
#include "../net/net_events.h"
#include "ynet_rpc.h"
#include "rpc_table.h"
#include "schedule.h"
#include "mem_cache.h"
#include "adt.h"
#include "dbg.h"

static net_prog_t  net_prog[MSG_MAX];

static void __request_nosys(void *arg)
{
        sockid_t sockid;
        msgid_t msgid;
        buffer_t buf;

        request_trans(arg, NULL, &sockid, &msgid, &buf, NULL);

        schedule_task_setname("nosys");
        mbuffer_free(&buf);
        rpc_reply_error(&sockid, &msgid, ENOSYS);
        return;
}

inline static void __request_stale(void *arg)
{
        sockid_t sockid;
        msgid_t msgid;
        buffer_t buf;

        request_trans(arg, NULL, &sockid, &msgid, &buf, NULL);

        schedule_task_setname("stale");
        mbuffer_free(&buf);
        rpc_reply_error(&sockid, &msgid, ESTALE);
        return;
}

int rpc_pack_len(void *buf, uint32_t len, int *msg_len, int *io_len)
{
        int ret;
        ynet_net_head_t *head;

        if (len > sizeof(uint32_t)) {
                head = buf;
                YASSERT(head->magic == YNET_PROTO_TCP_MAGIC);
        }

        if (len < sizeof(ynet_net_head_t)) {
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }

        head = buf;

        if (head->blocks) {
                *msg_len =  head->len - head->blocks;
                *io_len = head->blocks;
                YASSERT(*io_len > 0);
        } else {
                *msg_len =  head->len;
                *io_len = 0;
        }

        DBUG("magic %x, msg_len %u io_len %u\n", head->magic, *msg_len, *io_len);
        
        YASSERT(*msg_len > 0);

        return 0;
err_ret:
        return ret;
}

static int __rpc_request_handler(const nid_t *nid, const sockid_t *sockid,
                                 const ynet_net_head_t *head, buffer_t *buf)
{
        int ret;
        rpc_request_t *rpc_request;
        const msgid_t *msgid;
        net_prog_t *prog;
        net_request_handler handler;

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

        rpc_request->sockid = *sockid;
        rpc_request->msgid = *msgid;
        if (nid) {
                rpc_request->nid = *nid;
        } else {
                rpc_request->nid.id = 0;
        }

        mbuffer_init(&rpc_request->buf, 0);
        mbuffer_merge(&rpc_request->buf, buf);

#if 0
        if (unlikely(head->master_magic != ng.master_magic)) {
                DWARN("got stale msg, master_magic 0x%x -> 0x%x\n", ng.master_magic, head->master_magic);
                handler = __request_stale;
        } else {
                handler = prog->handler ? prog->handler : __request_nosys;
        }
#else
        UNIMPLEMENTED(__NULL__);
        handler = prog->handler ? prog->handler : __request_nosys;
#endif
        
        schedule_task_new("rpc", handler,
                          rpc_request, head->priority);

        return 0;
err_ret:
        return ret;
}

static void __rpc_reply_handler(const ynet_net_head_t *head, buffer_t *buf)
{
        int ret, retval;

        retval = ynet_pack_err(buf);
        if (retval)
                mbuffer_free(buf);

        DBUG("reply msg id (%u, %x)\n", head->msgid.idx,
             head->msgid.figerprint);

        ret = rpc_table_post(__rpc_table__, &head->msgid, retval, buf, head->load);
        if (unlikely(ret)) {
                mbuffer_free(buf);
        }
}

int rpc_pack_handler(const nid_t *nid, const sockid_t *sockid, buffer_t *buf)
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
                __rpc_request_handler(nid, sockid, &head, buf);
                break;
        case YNET_MSG_REP:
                __rpc_reply_handler(&head, buf);
                break;
        default:
                DERROR("bad msgtype\n");
        }

        ANALYSIS_END(0, 1000 * 100, NULL);

        return 0;
}

void rpc_request_register(int type, net_request_handler handler, void *context)
{
        net_prog_t *prog;

        prog = &net_prog[type];

        prog->handler = handler;
        prog->context = context;
}

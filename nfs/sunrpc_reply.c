

#include <arpa/inet.h>
#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_YRPC

#include "sunrpc_proto.h"
#include "net_global.h"
#include "job_dock.h"
#include "sunrpc_reply.h"
#include "job_tracker.h"
#include "ylib.h"
#include "core.h"
#include "corenet.h"
#include "nfs_conf.h"
#include "ynet_net.h"
#include "dbg.h"

#pragma pack(1)

typedef struct {
        uint32_t length;
        uint32_t xid;
        uint32_t msgtype;
        uint32_t replystate;
        auth_head_t veri;
        uint32_t acceptstate;
} sunrpc_reply_t;

#pragma pack()

int sunrpc_reply(const sockid_t *sockid, const sunrpc_request_t *req,
                 int state, void *res, xdr_ret_t xdr_ret)
{
        int ret;
        sunrpc_reply_t *rep;
        xdr_t xdr;
        buffer_t buf;

        if (sizeof(sunrpc_reply_t) > Y_PAGE_SIZE) {
                DERROR("why we get such a request? \n");

                YASSERT(0);
        }

        ret = mbuffer_init(&buf, sizeof(sunrpc_reply_t));
        if (ret)
                GOTO(err_ret, ret);

        rep = mbuffer_head(&buf);
        rep->xid = req->xid;
        rep->msgtype = htonl(SUNRPC_REP_MSG);
        rep->veri.flavor = 0;
        rep->veri.length = 0;
        rep->replystate = 0;
        rep->acceptstate = htonl(state);

        if (res && xdr_ret) {
                xdr.op = __XDR_ENCODE;
                xdr.buf = &buf;

                if(xdr_ret(&xdr, res))
                {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }

        rep->length = htonl(buf.len - sizeof(uint32_t));
        rep->length = htonl(1 << 31) | rep->length;

        DBUG("buf len %llu\n", (LLU)(buf.len - sizeof(uint32_t)));

#if ENABLE_CO_WORKER
        ret = corenet_tcp_send(sockid, &buf, 0);
        if (unlikely(ret))
                GOTO(err_free, ret);


        DBUG("--------------------reply success--------------------\n");
#else
        
        net_handle_t nh;
#if ENABLE_CORE_PIPELINE
        ret = core_pipeline_send(sockid, &buf, 0);
        if (unlikely(ret)) {
                ret = _errno_net(ret);
                if (ret == ENOSYS) {
                        sock2nh(&nh, sockid);
                        ret = sdevent_queue(&nh, &buf, 0);
                        if (unlikely(ret)) {
                                ret = _errno_net(ret);
                                GOTO(err_free, ret);
                        }
                } else
                        GOTO(err_free, ret);
        }
#else
        sock2nh(&nh, sockid);
        ret = sdevent_queue(&nh, &buf, 0);
        if (unlikely(ret)) {
                ret = _errno_net(ret);
                GOTO(err_free, ret);
        }
#endif
#endif

        return 0;
err_free:
        mbuffer_free(&buf);
err_ret:
        return ret;
}

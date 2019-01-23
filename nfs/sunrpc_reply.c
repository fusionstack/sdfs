

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

extern jobtracker_t *jobtracker;

int sunrpc1_reply_prep(job_t *job, xdr_ret_t xdr_ret, void *buf, int state)
{
        int ret;
        sunrpc_reply_t *rep;
        sunrpc_request_t *req;
        xdr_t xdr;

        if (sizeof(sunrpc_reply_t) > Y_PAGE_SIZE) {
                DERROR("why we get such a request? \n");

                YASSERT(0);
        }

        ret = mbuffer_init(&job->reply, sizeof(sunrpc_reply_t));
        if (ret)
                GOTO(err_ret, ret);

        rep = mbuffer_head(&job->reply);
        req = (void *)job->buf;

        rep->xid = req->xid;
        rep->msgtype = htonl(SUNRPC_REP_MSG);
        rep->veri.flavor = 0;
        rep->veri.length = 0;
        rep->replystate = 0;
        rep->acceptstate = htonl(state);

        if (buf && xdr_ret) {
                xdr.op = __XDR_ENCODE;
                xdr.buf = &job->reply;

                if(xdr_ret(&xdr, buf))
                {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }

        rep->length = htonl(job->reply.len - sizeof(uint32_t));
        rep->length = htonl(1 << 31) | rep->length;

        DBUG("buf len %llu\n", (LLU)(job->reply.len - sizeof(uint32_t)));

        return 0;
err_ret:
        return ret;
}

int sunrpc1_reply_send(job_t *job, buffer_t *_buf, mbuffer_op_t op)
{
        int ret;
        net_handle_t nh;
        buffer_t buf;

        YASSERT(_buf == &job->reply);
        YASSERT(op == FREE_JOB);

        mbuffer_init(&buf, 0);
        mbuffer_merge(&buf, _buf);
        sock2nh(&nh, &job->sock);

        job_destroy(job);
        ret = sdevent_queue(&nh, &buf, 0);
        if (ret) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

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

        net_handle_t nh;
        sock2nh(&nh, sockid);
        ret = sdevent_queue(&nh, &buf, 0);
        if (ret) {
                GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}

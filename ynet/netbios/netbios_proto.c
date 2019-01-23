

#include <arpa/inet.h>
#include <errno.h>

#define DBG_SUBSYS S_YRPC

#include "job_dock.h"
#include "net_global.h"
#include "rpc/auth.h"
#include "netbios_proto.h"
#include "job_tracker.h"
#include "../net/net_events.h"
#include "ynet_rpc.h"
#include "dbg.h"

const char job_netbios_request[] = "netbios_request";

extern net_request_handler netbios_request_handler;
jobtracker_t *netbios_jobtracker;

int netbios_pack_len(void *buf, uint32_t _len)
{
        uint32_t *length, len, type;

        if (_len < sizeof(uint32_t)) {
                DERROR("less then netbios_head_t\n");

                return 0;
        }

        length = buf;

        len = ntohl(*length) & (~(0xFF << 24));
        type = ntohl(*length) & (0xFF << 24);

        DBUG("netbios request len %u, is last %u\n", len, type);

        YASSERT(type == 0);

        return len + sizeof(uint32_t);
}

int netbios_pack_handler(void *self, void *_buf)
{
        int ret;
        smb_head_t *req;
        buffer_t *buf;
        job_t *job;
        ynet_sock_conn_t *sock;
        uint32_t len;
        net_handle_t *nh;
        net_prog_t *prog;

        buf = _buf;
        sock = self;

        ret = mbuffer_popmsg(buf, &len, sizeof(uint32_t));
        if (ret)
                GOTO(err_ret, ret);

        ret = job_create(&job, netbios_jobtracker, job_netbios_request);
        if (unlikely(ret))
                GOTO(err_ret, ret); //GOTO???

        req = (void *)job->buf;

        ret = mbuffer_popmsg(buf, req, sizeof(smb_head_t));
        if (ret)
                GOTO(err_job, ret);

        len = ntohl(len) & (~(0xFF << 24));

        if (memcmp(&req->protocol[1], "SMB", 3) != 0) {
                ret = EINVAL;
                GOTO(err_job, ret);
        }

        mbuffer_init(&job->request, 0);

        mbuffer_merge(&job->request, buf);

        DBUG("request len %u\n", job->request.len);

        nh = (void *)job->net;
        *nh = sock->nh;

        job->id.idx = req->mid;
        job->id.seq = 0;

        prog = &sock->proto.prog[0];

        YASSERT(prog);

        prog->handler(job, sock, NULL);

        return 0;
err_job:
        job_destroy(job);
err_ret:
        mbuffer_free(buf);
        return ret;
}

int netbios_accept_handler(int fd, void *context)
{
        int ret;
        net_proto_t proto;
        net_handle_t nh;

        (void) context;

        DBUG("new conn for sd %d\n", fd);

        _memset(&proto, 0x0, sizeof(net_proto_t));

        proto.head_len = sizeof(smb_head_t);
        proto.reader = net_events_handle_read;
        proto.writer = net_events_handle_write;
        proto.pack_len = netbios_pack_len;
        proto.pack_handler = netbios_pack_handler;
        proto.prog[0].handler = netbios_request_handler;
        proto.jobtracker = netbios_jobtracker;

        ret = sdevents_accept(fd, &nh, &proto,
                              YNET_RPC_NONBLOCK);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdevents_align(&nh, 4);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdevents_add(&nh, Y_EPOLL_EVENTS);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

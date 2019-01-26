

#include <arpa/inet.h>
#include <rpc/auth.h>
#include <errno.h>

#define DBG_SUBSYS S_YRPC

#include "configure.h"
#include "job_dock.h"
#include "net_global.h"
#include "sunrpc_proto.h"
#include "job_tracker.h"
#include "../net/net_events.h"
#include "../../ynet/sock/sock_tcp.h"
#include "ynet_rpc.h"
#include "dbg.h"
#include "xdr.h"
#include "nfs_job_context.h"
#include "nfs_events.h"
#include "nfs_state_machine.h"
#include "xdr_nfs.h"

const char job_sunrpc_request[] = "sunrpc_request";

extern event_job_t sunrpc_nfs3_handler;
extern event_job_t sunrpc_mount_handler;
extern event_job_t sunrpc_acl_handler;
extern event_job_t sunrpc_nlm_handler;

jobtracker_t *sunrpc_jobtracker;

#define NFS3_WRITE 7

typedef struct {
        struct list_head hook;
        buffer_t buf;
        sockid_t sockid;
} rpcreq_t;

typedef struct {
        struct list_head rpcreq_list;
        int rpcreq_count;
        sy_spinlock_t lock;
        sem_t sem;
} rpcreq_queue_t;

static int __sunrpc_request_handler(const sockid_t *sockid,
                                    buffer_t *buf);

static rpcreq_queue_t __rpcreq_queue__;
rpcreq_queue_t *rpcreq_queue = &__rpcreq_queue__;

int sunrpc_pack_len(void *buf, uint32_t len, int *msg_len, int *io_len)
{
        uint32_t *length, _len, credlen, verilen, headlen;
        sunrpc_request_t *req;
        auth_head_t *cred, *veri;
        void *msg;
        
        if (len < sizeof(sunrpc_request_t)) {
                DERROR("less then sunrpc_head_t\n");

                return 0;
        }

        length = buf;

        DBUG("sunrpc request len %u, is last %u\n", ntohl(*length) ^ (1 << 31),
             ntohl(*length) & (1 << 31));

        if (ntohl(*length) & (1 << 31)) {
                _len = (ntohl(*length) ^ (1 << 31)) + sizeof(uint32_t);
                DBUG("_len %u\n", _len);
        } else {
                _len = (ntohl(*length)) + sizeof(uint32_t);
                DBUG("_len else %u\n", _len);
        }

        req = buf;

	//if (ntohl(req->procedure) == NFS3_WRITE) {
	if (0) {
#if 0
		(void) msg;
		(void) veri;
                (void) cred;
                (void) headlen;
                (void) verilen;
                (void) credlen;

                *io_len = (_len / 1024) * 1024;
                *msg_len = _len - *io_len;
#else
                cred = buf + sizeof(*req);
                credlen = ntohl(cred->length);

                veri = (void *)cred + credlen + sizeof(*cred);
                verilen = ntohl(veri->length);

                msg = (void *)veri + verilen + sizeof(*veri);

                headlen =  msg - buf + sizeof(fileid_t) + sizeof(uint64_t)
                        + sizeof(uint32_t)  * 4;

                *msg_len = headlen;
                *io_len = _len - headlen;

                YASSERT(_len > headlen);
                DBUG("nfs write msg %u io %u\n", *msg_len, *io_len);
#endif
        } else {
                *msg_len =  _len;
                *io_len = 0;
        }

        DBUG("msg_len: %u, io_len: %u\n", *msg_len, *io_len);

        return 0;
}

static inline int __auth_unix(auth_unix_t *auth_unix, char *buf, int buflen)
{
        xdr_t xdr_auth;
        buffer_t _buf_tmp;

        xdr_auth.op = __XDR_DECODE;
        xdr_auth.buf = &_buf_tmp;
        mbuffer_init(xdr_auth.buf, 0);

        mbuffer_copy(xdr_auth.buf, buf, buflen);

        __xdr_uint32(&xdr_auth, &auth_unix->stamp);
        __xdr_string(&xdr_auth, &auth_unix->machinename, 255);
        __xdr_uint32(&xdr_auth, &auth_unix->uid);
        __xdr_uint32(&xdr_auth, &auth_unix->gid);

        DBUG("stamp: %u, name: %s, uid: %u, gid: %u\n",
                        auth_unix->stamp, auth_unix->machinename,
                        auth_unix->uid, auth_unix->gid);

        mbuffer_free(xdr_auth.buf);
        /*auth_unix = (auth_unix_t *)credbuf;*/
        /*xdr_authunix_parms(credbuf+sizeof(auth_head_t), auth_unix);*/

        return 0;
}

static int __sunrpc_pack_handler(const nid_t *nid, const sockid_t *sockid, buffer_t *buf)
{
        sunrpc_proto_t type;
        rpcreq_t *rpcreq;

        (void) nid;
        (void) rpcreq;
        
        mbuffer_get(buf, &type, sizeof(sunrpc_proto_t));

        type.msgtype = ntohl(type.msgtype);

        DBUG("get sunrpc pack %d msg %d id %u\n", ntohl(type.length) ^ (1 << 31),
             type.msgtype, ntohl(type.xid));

        switch (type.msgtype) {
        case SUNRPC_REQ_MSG:
                __sunrpc_request_handler(sockid, buf);

                break;
        case SUNRPC_REP_MSG:
                DERROR("bad msgtype\n");

                YASSERT(0);
                //break;
        default:
                DERROR("bad msgtype\n");

                YASSERT(0);
        }

        return 0;
}

int sunrpc_accept_handler(void *_sock, void *context)
{
        int ret;
        net_proto_t proto;
        net_handle_t nh;
        ynet_sock_conn_t *sock = _sock;
        int fd = sock->nh.u.sd.sd;
        
        (void) context;

        DBUG("new conn for sd %d\n", fd);

        _memset(&proto, 0x0, sizeof(net_proto_t));

        proto.head_len = sizeof(sunrpc_request_t);
        proto.reader = net_events_handle_read;
        proto.writer = net_events_handle_write;
        proto.pack_len = sunrpc_pack_len;
        proto.pack_handler = __sunrpc_pack_handler;
        //proto.prog[0].handler = sunrpc_request_handler;
        proto.jobtracker = sunrpc_jobtracker;

        ret = sdevent_accept(fd, &nh, &proto, YNET_RPC_NONBLOCK);
        if (ret)
                GOTO(err_ret, ret);

#if 0
        ret = sdevents_align(&nh, 4);
        if (ret)
                GOTO(err_ret, ret);
#endif

        ret = sdevent_add(&nh, NULL, Y_EPOLL_EVENTS, NULL, NULL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int sunrpc_accept(int srv_sd)
{
        int ret, sd;
        struct sockaddr_in sin;
        socklen_t alen;
        net_proto_t proto;
        net_handle_t nh;

        _memset(&sin, 0, sizeof(sin));
        alen = sizeof(struct sockaddr_in);

        sd = accept(srv_sd, &sin, &alen);
        if (sd < 0 ) {
                ret = errno; 
                GOTO(err_ret, ret);
        }

        ret = tcp_sock_tuning(sd, 1, YNET_RPC_NONBLOCK);
        if (ret)
                GOTO(err_ret, ret);
        
        _memset(&proto, 0x0, sizeof(net_proto_t));
        proto.head_len = sizeof(sunrpc_request_t);
        proto.reader = net_events_handle_read;
        proto.writer = net_events_handle_write;
        proto.pack_len = sunrpc_pack_len;
        proto.pack_handler = __sunrpc_pack_handler;
        //proto.prog[0].handler = sunrpc_request_handler;
        proto.jobtracker = sunrpc_jobtracker;

        nh.type = NET_HANDLE_TRANSIENT;
        nh.u.sd.sd = sd;

        ret = sdevent_open(&nh, &proto);
        if (ret)
                GOTO(err_sd, ret);

        ret = sdevent_add(&nh, NULL, Y_EPOLL_EVENTS_LISTEN, NULL, NULL);
        if (ret)
                GOTO(err_sd, ret);

        return 0;
err_sd:
        close(sd);
err_ret:
        return ret;
}

static int __sunrpc_request_handler(const sockid_t *sockid,
                                    buffer_t *buf)
{
        int ret;
        uid_t uid;
        gid_t gid;
        sunrpc_request_t req;
        auth_head_t cred, veri;
        auth_unix_t auth_unix;
        char credbuf[MAX_AUTH_BYTES];
        char veribuf[MAX_AUTH_BYTES];
        uint32_t is_last;
        //net_handle_t *nh;

        ret = mbuffer_popmsg(buf, &req, sizeof(req));
        if (unlikely(ret))
                GOTO(err_ret, ret); //GOTO???

        is_last = ntohl(req.length) & (1 << 31);

        if (is_last)
                req.length = ntohl(req.length) ^ (1 << 31);
        else
                req.length = ntohl(req.length);

        req.xid = req.xid;
        req.msgtype = ntohl(req.msgtype);
        req.rpcversion = ntohl(req.rpcversion);
        req.program = ntohl(req.program);
        req.progversion = ntohl(req.progversion);
        req.procedure = ntohl(req.procedure);

        if (unlikely(is_last != (uint32_t)(1 << 31))) {
                DERROR("%u:%u\n", is_last, (uint32_t)(1 << 31));

                DERROR("rpc request len %u xid %u version %u prog %u version"
                                " %u procedure %u\n", req.length, req.xid,
                                req.rpcversion, req.program, req.progversion,
                                req.procedure);

                UNIMPLEMENTED(__DUMP__);
        }

        DBUG("rpc request len %u xid %u version %u prog %u version"
                        " %u procedure %u\n", req.length, req.xid,
                        req.rpcversion, req.program, req.progversion,
                        req.procedure);

        mbuffer_get(buf, &cred, sizeof(auth_head_t));
        cred.flavor = ntohl(cred.flavor);
        cred.length = ntohl(cred.length);

        ret = mbuffer_popmsg(buf, credbuf, cred.length + sizeof(auth_head_t));
        if (unlikely(ret))
                GOTO(err_ret, ret); //GOTO???

        if (cred.length) {
                ret = __auth_unix(&auth_unix, credbuf+sizeof(auth_head_t), cred.length);
                if (unlikely(ret))
                        GOTO(err_ret, ret); //GOTO???

                uid = auth_unix.uid;
                gid = auth_unix.gid;
                yfree((void**)&auth_unix.machinename);
        }

        mbuffer_get(buf, &veri, sizeof(auth_head_t));
        veri.flavor = ntohl(veri.flavor);
        veri.length = ntohl(veri.length);

        DBUG("cred %u len %u veri %u len %u\n", cred.flavor, cred.length,
                        veri.flavor, veri.length);

        ret = mbuffer_popmsg(buf, veribuf, veri.length + sizeof(auth_head_t));
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        nfs_newtask(sockid, &req, uid, gid, buf);

        return 0;
err_ret:
        return ret;
}

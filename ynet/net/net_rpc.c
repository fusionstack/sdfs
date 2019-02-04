#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "configure.h"
#include "net_table.h"
#include "net_global.h"
#include "sdevent.h"
#include "ylib.h"
#include "corenet_connect.h"
#include "network.h"
#include "xnect.h"
#include "ylock.h"
#include "ynet_net.h"
#include "ynet_rpc.h"
#include "job_dock.h"
#include "rpc_proto.h"
#include "schedule.h"
#include "adt.h"
#include "squeue.h"
#include "net_table.h"
#include "net_rpc.h"
#include "dbg.h"

typedef struct {
        uint32_t op;
        uint32_t buflen;
        char buf[0];
} msg_t;

typedef enum {
        NET_RPC_NULL = 0,
        NET_RPC_HEARTBEAT,
        NET_COREINFO,
        NET_RPC_MAX,
} net_rpc_op_t;


static __request_handler_func__  __request_handler__[NET_RPC_MAX - NET_RPC_NULL];
static char  __request_name__[NET_RPC_MAX - NET_RPC_NULL][__RPC_HANDLER_NAME__ ];

static void __request_get_handler(int op, __request_handler_func__ *func, char *name)
{
        *func = __request_handler__[op - NET_RPC_NULL];
        strcpy(name, __request_name__[op - NET_RPC_NULL]);
}

static void __request_set_handler(int op, __request_handler_func__ func, const char *name)
{
        YASSERT(strlen(name) + 1 < __RPC_HANDLER_NAME__ );
        strcpy(__request_name__[op - NET_RPC_NULL], name);
        __request_handler__[op - NET_RPC_NULL] = func;
}

static void __getmsg(buffer_t *buf, msg_t **_req, int *buflen, char *_buf)
{
        msg_t *req;

        YASSERT(buf->len <= MEM_CACHE_SIZE4K);

        req = (void *)_buf;
        *buflen = buf->len - sizeof(*req);
        mbuffer_get(buf, req, buf->len);

        *_req = req;
}

static int __net_srv_heartbeat(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf)
{
        int buflen;
        msg_t *req;
        char buf[MAX_BUF_LEN];
        ynet_net_info_t *info;
        uint64_t *seq;

        ANALYSIS_BEGIN(0);
        __getmsg(_buf, &req, &buflen, buf);

        DBUG("heartbeat id (%u, %x)\n", msgid->idx, msgid->figerprint);

        _opaque_decode(req->buf, buflen,
                       &seq, NULL,
                       &info, NULL,
                       NULL);

#if 0 //_inet_ntoa maybe timeout
        DINFO("heartbeat from %s seq %ju\n", _inet_ntoa(sockid->addr), *seq);
#endif

        if (!net_isnull(&info->id))
                netable_updateinfo(info);

#if 0
        if (info->uptime < ng.uptime) {
                DINFO("update uptime %llu %llu\n",
                      (LLU)info->uptime, (LLU)ng.earliest_uptime);
                ng.earliest_uptime = info->uptime;
        }
#endif

        rpc_reply(sockid, msgid, NULL, 0);
        ANALYSIS_END(0, 1000 * 100, NULL);
        
        return 0;
}

int net_rpc_heartbeat(const sockid_t *sockid, uint64_t seq)
{
        int ret;
        char buf[MAX_BUF_LEN], info[MAX_BUF_LEN];
        uint32_t count, len;
        msg_t *req;
        net_handle_t nh;

        ANALYSIS_BEGIN(0);

        ret = rpc_getinfo(info, &len);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        req = (void *)buf;
        req->op = NET_RPC_HEARTBEAT;
        _opaque_encode(req->buf, &count, &seq, sizeof(seq), info, len, NULL);

#if 0
        DINFO("heartbeat to %s seq %ju\n", _inet_ntoa(sockid->addr), seq);
#endif
        
        sock2nh(&nh, sockid);
        ret = rpc_request_wait_sock("net_rpc_hb", &nh,
                                    req, sizeof(*req) + count,
                                    NULL, NULL,
                                    MSG_HEARTBEAT, 0, gloconf.hb_timeout);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ANALYSIS_END(0, 1000 * 500, NULL);

        return 0;
err_ret:
        return ret;
}

static void __request_handler(void *arg)
{
        int ret;
        msg_t req;
        sockid_t sockid;
        msgid_t msgid;
        buffer_t buf;
        __request_handler_func__ handler;
        char name[MAX_NAME_LEN];

        request_trans(arg, NULL, &sockid, &msgid, &buf, NULL);

        if (buf.len < sizeof(req)) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        mbuffer_get(&buf, &req, sizeof(req));

        DBUG("new job op %u\n", req.op);

#if 0
        /* heartbeat to other node timeout when admin ifdown */
        if (!netable_connected(net_getadmin())) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }
#endif

        //handler = __request_handler__[req.op - FS_RPC_MAX];
        __request_get_handler(req.op, &handler, name);
        if (handler == NULL) {
                ret = ENOSYS;
                DWARN("error op %u\n", req.op);
                GOTO(err_ret, ret);
        }

        schedule_task_setname(name);

        ret = handler(&sockid, &msgid, &buf);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        mbuffer_free(&buf);

        return ;
err_ret:
        mbuffer_free(&buf);
        rpc_reply_error(&sockid, &msgid, ret);
        return;
}

#if 1
static int __net_srv_corenetinfo(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf)
{
        int ret, buflen;
        msg_t *req;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        char infobuf[MAX_BUF_LEN];
        uint32_t infobuflen = MAX_BUF_LEN;

        __getmsg(_buf, &req, &buflen, buf);

        ret = corenet_tcp_getinfo(infobuf, &infobuflen);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        rpc_reply(sockid, msgid, infobuf, infobuflen);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

int net_rpc_coreinfo(const nid_t *nid, char *infobuf, int *infobuflen)
{
        int ret;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t count;
        msg_t *req;

        ret = network_connect(nid, NULL, 1, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ANALYSIS_BEGIN(0);

        //YASSERT(io->offset <= YFS_CHK_LEN_MAX);

        req = (void *)buf;
        req->op = NET_COREINFO;
        _opaque_encode(&req->buf, &count, net_getnid(), sizeof(nid_t), NULL);

        ret = rpc_request_wait("net_rpc_corenetinfo", nid,
                               req, sizeof(*req) + count, infobuf, infobuflen,
                               MSG_HEARTBEAT, 0, _get_timeout());
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}
#endif

int net_rpc_init()
{
        __request_set_handler(NET_RPC_HEARTBEAT, __net_srv_heartbeat, "net_srv_heartbeat");
        __request_set_handler(NET_COREINFO, __net_srv_corenetinfo, "net_srv_coreinfo");

        rpc_request_register(MSG_HEARTBEAT, __request_handler, NULL);

        return 0;
}

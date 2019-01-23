

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
#include "../net/xnect.h"
#include "net_table.h"
#include "rpc_table.h"
#include "configure.h"
#include "net_global.h"
#include "job_dock.h"
#include "msgarray.h"
#include "main_loop.h"
#include "schedule.h"
#include "bh.h"
#include "timer.h"
#include "adt.h"
#include "cluster.h"
#include "../../ynet/sock/sock_tcp.h"
#include "core.h"
#include "corerpc.h"
#include "corenet_maping.h"
#include "corenet.h"
#include "dbg.h"

static int __listen_sd__;

typedef struct {
        int hash;
        nid_t from;
        nid_t to;
        char uuid[UUID_LEN];
} corenet_msg_t;

#if 0
int corenet_connect_host(const char *host, sockid_t *sockid)
{
        int ret;
        char port[MAX_NAME_LEN];
        net_handle_t nh;
        corerpc_ctx_t *ctx;

        snprintf(port, MAX_NAME_LEN, "%u", gloconf.direct_port);

        DINFO("connect to %s:%s\n", host, port);

        ret = tcp_sock_hostconnect(&nh, host, port, 0, 3, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        // TODO nid?
        /*
        msg.hash = core->hash;
        msg.to = *nid;
        msg.from = *net_getnid();

        ret = send(nh.u.sd.sd, &msg, sizeof(msg), 0);
        if (ret < 0) {
                ret = errno;
                UNIMPLEMENTED(__DUMP__);
        }
         */

        sockid->sd = nh.u.sd.sd;
        sockid->addr = nh.u.sd.addr;
        sockid->seq = _random();
        sockid->type = SOCKID_CORENET;
        ret = ymalloc((void **)&ctx, sizeof(*ctx));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = tcp_sock_tuning(sockid->sd, 1, YNET_RPC_NONBLOCK);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        ctx->running = 0;
        ctx->sockid = *sockid;
        ret = corenet_add(NULL, sockid, ctx, corerpc_recv, corerpc_close, NULL, NULL);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        return 0;
err_ret:
        return ret;
}
#endif

/**
 * 包括两步骤：
 * - 建立连接: nid
 * - 协商core hash
 *
 * @param nid
 * @param sockid
 * @return
 */
int corenet_tcp_connect(const nid_t *nid, sockid_t *sockid)
{
        int ret;
        char host[MAX_NAME_LEN], port[MAX_NAME_LEN];
        net_handle_t nh;
        core_t *core = core_self();
        corenet_msg_t msg;
        corerpc_ctx_t *ctx;

        YASSERT(strlen(gloconf.uuid) < UUID_LEN);
        ret = network_connect(nid, NULL, 0, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = network_rname1(nid, host);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        snprintf(port, MAX_NAME_LEN, "%u", gloconf.direct_port);

        DINFO("connect to %s:%s\n", host, port);

        ret = tcp_sock_hostconnect(&nh, host, port, 0, 3, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        msg.hash = core->hash;
        msg.from = *net_getnid();
        msg.to = *nid;
        strncpy(msg.uuid, gloconf.uuid, UUID_LEN);

        ret = send(nh.u.sd.sd, &msg, sizeof(msg), 0);
        if (ret < 0) {
                ret = errno;
                UNIMPLEMENTED(__DUMP__);
        }

        sockid->sd = nh.u.sd.sd;
        sockid->addr = nh.u.sd.addr;
        sockid->seq = _random();
        sockid->type = SOCKID_CORENET;
        ret = ymalloc((void **)&ctx, sizeof(*ctx));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = tcp_sock_tuning(sockid->sd, 1, YNET_RPC_NONBLOCK);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        ctx->running = 0;
        sockid->rdma_handler = 0;
        ctx->sockid = *sockid;
        ctx->nid = *nid;
        ret = corenet_tcp_add(NULL, sockid, ctx, corerpc_recv, corerpc_close, NULL, NULL, network_rname(nid));
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        YASSERT(sockid->sd < nofile_max);
        
        return 0;
err_ret:
        return ret;
}

STATIC void *__corenet_accept__(void *arg)
{
        int ret;
        char buf[MAX_BUF_LEN];
        corenet_msg_t *msg;
        sockid_t *sockid;
        core_t *core;
        corerpc_ctx_t *ctx = arg;

        sockid = &ctx->sockid;

        DINFO("accept from %s, sd %d\n",  _inet_ntoa(sockid->addr), sockid->sd);

        ret = sock_poll_sd(sockid->sd, 1000 * 1000, POLLIN);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = recv(sockid->sd, buf, sizeof(*msg), 0);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        if (ret == 0) {
                DWARN("peer closed\n");
                ret = ECONNRESET;
                GOTO(err_ret, ret);
        }

        msg = (void*)buf;
        if (strcmp(gloconf.uuid, msg->uuid)) {
                DERROR("get wrong msg from %s\n", _inet_ntoa(sockid->addr));
                ret = ECONNRESET;
                GOTO(err_ret, ret);
        }

        YASSERT(sizeof(*msg) == ret);
        YASSERT(nid_cmp(&msg->to, net_getnid()) == 0);

        ret = tcp_sock_tuning(sockid->sd, 1, YNET_RPC_NONBLOCK);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        
        core = core_get(msg->hash);
        sockid->rdma_handler = 0;
        ctx->nid = msg->from;

        ret = corenet_maping_accept(core, &msg->from, sockid);
        if (unlikely(ret)) {
                UNIMPLEMENTED(__DUMP__);
        }

        DINFO("hash %d core:%p maping:%p, sd %u\n", msg->hash, core, core->maping, sockid->sd);
        
        ret = corenet_tcp_add(core->tcp_net, sockid, ctx, corerpc_recv,
                              corerpc_close, NULL, NULL, network_rname(&ctx->nid));
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        schedule_post(core->schedule);

        return NULL;
err_ret:
        close(sockid->sd);
        return NULL;
}

static int __corenet_accept()
{
        int ret, sd;
        socklen_t alen;
        struct sockaddr_in sin;
        corerpc_ctx_t *ctx;

        _memset(&sin, 0, sizeof(sin));
        alen = sizeof(struct sockaddr_in);

        sd = accept(__listen_sd__, &sin, &alen);
        if (sd < 0 ) {
                ret = errno;
		GOTO(err_ret, ret);
        }

        ret = ymalloc((void **)&ctx, sizeof(*ctx));
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        ctx->running = 0;
        ctx->sockid.sd = sd;
        ctx->sockid.type = SOCKID_CORENET;
        ctx->sockid.seq = _random();
        ctx->sockid.addr = sin.sin_addr.s_addr;
        ctx->nid.id = 0;

        ret = sy_thread_create2(__corenet_accept__, ctx, "__corenet_accept");
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        return 0;
err_ret:
        return ret;
}

static void *__corenet_passive(void *_arg)
{
        int ret;

        (void) _arg;
        DINFO("start...\n");

        main_loop_hold();

        while (1) {
                ret = sock_poll_sd(__listen_sd__, 1000 * 1000, POLLIN);
                if (unlikely(ret)) {
                        if (ret == ETIMEDOUT || ret == ETIME)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }

                DINFO("got new event\n");

                __corenet_accept();
        }

        return NULL;
err_ret:
        UNIMPLEMENTED(__DUMP__);
        return NULL;
}

int corenet_tcp_passive()
{
        int ret;
        char port[MAX_BUF_LEN];

        snprintf(port, MAX_BUF_LEN, "%u", gloconf.direct_port);
        ret = tcp_sock_hostlisten(&__listen_sd__, NULL, port,
                                  YNET_QLEN, YNET_RPC_BLOCK, 1);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        ret = sy_thread_create2(__corenet_passive, NULL, "corenet_passive");
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

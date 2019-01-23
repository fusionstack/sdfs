
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
#include "schedule.h"
#include "bh.h"
#include "timer.h"
#include "adt.h"
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
} corenet_msg_t;

int corenet_connect(const nid_t *nid, sockid_t *sockid)
{
        int ret;
        char host[MAX_NAME_LEN], port[MAX_NAME_LEN];
        net_handle_t nh;
        core_t *core = core_self();
        corenet_msg_t msg;
        corerpc_ctx_t *ctx;

        id2nh(&nh, nid);
        ret = network_connect2(&nh, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        strcpy(host, netable_rname_nid(nid));

        snprintf(port, MAX_NAME_LEN, "%u", gloconf.direct_port);

        DINFO("connect to %s:%s\n", host, port);

        //todo fix 从v4迁移过来是tunning为0, 但v3的tcp_sock_hostconnect默认是1
        ret = tcp_sock_hostconnect(&nh, host, port, 0, 3);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        msg.hash = core->hash;
        msg.to = *nid;
        msg.from = *net_getnid();

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
        ctx->sockid = *sockid;
        ret = corenet_add(NULL, sockid, ctx, corerpc_recv, corerpc_close, NULL, NULL);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        return 0;
err_ret:
        return ret;
}

static void *__corenet_accept__(void *arg)
{
        int ret;
        char buf[MAX_BUF_LEN];
        corenet_msg_t *msg;
        sockid_t *sockid;
        core_t *core;
        corerpc_ctx_t *ctx = arg;

        DINFO("accept %u\n", ctx->sockid.sd);

        sockid = &ctx->sockid;

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
        YASSERT(sizeof(*msg) == ret);
        YASSERT(nid_cmp(&msg->to, net_getnid()) == 0);

        ret = tcp_sock_tuning(sockid->sd, 1, YNET_RPC_NONBLOCK);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        core = core_get(msg->hash);
        ret = corenet_add(core->net, sockid, ctx, corerpc_recv, corerpc_close, NULL, NULL);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        schedule_post(core->schedule);
        ret = corenet_maping_accept(core->maping, &msg->from, sockid);
        if (unlikely(ret)) {
                UNIMPLEMENTED(__DUMP__);
        }

        return NULL;
err_ret:
        close(sockid->sd);
        return NULL;
}

static int __corenet_accept()
{
        int ret, sd;
        socklen_t alen;
        pthread_t th;
        pthread_attr_t ta;
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

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);
 
        ret = pthread_create(&th, &ta, __corenet_accept__, ctx);
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

int corenet_passive()
{
        int ret;
        pthread_t th;
        pthread_attr_t ta;
        char port[MAX_BUF_LEN];


extern int tcp_sock_hostlisten(int *srv_sd, const char *host,
                               const char *service, int qlen, int nonblock);

        snprintf(port, MAX_BUF_LEN, "%u", gloconf.direct_port);
        //从v4迁过来时,turnning为1
        ret = tcp_sock_hostlisten(&__listen_sd__, NULL, port,
                                  YNET_QLEN, YNET_RPC_BLOCK);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta,PTHREAD_CREATE_DETACHED);

        ret = pthread_create(&th, &ta, __corenet_passive, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

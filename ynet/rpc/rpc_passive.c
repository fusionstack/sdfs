#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <net/if.h>
#include <arpa/inet.h>

#define DBG_SUBSYS S_LIBYNET

#include "ynet_net.h"
#include "net_table.h"
#include "../sock/sock_tcp.h"
#include "net_global.h"
#include "../net/net_events.h"
#include "main_loop.h"
#include "../net/xnect.h"
#include "rpc_proto.h"
#include "sdevent.h"
#include "ylib.h"
#include "ynet_rpc.h"
#include "dbg.h"

static int __pasv_sd__ = -1;

int nid_update(nid_t *nid, const char *path);

int rpc_hostbind(int *sd, const char *host, const char *service, int nonblock)
{
        return net_hostbind(sd, host, service, nonblock);
}

int rpc_hostlisten(int *sd, const char *host, const char *service, int qlen,
                   int nonblock)
{
        return net_hostlisten(sd, host, service, qlen, nonblock);
}

int rpc_addrlisten(int *sd, uint32_t addr, uint32_t port, int qlen,
                   int nonblock)
{
        return net_addrlisten(sd, addr, port, qlen, nonblock);
}

int rpc_portlisten(int *sd, uint32_t addr, uint32_t *port, int qlen,
                   int nonblock)
{
        return net_portlisten(sd, addr, port, qlen, nonblock);
}

int rpc_getinfo(char *infobuf, uint32_t *infobuflen)
{
        return net_getinfo(infobuf, infobuflen, ng.port);
}

typedef struct {
        net_handle_t newnh;
} arg_t;

static void *__rpc_accept__(void *_arg)
{
        int ret;
        char buf[MAX_BUF_LEN];
        ynet_net_info_t *info;
        net_handle_t newnh;
        net_proto_t proto;
        arg_t *arg;

        ANALYSIS_BEGIN(0);
        arg = _arg;
        newnh = arg->newnh;

        _memset(&proto, 0x0, sizeof(net_proto_t));

        proto.head_len = sizeof(ynet_net_head_t);
        proto.pack_len = rpc_pack_len;
        proto.pack_handler = rpc_pack_handler;

        proto.reader = net_events_handle_read;
        proto.writer = net_events_handle_write;

        info = (void *)buf;
        ret = net_accept(&newnh, info, &proto);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        YASSERT(strcmp(info->name, "none"));

        if (net_isnull(&info->id) || net_islocal(&info->id)) {
                ret = sdevent_add(&newnh, NULL, Y_EPOLL_EVENTS, NULL, NULL);
                if (unlikely(ret)) {
                        DINFO("accept from %s, sd %u ret:%d \n",
                              _inet_ntoa(newnh.u.sd.addr), newnh.u.sd.sd, ret);
                        GOTO(err_sd, ret);
                }
        } else {
                ret = netable_accept(info, &newnh);
                if (unlikely(ret)) {
                        DINFO("accept from %s(%s), sd %u ret:%d\n",
                               _inet_ntoa(newnh.u.sd.addr), info->name, newnh.u.sd.sd, ret);
                        GOTO(err_sd, ret);
                }
        }

        ANALYSIS_END(0, IO_WARN, NULL);

        yfree((void **)&_arg);

        pthread_exit(NULL);
err_sd:
        sdevent_close_force(&newnh);
err_ret:
        yfree((void **)&_arg);
        pthread_exit(NULL);
}

static int __rpc_accept()
{
        int ret;
        net_handle_t newnh;
        arg_t *arg;

        ret = sock_accept(&newnh, __pasv_sd__, YNET_RPC_TUNNING, YNET_RPC_BLOCK);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&arg, sizeof(*arg));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        arg->newnh = newnh;

        ret = sy_thread_create2(__rpc_accept__, arg, "__rpc_accept");
        if (unlikely(ret))
                GOTO(err_free, ret);

        return 0;
err_free:
        yfree((void **)&arg);
err_ret:
        return ret;
}

static void *__rpc_accept_worker(void *_arg)
{
        int ret;
        sem_t *sem;

        sem = _arg;

        ret = sem_post(sem);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        main_loop_hold();

        while (1) {
                ret = sock_poll_sd(__pasv_sd__, 1000 * 1000, POLLIN);
                if (unlikely(ret)) {
                        if (ret == ETIMEDOUT || ret == ETIME)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }

                ret = __rpc_accept();
                if (unlikely(ret)) {
                        ret = _errno(ret);
                        if (ret == EAGAIN)
                                continue;
                        else
                                UNIMPLEMENTED(__DUMP__);
                }
        }

        return NULL;
err_ret:
        UNIMPLEMENTED(__DUMP__);
        return NULL;
}

int rpc_passive(uint32_t port)
{
        int ret, sd;
        net_handle_t nh;
        char _port[MAX_LINE_LEN];

        memset(&nh, 0x0, sizeof(nh));

        YASSERT(__pasv_sd__ == -1);

        if (port != (uint32_t)-1) {
                snprintf(_port, MAX_LINE_LEN, "%u", port);

                YASSERT(port > YNET_SERVICE_RANGE && port < 65535);
                ret = tcp_sock_hostlisten(&sd, NULL, _port,
                                          YNET_QLEN, YNET_RPC_BLOCK, 1);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        } else {
                port = YNET_PORT_RANDOM;
                while (srv_running) {
                        port = (uint16_t)(YNET_SERVICE_BASE
                                          + (random() % YNET_SERVICE_RANGE));

                        YASSERT(port > YNET_SERVICE_RANGE && port < 65535);
                        snprintf(_port, MAX_LINE_LEN, "%u", port);

                        ret = tcp_sock_hostlisten(&sd, NULL, _port,
                                                  YNET_QLEN, YNET_RPC_BLOCK, 1);
                        if (unlikely(ret)) {
                                if (ret == EADDRINUSE) {
                                        DBUG("port (%u + %u) %s\n", YNET_SERVICE_BASE,
                                             port - YNET_SERVICE_BASE, strerror(ret));
                                        continue;
                                } else
                                        GOTO(err_ret, ret);
                        } else {
                                break;
                        }
                }
        }

        __pasv_sd__ = sd;

#if !ENABLE_START_PARALLEL
        ret = rpc_start();
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif

        ng.port = port;
        ng.pasv_nh = nh;

        DINFO("listen %u, nid %u\n", port, net_getnid()->id);

        return 0;
err_ret:
        return ret;
}

int rpc_start()
{
        int ret;
        sem_t sem;

        while (__pasv_sd__ == -1) {
                DWARN("wait rpc passive inited\n");
                sleep(1);
        }

        ret = sem_init(&sem, 0, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sy_thread_create2(__rpc_accept_worker, &sem, "__rpc_accept_worker");
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sem_wait(&sem);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

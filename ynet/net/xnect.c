#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "../sock/ynet_sock.h"
#include "net_events.h"
#include "net_global.h"
#include "../include/ynet_rpc.h"
#include "sdevent.h"
#include "xnect.h"
#include "configure.h"
#include "schedule.h"
#include "ylib.h"
#include "ynet_net.h"
#include "net_table.h"
#include "dbg.h"

static int __sock_connect(net_handle_t *nh, const ynet_sock_info_t *info,
                          const void *infobuf, uint32_t infolen, int timeout)
{
        int ret;
        char buf[MAX_BUF_LEN];

        YASSERT(timeout < 30);

        ret = sdevent_connect(info, nh, &ng.op, YNET_RPC_BLOCK, timeout);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        DBUG("conneted sd %u\n", nh->u.sd.sd);

        if (infobuf) {
                ret = _send(nh->u.sd.sd, (void *)infobuf, infolen,
                            MSG_NOSIGNAL | MSG_DONTWAIT);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                } else if ((uint32_t)ret != infolen) {
                        ret = EBADF;
                        DWARN("bad sd %u\n", nh->u.sd.sd);
                        GOTO(err_ret, ret);
                }

        }

        if (ng.daemon) {
                ret = sock_poll_sd(nh->u.sd.sd, timeout * 1000 * 1000, POLLIN);
                if (unlikely(ret))
                        GOTO(err_fd, ret);

                ret = _recv(nh->u.sd.sd, (void *)buf, MAX_BUF_LEN, MSG_DONTWAIT);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_fd, ret);
                }

                if (ret == 0) {
                        ret = ECONNRESET;
                        GOTO(err_fd, ret);
                }
        }

        ret = sock_setnonblock(nh->u.sd.sd);
        if (unlikely(ret)) {
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_fd, ret);
        }

        return 0;
err_fd:
        sdevent_close_force(nh);
err_ret:
        return ret;
}

int net_connect(net_handle_t *sock, const ynet_net_info_t *info, int timeout)
{
        int ret;
        uint32_t infolen;
        char buf[MAX_BUF_LEN];

        YASSERT(!schedule_running());

        infolen = MAX_BUF_LEN;
        ret = rpc_getinfo(buf, &infolen);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        ret = __sock_connect(sock, &info->info[0], buf, infolen, timeout);
        if (unlikely(ret)) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }

        YASSERT(sock->u.sd.type == SOCKID_NORMAL);
        
        return 0;
err_ret:
        return ret;
}

int net_accept(net_handle_t *nh, ynet_net_info_t *info, const net_proto_t *proto)
{
        int ret, newsd;
        uint32_t buflen = MAX_BUF_LEN;
        char buf[MAX_BUF_LEN];

        newsd = nh->u.sd.sd;

retry:
        ret = sock_poll_sd(newsd, (_get_timeout() / 2) * 1000 * 1000, POLLIN );
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sy_peek(newsd, (void *)info, MAX_BUF_LEN);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        if (ret < (int)sizeof(info->len)) {
                DWARN("got ret %u\n", ret);
                goto retry;
        }

        ret = _recv(newsd, (void *)info, info->len, MSG_DONTWAIT);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        } else if ((uint32_t)ret != info->len) {
                ret = EBADF;
                GOTO(err_ret, ret);
        }

        if (!net_isnull(&info->id)) {
                ret = rpc_getinfo(buf, &buflen);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ret = _send(newsd, buf, buflen, 0);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                if (ret != (int)buflen) {
                        ret = ECONNRESET;
                        GOTO(err_ret, ret);
                }
        }

        YASSERT(info->len);

        ret = sdevent_open(nh, proto);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sock_setnonblock(newsd);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        return 0;
err_ret:
        return ret;
}



#define DBG_SUBSYS S_YRPC

#include "ynet_net.h"
#include "ynet_rpc.h"
#include "../sock/sock_tcp.h"
#include "dbg.h"

//inline int rpc_peek_sd_sync(int sd, char *buf, uint32_t buflen, int timeout)
int rpc_peek_sd_sync(int sd, char *buf, uint32_t buflen, int timeout)
{
        int ret;
        uint32_t left;

        left = buflen;
        while (left > 0) {
                ret = sock_poll_sd(sd, timeout, POLLIN);
                if (ret) {
                        if (left == buflen)     /* none recv'ed */
                                GOTO(err_ret, ret);
                        else
                                break;
                }

                ret = sy_peek(sd, buf, left);
                if (ret < 0) { /*XXX:non block io will return 0*/
                        DERROR("peek error\n");
                        ret = -ret;

                        if (ret == EAGAIN || ret == EINTR)
                                continue;

                        if (left == buflen)     /* none recv'ed */
                                GOTO(err_ret, ret);
                        else {
                                DERROR("%d, %p, %u\n", sd, buf, left);
                                break;
                        }
                }

                buf += (uint32_t)ret;
                left -= (uint32_t)ret;

                if (left < buflen)
                        break;
        }

        return buflen - left;
err_ret:
        return -ret;
}

int sock_recv_sd_sync(int sd, char *buf, uint32_t buflen, int timeout)
{
        int ret;
        uint32_t left;

        left = buflen;

        while (left > 0) {
                ret = sock_poll_sd(sd, timeout, POLLIN);
                if (ret) {
                        if (left == buflen)     /* none recv'ed */
                                GOTO(err_ret, ret);
                        else
                                break;
                }

                ret = _recv(sd, buf, left, 0);
                if (ret == 0)
                        break;
                else if (ret < 0) {
                        ret = -ret;

                        if (ret == EAGAIN || ret == EINTR)
                                continue;

                        if (left == buflen)     /* none recv'ed */
                                GOTO(err_ret, ret);
                        else {
                                DERROR("%d, %p, %u\n", sd, buf, left);
                                break;
                        }
                }

                buf += (uint32_t)ret;
                left -= (uint32_t)ret;
        }

        return buflen - left;
err_ret:
        return -ret;
}

int rpc_discard_sd_sync(int sd, uint32_t len, int timeout)
{
        int ret;
        uint32_t left;
        char buf[MAX_BUF_LEN];

        left = len;

        while (left > 0) {
                left = left < MAX_BUF_LEN ? left : MAX_BUF_LEN;

                ret = sock_recv_sd_sync(sd, buf, left, timeout);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                left -= (uint32_t)ret;
        }

        ret = (int)(len - left);
        DBUG("%d messages discarded\n", ret);

        return ret;
err_ret:
        return -ret;
}


extern int rpc_peek_sd_sync(int sd, char *buf, uint32_t buflen, int timeout);
extern int rpc_discard_sd_sync(int sd, uint32_t len, int timeout);
int rpc_accept(int *cli_sd, int srv_sd, int tuning, int nonblock)
{
        int ret;
        net_handle_t newnh;
        
        ret = tcp_sock_accept(&newnh, srv_sd, tuning, nonblock);
        if (ret)
                GOTO(err_ret, ret);

        *cli_sd = newnh.u.sd.sd;

        return 0;
err_ret:
        return ret;
}


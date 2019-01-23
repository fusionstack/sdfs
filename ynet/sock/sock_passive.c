#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "sock_tcp.h"
#include "dbg.h"

inline int sock_hostbind(int *srv_sd, const char *host, const char *service, int nonblock)
{
        return tcp_sock_hostbind(srv_sd, host, service, nonblock);
}

inline int sock_hostlisten(int *srv_sd, const char *host, const char *service,
                    int qlen, int nonblock)
{
        return tcp_sock_hostlisten(srv_sd, host, service, qlen, nonblock, 1);
}

inline int sock_addrlisten(int *srv_sd, uint32_t addr, uint32_t port, int qlen, int nonblock)
{
        return tcp_sock_addrlisten(srv_sd, addr, port, qlen, nonblock);
}

inline int sock_portlisten(int *srv_sd, uint32_t addr, uint32_t *port, int qlen, int nonblock)
{
        return tcp_sock_portlisten(srv_sd, addr, port, qlen, nonblock);
}

inline int sock_accept(net_handle_t *nh, int srv_sd, int tuning,
                       int nonblock)
{
        return tcp_sock_accept(nh, srv_sd, tuning, nonblock);
}

inline int sock_getinfo(uint32_t *info_count, ynet_sock_info_t *info,
                 uint32_t info_count_max, uint32_t port)
{
        return tcp_sock_getaddr(info_count, info, info_count_max, port);
}

inline int sock_setblock(int sd)
{
        int ret, flags;

        flags = fcntl(sd, F_GETFL);
        if (flags == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        if (flags & O_NONBLOCK) {
                flags = flags ^ O_NONBLOCK;

                ret = fcntl(sd, F_SETFL, flags);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

inline int sock_setnonblock(int sd)
{
        int ret, flags;

        flags = fcntl(sd, F_GETFL);
        if (flags == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        if ((flags & O_NONBLOCK) == 0) {
                flags = flags | O_NONBLOCK;
                ret = fcntl(sd, F_SETFL, flags);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

inline int sock_checknonblock(int sd)
{
        int ret, flags;

        flags = fcntl(sd, F_GETFL);
        if (flags == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        YASSERT(flags & O_NONBLOCK);

        return 0;
err_ret:
        return ret;
}

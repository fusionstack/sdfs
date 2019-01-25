#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <netdb.h>
#include <fcntl.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "sock_udp.h"
#include "net_global.h"
#include "configure.h"
#include "ylib.h"
#include "adt.h"
#include "ynet_conf.h"
#include "ynet_sock.h"
#include "dbg.h"

static int __udp_sock_bind(int *srv_sd, struct sockaddr_in *sin, int nonblock)
{
        int ret, sd;
        struct protoent ppe, *result;
        char buf[MAX_BUF_LEN];

        (void) nonblock;

        DBUG("nonblock not set\n");

        /* map protocol name to protocol number */
        ret = getprotobyname_r("udp", &ppe, buf, MAX_BUF_LEN, &result);
        if (unlikely(ret)) {
                DERROR("can't get \"tcp\" protocol entry\n");
                GOTO(err_ret, ret);
        }

        /* allocate a socket */
        sd = socket(PF_INET, SOCK_DGRAM, 0);
        if (sd == -1) {
                ret = errno;
                DERROR("proto %d name %s\n", ppe.p_proto, ppe.p_name);
                GOTO(err_ret, ret);
        }

        /* bind the socket */
        ret = bind(sd, (struct sockaddr *)sin, sizeof(struct sockaddr));
        if (ret == -1) {
                ret = errno;
                GOTO(err_sd, ret);
        }

        *srv_sd = sd;

        return 0;
err_sd:
        (void) close(sd);
err_ret:
        return ret;
}

static int __udp_sock_listen(int *srv_sd, const char *host, const char *service, int nonblock)
{
        int ret;
        struct servent result, *pse;
        char buf[MAX_BUF_LEN];
        struct sockaddr_in sin;

        _memset(&sin, 0, sizeof(sin));
        sin.sin_family = AF_INET;

        if (host) {
                ret = _inet_addr((struct sockaddr *)&sin, host);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        } else
                sin.sin_addr.s_addr = htons(INADDR_ANY);

        getservbyname_r(service, "udp", &result, buf, MAX_BUF_LEN, &pse);
        if (pse)
                sin.sin_port = pse->s_port;
        else if ((sin.sin_port = htons((unsigned short)atoi(service))) == 0) {
                DERROR("can't get \"%s\" service entry\n", service);
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        ret = __udp_sock_bind(srv_sd, &sin, nonblock);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int udp_sock_listen(int *srv_sd, const char *host, const char *_service, int nonblock)
{
        int ret, port;
        char service[MAX_LINE_LEN];

        if (_service) {
                return  __udp_sock_listen(srv_sd, host, _service, nonblock);
        }

        while (1) {
                port = (uint16_t)(YNET_SERVICE_BASE
                                  + (random() % YNET_SERVICE_RANGE));

                snprintf(service, MAX_LINE_LEN, "%d", port);
                ret = __udp_sock_listen(srv_sd, host, service, nonblock);
                if (unlikely(ret)) {
                        if (ret == EADDRINUSE) {
                                DBUG("port (%u + %u) %s\n", YNET_SERVICE_BASE,
                                     port - YNET_SERVICE_BASE, strerror(ret));
                                continue;
                        }

                        GOTO(err_ret, ret);
                } else
                        break;
        }

        return 0;
err_ret:
        return ret;
}

int udp_sock_broadcast(int sd, uint32_t net, uint32_t mask, uint32_t port, const void *buf, int buflen)
{
        int ret, on;
        struct sockaddr_in addr;

        memset(&addr, 0x0, sizeof(struct sockaddr_in));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        if (gloconf.solomode)
                addr.sin_addr.s_addr = htonl(INADDR_BROADCAST);
        else
                addr.sin_addr.s_addr = net | ~mask;

        on = 1;
        ret = setsockopt(sd, SOL_SOCKET, SO_BROADCAST, &on, sizeof(int));
        if (unlikely(ret)) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = sendto(sd, buf, buflen, 0, &addr, sizeof(struct sockaddr_in));
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        on = 0;
        ret = setsockopt(sd, SOL_SOCKET, SO_BROADCAST, &on, sizeof(int));
        if (unlikely(ret)) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __udp_sock_connect(int *srv_sd, struct sockaddr_in *sin, int nonblock)
{
        int ret, sd;
        struct protoent ppe, *result;
        char buf[MAX_BUF_LEN];

        (void) nonblock;

        DBUG("nonblock not set\n");

        /* map protocol name to protocol number */
        ret = getprotobyname_r("udp", &ppe, buf, MAX_BUF_LEN, &result);
        if (unlikely(ret)) {
                DERROR("can't get \"tcp\" protocol entry\n");
                GOTO(err_ret, ret);
        }

        /* allocate a socket */
        sd = socket(PF_INET, SOCK_DGRAM, ppe.p_proto);
        if (sd == -1) {
                ret = errno;
                DERROR("proto %d name %s\n", ppe.p_proto, ppe.p_name);
                GOTO(err_ret, ret);
        }

        /* bind the socket */
        ret = connect(sd, (struct sockaddr *)sin, sizeof(struct sockaddr));
        if (ret == -1) {
                ret = errno;
                GOTO(err_sd, ret);
        }

        *srv_sd = sd;

        return 0;
err_sd:
        (void) close(sd);
err_ret:
        return ret;
}

int udp_sock_connect(int *srv_sd, const char *host, const char *service, int nonblock)
{
        int ret;
        struct servent result, *pse;
        char buf[MAX_BUF_LEN];
        struct sockaddr_in sin;

        _memset(&sin, 0, sizeof(sin));
        sin.sin_family = AF_INET;

        if (host) {
                ret = _inet_addr((struct sockaddr *)&sin, host);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        } else
                sin.sin_addr.s_addr = htons(INADDR_ANY);

        getservbyname_r(service, "udp", &result, buf, MAX_BUF_LEN, &pse);
        if (pse)
                sin.sin_port = pse->s_port;
        else if ((sin.sin_port = htons((unsigned short)atoi(service))) == 0) {
                DERROR("can't get \"%s\" service entry\n", service);
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        ret = __udp_sock_connect(srv_sd, &sin, nonblock);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

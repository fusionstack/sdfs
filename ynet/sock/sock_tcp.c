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

#include "sock_tcp.h"
#include "net_global.h"
#include "sysutil.h"
#include "configure.h"
#include "ylib.h"
#include "ynet_conf.h"
#include "ynet_sock.h"
#include "dbg.h"

static int __tcp_connect(int s, const struct sockaddr *sin, socklen_t addrlen, int timeout)
{
        int  ret, flags, err;
        socklen_t len;

        YASSERT(timeout < 30);
        /*
         * fill in sockaddr_in structure
         */

        flags = fcntl(s, F_GETFL, 0);
        if (flags < 0 ) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = fcntl(s, F_SETFL, flags | O_NONBLOCK);
        if (ret < 0 ) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = connect(s, sin, addrlen);
        if (ret < 0 ) {
                ret = errno;
                if (ret != EINPROGRESS ) {
                        GOTO(err_ret, ret);
                }
        } else
                goto out;

        ret = sock_poll_sd(s, timeout * 1000 * 1000, POLLOUT);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        len = sizeof(err);

        ret = getsockopt(s, SOL_SOCKET, SO_ERROR, &err, &len);
        if (ret < 0)
                GOTO(err_ret, ret);

        if (err) {
                ret = err;
                GOTO(err_ret, ret);
        }

out:
        ret = fcntl(s, F_SETFL, flags);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

#if 0
static int __tcp_accept(int s, struct sockaddr *sin, socklen_t *addrlen, int timeout)
{
        int  ret, flags, fd;

        /*
         * fill in sockaddr_in structure
         */

        flags = fcntl(s, F_GETFL, 0);
        if (flags < 0 ) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = fcntl(s, F_SETFL, flags | O_NONBLOCK);
        if (ret < 0 ) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = sock_poll_sd(s, timeout, POLLIN | POLLOUT);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        fd = accept(s, sin, addrlen);
        if (fd < 0 ) {
                ret = errno;
		GOTO(err_ret, ret);
        }

        ret = fcntl(s, F_SETFL, flags);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return fd;
err_ret:
        return -ret;
}

#else

static int __tcp_accept(int s, struct sockaddr *sin, socklen_t *addrlen, int timeout)
{
        int  ret, fd;

        (void) timeout;
        /*
         * fill in sockaddr_in structure
         */

        ANALYSIS_BEGIN(0);

        fd = accept(s, sin, addrlen);
        if (fd < 0 ) {
                ret = errno;
		GOTO(err_ret, ret);
        }

        ANALYSIS_END(0, 1000 * 100, NULL);

        return fd;
err_ret:
        return -ret;
}
#endif

int tcp_sock_tuning(int sd, int tuning, int nonblock)
{
        int ret, keepalive, nodelay, oob_inline, xmit_buf, flag;
        struct linger lin __attribute__((unused));
        struct timeval tv;
        socklen_t size;

        if (tuning == 0)
                return 0;

        flag = fcntl(sd, F_GETFL);
        if (flag < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = fcntl(sd, F_SETFL, flag | O_CLOEXEC);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        /*
         * If SO_KEEPALIVE is disabled (default), a TCP connection may remain
         * idle until the connection is released at the protocol layer. If
         * SO_KEEPALIVE is enabled and the connection has been idle for two
         * __hours__, TCP sends a packet to the remote socket, expecting the
         * remote TCP to acknowledge that the connection is still active. If
         * the remote TCP does not respond in a timely manner, TCP continues to
         * send keepalive packets according to the normal retransmission
         * algorithm. If the remote TCP does not respond within a particular
         * time limit, TCP drops the connection. The next socket system call
         * (for example, _recv()) returns an error, and errno is set to
         * ETIMEDOUT.
         */
        keepalive = 1;
        ret = setsockopt(sd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(int));
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        /*
         * If l_onoff is zero (the default action), close() returns immediately,
         * but the system tries to transmit any unsent data and release the
         * protocol connection gracefully. If l_onoff is non-zero and l_linger
         * is zero, close() returns immediately, any unsent data is discarded,
         * and the protocol connection is aborted. If both l_onoff and l_linger
         * are non-zero, close() does not return until the system has tried to
         * transmit all unsent data and release the connection gracefully. In
         * that case, close() can return an error, and errno may be set to
         * ETIMEDOUT, if the system is unable to transmit the data after a
         * protocol-defined time limit. Note that the value of l_linger is
         * treated simply as a boolean; a non-zero value is not interpreted as
         * a time limit( see _XOPEN_SOURCE_EXTENDED Only below). SO_LINGER does
         * not affect the actions taken when the function shutdown() is called.
	 */
	if (nonblock == 1){
                ret = sock_setnonblock(sd);
		if (unlikely(ret)) {
			DERROR("%d - %s\n", ret, strerror(ret));
			GOTO(err_ret, ret);
		}
	}

        lin.l_onoff = 1;
        lin.l_linger = 15;      /* how many seconds to linger for */
#if 0
        ret = setsockopt(sd, SOL_SOCKET, SO_LINGER, &lin, sizeof(lin));
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }
#endif

        nodelay = 1;

        ret = setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(int));
        if (ret == -1) {
                ret = errno;
                if (ret == EOPNOTSUPP) {
                        //nothing todo;
                } else
                        GOTO(err_ret, ret);
        }

        oob_inline = 1;

        ret = setsockopt(sd, SOL_SOCKET, SO_OOBINLINE, &oob_inline,
                         sizeof(int));
        if (ret == -1) {
                ret = errno;
                if (ret == EOPNOTSUPP) {
                        //nothing todo;
                } else
                        GOTO(err_ret, ret);
        }

        tv.tv_sec = 30;
        tv.tv_usec = 0;
        ret = setsockopt(sd, SOL_SOCKET, SO_SNDTIMEO, (void *)&tv,
                         sizeof(struct timeval));
        if (ret == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        ret = setsockopt(sd, SOL_SOCKET, SO_RCVTIMEO, (void *)&tv,
                         sizeof(struct timeval));
        if (ret == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        xmit_buf = gloconf.wmem_max;
        ret = setsockopt(sd, SOL_SOCKET, SO_SNDBUF, &xmit_buf, sizeof(int));
        if (ret == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        xmit_buf = gloconf.rmem_max;
        ret = setsockopt(sd, SOL_SOCKET, SO_RCVBUF, &xmit_buf, sizeof(int));
        if (ret == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        xmit_buf = 0;
        size = sizeof(int);

        ret = getsockopt(sd, SOL_SOCKET, SO_SNDBUF, &xmit_buf, &size);
        if (ret == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        if (xmit_buf != gloconf.wmem_max * 2) {
                SERROR(0, "Can't set tcp send buf to %d (got %d)\n",
                       gloconf.wmem_max, xmit_buf);
        }

        DBUG("send buf %u\n", xmit_buf);

        xmit_buf = 0;
        size = sizeof(int);

        ret = getsockopt(sd, SOL_SOCKET, SO_RCVBUF, &xmit_buf, &size);
        if (ret == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        if (xmit_buf != gloconf.rmem_max * 2) {
                SERROR(1, "Can't set tcp recv buf to %d (got %d)\n",
                       gloconf.rmem_max, xmit_buf);
        }

        DBUG("recv buf %u\n", xmit_buf);

        return 0;
err_ret:
        return ret;
}

int tcp_sock_bind(int *srv_sd, struct sockaddr *sin, int nonblock, int tuning)
{
        int ret, sd, opt;
        struct protoent ppe, *result;
        char buf[MAX_BUF_LEN];
        socklen_t slen;

        /* map protocol name to protocol number */
        ret = getprotobyname_r(YNET_TRANSPORT, &ppe, buf, MAX_BUF_LEN, &result);
        if (unlikely(ret)) {
//                ret = ENOENT;
                DERROR("can't get \"tcp\" protocol entry\n");
                GOTO(err_ret, ret);
        }

        /* allocate a socket */
        sd = socket(sin->sa_family, SOCK_STREAM, ppe.p_proto);
        if (sd == -1) {
                ret = errno;

                DERROR("proto %d name %s\n", ppe.p_proto, ppe.p_name);

                GOTO(err_ret, ret);
        }

        if (tuning) {
                ret = tcp_sock_tuning(sd, 1, nonblock);
                if (unlikely(ret))
                        GOTO(err_sd, ret);
        }

        opt = 1;
        ret = setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        if (ret == -1) {
                ret = errno;
                GOTO(err_sd, ret);
        }

        if (sin->sa_family == AF_INET6) {
                opt = 1;
                ret = setsockopt(sd, IPPROTO_IPV6, IPV6_V6ONLY, &opt, sizeof(opt));
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_sd, ret);
                }
        }

        /* bind the socket */
        slen = (sin->sa_family == AF_INET6) ? sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in);
        ret = bind(sd, (struct sockaddr *)sin, slen);
        if (ret == -1) {
                ret = errno;
                GOTO(err_sd, ret);
        }

        *srv_sd = sd;

        return 0;
err_sd:
        (void) sy_close(sd);
err_ret:
        return ret;
}

int tcp_sock_listen(int *srv_sd, struct sockaddr *sin, int qlen, int nonblock, int tuning)
{
        int ret, sd;

        ret = tcp_sock_bind(&sd, sin, nonblock, tuning);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = listen(sd, qlen);
        if (ret == -1) {
                ret = errno;
                GOTO(err_sd, ret);
        }

        *srv_sd = sd;

        return 0;
err_sd:
        (void) sy_close(sd);
err_ret:
        return ret;
}

int tcp_sock_hostbind(int *srv_sd, const char *host, const char *service, int nonblock)
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
                sin.sin_addr.s_addr = INADDR_ANY;

        getservbyname_r(service, "tcp", &result, buf, MAX_BUF_LEN, &pse);
        if (pse)
                sin.sin_port = pse->s_port;
        else if ((sin.sin_port=htons((unsigned short)atoi(service))) == 0) {
                DERROR("can't get \"%s\" service entry\n", service);
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        ret = tcp_sock_bind(srv_sd, (struct sockaddr *)&sin, nonblock, 1);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int tcp_sock_hostlisten(int *srv_sd, const char *host, const char *service,
                        int qlen, int nonblock, int tuning)
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
                sin.sin_addr.s_addr = INADDR_ANY;

        getservbyname_r(service, "tcp", &result, buf, MAX_BUF_LEN, &pse);
        if (pse)
                sin.sin_port = pse->s_port;
        else if ((sin.sin_port=htons((unsigned short)atoi(service))) == 0) {
                DERROR("can't get \"%s\" service entry\n", service);
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        ret = tcp_sock_listen(srv_sd, (struct sockaddr *)&sin, qlen, nonblock, tuning);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int tcp_sock_hostlisten6(int *srv_sd, const char *host, const char *service,
                        int qlen, int nonblock, int tuning)
{
        int ret;
        struct servent result, *pse;
        char buf[MAX_BUF_LEN];
        struct sockaddr_in6 sin;

        _memset(&sin, 0, sizeof(sin));

        sin.sin6_family = PF_INET6;
        if (host) {
                //inet_pton(AF_INET6, host, &sin.sin6_addr);
                ret = _inet_addr((struct sockaddr *)&sin, host);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        } else
                sin.sin6_addr = in6addr_any;

        getservbyname_r(service, "tcp", &result, buf, MAX_BUF_LEN, &pse);
        if (pse)
                sin.sin6_port = pse->s_port;
        else if ((sin.sin6_port=htons((unsigned short)atoi(service))) == 0) {
                DERROR("can't get \"%s\" service entry\n", service);
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        ret = tcp_sock_listen(srv_sd, (struct sockaddr *)&sin, qlen, nonblock, tuning);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int tcp_sock_addrlisten(int *srv_sd, uint32_t addr, uint32_t port, int qlen, int nonblock)
{
        int ret;
        struct sockaddr_in sin;

        _memset(&sin, 0, sizeof(sin));
        sin.sin_family = AF_INET;

        if (addr != 0)
                sin.sin_addr.s_addr = addr;
        else
                sin.sin_addr.s_addr = INADDR_ANY;

        sin.sin_port = htons(port);

        ret = tcp_sock_listen(srv_sd, (struct sockaddr *)&sin, qlen, nonblock, 1);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int tcp_sock_portlisten(int *srv_sd, uint32_t addr, uint32_t *_port, int qlen, int nonblock)
{
        int ret;
        struct sockaddr_in sin;
        uint16_t port = 0;

        _memset(&sin, 0, sizeof(sin));
        sin.sin_family = AF_INET;

        if (addr != 0)
                sin.sin_addr.s_addr = addr;
        else
                sin.sin_addr.s_addr = INADDR_ANY;

        while (srv_running) {
                port = (uint16_t)(YNET_SERVICE_BASE
                                  + (random() % YNET_SERVICE_RANGE));

                sin.sin_port = htons(port);

                ret = tcp_sock_listen(srv_sd, (struct sockaddr *)&sin, qlen, nonblock, 1);
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

        *_port = port;

        return 0;
err_ret:
        return ret;
}

int tcp_sock_accept(net_handle_t *nh, int srv_sd, int tuning, int nonblock)
{
        int ret, sd;
        struct sockaddr_in sin;
        socklen_t alen;

        _memset(&sin, 0, sizeof(sin));
        alen = sizeof(struct sockaddr_in);

        sd = __tcp_accept(srv_sd, (struct sockaddr *)&sin, &alen, _get_timeout() / 2);
        if (sd < 0) {
	        ret = -sd;
                DERROR("srv_sd %d, %u\n", srv_sd, ret);
                GOTO(err_ret, ret);
        }

        ret = tcp_sock_tuning(sd, tuning, nonblock);
        if (unlikely(ret))
                GOTO(err_sd, ret);

        YASSERT(sd > 0);

        memset(nh, 0x0, sizeof(*nh));
        nh->type = NET_HANDLE_TRANSIENT;
        nh->u.sd.sd = sd;
        nh->u.sd.addr = sin.sin_addr.s_addr;

        return 0;
err_sd:
        (void) sy_close(sd);
err_ret:
        return ret;
}

int tcp_sock_accept_sd(int *_sd, int srv_sd, int tuning, int nonblock)
{
        int ret, sd;
        struct sockaddr_in sin;
        socklen_t alen;

        _memset(&sin, 0, sizeof(sin));
        alen = sizeof(struct sockaddr_in);

        sd = __tcp_accept(srv_sd, (struct sockaddr *)&sin, &alen, _get_timeout() / 2);
        if (sd < 0) {
	        ret = -sd;
                DERROR("srv_sd %d\n", srv_sd);
                GOTO(err_ret, ret);
        }

        ret = tcp_sock_tuning(sd, tuning, nonblock);
        if (unlikely(ret))
                GOTO(err_sd, ret);

        *_sd = sd;

        return 0;
err_sd:
        (void) sy_close(sd);
err_ret:
        return ret;
}

int tcp_sock_connect(net_handle_t *nh, struct sockaddr_in *sin, int nonblock, int timeout, int tuning)
{
        int ret, sd;

        sd = socket(PF_INET, SOCK_STREAM, 0);
        if (sd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        //ret = connect(sd, (struct sockaddr*)sin, sizeof(struct sockaddr));
        ret = __tcp_connect(sd, (struct sockaddr*)sin, sizeof(struct sockaddr),
                            timeout);
        if (unlikely(ret)) {
                GOTO(err_sd, ret);
        }

        if (tuning) {
                ret = tcp_sock_tuning(sd, 1, nonblock);
                if (unlikely(ret))
                        GOTO(err_sd, ret);
        }

        DBUG("new sock %d connected\n", sd);
        nh->u.sd.sd = sd;
        nh->u.sd.addr = sin->sin_addr.s_addr;
        //sock->proto = ng.op;

        return 0;
err_sd:
        (void) sy_close(sd);
err_ret:
        return ret;
}

#if 1

static int __tcp_sock_getaddr(uint32_t network, uint32_t mask, uint32_t *_addr)
{
        int ret, sd, i, done;
        uint32_t addr;
        char buf[MAX_BUF_LEN];
        struct ifconf ifc;
        struct ifreq *ifcreq, ifr;
        struct sockaddr_in localaddr, *sin;

        ret = inet_aton(YNET_LOCALHOST, &localaddr.sin_addr);
        if (ret == 0) {
                ret = EINVAL;
                DERROR("ret (%d) %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        sd = socket(PF_INET, SOCK_STREAM, 0);
        if (sd == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        ifc.ifc_len = MAX_BUF_LEN;
        ifc.ifc_buf = buf;

        ret = ioctl(sd, SIOCGIFCONF, &ifc);
        if (ret == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_sd, ret);
        }

        ifcreq = ifc.ifc_req;

        done = 0;
        for (i = ifc.ifc_len / sizeof(struct ifreq); --i >= 0; ifcreq++) {

                _strncpy(ifr.ifr_name, ifcreq->ifr_name,
                         _strlen(ifcreq->ifr_name) + 1);

                ret = ioctl(sd, SIOCGIFFLAGS, &ifr);
                if (unlikely(ret)) {
                        ret = errno;
                        DERROR("ret (%d) %s\n", ret, strerror(ret));
                        GOTO(err_sd, ret);
                }

                if ((ifr.ifr_flags & IFF_UP) == 0)
                        continue;

                sin = (struct sockaddr_in *)&ifcreq->ifr_addr;
                addr = sin->sin_addr.s_addr;
                DBUG("ifname %s, %s\n", ifcreq->ifr_name, _inet_ntoa(addr));
                //DBUG("got sock info %s %u %s\n", _inet_ntoa(addr), i, ifcreq->ifr_name);
                if ((addr & mask) == (network & mask)) {
                        DINFO("ifname %s, %s\n", ifcreq->ifr_name, _inet_ntoa(addr));
                        //DINFO("got sock info %u & %u  %u & %u\n", addr, mask, network, mask);
                        done = 1;
                        break;
                }
        }

        sy_close(sd);

        if (done == 0) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }

        *_addr = addr;
        
        return 0;
err_sd:
        (void) sy_close(sd);
err_ret:
        return ret;
}

int tcp_sock_getaddr(uint32_t *info_count, ynet_sock_info_t *info,
                     uint32_t info_count_max, uint32_t port)
{
        int ret, i, count;
        uint32_t addr;

        count = 0;
        for (i = 0; i < netconf.count; i++) {
                YASSERT(count < (int)info_count_max);
                ret = __tcp_sock_getaddr(netconf.network[i].network,
                                         netconf.network[i].mask, &addr);
                if (unlikely(ret)) {
                        continue;
                }

                DBUG("info[%u] addr %u\n", count, addr);
                
                info[count].addr = addr;
                info[count].port = htons(port);
                count++;
        }

        DINFO("get sock count %u\n", count);
        
        if (count == 0) {
                ret = ENONET;
                DBUG("connect refused\n");
                GOTO(err_ret, ret);
        }

        *info_count = count;
        
        return 0;
err_ret:
        return ret;
}



#else

int tcp_sock_getaddr(uint32_t *info_count, ynet_sock_info_t *info,
                     uint32_t info_count_max, uint32_t port)
{
        int ret, sd, i, j, done;
        uint32_t count, addr;
        char buf[MAX_BUF_LEN];
        struct ifconf ifc;
        struct ifreq *ifcreq, ifr;
        struct sockaddr_in localaddr, *sin;

        ret = inet_aton(YNET_LOCALHOST, &localaddr.sin_addr);
        if (ret == 0) {
                ret = EINVAL;
                DERROR("ret (%d) %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        sd = socket(PF_INET, SOCK_STREAM, 0);
        if (sd == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        ifc.ifc_len = MAX_BUF_LEN;
        ifc.ifc_buf = buf;

        ret = ioctl(sd, SIOCGIFCONF, &ifc);
        if (ret == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_sd, ret);
        }

        ifcreq = ifc.ifc_req;

        count = 0;

        for (i = ifc.ifc_len / sizeof(struct ifreq); --i >= 0; ifcreq++) {

                _strncpy(ifr.ifr_name, ifcreq->ifr_name,
                         _strlen(ifcreq->ifr_name) + 1);

                ret = ioctl(sd, SIOCGIFFLAGS, &ifr);
                if (unlikely(ret)) {
                        ret = errno;
                        DERROR("ret (%d) %s\n", ret, strerror(ret));
                        GOTO(err_sd, ret);
                }

                if ((ifr.ifr_flags & IFF_UP) == 0)
                        continue;

                sin = (struct sockaddr_in *)&ifcreq->ifr_addr;
                addr = sin->sin_addr.s_addr;
                DBUG("ifname %s, %s\n", ifcreq->ifr_name, _inet_ntoa(addr));

                done = 0;
                //DBUG("got sock info %s %u %s\n", _inet_ntoa(addr), i, ifcreq->ifr_name);

                for (j = 0; j < netconf.count; j++) {
                        if ((addr & netconf.network[j].mask)
                            == (netconf.network[j].network & netconf.network[j].mask)) {
                                DBUG("got sock info %u & %u  %u & %u\n",
                                      addr, netconf.network[j].mask,
                                      netconf.network[j].network, netconf.network[j].mask);

                                done = 1;
                                break;
                        }
                }

                if (done) {
                        info[count].addr = addr;
                        info[count].port = htons(port);

                        DINFO("got sock info %s:%u\n",
                              _inet_ntoa(info[count].addr), port);

                        count++;
                }
        }

        (void) info_count_max;

        YASSERT(count <= netconf.count);
        
        ret = sy_close(sd);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (count != 0)
                *info_count = count;
        else {
                ret = ECONNREFUSED;
                DINFO("connect refused\n");
                GOTO(err_ret, ret);
        }

        return 0;
err_sd:
        (void) sy_close(sd);
err_ret:
        return ret;
}

#endif

int tcp_sock_hostconnect(net_handle_t *nh, const char *host,
                         const char *service, int nonblock, int timeout, int tuning)
{
        int ret;
        struct servent result, *pse;
        char buf[MAX_BUF_LEN];
        struct sockaddr_in sin;

        _memset(&sin, 0, sizeof(struct sockaddr_in));
        sin.sin_family = AF_INET;

        getservbyname_r(service, "tcp", &result, buf, MAX_BUF_LEN, &pse);
        if (pse)
                sin.sin_port = pse->s_port;
        else if ((sin.sin_port = htons(atoi(service))) == 0) {
                ret = ENOENT;
                DERROR("get port from service (%s)\n", service);
                YASSERT(0);
                GOTO(err_ret, ret);
        }

        ret = _inet_addr((struct sockaddr *)&sin, host);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DBUG("host %s service %s --> ip %s port %d\n",
             host, service, 
             inet_ntoa(sin.sin_addr), ntohs(sin.sin_port));

        ret = tcp_sock_connect(nh, &sin, nonblock, timeout, tuning);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int tcp_sock_close(int sd)
{
        int ret;
#if 0
        /* __XXX__ */
        ret = sy_shutdown(sd);
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif
        ret = sy_close(sd);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

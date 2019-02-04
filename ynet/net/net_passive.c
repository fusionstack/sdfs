#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define DBG_SUBSYS S_LIBYNET

#include "../sock/ynet_sock.h"
#include "configure.h"
#include "net_global.h"
#include "ynet_conf.h"
#include "schedule.h"
#include "ynet_net.h"
#include "dbg.h"

extern int node_get_deleting();

int net_hostbind(int *srv_sd, const char *host, const char *service, int nonblock)
{
        return sock_hostbind(srv_sd, host, service, nonblock);
}

int net_hostlisten(int *srv_sd, const char *host, const char *service,
                   int qlen, int nonblock)
{
        return sock_hostlisten(srv_sd, host, service, qlen, nonblock);
}

int net_addrlisten(int *srv_sd, uint32_t addr, uint32_t port, int qlen, int nonblock)
{
        return sock_addrlisten(srv_sd, addr, port, qlen, nonblock);
}

int net_portlisten(int *srv_sd, uint32_t addr, uint32_t *port, int qlen, int nonblock)
{
        return sock_portlisten(srv_sd, addr, port, qlen, nonblock);
}

#define NETINFO_TIMEOUT (10 * 60)

static int __net_getinfo(uint32_t *_count, ynet_sock_info_t *socks,
                         uint32_t info_count_max, uint32_t port)
{
        int ret, herrno = 0, retry = 0;
        ynet_sock_info_t *_sock, *sock;
        uint32_t i, count, j;
        struct hostent hostbuf, *result;
        char _info[MAX_BUF_LEN], buf[MAX_BUF_LEN], hostname[MAX_BUF_LEN];
        struct in_addr sin;

        ret = net_gethostname(hostname, MAX_NAME_LEN);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        if (strlen(ng.name) == 0) {
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }

#if 1
        if (strcmp(hostname, ng.name) != 0) {
                //DINFO("hostname %s -- > %s\n", ng.name, hostname);
        }
#endif

        _sock = (void *)_info;
        ret = sock_getinfo(&count, _sock, info_count_max, port);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

retry:
        ret = gethostbyname_r(hostname, &hostbuf, buf, sizeof(buf),  &result, &herrno);
        if (unlikely(ret)) {
                ret = errno;
                if (ret == EALREADY || ret == EAGAIN) {
                        ret = EAGAIN;
                        USLEEP_RETRY(err_ret, ret, retry, retry, 5, (100 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        if (result == NULL) {
                ret = ENONET;
                DERROR("hostname %s not exist\n", hostname);
                GOTO(err_ret, ret);
        }

        *_count = 0;
        for (i = 0; i < count; i++) {
                sock = &_sock[i];

                DINFO("hostname:%s count:%d addr %s %u\n", hostname, count,
                      _inet_ntoa(sock->addr), ntohl(sock->addr));

                for (j = 0; result->h_addr_list[j] != NULL; j++) {
                        //YASSERT(j < *count);
                        sin = *(struct in_addr *)(result->h_addr_list[j]);
                        DINFO("result addr %s %u\n", inet_ntoa(sin), ntohl(sin.s_addr));

                        if (sock->addr == sin.s_addr) {
                                //DINFO("addr %s %u\n", inet_ntoa(sin), ntohl(sin.s_addr));
                                socks[*_count] = *sock;
                                *_count = *_count + 1;
                        }
                }
        }        

        YASSERT(*_count);
        DBUG("count %u\n", *_count);

        return 0;
err_ret:
        return ret;
}

static void __net_checkinfo(const ynet_sock_info_t *sock, int count)
{
        int i;

        if (ng.daemon) {
                YASSERT(count);
        }

        if (count == 1)
                return;

        //YASSERT(count > 1);

        for (i = 1; i < count; i++) {
                if (sock[i].addr == sock[0].addr && sock[i].port == sock[0].port) {
                        YASSERT(0);
                }
        }

        if (count > 2)
                __net_checkinfo(&sock[1], count - 1);
}

int net_getinfo(char *infobuf, uint32_t *infobuflen, uint32_t port)
{
        int ret;
        ynet_net_info_t *info;
        uint32_t info_count_max, count = 0;
        char hostname[MAX_NAME_LEN];

        if (ng.daemon && port == (uint32_t)-1) {
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }

        while (ng.daemon && ng.local_nid.id == 0) {
                DWARN("wait nid inited\n");
                sleep(1);
        }

        info = (ynet_net_info_t *)infobuf;
        _memset(infobuf, 0x0, sizeof(ynet_net_info_t));
        info->info_count = 0;

        if (port != (uint32_t)-1) {
                info_count_max = (*infobuflen - sizeof(ynet_net_info_t))
                        / sizeof(ynet_sock_info_t);

                ret = __net_getinfo(&count, info->info,
                                    info_count_max, port);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }

                info->info_count = count;
                        
                YASSERT(strlen(ng.name));
                if (ng.daemon) {
                        YASSERT(count);
                }
        }

        if (ng.daemon) {
                YASSERT(count);
        }
                
        info->id = *net_getnid();
        info->magic = YNET_PROTO_TCP_MAGIC;
        info->uptime = ng.uptime;
        uuid_unparse(ng.nodeid, info->nodeid);

        ret = gethostname(hostname, MAX_NAME_LEN);
        if (unlikely(ret < 0)) {
                ret = errno;
                GOTO(err_ret, ret);
        }
                
        snprintf(info->name, MAX_NAME_LEN, "%s:%s", hostname, ng.name);

        DINFO("info.name %s\n", info->name);

        if (port != YNET_PORT_NULL)
                YASSERT(info->info_count);

        *infobuflen = sizeof(ynet_net_info_t)
                + sizeof(ynet_sock_info_t) * info->info_count;

        info->len = *infobuflen;

        if (strcmp(info->name, "none") == 0) {
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }

        ng.info_time = gettime();
        _memcpy(ng.info_local, info, info->len);

        DBUG("local nid "NID_FORMAT" info_count %u port %u\n",
             NID_ARG(&info->id), info->info_count, port);

        __net_checkinfo(info->info, info->info_count);
        ((ynet_net_info_t *)infobuf)->deleting = 0;

        return 0;
err_ret:
        return ret;
}

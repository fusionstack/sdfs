

#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

#define DBG_SUBSYS S_YRPC

#include "ynet_net.h"
#include "../sock/sock_tcp.h"
#include "job_tracker.h"
#include "net_global.h"
#include "netbios_proto.h"
#include "netbios_passive.h"
#include "dbg.h"

net_request_handler netbios_request_handler;
extern jobtracker_t *netbios_jobtracker;

void netbios_procedure_register(const char *name, net_request_handler handler)
{
        (void) name;

        netbios_request_handler = handler;
}

int netbios_tcp_passive(uint32_t *port)
{
        int ret;
        net_proto_t proto;
        net_handle_t nh;

        _memset(&proto, 0x0, sizeof(net_proto_t));

        proto.reader = netbios_accept_handler;

        ret = sdevents_listen(&nh, port, &proto,
                              YNET_RPC_NONBLOCK);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdevents_add(&nh, Y_EPOLL_EVENTS_LISTEN);
        if (ret)
                GOTO(err_ret, ret);

        ret = jobtracker_create(&netbios_jobtracker, gloconf.yfs_jobtracker, "netbios");
        if (ret)
                GOTO(err_ret, ret);/*XXX --gray*/

        return 0;
err_ret:
        return ret;
}

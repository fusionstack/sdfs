

#include <stdint.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "../sock/ynet_sock.h"
#include "net_global.h"
#include "ynet_net.h"
#include "xnect.h"
#include "ylib.h"
#include "configure.h"
#include "net_table.h"
#include "dbg.h"

inline int net_hostconnect(int *sd, const char *host, const char *service, int nonblock)
{
        return sock_hostconnect(sd, host, service, nonblock, gloconf.rpc_timeout / 3);
}

inline int net_info2nid(net_handle_t *nh, const ynet_net_info_t *info)
{
        return netable_connect_info(nh, info, 1);
}

int net_nid2info(ynet_net_info_t *info, uint32_t *buflen, const net_handle_t *nid)
{
        int ret;

        ret = netable_getinfo(&nid->u.nid, info, buflen);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

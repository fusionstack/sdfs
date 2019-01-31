

#include <string.h>
#include <stdlib.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "../sock/ynet_sock.h"
#include "job.h"
#include "job_dock.h"
#include "net_table.h"
#include "configure.h"
#include "net_global.h"
#include "ynet_net.h"
#include "dbg.h"

#if 0
inline int net_recv_sd(int sd, void **rep, int timeout)
{
        return sock_recv_sd_malloc(sd, rep, timeout);
}

inline int net_peek_sd_sync(int sd, char *buf, uint32_t buflen, int timeout)
{
        return sock_peek_sd_sync(sd, buf, buflen, timeout);
}

inline int net_discard_sd_sync(int sd, uint32_t len, int timeout)
{
        return sock_discard_sd_sync(sd, len, timeout);
}
#endif

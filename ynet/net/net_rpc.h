#ifndef __NET_RPC_H__
#define __NET_RPC_H__

#include "job_dock.h"
#include "net_global.h"

int net_rpc_heartbeat(const sockid_t *sockid, uint64_t seq);
int net_rpc_coreinfo(const nid_t *nid, char *infobuf, int *infobuflen);
int net_rpc_init(void);

#endif

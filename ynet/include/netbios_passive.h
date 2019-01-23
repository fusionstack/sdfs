#ifndef __NETBIOS_PASSIVE_H__
#define __NETBIOS_PASSIVE_H__

#include <rpc/xdr.h>

#include "job.h"
#include "sdevent.h"

extern int netbios_tcp_passive(uint32_t *port);
void netbios_procedure_register(const char *name, net_request_handler handler);

#endif

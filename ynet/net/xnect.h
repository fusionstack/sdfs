#ifndef __XNECT_H__
#define __XNECT_H__

#include "ynet_net.h"

int net_accept(net_handle_t *nh, ynet_net_info_t *info, const net_proto_t *proto);
int net_connect(net_handle_t *nh, const ynet_net_info_t *info, int timeout);


#endif

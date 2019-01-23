#ifndef __HEART_BEAT_H__
#define __HEART_BEAT_H__

#include "ynet_net.h"

int heartbeat_add(const sockid_t *sockid, const nid_t *parent, suseconds_t timeout, time_t ltime);

#endif

#ifndef __NETWORK_H__
#define __NETWORK_H__

#include <stdio.h>

#include "ynet_net.h"
#include "job_dock.h"
#include "adt.h"
#include "etcd.h"
#include "sdfs_conf.h"
#include "net_global.h"

/*network.c*/
int network_init(void);

int network_connect_master(void);
int network_connect(const nid_t *nid, time_t *_ltime, int _timeout, int force);
int network_connect_wait(const nid_t *nid, time_t *_ltime, int _timeout, int force);

int network_connect_byname(const char *name, nid_t *nid);

void network_ltime_reset(const nid_t *nid, time_t ltime, const char *why);
int network_ltime(const nid_t *nid, time_t *ltime);
time_t network_ltime1(const nid_t *nid);

const char *network_rname(const nid_t *nid);
int network_rname1(const nid_t *nid, char *name);

void network_close(const nid_t *nid, const char *why, const time_t *ltime);

//just for compatible, will be removed
int network_connect2(const net_handle_t *nh, int force);
int network_connect1(const nid_t *nid);


#endif

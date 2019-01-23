#ifndef __CONN_H__
#define __CONN_H__

int conn_init();
int conn_retry(const nid_t *_nid);
int conn_register();
int conn_getinfo(const nid_t *nid, ynet_net_info_t *info);
int conn_setinfo();
int conn_online(const nid_t *nid, int timeout);
int conn_faultdomain(int *_total, int *_online);
int conn_listnode(nid_t *array, int *_count);

#endif

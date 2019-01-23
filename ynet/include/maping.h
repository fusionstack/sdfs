#ifndef __MAPING_H__
#define __MAPING_H__

/*from maping.c*/

#define HOST2NID    "network/host2nid"
#define NID2NETINFO "network/nid2netinfo"
#define NAME2NID    "storage/name2nid"
#define ID2NID      "storage/id2nid"
#define ROOT      "storage/root"
#define MAPING_MISC "misc"

int maping_init(void);
int maping_nid2netinfo(const nid_t *nid, ynet_net_info_t *info);
int maping_cleanup(const char *type);
int maping_host2nid(const char *hostname, nid_t *nid);
int maping_addr2nid(const char *addr, nid_t *nid);
int maping_nid2host(const nid_t *nid, char *hostname);
int maping_getmaster(nid_t *nid, int force);
int maping_set(const char *type, const char *_key, const char *value);
int maping_get(const char *type, const char *_key, char *value, time_t *ctime);
int maping_drop(const char *type, const char *_key);

#endif

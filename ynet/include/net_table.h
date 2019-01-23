#ifndef __NET_TABLE_H__
#define __NET_TABLE_H__

#include "ynet_net.h"
#include "maping.h"

typedef enum {
        NETABLE_NULL,
        NETABLE_CONN,
        NETABLE_DEAD,
} netstatus_t;

typedef struct {
        net_handle_t sock;
} socklist_t;

typedef struct {
        uint32_t prev;
        uint32_t now;
} ltime_t;

typedef struct __connection {
        net_handle_t nh;
        netstatus_t status;
        sy_spinlock_t load_lock;

        char lname[MAX_NAME_LEN];
        ynet_net_info_t *info;

        uint64_t load;        ///< latency，用于副本读的负载均衡
        ltime_t ltime;
        time_t update;
        time_t last_retry;
        uint32_t timeout;

        struct list_head reset_handler;

        net_handle_t sock;
} ynet_net_conn_t;

int netable_init(int daemon);
int netable_destroy(void);

int netable_accept(const ynet_net_info_t *info, const net_handle_t *sock);

int netable_connect_info(net_handle_t *nh, const ynet_net_info_t *info, int force);
int netable_updateinfo(const ynet_net_info_t *info);
int netable_getinfo(const nid_t *nid, ynet_net_info_t *info, uint32_t *buflen);

int netable_connected(const nid_t *nid);
int netable_connectable(const nid_t *nid, int force);

int netable_add_reset_handler(const nid_t *nid, func1_t handler, void *ctx);
void netable_close(const nid_t *nid, const char *resion, const time_t *ltime);
void netable_close_withrpc(const sockid_t *sockid, const nid_t *nid, const char *why);

const char *netable_rname(const void *_nh);
const char *netable_rname_nid(const nid_t *nid);
int netable_rname1(const nid_t *nid, char *name);

int netable_getname(const nid_t *nid, char *name);
int netable_gethost(const nid_t *nid, char *name);
int netable_getsock(const nid_t *nid, sockid_t *sockid);

time_t netable_conn_time(const nid_t *nid);
void netable_ltime_reset(const nid_t *nid, time_t ltime, const char *why);

//void netable_closewait(const net_handle_t *nh, const char *why);
void netable_select(const nid_t *nids, int count, nid_t *nid);
void netable_sort(nid_t *nid, int count);

void netable_load_update(const nid_t *nid, uint64_t load);
int netable_update_retry(const nid_t *nid);

void netable_iterate(void);
int netable_dump_hb_timeout(char *lname);
void netable_update(const nid_t *nid);
time_t netable_last_update(const nid_t *nid);
int netable_start();

//just for compatible, will be removed
int netable_msgpush(const nid_t *nid, const void *buf, int len);
void netable_put(net_handle_t *nh, const char *why);
int netable_send(const net_handle_t *nh, job_t *job, uint64_t hash, int is_request);
int netable_msgget(const nid_t *nid, void *buf, int len);
int netable_msgpop(const nid_t *nid, void *buf, int len);


#endif

#ifndef __MDS_H__
#define __MDS_H__

#include <semaphore.h>
#include <pthread.h>

#include "diskpool.h"
#include "jnl_proto.h"
#include "nodepool.h"
#include "sdfs_list.h"
#include "ylock.h"
#include "ynet_rpc.h"
#include "yatomic.h"
#include "file_proto.h"
#include "yfs_conf.h"

enum {
        MDS_NULL,
        MDS_PRIMARY,        /* primary mds */
        MDS_SHADOW,         /* shadow mds, rd only, no cds info */
        MDS_SHADOW_FORCE,
};

typedef enum {
        MDS_SYNC_DONE = 0,
        MDS_SYNC_RUNNING,
} mds_sync_status_t;

typedef struct {
    int mds_type;
    time_t uptime;
    int metano;

    mds_sync_status_t status;

    yatomic_t fileid;
    uint64_t diskid_max;
    sy_spinlock_t diskid_lock;

    uint32_t version;

    struct disk_pool  diskpool;   /* diskid -> disk_info */
    struct node_pool  nodepool_hdd;   /*hdd disk list */
    struct node_pool  nodepool_ssd;   /*ssd disk list */
    char consistent_leak[MAX_NAME_LEN];
    int (*scan_continue)();
} mds_info_t;

extern mds_info_t mds_info;

int mds_request_table(char *req_buf, uint32_t reqlen,
                      char *rep_buf, uint32_t *_replen, const nid_t *peer);

#endif

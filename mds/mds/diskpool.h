#ifndef __DISKPOOL_H__
#define __DISKPOOL_H__

#include <stdint.h>
#include <semaphore.h>
#include <pthread.h>

#include "chk_proto.h"
#include "disk_proto.h"
#include "sdfs_list.h"
#include "node_proto.h"
#include "md_proto.h"
#include "net_table.h"
#include "skiplist.h"
#include "yfsmds_conf.h"
#include "ylock.h"
#include "ynet_rpc.h"

struct disk_chkinfo {
        chkid_t chkid;
};

struct disk_info {
        diskid_t diskid;
        net_handle_t nh;

        sy_rwlock_t rwlock;

        struct list_head list;

        diskinfo_stat_t diskinfo;
        disk_stat_t     diskstat;

        time_t dead_time;       /* the time when it goes dead */
        time_t lasthb_time;     /* the time getting last heart beat message */

        char net[NET_HANDLE_LEN];

        int maxlevel;
        int chunksize;
        chkid_t min;
        chkid_t max;

        int group;

        struct skiplist         **chk_list;
        sy_rwlock_t             *chk_rwlock;
        uint32_t volreptnum;
        volrept_t *volrept;
};

#define DISKINFO2PINGSTAT(di, st) \
do { \
        (st)->diskid      = (di)->diskid;                 \
        (st)->load = 0;                                   \
        (st)->sockaddr    = 0;     \
        (st)->sockport    = 0; \
        (st)->diskstat    = (di)->diskstat; \
        (st)->disktotal   = (di)->diskinfo.ds_blocks * (di)->diskinfo.ds_frsize; \
        (st)->diskfree    = (di)->diskinfo.ds_bfree * (di)->diskinfo.ds_bsize; \
        strncpy((st)->rname, netable_rname(&(di)->nh), 31); (st)->rname[31] = '\0'; \
} while (0)

struct disk_pool {
        int maxlevel;
        int chunksize;
        diskid_t min;
        diskid_t max;

        int group;

        struct list_head        dead_list;
        sy_rwlock_t             dead_rwlock;
        sem_t                   dead_sem;

        struct list_head        *stat_list[DISK_STAT_DEAD];
        sy_rwlock_t             *stat_rwlock[DISK_STAT_DEAD];

        struct skiplist         **disk_list;
        diskinfo_stat_t         *disk_info;
        sy_rwlock_t             *disk_rwlock;
};

extern int diskpool_init(struct disk_pool *);
extern int diskpool_destroy(struct disk_pool *, int group);

extern int diskpool_join(const diskid_t *, const diskinfo_stat_t *);
extern int diskpool_dead(const diskid_t *);
extern int diskpool_hb(const diskid_t *, uint32_t, const diskinfo_stat_diff_t *,
                       uint32_t volreptnum,
                       const volrept_t *chkrept, const uuid_t *nodeid);
extern int diskpool_rept(diskid_t *, uint32_t chkreptnum, chkjnl_t *);
extern int diskpool_disk2netinfo_multi(int disk_num, net_handle_t *disks,
                                       char **info, uint32_t *infolen, uint32_t *reps);
extern int diskpool_statvfs(diskinfo_stat_t *);
int diskpool_list(char *buf, uint32_t *buflen);
int diskpool_info(char *buf, uint32_t *buflen);
extern int diskpool_dump(const struct disk_info *di);
extern int diskpool_isdead(struct disk_pool *dp, const diskid_t *diskid, int *isdead,
                           time_t *dead_time);
extern int diskpool_isvalid(struct disk_pool *dp, const diskid_t *diskid, int *isvalid);
int diskpool_count(int *_count);

#endif

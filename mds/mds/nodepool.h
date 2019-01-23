#ifndef __NODEPOOL_H__
#define __NODEPOOL_H__

#include <pthread.h>

#include "yatomic.h"
#include "ylock.h"
#include "sdfs_list.h"
#include "disk_proto.h"

#define NODEPOOL_MAX_CHOOSED 1024 * 128

typedef struct {
        struct list_head list;
        diskid_t         diskid;
} disk_head_t;

struct node_info {
        struct list_head list;

        uuid_t           nodeid;
        char             name[MAX_NAME_LEN];

        int              tmp_used;
        int              choosed;              /* The number of this node has been choosed */
        yatomic_t        disk_number;

        struct list_head disk_list;
        sy_rwlock_t      disk_rwlock;
};

struct node_pool {
        int              group;

        yatomic_t        disk_total;
        uint32_t        node_total;

        hashtable_t         ht_nodelist;
        sy_rwlock_t      ht_nodelist_rwlock;

        struct list_head nodelist;
        sy_rwlock_t      nodelist_rwlock;
        int              choosed_total;
};

/*----------------------------nodepool.c-------------------------------*/
extern int nodepool_init();
extern int nodepool_destroy(struct node_pool *, int group);

extern int nodepool_addisk(const uuid_t *_nodeid, const diskid_t *diskid, uint32_t tier);
extern int nodepool_get(uint32_t reps, net_handle_t *disks, int hardend, uint32_t tier);
extern int nodepool_nodedead(diskid_t *, uuid_t *);
extern int nodepool_diskdead(const diskid_t *diskid, uint32_t tier);
extern void nodepool_hash_print(void);
extern int nodepool_get_node_num(uint32_t *node_num);

/*----------------------------nodepool_hdd.c-------------------------------*/
extern int nodepool_init_hdd();
extern int nodepool_destroy_hdd(struct node_pool *, int group);

extern int nodepool_addisk_hdd(const uuid_t *_nodeid, const diskid_t *diskid);
extern int nodepool_get_hdd(uint32_t reps, net_handle_t *disks, int hardend);
extern int nodepool_nodedead_hdd(diskid_t *, uuid_t *);
extern int nodepool_diskdead_hdd(const diskid_t *diskid);
extern void nodepool_hash_print_hdd(void);
extern int nodepool_get_node_num_hdd(uint32_t *node_num);

/*----------------------------nodepool_ssd.c-------------------------------*/
extern int nodepool_init_ssd();
extern int nodepool_destroy_ssd(struct node_pool *, int group);

extern int nodepool_addisk_ssd(const uuid_t *_nodeid, const diskid_t *diskid);
extern int nodepool_get_ssd(uint32_t reps, net_handle_t *disks, int hardend);
extern int nodepool_nodedead_ssd(diskid_t *, uuid_t *);
extern int nodepool_diskdead_ssd(const diskid_t *diskid);
extern void nodepool_hash_print_ssd(void);
extern int nodepool_get_node_num_ssd(uint32_t *node_num);

#endif

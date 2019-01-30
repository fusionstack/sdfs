

#include <errno.h>

#define DBG_SUBSYS S_YFSMDS

#include "disk_proto.h"
#include "mds.h"
#include "nodepool.h"
#include "heap.h"
#include "ylib.h"
#include "yfsmds_conf.h"
#include "dbg.h"

// #define YFS_DEBUG

extern mds_info_t mdsinfo;

int nodepool_dump();

void nodepool_hash_print(void)
{
        nodepool_hash_print_ssd();
        nodepool_hash_print_hdd();
}

int nodepool_destroy(struct node_pool *np, int group)
{
        nodepool_destroy_ssd(np, group);
        nodepool_destroy_hdd(np, group);

        return 0;
}

int nodepool_init()
{
        int ret = 0;

        ret = nodepool_init_ssd();
        if (ret)
                GOTO(err_ret, ret);

        ret = nodepool_init_hdd();
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int nodepool_addisk(const uuid_t *_nodeid, const diskid_t *diskid, uint32_t tier)
{
        int ret;
        
        if (tier == TIER_SSD) {
                ret = nodepool_addisk_ssd(_nodeid, diskid);
                if (ret)
                        GOTO(err_ret, ret);
        } else if (tier == TIER_HDD) {
                ret = nodepool_addisk_hdd(_nodeid, diskid);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                DERROR("invalid tier, diskid:"DISKID_FORMAT", tier:%u\n", DISKID_ARG(diskid), tier);
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        nodepool_dump();
        
        return 0;
err_ret:
        nodepool_dump();
        return ret;
}


/*
 * 在没空间时，之前加过重试，但是会导致没空间时，创建数据块性能下降。
 * 所以这里先关掉重试。重试应该放在最外层才合适？ */
int nodepool_get(uint32_t reps, net_handle_t *disks, int hardend, uint32_t tier)
{
        int ret = 0;

        if (tier == TIER_SSD) {
                ret = nodepool_get_ssd(reps, disks, hardend);
                if (ret) {
                        if (ret == ENOSPC) {/*try get node from hdd if ssd no space left*/
                                ret = nodepool_get_hdd(reps, disks, hardend);
                                if (ret)
                                        GOTO(err_ret, ret);
                        } else {
                                GOTO(err_ret, ret);
                        }
                }
        } else if (tier == TIER_HDD) {
                ret = nodepool_get_hdd(reps, disks, hardend);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                DERROR("nodepool_get fail, invalid tier:%u\n", tier);
                return EINVAL;
        }

        return 0;
err_ret:
        return ret;
}

/**
 * @note useless now.
 * delete diskid in node_pool, condition: dead or overload etc.
 */
int nodepool_nodedead(diskid_t *diskid, uuid_t *nodeid)
{
        int ret = 0;

        ret = nodepool_nodedead_ssd(diskid, nodeid);
        if (ret)
                GOTO(err_ret, ret);

        ret = nodepool_nodedead_hdd(diskid, nodeid);
        if (ret)
                GOTO(err_ret, ret);

        nodepool_dump();
        
        return 0;
err_ret:
        nodepool_dump();
        return ret;
}

int nodepool_diskdead(const diskid_t *diskid, uint32_t tier)
{
        int ret = 0;

        if (tier == TIER_SSD) {
                ret = nodepool_diskdead_ssd(diskid);
                if (ret)
                        GOTO(err_ret, ret);
        } else if (tier == TIER_HDD) {
                ret = nodepool_diskdead_hdd(diskid);
                if (ret)
                        GOTO(err_ret, ret);
        } else if (tier == TIER_ALL){
                ret = nodepool_diskdead_ssd(diskid);
                if (ret)
                        GOTO(err_ret, ret);

                ret = nodepool_diskdead_hdd(diskid);
                if (ret)
                        GOTO(err_ret, ret);

        } else {
                DERROR("invalid tier, diskid:"DISKID_FORMAT", tier:%u\n", DISKID_ARG(diskid), tier);
                return EINVAL;
        }

        nodepool_dump();

        return 0;
err_ret:
        nodepool_dump();
        return ret;
}

/*
*Date   : 2017.04.18
*Author : Yang
*nodepool_get_node_num : get node number of NAS
*/
int nodepool_get_node_num(uint32_t *node_num)
{
        int ret;
        uint32_t ssd_num = 0, hdd_num = 0;

        ret = nodepool_get_node_num_ssd(&ssd_num);
        if (ret)
                GOTO(err_ret, ret);

        ret = nodepool_get_node_num_hdd(&hdd_num);
        if (ret)
                GOTO(err_ret, ret);

        *node_num = ssd_num + hdd_num;

        return 0;
err_ret:
        return ret;
}

static void __nodepool_dump__(void *arg, void *data)
{
        struct node_info *_data;
        struct list_head *pos;
        disk_head_t *tmp;
        char *buf = arg;

        _data = (struct node_info *)data;

        snprintf(buf + strlen(buf), MAX_NAME_LEN, "%s ", _data->name);
        list_for_each(pos, &_data->disk_list) {
                tmp = list_entry(pos, disk_head_t, list);
                snprintf(buf + strlen(buf), MAX_NAME_LEN, "%d,", tmp->diskid.id);
                DBUG("nid:"DISKID_FORMAT"\n", DISKID_ARG(&tmp->diskid));
        }

        if (buf[strlen(buf) - 1] == ',') {
                buf[strlen(buf) - 1] = '\0';
        }
        
        snprintf(buf + strlen(buf), MAX_NAME_LEN, "\n");
}

static void __nodepool_dump(struct node_pool *np, char *buf)
{
        hash_iterate_table_entries(np->ht_nodelist, __nodepool_dump__, buf);
}

int nodepool_dump()
{
        int ret;
        char buf[MAX_BUF_LEN];

        buf[0] = '\0';

        __nodepool_dump(&mds_info.nodepool_ssd, buf);
        __nodepool_dump(&mds_info.nodepool_hdd, buf);

        if (strlen(buf) == 0)
                return 0;
        
        if (buf[strlen(buf) - 1] == '\n') {
                buf[strlen(buf) - 1] = '\0';
        }

        ret = etcd_update_text(ETCD_DISKMAP, "diskmap", buf, NULL, -1);
        if (ret) {
                if (ret == ENOENT) {
                        ret = etcd_create_text(ETCD_DISKMAP, "diskmap", buf, -1);
                        if (ret)
                                GOTO(err_ret, ret);
                } else 
                        GOTO(err_ret, ret);
        }

        DINFO("update diskmap\n");
        
        DBUG("------\n%s\n-----", buf);

        return 0;
err_ret:
        return ret;
}

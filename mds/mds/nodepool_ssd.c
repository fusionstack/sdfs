

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

static inline int hash_cmp_fun(const void *data_a, const void *data_b)
{
        struct node_info *node;
        char *_uuida;
        char _uuidb[MAX_NAME_LEN];

        node = (struct node_info *)data_a;
        _uuida = (char *)data_b;

        uuid_unparse(node->nodeid, _uuidb);

        return memcmp(_uuidb, _uuida, strlen(_uuidb));
}

static inline uint32_t hash_str2key(const void *s)
{
        char *str = (char *)s;
        return hash_str(str);
}

static void __ht_print(void *arg, void *data)
{
        (void) arg;
        struct node_info *_data;
        struct node_pool *np;
        struct list_head *pos;
        disk_head_t *tmp;
        struct list_head *q;
        struct node_info *ni_tmp;

        np = &mds_info.nodepool_ssd;
        _data = (struct node_info *)data;

	DINFO("**************start print list******************\n");
        if (list_empty(&np->nodelist)) {
		DINFO("no node in list\n");
        } else {
                list_for_each_safe(pos, q, &np->nodelist) {
                        ni_tmp = list_entry(pos, struct node_info, list);
			DINFO("$$ %s $$\n", ni_tmp->name);
		}
	}
	DINFO("**************stop print list******************\n");

	DINFO("**************start print hash*****************\n");
        DINFO("node: %s, disk_num: %llu/%lluu, choosed: %u/%u, %.4f, %.4f\n",
               _data->name, (LLU)_data->disk_number.value, (LLU)np->disk_total.value,
               _data->choosed, np->choosed_total,
               (float)_data->disk_number.value/(float)np->disk_total.value,
               (float)_data->choosed/(float)np->choosed_total);

        list_for_each(pos, &_data->disk_list) {
                tmp = list_entry(pos, disk_head_t, list);
                DINFO(""DISKID_FORMAT"\n", DISKID_ARG(&tmp->diskid));
        }
	DINFO("**************stop print hash*****************\n");
}

void nodepool_hash_print_ssd(void)
{
        struct node_pool *np;
        np = &mds_info.nodepool_ssd;

        hash_iterate_table_entries(np->ht_nodelist, __ht_print, NULL);
}

int nodepool_destroy_ssd(struct node_pool *np, int group)
{
        (void) np;
        (void) group;

        return 0;
}

int nodepool_init_ssd()
{
        struct node_pool *np = &mds_info.nodepool_ssd;

        sy_rwlock_init(&np->ht_nodelist_rwlock, NULL);
        np->ht_nodelist = hash_create_table(hash_cmp_fun, hash_str2key, "nodepool");

        sy_rwlock_init(&np->nodelist_rwlock, NULL);
        INIT_LIST_HEAD(&np->nodelist);

        np->choosed_total = 0;
        np->node_total = 0;

        yatomic_init(&np->disk_total, 0);

        DINFO("Node pool inited\n");

        return 0;
}

static inline int __node_info_create(struct node_info **_ni, const uuid_t *_nodeid,
                                     const char *nodeid)
{
        int ret;
        struct node_info *ni;

        *_ni = NULL;

        ret = ymalloc((void **)&ni, sizeof(struct node_info));
        if (ret)
                GOTO(err_ret, ret);

        _memcpy(ni->nodeid, _nodeid, sizeof(uuid_t));
        _memcpy(ni->name, nodeid, strlen(nodeid));

        INIT_LIST_HEAD(&ni->disk_list);

        ni->tmp_used = 0;
        ni->choosed  = 0;

        yatomic_init(&ni->disk_number, 1);
        sy_rwlock_init(&ni->disk_rwlock, NULL);

        *_ni = ni;

        return 0;
err_ret:
        return ret;
}

int nodepool_addisk_ssd(const uuid_t *_nodeid, const diskid_t *diskid)
{
        int ret, found;
        char nodeid[MAX_NAME_LEN];
        struct node_info *ni = NULL;
        struct node_pool *np;
        disk_head_t *disk_head;

        np = &mds_info.nodepool_ssd;

        _memset(nodeid, 0x0, MAX_NAME_LEN);
        (void) uuid_unparse(*_nodeid, nodeid);
        if (nodeid[0] == '\0') {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        /* @res di */
        ret = ymalloc((void **)&disk_head, sizeof(disk_head_t));
        if (ret)
                GOTO(err_ret, ret);

        disk_head->diskid.id = diskid->id;

        ret = sy_rwlock_rdlock(&np->ht_nodelist_rwlock);
        if (ret)
                GOTO(err_di, ret);

        ni = hash_table_find(np->ht_nodelist, (void *)nodeid);

        (void) sy_rwlock_unlock(&np->ht_nodelist_rwlock);

        if (ni == NULL) {
                found = 0;
                /* @res ni */
                ret = __node_info_create(&ni, _nodeid, nodeid);
                if (ret)
                        GOTO(err_di, ret);
        } else {
                found = 1;
                ret = yatomic_get_and_inc(&ni->disk_number, NULL);
                if (ret)
                        GOTO(err_di, ret);
        }

        ret = sy_rwlock_wrlock(&ni->disk_rwlock);
        if (ret)
                GOTO(err_ni, ret);

        list_add(&disk_head->list, &ni->disk_list);

        sy_rwlock_unlock(&ni->disk_rwlock);

        if (found == 0) {
                ret = sy_rwlock_wrlock(&np->ht_nodelist_rwlock);
                if (ret)
                        GOTO(err_ni, ret);

                ret = hash_table_insert(np->ht_nodelist, (void *)ni, (void *)nodeid, 0);

                sy_rwlock_unlock(&np->ht_nodelist_rwlock);

                ret = sy_rwlock_wrlock(&np->nodelist_rwlock);
                if (ret)
                        GOTO(err_ni, ret);

                list_add(&ni->list, &np->nodelist);

                np->node_total++;

                sy_rwlock_unlock(&np->nodelist_rwlock);

                DINFO("create node %s \n", nodeid)
        }

        ret = yatomic_get_and_inc(&np->disk_total, NULL);
        if (ret)
                GOTO(err_ni, ret);

        DINFO("disk "DISKID_FORMAT" add to node %s\n",
               DISKID_ARG(diskid), nodeid);

        return 0;

err_ni:
        (void ) yfree((void **) &ni);
err_di:
        (void) yfree((void **) &disk_head);
err_ret:
        return ret;
}

static float __get_diff(struct node_info *ni)
{
        struct node_pool *np;
        float di, dit, ch, cht, ret;

        np = &mds_info.nodepool_ssd;

        di  = (float) ni->disk_number.value;
        dit = (float) np->disk_total.value;
        ch  = (float) ni->choosed;
        cht = (float) np->choosed_total;

        if (np->disk_total.value == 0 || np->choosed_total == 0)
                ret = 0.0;
        else
                ret = di / dit - ch / cht;

        return  ret;
}

static void *__get_the_biggest(struct list_head *list)
{
        struct node_info *ni_tmp;
        struct node_info *biggest;
        struct list_head *pos;
        struct list_head *q;

        biggest = NULL;

        list_for_each_safe(pos, q, list) {
                ni_tmp = list_entry(pos, struct node_info, list);
                if (ni_tmp->tmp_used)
                        continue;

                if (biggest == NULL)
                        biggest = ni_tmp;
                else if (__get_diff(biggest) < __get_diff(ni_tmp)) {
                        biggest = ni_tmp;
                        break;
                }
        }

        if (biggest == NULL && !list_empty(list)) {
                biggest = list_entry(list->next, struct node_info, list);

                list_move_tail(list->next, list);
        }

        biggest->tmp_used = 1;

        return (void *) biggest;
}

static int nodepool_reset_ssd(void)
{
        struct list_head *pos;
        struct list_head *q;
        struct node_pool *np;
        struct node_info *ni_tmp;

        np = &mds_info.nodepool_ssd;

        np->choosed_total = 0;

        list_for_each_safe(pos, q, &np->nodelist) {
                ni_tmp = list_entry(pos, struct node_info, list);
                ni_tmp->choosed = 0;
        }

        return 0;
}

/*
 * 在没空间时，之前加过重试，但是会导致没空间时，创建数据块性能下降。
 * 所以这里先关掉重试。重试应该放在最外层才合适？ */
int nodepool_get_ssd(uint32_t reps, net_handle_t *disks, int hardend)
{
        int ret, dup;
        uint32_t i, j;
        struct node_pool *np;
        disk_head_t *tmp;
        struct node_info *ni[YFS_CHK_REP_MAX];
        static int alert = 0;

        YASSERT(reps <= YFS_CHK_REP_MAX);

        np = &mds_info.nodepool_ssd;

        // nodepool_hash_print();

        if ((uint32_t)np->disk_total.value < reps) {
                DBUG("disk total:%llu, but reps: %u\n", (LLU)np->disk_total.value, reps);
                ret = ENOSPC;
                goto err_ret;
        }

        /* @res nodelist_lock */
        ret = sy_rwlock_wrlock(&np->nodelist_rwlock);
        if (ret)
                GOTO(err_ret, ret);

        if (hardend) {
                if (np->node_total < reps) {
                        if (alert % 10240 == 0)
                                DWARN("node total:%llu, but reps: %u\n",
                                      (LLU)np->node_total, reps);

                        alert++;

                        ret = ENOSPC;
                        goto err_nodelist_lock;
                }
        }

        if (np->choosed_total > NODEPOOL_MAX_CHOOSED) {
                ret = nodepool_reset_ssd();
                if (ret)
                        GOTO(err_nodelist_lock, ret);

                DINFO("chunk choosed > %d, reset it\n", NODEPOOL_MAX_CHOOSED);
        }

        i = 0;
        while (i < reps) {
                ni[i] = (struct node_info *) __get_the_biggest(&np->nodelist);

                ret = sy_rwlock_wrlock(&ni[i]->disk_rwlock);
                if (ret)
                        GOTO(err_nodelist_lock, ret);

                tmp = list_entry(ni[i]->disk_list.next, disk_head_t, list);
                list_move_tail(ni[i]->disk_list.next, &ni[i]->disk_list);

                sy_rwlock_unlock(&ni[i]->disk_rwlock);

                if (ni[i]->tmp_used) {
                        dup = 0;
                        for (j = 0; j < i; j++) {
                                if (nid_cmp(&disks[j].u.nid, &tmp->diskid) == 0) {
                                        DBUG("dup disk "DISKID_FORMAT" %u\n",
                                             DISKID_ARG(&tmp->diskid), i);

                                        dup = 1;
                                        break;
                                }
                        }

                        if (dup) {
                                ni[i]->choosed++;
                                np->choosed_total++;

                                continue;
                        }
                }

                disks[i].u.nid = tmp->diskid;
                disks[i].type = NET_HANDLE_PERSISTENT;

                ni[i]->choosed++;
                np->choosed_total++;
                i++;
        }

#ifdef YFS_DEBUG
        char nodeids[MAX_PATH_LEN] = {0};
        for (i = 0; i < reps; i++) {
                ni[i]->tmp_used = 0;
                strcat(nodeids, ni[i]->name);
                strcat(nodeids, "\t");
        }
        DBUG("nodeids %s\n", nodeids);
#else
        for (i = 0; i < reps; i++)
                ni[i]->tmp_used = 0;
#endif

        sy_rwlock_unlock(&np->nodelist_rwlock);

        return 0;

err_nodelist_lock:
        sy_rwlock_unlock(&np->nodelist_rwlock);
err_ret:
        return ret;
}

/**
 * @note useless now.
 * delete diskid in node_pool, condition: dead or overload etc.
 */
int nodepool_nodedead_ssd(diskid_t *diskid, uuid_t *nodeid)
{
        int ret;
        struct node_pool *np;
        struct node_info *ni;
        char _nodeid[MAX_NAME_LEN];
        disk_head_t *tmp;
        struct node_info *ni_tmp;
        struct list_head *pos;
        struct list_head *q;

        np = &mds_info.nodepool_ssd;

        _memset(_nodeid, 0x0, MAX_NAME_LEN);
        (void) uuid_unparse(*nodeid, _nodeid);
        if (nodeid[0] == '\0') {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        ret = sy_rwlock_wrlock(&np->ht_nodelist_rwlock);
        if (ret)
                GOTO(err_ret, ret);

        ni = hash_table_find(np->ht_nodelist, (void *)_nodeid);

        sy_rwlock_unlock(&np->ht_nodelist_rwlock);

        if (ni == NULL) {
                DERROR("Not reported disk "DISKID_FORMAT" (%s) dead\n",
                                DISKID_ARG(diskid), _nodeid);
                GOTO(err_ret, ret);
        }

        ret = sy_rwlock_wrlock(&ni->disk_rwlock);
        if (ret)
                GOTO(err_ret, ret);

        list_for_each_safe(pos, q, &ni->disk_list) {
                tmp = list_entry(pos, disk_head_t, list);

                if (tmp->diskid.id == diskid->id ) {

                        list_del(pos);
                        yfree((void **)&tmp);

                        DINFO("disk "DISKID_FORMAT" on %s dead\n",
                                        DISKID_ARG(diskid), _nodeid);

                        ret = yatomic_get_and_dec(&np->disk_total, NULL);
                        if (ret)
                                GOTO(err_disk_lock, ret);

                        ret = yatomic_get_and_dec(&ni->disk_number, NULL);
                        if (ret)
                                GOTO(err_disk_lock, ret);

                        break;
                }
        }

        sy_rwlock_unlock(&ni->disk_rwlock);

        // Delete the node if no disk on it
        if (ni->disk_number.value == 0) {
                DINFO("No disk on node %s, delete it\n", ni->name);

                ret = sy_rwlock_wrlock(&np->ht_nodelist_rwlock);
                if (ret)
                        GOTO(err_ret, ret);

                ret = hash_table_remove(np->ht_nodelist, (void *)ni->name, NULL);

                if (ret) {
                        sy_rwlock_unlock(&np->ht_nodelist_rwlock);
                        GOTO(err_ret, ret);
                }

                sy_rwlock_unlock(&np->ht_nodelist_rwlock);

                ret = sy_rwlock_wrlock(&np->nodelist_rwlock);
                if (ret)
                        GOTO(err_ret, ret);

                list_for_each_safe(pos, q, &np->nodelist) {
                        ni_tmp = list_entry(pos, struct node_info, list);

                        if (_strcmp(ni->name, ni_tmp->name) == 0) {
                                DINFO("delete node %s\n", ni->name);

                                list_del(pos);
                                yfree((void **)&ni_tmp);
                        }
                }

                sy_rwlock_unlock(&np->nodelist_rwlock);
        }

        return 0;

err_disk_lock:
        sy_rwlock_unlock(&ni->disk_rwlock);

err_ret:
        return ret;
}

int nodepool_diskdead_ssd(const diskid_t *diskid)
{
        int ret;
        struct list_head *q_i;
        struct list_head *q_j;
        struct list_head *pos_i;
        struct list_head *pos_j;
        struct node_pool *np;
        struct node_info *ni_tmp;
        disk_head_t *di_tmp;

        np = &mds_info.nodepool_ssd;

        ret = sy_rwlock_wrlock(&np->nodelist_rwlock);
        if (ret)
                GOTO(err_ret, ret);

        list_for_each_safe(pos_i, q_i, &np->nodelist) {
                ni_tmp = list_entry(pos_i, struct node_info, list);

                ret = sy_rwlock_wrlock(&ni_tmp->disk_rwlock);
                if (ret)
                        GOTO(err_nodelist_lock, ret);

                list_for_each_safe(pos_j, q_j, &ni_tmp->disk_list) {
                        di_tmp = list_entry(pos_j, disk_head_t, list);

                        if (di_tmp->diskid.id == diskid->id ) {

                                list_del(pos_j);
                                yfree((void **)&di_tmp);

                                DINFO("disk "DISKID_FORMAT" name:(%s) on %s dead\n",
                                      DISKID_ARG(diskid), netable_rname_nid(diskid), ni_tmp->name);

                                ret = yatomic_get_and_dec(&ni_tmp->disk_number, NULL);
                                if (ret)
                                        GOTO(err_disk_lock, ret);

                                ret = yatomic_get_and_dec(&np->disk_total, NULL);
                                if (ret)
                                        GOTO(err_disk_lock, ret);

                                break;
                        }
                }

                sy_rwlock_unlock(&ni_tmp->disk_rwlock);

                if (ni_tmp->disk_number.value == 0) {
                        DINFO("No disk on node %s, delete it\n", ni_tmp->name);

                        ret = sy_rwlock_wrlock(&np->ht_nodelist_rwlock);
                        if (ret)
                                GOTO(err_nodelist_lock, ret);

                        ret = hash_table_remove(np->ht_nodelist, (void *)ni_tmp->name, NULL);
                        if (ret) {
                                sy_rwlock_unlock(&np->ht_nodelist_rwlock);
                                GOTO(err_nodelist_lock, ret);
                        }

                        np->node_total--;

                        sy_rwlock_unlock(&np->ht_nodelist_rwlock);

                        list_del(pos_i);
                        yfree((void **)&ni_tmp);
                }
        }

        sy_rwlock_unlock(&np->nodelist_rwlock);

        return 0;
err_disk_lock:
        sy_rwlock_unlock(&ni_tmp->disk_rwlock);
err_nodelist_lock:
        sy_rwlock_unlock(&np->nodelist_rwlock);
err_ret:
        return ret;
}

/*
*Date   : 2017.04.18
*Author : Yang
*nodepool_get_node_num_ssd : get ssd node number of NAS
*/
int nodepool_get_node_num_ssd(uint32_t *node_num)
{
        int ret;

        YASSERT(NULL != node_num);
        struct node_pool *np = &mds_info.nodepool_ssd;
        ret = sy_rwlock_rdlock(&np->nodelist_rwlock);
        if (ret)
                return ret;

        if (gloconf.testing) {
                //for python test/test.py
                *node_num = np->disk_total.value;
        } else {
                *node_num = np->node_total;
        }

        sy_rwlock_unlock(&np->nodelist_rwlock);
        return 0;
}

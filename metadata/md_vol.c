#include <sys/types.h>
#include <dirent.h>
#include <string.h>

#define DBG_SUBSYS S_YFSMDC

#include "align.h"
#include "net_global.h"
#include "job_dock.h"
#include "ynet_rpc.h"
#include "ylib.h"
#include "md_proto.h"
#include "md_lib.h"
#include "redis.h"
#include "dir.h"
#include "md.h"
#include "md_db.h"
#include "quota.h"
#include "schedule.h"
#include "redis_conn.h"
#include "sdfs_quota.h"
#include "dbg.h"


#if 1

typedef struct {
        char name[MAX_NAME_LEN];
        int port;
} redis_addr_t;

static dirop_t *dirop = &__dirop__;
static inodeop_t *inodeop = &__inodeop__;

static int __md_mkvol_slot(const char *name, int sharding, int replica, const redis_addr_t *addr)
{
        int ret, i, j, k;
        char key[MAX_PATH_LEN], value[MAX_BUF_LEN];

        k = 0;
        for (i = 0; i < sharding; i++) {
                for (j = 0; j < replica; j++) {
                        k = i * replica + j;
                        snprintf(key, MAX_NAME_LEN, "%s/wait/%d/redis/%d.wait", name, i, j);
                        snprintf(value, MAX_NAME_LEN, "%s,%d", addr[k].name, addr[k].port);

                        DINFO("set %s %s\n", key, value);
                        ret = etcd_create_text(ETCD_VOLUME, key, value, -1);
                        if (ret) {
                                if (ret == EEXIST) {
                                        continue;
                                } else {
                                        GOTO(err_ret, ret);
                                }
                        }
                }
        }

        return 0;
err_ret:
        return ret;
}

static int __md_mkvol_online(const char *name, int disk)
{
        int ret, instence;
        char key[MAX_PATH_LEN], value[MAX_BUF_LEN];
        
        snprintf(key, MAX_NAME_LEN, "%s/disk/%d/instence", name, disk);
        ret = etcd_get_text(ETCD_REDIS, key, value, NULL);
        if (ret)
                return 0;

        instence = atoi(value);
        if (instence >= 64) {
                DINFO("skip disk %s:%d\n", name, disk);
                return 0;
        }

        snprintf(key, MAX_NAME_LEN, "%s/disk/%d/trigger", name, disk);
        ret = etcd_update_text(ETCD_REDIS, key, "1", NULL, -1);
        if (ret)
                return 0;

        int retry = 0;
        while (1) {
                ret = etcd_get_text(ETCD_REDIS, key, value, NULL);
                if (ret)
                        return 0;

                if (atoi(value) != 0) {
                        DBUG("disk %s:%d, trigger %s, retry %u\n", name, disk, value, retry);

                        if (retry > 100) {
                                DWARN("disk %s:%d, trigger %s not online\n", name, disk, value);
                                return 0;
                        } else {
                                retry++;
                                usleep(100 * 1000);
                        }
                } else {
                        break;
                }
        }
        
        return 1;
}

typedef struct {
        struct list_head hook;
        char name[MAX_NAME_LEN];
        int count;
        int disk[0];
} redis_list_t;


static int __md_mkvol_getredis_disk(const char *hostname, int *disk_array, int *_count)
{
        int ret;
        etcd_node_t *array, *node;
        char key[MAX_PATH_LEN];

        snprintf(key, MAX_NAME_LEN, "%s/%s/disk", ETCD_REDIS, hostname);
        ret = etcd_list(key, &array);
        if (ret)
                GOTO(err_ret, ret);

        int count = 0, disk;
        for (int i = 0; i < array->num_node; i++) {
                node = array->nodes[i];

                DBUG("disk[%s], total %u\n", node->key, node->value, array->num_node);
                disk = atoi(node->key);
                if (__md_mkvol_online(hostname, disk)) {
                        disk_array[count] = disk;
                        count++;
                }
        }

        *_count = count;

        free_etcd_node(array);

        if (count == 0) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}

static int __md_mkvol_getredis(struct list_head *list, int *list_count)
{
        int ret, i;
        etcd_node_t *array, *node;
        int disk[128], count;
        redis_list_t *ent;

        ret = etcd_list(ETCD_REDIS, &array);
        if (ret)
                GOTO(err_ret, ret);

        for (i = 0; i < array->num_node; i++) {
                node = array->nodes[i];

                DBUG("key %s value %s\n", node->key, node->value);
                ret = __md_mkvol_getredis_disk(node->key, disk, &count);
                if (ret) {
                        if (ret == ENONET) {
                                continue;
                        } else
                                GOTO(err_free, ret);
                }

                ret = ymalloc((void *)&ent, sizeof(*ent) + sizeof(int) * count);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);
        
                memcpy(ent->disk, disk, sizeof(int) * count);
                strcpy(ent->name, node->key);
                ent->count = count;

                list_add_tail(&ent->hook, list);
        }

        *list_count = array->num_node;

        free_etcd_node(array);
        
        return 0;
err_free:
        free_etcd_node(array);
err_ret:
        return ret;
}

static int __md_mkvol_trigger(struct list_head *list)
{
        int ret;
        char key[MAX_PATH_LEN];
        redis_list_t *ent;
        struct list_head *pos;

        list_for_each(pos, list) {
                ent = (void *)pos;

                for (int i = 0; i < ent->count; i++) {
                        snprintf(key, MAX_NAME_LEN, "%s/disk/%d/trigger", ent->name, i);
                        ret = etcd_update_text(ETCD_REDIS, key, "1", NULL, -1);
                        if (ret)
                                GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

static int __md_mkvol_getredis_solo(struct list_head *_list, int list_count,
                                    int replica, redis_addr_t *addr)
{
        int ret;
        redis_list_t *ent;

        YASSERT(gloconf.solomode);
        YASSERT(list_count == 1);

        ent = (void *)_list->next;

        if (ent->count < replica) {
                ret = ENOSPC;
                GOTO(err_ret, ret);
        }

        int idx = _random();
        for (int i = 0; i < replica; i++) {
                strcpy(addr[i].name, ent->name);
                addr[i].port = ent->disk[(i + idx) % ent->count];
        }

        return 0;
err_ret:
        return ret;
}


static int __md_mkvol_getredis_replica(const char *name, int idx, struct list_head *_list, int list_count,
                                       int replica, redis_addr_t *addr)
{
        int ret;
        struct list_head list, *pos, *n;
        redis_list_t *ent;

        if (gloconf.solomode && list_count == 1) {
                return __md_mkvol_getredis_solo(_list, list_count, replica, addr);
        } else if (replica > list_count) {
                ret = ENOSPC;
                GOTO(err_ret, ret);
        }

        INIT_LIST_HEAD(&list);

        int i = 0;
        list_for_each_safe(pos, n, _list) {
                ent = (void *)pos;
                list_del(pos);
                list_add_tail(&ent->hook, &list);

                strcpy(addr[i].name, ent->name);
                addr[i].port = ent->disk[_random() % ent->count];
 
                DINFO("volume %s sharding[%u] replica[%d], node %s:%u, total %u\n",
                      name, idx, i, addr[i].name, addr[i].port, ent->count);
                i++;
                if (i >= replica) {
                        break;
                }
        }

        YASSERT(i == replica);
        list_splice_tail_init(&list, _list);
        
        return 0;
err_ret:
        return ret;
}

inline static int __md_mkvol_set_redis(const char *name, int sharding, int replica)
{
        int ret, count;
        struct list_head list;
        redis_addr_t *addr;
        struct list_head *pos, *n;

        INIT_LIST_HEAD(&list);
        ret = __md_mkvol_getredis(&list, &count);
        if (ret)
                GOTO(err_ret, ret);

        ret = ymalloc((void *)&addr, sizeof(*addr) * sharding * replica);
        if (ret)
                UNIMPLEMENTED(__DUMP__);


        int idx = 0;
        for (int i = 0; i < mdsconf.redis_sharding; i++) {
                ret = __md_mkvol_getredis_replica(name, i, &list, count, replica, &addr[idx]);
                if (ret)
                        GOTO(err_free, ret);
                
                idx += replica;
        }

        ret = __md_mkvol_slot(name, sharding, replica, addr);
        if (ret)
                GOTO(err_free, ret);

        ret = __md_mkvol_trigger(&list);
        if (ret)
                GOTO(err_free, ret);
        
        list_for_each_safe(pos, n, &list) {
                list_del(pos);
                yfree((void **)&pos);
        }

        yfree((void **)&addr);
        
        return 0;
err_free:
        list_for_each_safe(pos, n, &list) {
                list_del(pos);
                yfree((void **)&pos);
        }

        yfree((void **)&addr);
err_ret:
        return ret;
}


#endif
        
int md_mkvol(const char *name, const setattr_t *setattr, fileid_t *_fileid)
{
        int ret;
        fileid_t fileid;
        char key[MAX_PATH_LEN], value[MAX_BUF_LEN];
        uint64_t volid;

        snprintf(key, MAX_NAME_LEN, "%s/sharding", name);
        snprintf(value, MAX_NAME_LEN, "%d", mdsconf.redis_sharding);
        ret = etcd_create_text(ETCD_VOLUME, key, value, -1);
        if (ret) {
                if (ret == EEXIST) {
                        //pass
                } else
                        GOTO(err_ret, ret);
        }

        snprintf(key, MAX_NAME_LEN, "%s/replica", name);
        snprintf(value, MAX_NAME_LEN, "%d", mdsconf.redis_replica);
        ret = etcd_create_text(ETCD_VOLUME, key, value, -1);
        if (ret) {
                if (ret == EEXIST) {
                        //pass
                } else
                        GOTO(err_ret, ret);
        }

        ret = md_newid(idtype_fileid, &volid);
        if (ret)
                GOTO(err_ret, ret);

        snprintf(key, MAX_NAME_LEN, "%s/volid", name);
        snprintf(value, MAX_NAME_LEN, "%ju", volid);
        ret = etcd_create_text(ETCD_VOLUME, key, value, -1);
        if (ret) {
                if (ret == EEXIST) {
                        ret = etcd_get_text(ETCD_VOLUME, key, value, NULL);
                        if (ret)
                                GOTO(err_ret, ret);

                        volid = atol(value);
                } else
                        GOTO(err_ret, ret);
        }

        ret = __md_mkvol_set_redis(name, mdsconf.redis_sharding, mdsconf.redis_replica);
        if (ret)
                GOTO(err_ret, ret);
        
#if 0
        (void) parent;
        (void) setattr;
        (void) _fileid;
        (void) fileid;
#endif

        int retry = 0;
retry:
        ret = redis_conn_vol(volid);
        if (ret) {
                USLEEP_RETRY(err_ret, ret, retry, retry, 30, (1000 * 1000));
        }
        
        ret = inodeop->mkvol(setattr, &volid, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        snprintf(key, MAX_NAME_LEN, "%s/id", name);
        ret = etcd_create(ETCD_VOLUME, key, &fileid, sizeof(fileid), -1);
        if (ret)
                GOTO(err_ret, ret);
        
        if (_fileid) {
                *_fileid = fileid;
        }
        
        return 0;
err_ret:
        return ret;
}

int md_lookupvol(const char *name, fileid_t *fileid)
{
        int ret, size = sizeof(*fileid);
        char key[MAX_PATH_LEN];

        snprintf(key, MAX_NAME_LEN, "%s/id", name);
        
        ret = etcd_get_bin(ETCD_VOLUME, key, fileid, &size, NULL);
        if (ret)
                GOTO(err_ret, ret);

        YASSERT(size == sizeof(*fileid));

        return 0;
err_ret:
        ret = (ret == ENOKEY) ? ENOENT : ret;
        return ret;
}

int md_dirlist(const dirid_t *dirid, uint32_t count, uint64_t offset, dirlist_t **dirlist)
{
        return dirop->dirlist(dirid, count, offset, dirlist);
}

static int __md_rmvol_inode(const char *name)
{
        int ret;
        fileid_t fileid;
        uint64_t count;

        ret = md_lookupvol(name, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        //rmove inode
        ret = inodeop->childcount(&fileid, &count);
        if (ret) {
                if (ret == ENOENT) {//already removed
                        //pass
                } else
                        GOTO(err_ret, ret);
        } else {
                if (count) {
                        ret = ENOTEMPTY;
                        GOTO(err_ret, ret);
                }

                ret = inodeop->unlink(&fileid, NULL);
                if (ret) {
                        if (ret == ENOENT) {
                                DWARN(CHKID_FORMAT" not found\n", CHKID_ARG(&fileid));
                        } else
                                GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

static int __md_rmvol_config(const char *name, int *sharding, int *replica)
{
        int ret;
        char key[MAX_PATH_LEN], value[MAX_BUF_LEN], bak[MAX_BUF_LEN];

retry1:
        snprintf(bak, MAX_NAME_LEN, "%s/sharding.bak", name);
        ret = etcd_get_text(ETCD_VOLUME, bak, value, NULL);
        if (ret) {
                if (ret == ENOKEY) {
                        snprintf(key, MAX_NAME_LEN, "%s/sharding", name); 
                        ret = etcd_get_text(ETCD_VOLUME, key, value, NULL);
                        if (ret)
                                GOTO(err_ret, ret);

                        ret = etcd_create_text(ETCD_VOLUME, bak, value, -1);
                        if (ret)
                                GOTO(err_ret, ret);

                        goto retry1;
                } else
                        GOTO(err_ret, ret);
        }

        *sharding = atoi(value);
        DINFO("%s sharding %u\n", name, *sharding);
        
retry2:
        snprintf(bak, MAX_NAME_LEN, "%s/replica.bak", name);
        ret = etcd_get_text(ETCD_VOLUME, bak, value, NULL);
        if (ret) {
                if (ret == ENOKEY) {
                        snprintf(key, MAX_NAME_LEN, "%s/replica", name); 
                        ret = etcd_get_text(ETCD_VOLUME, key, value, NULL);
                        if (ret)
                                GOTO(err_ret, ret);

                        ret = etcd_create_text(ETCD_VOLUME, bak, value, -1);
                        if (ret)
                                GOTO(err_ret, ret);

                        goto retry2;
                } else
                        GOTO(err_ret, ret);
        }

        *replica = atoi(value);
        DINFO("%s replica %u\n", name, *replica);
        
        snprintf(key, MAX_NAME_LEN, "%s/sharding", name);
        etcd_del(ETCD_VOLUME, key);

        snprintf(key, MAX_NAME_LEN, "%s/replica", name);
        etcd_del(ETCD_VOLUME, key);

        snprintf(key, MAX_NAME_LEN, "%s/volid", name);
        etcd_del(ETCD_VOLUME, key);
        
        snprintf(key, MAX_NAME_LEN, "%s/id", name);
        etcd_del(ETCD_VOLUME, key);
        
        return 0;
err_ret:
        ret = (ret == ENOKEY) ? ENOENT : ret;
        return ret;
}

static int __md_rmvol_sharding(const char *name, int slot, int replica)
{
        int ret, i, retry;
        char key[MAX_PATH_LEN], value[MAX_BUF_LEN];

        for (i = 0; i < replica; i++) {
                snprintf(key, MAX_NAME_LEN, "%s/slot/%u/redis/%u", name, slot, i);

                retry = 0;
        retry:
                ret = etcd_get_text(ETCD_VOLUME, key, value, NULL);
                if (ret) {
                        if (ret == ENOKEY) {
                                //redis exited
                                DINFO("redis %s existed\n", key);
                                continue;
                        } else
                                GOTO(err_ret, ret);
                }

                if (retry < 10) {
                        sleep(1);
                        retry++;
                        goto retry;
                } else {
                        DWARN("wait redis %s exit fail, force remove it\n", key);
                }
        }

        snprintf(key, MAX_NAME_LEN, "%s/slot/%u", name, slot);
        ret = etcd_del_dir(ETCD_VOLUME, key, 1);
        if (ret) {
                if (ret == ENOKEY) {
                        //pass
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __md_rmvol_cleanup(const char *name)
{
        int ret;
        char key[MAX_PATH_LEN];
        
        snprintf(key, MAX_NAME_LEN, "%s/slot", name);
        ret = etcd_del_dir(ETCD_VOLUME, key, 0);
        if (ret) {
                if (ret == ENOKEY) {
                        //pass
                } else
                        GOTO(err_ret, ret);
        }
        
        snprintf(key, MAX_NAME_LEN, "%s/sharding.bak", name);
        etcd_del(ETCD_VOLUME, key);

        snprintf(key, MAX_NAME_LEN, "%s/replica.bak", name);
        etcd_del(ETCD_VOLUME, key);

        snprintf(key, MAX_NAME_LEN, "%s", name);
        ret = etcd_del_dir(ETCD_VOLUME, key, 0);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        ret = (ret == ENOKEY) ? ENOENT : ret;
        return ret;
}

int md_rmvol(const char *name)
{
        int ret, sharding, replica, i;

        ret = __md_rmvol_inode(name);
        if (ret)
                GOTO(err_ret, ret);

        ret = __md_rmvol_config(name, &sharding, &replica);
        if (ret)
                GOTO(err_ret, ret);

        for (i = 0; i < sharding; i++) {
                __md_rmvol_sharding(name, i, replica);
        }

        ret = __md_rmvol_cleanup(name);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

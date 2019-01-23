

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSMDS

#include "chk_proto.h"
#include "disk_proto.h"
#include "diskpool.h"
#include "mds.h"
#include "md_proto.h"
#include "node_proto.h"
#include "ylib.h"
#include "yfscli_conf.h"
#include "yfsmds_conf.h"
#include "ynet_rpc.h"
#include "net_global.h"
#include "network.h"
#include "bmap.h"
#include "dbg.h"

#define YFS_MDS_DISKPOOL_GROUP_DEF 9
#define YFS_MDS_CHKPOOL_GROUP_DEF 9

#define YFS_MDS_DIR_DISKID_PRE "/disk"

int diskpool_stat_del(diskinfo_stat_t *stat, diskinfo_stat_t *old);

int __diskpool_switch_dead(struct disk_pool *dp, struct disk_info *di,
                           disk_stat_t oldstat, disk_stat_t newstat, int grp)
{
        int ret;

        (void) newstat;

        ret = sy_rwlock_wrlock(&dp->stat_rwlock[oldstat][grp]);
        if (ret)
                GOTO(err_ret, ret);

        list_del_init(&di->list);

        sy_rwlock_unlock(&dp->stat_rwlock[oldstat][grp]);

        ret = sy_rwlock_wrlock(&dp->dead_rwlock);
        if (ret)
                GOTO(err_ret, ret);

        list_add(&di->list, &dp->dead_list);

        sy_rwlock_unlock(&dp->dead_rwlock);

        return 0;
err_ret:
        return ret;
}

int __diskpool_switch_recover(struct disk_pool *dp, struct disk_info *di,
                              disk_stat_t oldstat, disk_stat_t newstat, int grp)
{
        int ret;

        DBUG("stat old %d, new %d\n", oldstat, newstat);

        ret = sy_rwlock_wrlock(&dp->dead_rwlock);
        if (ret)
                GOTO(err_ret, ret);

        list_del_init(&di->list);

        sy_rwlock_unlock(&dp->dead_rwlock);

        ret = sy_rwlock_wrlock(&dp->stat_rwlock[newstat][grp]);
        if (ret)
                GOTO(err_ret, ret);

        list_add(&di->list, &dp->stat_list[newstat][grp]);

        sy_rwlock_unlock(&dp->stat_rwlock[newstat][grp]);

        return 0;
err_ret:
        return ret;
}

int __diskpool_kill(struct disk_info *di)
{
        int ret, grp;
        struct disk_pool *dp;
        disk_stat_t oldstat, newstat;

        dp = &mds_info.diskpool;

        grp = di->diskid.id % dp->group;

        ret = sy_rwlock_wrlock(&di->rwlock);
        if (ret)
                GOTO(err_ret, ret);

        oldstat = newstat = di->diskstat;

        if (oldstat != DISK_STAT_DEAD) {
                diskpool_stat_del(&dp->disk_info[grp], &di->diskinfo);

                di->diskstat = DISK_STAT_DEAD;
                di->dead_time = time(NULL);

                newstat = di->diskstat;
        }

        netable_put(&di->nh, "__diskpool_kill");

        _memset(&di->nh, 0x0, sizeof(net_handle_t));


        DBUG("stat old %d, new %d\n", oldstat, newstat);

        if (newstat != oldstat) {
                ret = __diskpool_switch_dead(dp, di, oldstat, newstat, grp);
                if (ret)
                        GOTO(err_lock, ret);

                ret = nodepool_diskdead(&di->diskid, TIER_ALL);
                if (ret)
                        GOTO(err_lock, ret);
        }

        sy_rwlock_unlock(&di->rwlock);

        return 0;
err_lock:
        sy_rwlock_unlock(&di->rwlock);
err_ret:
        return ret;
}

static void __diskpool_checkinfo(void *_node)
{
        /*uint32_t i;*/
        struct disk_info *di;
        /*volrept_t *rept;*/

        di = _node;

        if (di->diskstat == DISK_STAT_DEAD)
                return;

        if (di->lasthb_time == -1) {
                DBUG("cds %s still in rept just skip\n", netable_rname(&di->nh));

                return;
        }

        if (di->volrept) {
                /*for (i = 0; i < di->volreptnum; i++) {*/
                        /*rept = &di->volrept[i];*/
                        /*(void) lvs_stat_update(rept->volid, rept->size);*/

                        /*DBUG("volume %u size %llu\n", (uint32_t)rept->volid, (LLU)rept->size);*/
                /*}*/
        }

#if 0
        if (now - di->lasthb_time > gloconf.hb_interval) {
                DWARN("disk %s id %llu_v%u, timeout %lu, assumed died\n",
                      netable_rname(&di->nh), (LLU)di->diskid.id,
                      di->diskid.version, now - di->lasthb_time);
                __diskpool_kill(di);
        }
#endif
}

static int __diskpool_update_volume(struct disk_info *di, const volrept_t *volrept,
                                    uint32_t volreptnum)
{
        int ret;

#if 0
        uint32_t i;

        for (i = 0; i < volreptnum; i++) {
                DINFO("volume %u size %llu\n", (uint32_t)volrept[i].volid, (LLU)volrept[i].size);
        }
#endif

        if (di->volreptnum != volreptnum) {
                ret = yrealloc((void **)&di->volrept, sizeof(volrept_t) * di->volreptnum,
                               sizeof(volrept_t) * volreptnum);
                if (ret)
                        GOTO(err_ret, ret);

                di->volreptnum = volreptnum;
        }

        memcpy(di->volrept, volrept, sizeof(volrept_t) * volreptnum);

        return 0;
err_ret:
        return ret;
}

static void *__diskpool_watcher(void *nil)
{
        int ret, i;
        struct disk_pool *dp;

        (void) nil;

        dp = &mds_info.diskpool;

        while (srv_running) {
                for (i = 0; i < dp->group; i++) {
                        ret = sy_rwlock_wrlock(&dp->disk_rwlock[i]);
                        if (ret)
                                DERROR("lock error")

                        skiplist_iterate(dp->disk_list[i], __diskpool_checkinfo);

                        sy_rwlock_unlock(&dp->disk_rwlock[i]);
                }

                /*lvs_stat_sync();*/

                sleep(gloconf.rpc_timeout);
        }

        return NULL;
}

disk_stat_t getdiskstat(const diskinfo_stat_t *diskstat)
{
        if (DISKFREE(diskstat))
                return DISK_STAT_FREE;
        else
                return DISKSTAT(diskstat);
}

int diskpool_stat_add(diskinfo_stat_t *stat, diskinfo_stat_t *new)
{
        if (stat->ds_bsize == 0)
                stat->ds_bsize = new->ds_bsize;

        if (stat->ds_frsize == 0)
                stat->ds_frsize = new->ds_frsize;

        if (new->ds_frsize == stat->ds_frsize)
                stat->ds_blocks += new->ds_blocks;
        else
                stat->ds_blocks += (new->ds_blocks * new->ds_frsize)
                                   / stat->ds_frsize;

        if (new->ds_bsize == stat->ds_bsize) {
                stat->ds_bfree += new->ds_bfree;
                stat->ds_bavail += new->ds_bavail;
        } else {
                stat->ds_bfree += ( new->ds_bfree * new->ds_bsize)
                                  / stat->ds_bsize;
                stat->ds_bavail += (new->ds_bavail * new->ds_bsize)
                                   / stat->ds_bsize;
        }

        stat->ds_files += new->ds_files;
        stat->ds_ffree += new->ds_ffree;
        stat->ds_favail += new->ds_favail;
        /* stat->ds_fsid = 0; */
        /* stat->ds_flag = 0; */
        if (new->ds_namemax < stat->ds_namemax)
                stat->ds_namemax = new->ds_namemax;

        return 0;
}

int diskpool_stat_del(diskinfo_stat_t *stat, diskinfo_stat_t *old)
{
        if (stat->ds_bsize == 0 || stat->ds_frsize == 0) {
                DBUG("ds_bsize (%u) / ds_frsize (%u) == 0\n",
                     stat->ds_bsize, stat->ds_frsize);
        }

        if (old->ds_frsize == stat->ds_frsize)
                stat->ds_blocks -= old->ds_blocks;
        else
                stat->ds_blocks -= (old->ds_blocks * old->ds_frsize)
                                   / stat->ds_frsize;

        if (old->ds_bsize == stat->ds_bsize) {
                stat->ds_bfree -= old->ds_bfree;
                stat->ds_bavail -= old->ds_bavail;
        } else {
                stat->ds_bfree -= (old->ds_bfree * old->ds_bsize)
                                  / stat->ds_bsize;
                stat->ds_bavail -= (old->ds_bavail * old->ds_bsize)
                                   / stat->ds_bsize;
        }

        stat->ds_files -= old->ds_files;
        stat->ds_ffree -= old->ds_ffree;
        stat->ds_favail -= old->ds_favail;

        return 0;
}

int diskpool_stat_update(diskinfo_stat_t *stat, const diskinfo_stat_diff_t *diff)
{
        if (stat->ds_bsize == 0 || stat->ds_frsize == 0) {
                DBUG("ds_bsize (%u) / ds_frsize (%u) == 0\n",
                     stat->ds_bsize, stat->ds_frsize);
        }

        if (diff->ds_bsize == stat->ds_bsize) {
                stat->ds_bfree += diff->ds_bfree;
                stat->ds_bavail += diff->ds_bavail;
        } else {
                stat->ds_bfree += ( diff->ds_bfree * diff->ds_bsize)
                        / stat->ds_bsize;
                stat->ds_bavail += (diff->ds_bavail * diff->ds_bsize)
                        / stat->ds_bsize;
        }

        if (stat->ds_bfree >= stat->ds_blocks
            || stat->ds_bavail >= stat->ds_blocks) {
                DBUG("ds_bfree (%llu) ds_bavail (%llu) >= ds_blocks (%llu)\n",
                     (LLU)stat->ds_bfree,
                     (LLU)stat->ds_bavail,
                     (LLU)stat->ds_blocks);
        }

        stat->ds_ffree += diff->ds_ffree;
        stat->ds_favail += diff->ds_favail;

        return 0;
}

int diskpool_destroy(struct disk_pool *dp, int group)
{
        int i, j;

        sy_rwlock_destroy(&dp->dead_rwlock);
        sem_destroy(&dp->dead_sem);

        if (dp->stat_list != NULL) {
                for (i = 0; i < dp->group; i++) {
                        for (j = 0; j < DISK_STAT_DEAD; j++)
                                sy_rwlock_destroy(&dp->stat_rwlock[j][i]);
                }

                for (i = group; i > 0; i--) {
                        (void) skiplist_destroy(dp->disk_list[i]);

                        sy_rwlock_destroy(&dp->disk_rwlock[i]);
                }

                yfree((void **)&dp->stat_list);
        }

        return 0;
}

int diskpool_init(struct disk_pool *dp)
{
        int ret, group, i, j;
        uint32_t len;
        void *ptr;
        pthread_t th;
        pthread_attr_t ta;

        mds_info.diskid_max = 0;
        ret = sy_spin_init(&mds_info.diskid_lock);
        if (ret)
                GOTO(err_ret, ret);

        yatomic_init(&mds_info.fileid,   FILEID_FROM);

        group = YFS_MDS_DISKPOOL_GROUP_DEF;

        len = (sizeof(struct list_head) * DISK_STAT_DEAD
               + sizeof(sy_rwlock_t) * DISK_STAT_DEAD
               + sizeof(struct skiplist *) + sizeof(diskinfo_stat_t)
               + sizeof(sy_rwlock_t)) * group;

        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_ret, ret);

        dp->maxlevel = SKIPLIST_MAX_LEVEL;
        dp->chunksize = SKIPLIST_CHKSIZE_DEF;
        dp->min.id = 0;
        dp->max.id = UINT32_MAX;

        dp->group = group;

        INIT_LIST_HEAD(&dp->dead_list);
        sy_rwlock_init(&dp->dead_rwlock, NULL);
        sem_init(&dp->dead_sem, 0, 0);

        for (j = 0; j < DISK_STAT_DEAD; j++) {
                dp->stat_list[j] = ptr;
                ptr += sizeof(struct list_head) * dp->group;
                dp->stat_rwlock[j] = ptr;
                ptr += sizeof(sy_rwlock_t) * dp->group;
        }

        dp->disk_list = ptr;
        ptr += sizeof(struct skiplist *) * dp->group;
        dp->disk_info = ptr;
        ptr += sizeof(diskinfo_stat_t) * dp->group;
        dp->disk_rwlock = ptr;

        for (j = 0; j < DISK_STAT_DEAD; j++) {
                for (i = 0; i < group; i++) {
                        INIT_LIST_HEAD(&dp->stat_list[j][i]);
                        sy_rwlock_init(&dp->stat_rwlock[j][i], NULL);
                }
        }

        for (i = 0; i < group; i++) {
                ret = skiplist_create(nid_void_cmp, dp->maxlevel, dp->chunksize,
                                      (void *)&dp->min, (void *)&dp->max,
                                      &dp->disk_list[i]);
                if (ret)
                        GOTO(err_list, ret);

                sy_rwlock_init(&dp->disk_rwlock[i], NULL);
        }

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta,PTHREAD_CREATE_DETACHED);

        ret = pthread_create(&th, &ta, __diskpool_watcher, NULL);
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return 0;
err_list:
        (void) diskpool_destroy(dp, i--);
err_ret:
        return ret;
}

int diskpool_newjoin(const diskinfo_stat_t *diskstat, net_handle_t *nh)
{
        int ret, grp, i;
        uint32_t len;
        struct disk_info *di;
        void *ptr;
        struct disk_pool *dp;
        disk_stat_t newstat;

        DINFO("disk %s newjoin: "DISKID_FORMAT"\n",
              netable_rname(nh), DISKID_ARG(&nh->u.nid));

        ret = sy_spin_lock(&mds_info.diskid_lock);
        if (ret)
                GOTO(err_ret, ret);

        if (mds_info.diskid_max <= nh->u.nid.id)
                mds_info.diskid_max = nh->u.nid.id + 1;

        ret = sy_spin_unlock(&mds_info.diskid_lock);
        if (ret)
                GOTO(err_ret, ret);

        len = sizeof(struct disk_info);

        ret = ymalloc((void **)&di, len);
        if (ret)
                GOTO(err_ret, ret);

        grp = YFS_MDS_CHKPOOL_GROUP_DEF;

        len = (sizeof(struct skiplist *) + sizeof(sy_rwlock_t)) * grp;

        ret = ymalloc((void **)&ptr, len);
        if (ret)
                GOTO(err_di, ret);

        di->chk_list = ptr;
        ptr += sizeof(struct skiplist *) * grp;
        di->chk_rwlock = ptr;

        di->maxlevel = SKIPLIST_MAX_LEVEL;
        di->chunksize = SKIPLIST_CHKSIZE_DEF;
        di->min.id = 0;
        di->min.volid = 0;
        di->max.id = UINT64_MAX;
        di->max.volid = UINT64_MAX;
        di->lasthb_time = -1;
        di->dead_time = 0;
        di->nh = *nh;
        di->volreptnum = 0;
        di->volrept = 0;

        di->group = 0;
        for (i = 0; i < grp; i++) {
                ret = skiplist_create(nid_void_cmp, di->maxlevel, di->chunksize,
                                      (void *)&di->min, (void *)&di->max,
                                      &di->chk_list[i]);
                if (ret)
                        GOTO(err_list, ret);

                sy_rwlock_init(&di->chk_rwlock[i], NULL);

                di->group++;
        }

        _memcpy(di->net, nh, NET_HANDLE_LEN);

        di->diskid = nh->u.nid;

        sy_rwlock_init(&di->rwlock, NULL);

        INIT_LIST_HEAD(&di->list);

        _memcpy(&di->diskinfo, diskstat, sizeof(diskinfo_stat_t));

        di->diskstat = getdiskstat(&di->diskinfo);
        newstat = di->diskstat;

        DBUG("diskstat bsize %u, bavail %llu, ffree %llu diskstat %d\n",
             di->diskinfo.ds_bsize, (LLU)di->diskinfo.ds_bavail,
             (LLU)di->diskinfo.ds_ffree, di->diskstat);

        dp = &mds_info.diskpool;

        grp = di->diskid.id % dp->group;

        ret = sy_rwlock_wrlock(&dp->disk_rwlock[grp]);
        if (ret)
                GOTO(err_list, ret);

        ret = skiplist_put(dp->disk_list[grp], (void *)&di->diskid, (void *)di);
        if (ret)
                GOTO(err_lock, ret);

        ret = sy_rwlock_wrlock(&dp->stat_rwlock[newstat][grp]);
        if (ret)
                GOTO(err_lock, ret);

        list_add(&di->list, &dp->stat_list[newstat][grp]);

        sy_rwlock_unlock(&dp->stat_rwlock[newstat][grp]);

        diskpool_stat_add(&dp->disk_info[grp], &di->diskinfo);

        sy_rwlock_unlock(&dp->disk_rwlock[grp]);

        return 0;
err_lock:
        sy_rwlock_unlock(&dp->disk_rwlock[grp]);
err_list:
        for (i = 0; i < di->group; i++) {
                (void) skiplist_destroy(di->chk_list[i]);
                sy_rwlock_destroy(&di->chk_rwlock[i]);
        }

        yfree((void **)&ptr);
err_di:
        yfree((void **)&di);
err_ret:
        return ret;
}

int diskpool_rejoin(const diskinfo_stat_t *diskstat, net_handle_t *nh)
{
        int ret, grp;
        struct disk_pool *dp;
        struct disk_info *di;
        disk_stat_t oldstat, newstat;

        dp = &mds_info.diskpool;

        grp = nh->u.nid.id % dp->group;

        ret = sy_rwlock_rdlock(&dp->disk_rwlock[grp]);
        if (ret)
                GOTO(err_ret, ret);

        ret = skiplist_get(dp->disk_list[grp], (void *)&nh->u.nid,
                           (void **)&di);
        if (ret == ENOENT) {    /* new disk, what's the tricky ? */
                sy_rwlock_unlock(&dp->disk_rwlock[grp]);

                ret = diskpool_newjoin(diskstat, nh);
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        }

        DINFO("disk %s rejoin: "DISKID_FORMAT"\n",
             netable_rname(nh), DISKID_ARG(&nh->u.nid));

        ret = sy_rwlock_wrlock(&di->rwlock);
        if (ret)
                GOTO(err_lock, ret);

        oldstat = di->diskstat;

        if (oldstat != DISK_STAT_DEAD) {
                sy_rwlock_unlock(&di->rwlock);
                ret = EAGAIN;
                DWARN("disk %s rejoin busy, retry it\n", netable_rname(nh));
                goto err_lock;
        }

        _memcpy(&di->diskinfo, diskstat, sizeof(diskinfo_stat_t));

        di->diskstat = getdiskstat(&di->diskinfo);
        di->lasthb_time = -1;
        di->dead_time = 0;
        di->nh = *nh;
        di->volreptnum = 0;
        di->volrept = 0;
        newstat = di->diskstat;

        sy_rwlock_unlock(&di->rwlock);

        if (newstat != oldstat) {
                ret = __diskpool_switch_recover(dp, di, oldstat, newstat, grp);
                if (ret)
                        GOTO(err_lock, ret);
        }

        diskpool_stat_add(&dp->disk_info[grp], &di->diskinfo);

        sy_rwlock_unlock(&dp->disk_rwlock[grp]);

out:
        return 0;
err_lock:
        sy_rwlock_unlock(&dp->disk_rwlock[grp]);
err_ret:
        return ret;
}

static inline int __build_diskpath(char *path, const diskid_t *diskid)
{
        char fpath[MAX_PATH_LEN];

        (void) cascade_id2path(fpath, MAX_PATH_LEN, diskid->id);

        (void) sprintf(path, "%s/%s/%s", ng.home, YFS_MDS_DIR_DISKID_PRE,
                       fpath);

        return 0;
}

/**
 * @param chkrept[out] 0 or 1
 */

static void __diskpool_reset(void *_nid, void *ctx)
{
        const nid_t *nid = _nid;

        (void) ctx;

        DINFO("%s reset\n", network_rname(nid));
        
        nodepool_diskdead(nid, TIER_ALL);

        diskpool_dead(nid);
}

int diskpool_join(const diskid_t *diskid, const diskinfo_stat_t *diskstat)
{
        int ret;
        net_handle_t nh;

        DBUG("disk "DISKID_FORMAT" join\n", DISKID_ARG(diskid));

        nh.u.nid = *diskid;
        nh.type = NET_HANDLE_PERSISTENT;

        ret = diskpool_rejoin(diskstat, &nh);
        if (ret) {
                if (ret == EAGAIN)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        ret = netable_add_reset_handler(diskid, __diskpool_reset, NULL);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int diskpool_dead(const diskid_t *diskid)
{
        int ret, grp;
        struct disk_pool *dp;
        struct disk_info *di;
        disk_stat_t oldstat, newstat;

        dp = &mds_info.diskpool;

        grp = diskid->id % dp->group;

        ret = sy_rwlock_wrlock(&dp->disk_rwlock[grp]);
        if (ret)
                GOTO(err_ret, ret);

        ret = skiplist_get(dp->disk_list[grp], (void *)diskid, (void **)&di);
        if (ret)
                goto err_lock;

        DBUG("disk "DISKID_FORMAT" dead\n", DISKID_ARG(diskid));

        ret = sy_rwlock_wrlock(&di->rwlock);
        if (ret)
                GOTO(err_lock, ret);

        oldstat = newstat = di->diskstat;

        if (oldstat != DISK_STAT_DEAD) {
                diskpool_stat_del(&dp->disk_info[grp], &di->diskinfo);

                di->diskstat = DISK_STAT_DEAD;
                di->dead_time = time(NULL);

                newstat = di->diskstat;
        }

        sy_rwlock_unlock(&di->rwlock);

        DBUG("stat old %d, new %d\n", oldstat, newstat);

        if (newstat != oldstat) {
                ret = __diskpool_switch_dead(dp, di, oldstat, newstat, grp);
                if (ret)
                        GOTO(err_lock, ret);
        }

        sy_rwlock_unlock(&dp->disk_rwlock[grp]);

        DBUG("disk "DISKID_FORMAT" DEAD\n", DISKID_ARG(diskid));

        return 0;
err_lock:
        sy_rwlock_unlock(&dp->disk_rwlock[grp]);
err_ret:
        return ret;
}

int diskpool_hb(const diskid_t *diskid, uint32_t tier, const diskinfo_stat_diff_t *diff,
                uint32_t volreptnum, const volrept_t *volrept, const uuid_t *nodeid)
{
        int ret, grp;
        struct disk_pool *dp;
        struct disk_info *di;
        disk_stat_t oldstat, newstat;
#if 0
        DBUG("diff bsize %u bfree %llu bavail %llu ffree %llu favail %llu\n",
             diff->ds_bsize, diff->ds_bfree, diff->ds_bavail, diff->ds_ffree,
             diff->ds_favail);
#endif

        dp = &mds_info.diskpool;

        grp = diskid->id % dp->group;

        ret = sy_rwlock_wrlock(&dp->disk_rwlock[grp]);
        if (ret)
                GOTO(err_ret, ret);

        ret = skiplist_get(dp->disk_list[grp], (void *)diskid, (void **)&di);
        if (ret)
                GOTO(err_lock, ret);

        ret = sy_rwlock_wrlock(&di->rwlock);
        if (ret)
                GOTO(err_lock, ret);

        di->lasthb_time = time(NULL);
//        DERROR("di %p update %lu\n", di, di->lasthb_time);

        if (di->diskstat == DISK_STAT_DEAD) {
                DWARN("orphan pack from %s\n", netable_rname(&di->nh));
                sy_rwlock_unlock(&di->rwlock);
                ret = EPERM;
                goto err_lock;
        }

        sy_rwlock_unlock(&di->rwlock);

        if (STAT_ISDIFF(diff)) {
                /* we has this disk, update the disk group info */
                ret = sy_rwlock_wrlock(&di->rwlock);
                if (ret)
                        GOTO(err_lock, ret);

                oldstat = di->diskstat;

                diskpool_stat_update(&dp->disk_info[grp], diff);

                /* we has this disk, update the disk group info */
                diskpool_stat_update(&di->diskinfo, diff);

                DBUG("diskfree %llu %llu ffree %llu\n",
                      (LLU)di->diskinfo.ds_bsize * (LLU)di->diskinfo.ds_bavail,
                      (LLU)mdsconf.disk_keep, (LLU)di->diskinfo.ds_ffree);

                newstat = getdiskstat(&di->diskinfo);

                di->diskstat = newstat;

                if (newstat != oldstat) {
                        ret = __diskpool_switch_recover(dp, di, oldstat, newstat, grp);
                        if (ret) {
                                sy_rwlock_unlock(&di->rwlock);
                                GOTO(err_lock, ret);
                        }

                        if (newstat == DISK_STAT_OVERLOAD) {
                                DINFO("disk "DISKID_FORMAT" tier %u overload, marked it as dead\n",
                                                DISKID_ARG(&di->diskid), tier);

                                ret = nodepool_diskdead(&di->diskid, tier);
                                if (ret) {
                                        sy_rwlock_unlock(&di->rwlock);
                                        GOTO(err_lock, ret);
                                }

                        } else if (newstat == DISK_STAT_FREE) {
                                DINFO("disk "DISKID_FORMAT" tier:%u free again, marked it as alive\n",
                                                DISKID_ARG(&di->diskid), tier);

                                ret = nodepool_addisk(nodeid, &di->diskid, tier);
                                if (ret) {
                                        sy_rwlock_unlock(&di->rwlock);
                                        GOTO(err_lock, ret);
                                }
                        }
                }

                sy_rwlock_unlock(&di->rwlock);

                DBUG("oldstat %d, newstat %d\n", oldstat, newstat);
        }

        ret = __diskpool_update_volume(di, volrept, volreptnum);
        if (ret)
                GOTO(err_lock, ret);

        sy_rwlock_unlock(&dp->disk_rwlock[grp]);

        return 0;
err_lock:
        sy_rwlock_unlock(&dp->disk_rwlock[grp]);
err_ret:
        return ret;
}

/**
 * @param infobuf
 * @param infobuflen
 * @param reps
 */
int diskpool_disk2netinfo_multi(int disk_num, net_handle_t *disks,
                char **infobuf, uint32_t *infobuflen, uint32_t *reps)
{
        int ret;
        uint32_t left, infolen, _reps;
        char *info;

        info = *infobuf;
        left = *infobuflen;

        *infobuflen = 0;
        *reps = 0;

        _reps = 0;
        for (disk_num--; disk_num >= 0; disk_num--) {
                infolen = left;

                ret = netable_getinfo(&disks[disk_num].u.nid, (void *)info, &infolen);
                if (ret)
                        continue;

                ynet_net_info_dump(info);

                info += infolen;
                left -= infolen;
                _reps++;
        }

        *infobuflen = info - *infobuf;
        *infobuf = info;
        *reps = _reps;

        DBUG("infobuflen %u reps %u\n", *infobuflen, *reps);

        return 0;
}

int diskpool_statvfs(diskinfo_stat_t *stat)
{
        int grp, ret;
        struct disk_pool *dp;

        dp = &mds_info.diskpool;

        for (grp = dp->group - 1; grp >= 0; grp--) {
                ret = sy_rwlock_rdlock(&dp->disk_rwlock[grp]);
                if (ret)
                        GOTO(err_ret, ret);

                if (dp->disk_info[grp].ds_bsize == 0) {
#if 0
                        DBUG("null group %d, by passed\n", grp);
#endif
                        sy_rwlock_unlock(&dp->disk_rwlock[grp]);

                        continue;
                } else {
#if 0
                        DBUG("add diskpool group %d\n", grp);
#endif
                }
#if 0
                fprintf(stderr, "diskpool_statvfs=======================>(1)\n");
                DUMP_DISKSTAT(stat);
#endif
                ret = diskpool_stat_add(stat, &dp->disk_info[grp]);
                if (ret)
                        GOTO(err_lock, ret);
#if 0
                fprintf(stderr, "diskpool_statvfs=======================>(2)\n");
                DUMP_DISKSTAT(stat);
#endif
                if (stat->ds_bfree >= stat->ds_blocks
                    || stat->ds_bavail >= stat->ds_blocks) {
                        DBUG("ds bfree(%llu) bavail(%llu) >= blocks(%llu)\n",
                             (LLU)stat->ds_bfree,
                             (LLU)stat->ds_bavail,
                             (LLU)stat->ds_blocks);
                }

                sy_rwlock_unlock(&dp->disk_rwlock[grp]);
        }

        return 0;
err_lock:
        sy_rwlock_unlock(&dp->disk_rwlock[grp]);
err_ret:
        return ret;
}

int diskpool_list(char *buf, uint32_t *buflen)
{
        int stat, grp, ret, max, i;
        struct disk_pool *dp;
        struct disk_info *di;
        diskid_t *diskid;

        dp = &mds_info.diskpool;

        max = *buflen / sizeof(diskid_t);
        diskid = (void *)buf;
        i = 0;
        for (stat = DISK_STAT_DEAD - 1; stat >= 0; stat--) {
                for (grp = dp->group - 1; grp >= 0; grp--) {
                        ret = sy_rwlock_rdlock(&dp->stat_rwlock[stat][grp]);
                        if (ret)
                                GOTO(err_ret, ret);

                        list_for_each_entry(di, &dp->stat_list[stat][grp], list) {
                                diskid[i] = di->diskid;
                                i++;

                                if (i >= max) {
                                        (void) sy_rwlock_unlock(&dp->stat_rwlock[stat][grp]);
                                        goto out;
                                }
                        }

                        (void) sy_rwlock_unlock(&dp->stat_rwlock[stat][grp]);
                }
        }

out:
        *buflen = sizeof(*diskid) * i;
        DBUG("buflen %u\n", *buflen);

        return 0;
err_ret:
        return ret;
}

int diskpool_info(char *buf, uint32_t *buflen)
{
        int stat, grp, ret;
        struct disk_pool *dp;
        struct disk_info *di;
        uint32_t left;
        diskping_stat_t *ds;

        dp = &mds_info.diskpool;

        left = *buflen;
        ds = (diskping_stat_t *)buf;

        ret = sy_rwlock_rdlock(&dp->dead_rwlock);
        if (ret)
                GOTO(err_ret, ret);

        list_for_each_entry(di, &dp->dead_list, list) {
                if (left < sizeof(diskping_stat_t)) {
                        DBUG("too few buffer left\n");
                        break;
                }

                DISKINFO2PINGSTAT(di, ds);
                ds = (void *)ds + sizeof(diskping_stat_t);
                left -= sizeof(diskping_stat_t);
        }

        (void) sy_rwlock_unlock(&dp->dead_rwlock);

        for (stat = DISK_STAT_DEAD - 1; stat >= 0; stat--) {
                for (grp = dp->group - 1; grp >= 0; grp--) {
                        ret = sy_rwlock_rdlock(&dp->stat_rwlock[stat][grp]);
                        if (ret)
                                GOTO(err_ret, ret);

                        list_for_each_entry(di,
                                            &dp->stat_list[stat][grp], list) {
                                if (left < sizeof(diskping_stat_t)) {
                                        DBUG("too few buffer left\n");
                                        break;
                                }

                                DISKINFO2PINGSTAT(di, ds);

                                ds = (void *)ds + sizeof(diskping_stat_t);
                                left -= sizeof(diskping_stat_t);
                        }

                        (void) sy_rwlock_unlock(&dp->stat_rwlock[stat][grp]);
                }
        }

        *buflen -= left;
        DBUG("buflen %u\n", *buflen);

        return 0;
err_ret:
        return ret;
}


void __chk_dump(void *arg)
{
        (void) arg;

#if 0
        struct chunk_info *ci;

        ci = arg;

        DBUG("chkid %llu_v%u max_version %u rep %d (%d)\n",
              (LLU)ci->chkid.id, ci->chkid.version, ci->max_version,
              ci->replica_num, ci->replica_max);
#endif
}

int diskpool_dump(const struct disk_info *di)
{
        int i;
        time_t now;

        UNIMPLEMENTED(__WARN__);

        now = time(NULL);

        DBUG("group %d diskid "DISKID_FORMAT" stat %d\n", di->group, DISKID_ARG(&di->diskid), di->diskstat);
        DBUG("dead_time: %f lasthb_time %f\n", difftime(now, di->dead_time), difftime(now, di->lasthb_time));

        for (i = 0; i < di->group; ++i) {
                sy_rwlock_rdlock(&di->chk_rwlock[i]);
                skiplist_iterate(di->chk_list[i], __chk_dump);
                sy_rwlock_unlock(&di->chk_rwlock[i]);
        }

        return 0;
}

int diskpool_isdead(struct disk_pool *dp, const diskid_t *diskid, int *isdead, time_t *dead_time)
{
        int ret, grp;
        struct disk_info *di;

        grp = diskid->id % dp->group;

        *isdead = 0;
        *dead_time = 0;

        ret = sy_rwlock_rdlock(&dp->disk_rwlock[grp]);
        if (ret)
                GOTO(err_ret, ret);

        ret = skiplist_get(dp->disk_list[grp], (void *)diskid, (void **)&di);
        if (ret)
                GOTO(err_lock, ret);

        if (di->diskstat == DISK_STAT_DEAD) {
                *isdead = 1;
                *dead_time = di->dead_time;
                YASSERT(*dead_time);
        }

        sy_rwlock_unlock(&dp->disk_rwlock[grp]);

        return 0;
err_lock:
        sy_rwlock_unlock(&dp->disk_rwlock[grp]);
err_ret:
        return ret;
}

int diskpool_isvalid(struct disk_pool *dp, const diskid_t *diskid, int *isvalid)
{
        int ret, grp;
        struct disk_info *di;

        grp = diskid->id % dp->group;

        *isvalid = 0;

        ret = sy_rwlock_rdlock(&dp->disk_rwlock[grp]);
        if (ret)
                GOTO(err_ret, ret);

        ret = skiplist_get(dp->disk_list[grp], (void *)diskid, (void **)&di);
        if (ret)
                GOTO(err_lock, ret);

        if (di->diskstat == DISK_STAT_FREE)
                *isvalid = 1;

        sy_rwlock_unlock(&dp->disk_rwlock[grp]);

        return 0;
err_lock:
        sy_rwlock_unlock(&dp->disk_rwlock[grp]);
err_ret:
        return ret;
}

int diskpool_count(int *_count)
{
        int stat, grp, ret, count;
        struct disk_pool *dp;
        struct disk_info *di;

        dp = &mds_info.diskpool;

        count = 0;
        for (stat = DISK_STAT_DEAD - 1; stat >= 0; stat--) {
                for (grp = dp->group - 1; grp >= 0; grp--) {
                        ret = sy_rwlock_rdlock(&dp->stat_rwlock[stat][grp]);
                        if (ret)
                                GOTO(err_ret, ret);

                        list_for_each_entry(di, &dp->stat_list[stat][grp], list) {
                                count++;
                        }

                        (void) sy_rwlock_unlock(&dp->stat_rwlock[stat][grp]);
                }
        }

        *_count = count;

        return 0;
err_ret:
        return ret;
}

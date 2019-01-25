#include <sys/statvfs.h>
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>
#include <poll.h>
#include <errno.h>
#include <unistd.h>
#define DBG_SUBSYS S_YFSCDS

#include "configure.h"
#include "yfs_conf.h"
#include "chk_proto.h"
#include "cds_volume.h"
#include "disk.h"
#include "cds.h"
#include "job_dock.h"
#include "md_proto.h"
#include "md_lib.h"
#include "node_proto.h"
#include "ylib.h"
#include "yfscds_conf.h"
#include "yfscli_conf.h"
#include "ynet.h"
#include "mond_rpc.h"
#include "net_global.h"
#include "msgqueue.h"
#include "dbg.h"

extern net_global_t ng;
//XXX
int overload = 0;
#define OVERLOAD_FLAG "overload"
//XXX
extern jobtracker_t *jobtracker;

#if 1

int __hb(const diskid_t *diskid)
{
        int ret;
        struct statvfs fsbuf;
        volinfo_t *info;
        diskinfo_stat_diff_t diff;

        ret = disk_statvfs(&fsbuf);
        if (ret) {
                GOTO(err_ret, ret);
        }
#if 0
        DUMP_VFSTAT(&fsbuf);
#endif
        //XXX
        if (overload == 1 || CDS_TYPE_CACHE == cds_info.type) {
                fsbuf.f_bfree = 0;
                fsbuf.f_bavail = 0;
                fsbuf.f_ffree = 0;
                fsbuf.f_favail = 0;
        }

        ret = cds_volume_get(&info);
        if (ret)
                GOTO(err_ret, ret);

        diff.ds_bfree = fsbuf.f_bfree
                - cds_info.hb_service.fsbuf.f_bfree;
        diff.ds_bavail = fsbuf.f_bavail
                - cds_info.hb_service.fsbuf.f_bavail;
        diff.ds_ffree = fsbuf.f_ffree
                - cds_info.hb_service.fsbuf.f_ffree;
        diff.ds_favail = fsbuf.f_favail
                - cds_info.hb_service.fsbuf.f_favail;

        diff.ds_bsize = cds_info.hb_service.fsbuf.f_bsize;

        YASSERT(diff.ds_bsize == 4096);

        DBUG("try to send heartbeat message diff %lld %lld\n",
              (long long)diff.ds_bfree, (long long)diff.ds_bavail);
        ret = mond_rpc_diskhb(diskid, cds_info.tier, (const uuid_t *)&ng.nodeid, &diff, info);
        if (ret)
                GOTO(err_free, ret);

        cds_info.hb_service.fsbuf.f_bfree  = fsbuf.f_bfree;
        cds_info.hb_service.fsbuf.f_bavail = fsbuf.f_bavail;
        cds_info.hb_service.fsbuf.f_ffree  = fsbuf.f_ffree;
        cds_info.hb_service.fsbuf.f_favail = fsbuf.f_favail;
        
        yfree((void **)&info);
        
        return 0;
err_free:
        yfree((void **)&info);
err_ret:
        return ret;
}

#else

int __hb(const diskid_t *diskid)
{
        int ret;
        struct statvfs fsbuf;
        job_t *job;
        volinfo_t *info;
        uint32_t reqlen;
        mdp_hb_req_t *hb_req;

        ret = disk_statvfs(&fsbuf);
        if (ret) {
                GOTO(err_ret, ret);
        }
#if 0
        DUMP_VFSTAT(&fsbuf);
#endif
        //XXX
        if (overload == 1 || CDS_TYPE_CACHE == cds_info.type) {
                fsbuf.f_bfree = 0;
                fsbuf.f_bavail = 0;
                fsbuf.f_ffree = 0;
                fsbuf.f_favail = 0;
        }

        ret = job_create(&job, jobtracker, "mdc_msger");
        if (ret) {
                if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        ret = cds_volume_get(&info);
        if (ret)
                GOTO(err_ret, ret);

        reqlen = sizeof(mdp_hb_req_t) + sizeof(volrept_t) * info->volreptnum;

        ret = rpc1_request_prep(job, (void **)&hb_req, reqlen, MSG_MDP);
        if (ret)
                GOTO(err_free, ret);

        memcpy(&hb_req->info, info, sizeof(volinfo_t)
               + sizeof(volrept_t) * info->volreptnum);

        yfree((void **)&info);

        hb_req->op = MDP_DISKHB;
        hb_req->diskid = *diskid;
        hb_req->tier = cds_info.tier;
        hb_req->diff.ds_bfree = fsbuf.f_bfree
                - cds_info.hb_service.fsbuf.f_bfree;
        hb_req->diff.ds_bavail = fsbuf.f_bavail
                - cds_info.hb_service.fsbuf.f_bavail;
        hb_req->diff.ds_ffree = fsbuf.f_ffree
                - cds_info.hb_service.fsbuf.f_ffree;
        hb_req->diff.ds_favail = fsbuf.f_favail
                - cds_info.hb_service.fsbuf.f_favail;

        hb_req->diff.ds_bsize = cds_info.hb_service.fsbuf.f_bsize;

        YASSERT(hb_req->diff.ds_bsize == 4096);

        DBUG("try to send heartbeat message diff %lld %lld\n",
             (long long)hb_req->diff.ds_bfree,
             (long long)hb_req->diff.ds_bavail);

        cds_info.hb_service.fsbuf.f_bfree  = fsbuf.f_bfree;
        cds_info.hb_service.fsbuf.f_bavail = fsbuf.f_bavail;
        cds_info.hb_service.fsbuf.f_ffree  = fsbuf.f_ffree;
        cds_info.hb_service.fsbuf.f_favail = fsbuf.f_favail;

        _memcpy(&hb_req->nodeid, &ng.nodeid, sizeof(uuid_t));

        ret = rpc1_request_queue_wait(job, &ng.mds_nh, KEEP_JOB, NIO_BLOCK);
        if (ret) {
                if (ret == ECONNREFUSED || ret == EHOSTUNREACH || ret == ETIMEDOUT)
                        goto err_job;
                else
                        GOTO(err_job, ret);
        }

        rpc1_request_finished(job);
        job_destroy(job);

        return 0;
err_free:
        yfree((void **)&info);
err_job:
        job_destroy(job);
err_ret:
        return ret;
}

#endif

int hb_msger(const diskid_t *diskid)
{
        int ret;
        time_t prev, now;

        ret = network_connect(net_getadmin(), &prev, 0, 0);
        if (ret)
                GOTO(err_ret, ret);

        while (srv_running) {
                sleep(10);

                ret = network_connect_mond(0);
                if (ret)
                        GOTO(err_ret, ret);

                ret = network_connect(net_getadmin(), &now, 0, 0);
                if (ret)
                        GOTO(err_ret, ret);

                if (prev != now) {
                        ret = ECONNRESET;
                        GOTO(err_ret, ret);
                }

                prev = now;
                
                ret = __hb(diskid);
                if (ret) {
                        DWARN("hb fail\n");
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

void *cds_hb(void *noop)
{
        int ret;

        (void) noop;

        while (srv_running) {
#if 0
                if (cds_info.hb_service.stop
                    || cds_info.hb_service.fsbuf.f_fsid != YFS_CDS_EXT3_FSID) {
                        DWARN("got f_type %lu != %lu\n",
                              cds_info.hb_service.fsbuf.f_fsid, YFS_CDS_EXT3_FSID);
                        break;
                }
#else
                if (cds_info.hb_service.stop)
                        break;
#endif

                DINFO("begin diskjoin ...\n");
                ret = disk_join(&ng.local_nid, &cds_info.hb_service.fsbuf);
                if (ret) {
                        DBUG("ret (%d) %s\n", ret, strerror(ret));

                        if (ret == EAGAIN) {
                                netable_put(&ng.mds_nh, "cds rejoin busy");
                        }

                        sleep(random() % 9);
                        goto reconnect;
                }

                DINFO("diskjoin ok...\n");

                cds_info.hb_service.diskid = ng.local_nid;

                if (cds_info.hb_service.running == 0) {
                        cds_info.hb_service.running = 1;
                        
                        sem_post(&cds_info.hb_service.sem);
                }

                hb_msger(&ng.local_nid);

                if (cds_info.hb_service.running == 0) {
                        goto out;
                }

        reconnect:
                if (srv_running) {
                        //netable_put(&ng.mds_nh, "cds hb error");

                        //cds_jnl_close(&ng.mds_nh);

                        ret = network_connect_mond(0);
                        if (ret) {
                                DERROR("connect fail\n");
                                sleep(10);
                                goto reconnect;
                        }
                }
        }

        cds_info.hb_service.running = 0;

out:
        sem_post(&cds_info.hb_service.sem);

        pthread_exit(NULL);
}

int hb_service_init(hb_service_t *hbs, int servicenum)
{
        int ret, i;
        uint32_t len;
        void *ptr;
        struct stat stbuf;
        char dpath[MAX_PATH_LEN];
        pthread_t th;
        pthread_attr_t ta;

        len = sizeof(struct list_head) * servicenum;
        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_ret, ret);

        hbs->list = (struct list_head *)ptr;
        for (i = 0; i < servicenum; i++)
                INIT_LIST_HEAD(&hbs->list[i]);

        len = sizeof(sy_rwlock_t) * servicenum;
        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_list, ret);

        hbs->rwlock = (sy_rwlock_t *)ptr;
        for (i = 0; i < servicenum; i++)
                (void) sy_rwlock_init(&hbs->rwlock[i], NULL);

        (void) sem_init(&hbs->sem, 0, 0);
        (void) sem_init(&cds_info.hb_sem, 0, 0);

        hbs->stop = 0;
        hbs->running = 0;

        hbs->diskid.id = DISKID_NULL;

        hbs->servicenum = servicenum;

        ret = disk_statvfs(&hbs->fsbuf);
        if (ret) {
                GOTO(err_sem, ret);
        }

        DBUG("diskfree %llu ffree %llu aval %llu\n",
              (LLU)hbs->fsbuf.f_bsize * (LLU)hbs->fsbuf.f_bavail,
             (LLU)hbs->fsbuf.f_ffree, (LLU)hbs->fsbuf.f_favail);

        // This cds overload flag is true
        // XXX
        snprintf(dpath, MAX_PATH_LEN, "%s/%s",
                 ng.home, OVERLOAD_FLAG);

        if (stat(dpath, &stbuf) == 0) {
                overload = 1;
                DWARN("%s found, set to ro mode \n", dpath);
        } else {
                overload = 0;
                DBUG("%s not found, set to rw mode\n", dpath);
        }

        YASSERT(strlen(ng.home) != 0);

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);
        
        ret = pthread_create(&th, &ta, cds_hb, NULL);
        if (ret)
                GOTO(err_ret, ret);

        _sem_wait(&cds_info.hb_service.sem);

        if (cds_info.hb_service.running == 1) {
                DBUG("cds heartbeats thread started\n");
        } else {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

#if 0
        if (hbs->fsbuf.f_fsid != YFS_CDS_EXT3_FSID) {
                ret = EINVAL;
                DWARN("got f_type %lu != %lu\n", hbs->fsbuf.f_fsid,
                      YFS_CDS_EXT3_FSID);
                GOTO(err_sem, ret);
        }
#endif

        hbs->inited = 1;

        return 0;
err_sem:
        (void) sem_destroy(&hbs->sem);

        for (i = 0; i < hbs->servicenum; i++)
                (void) sy_rwlock_destroy(&hbs->rwlock[i]);

        yfree((void **)&hbs->rwlock);
err_list:
        yfree((void **)&hbs->list);
err_ret:
        return ret;
}

int hb_service_destroy()
{
        int ret;

        DINFO("wait for cds hb destroy...\n");

        cds_info.hb_service.running = 0;

        sem_post(&cds_info.hb_sem);

        while (1) {
                ret = sem_wait(&cds_info.hb_service.sem);
                if (ret) {
                        ret = errno;
                        if (ret == EINTR)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }

                break;
        }

        DINFO("cds hb destroyed\n");

        return 0;
err_ret:
        return ret;
}

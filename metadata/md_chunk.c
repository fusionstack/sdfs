#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSMDC

#include "job_dock.h"
#include "chk_proto.h"
#include "yfs_chunk.h"
#include "net_global.h"
#include "ynet.h"
#include "ylib.h"
#include "sysutil.h"
#include "md_proto.h"
#include "md_lib.h"
#include "yfscds_conf.h"
#include "net_global.h"
#include "redis.h"
#include "allocator.h"
#include "md_db.h"
#include "dbg.h"


typedef struct {
        diskid_t diskid;
        int online;
} chkloc_info_t;

static chunkop_t *chunkop = &__chunkop__;

/** allocate new chunk
 * @param chkrep
 * @param path
 * @param chk[in,out]
 */
int md_chkget(const fileinfo_t *_md, chkinfo_t *chkinfo, const chkid_t *chkid,
              const nid_t *_peer)
{
        int ret;
        char buf[MAX_BUF_LEN];
        fileinfo_t *md;
        
        (void) _peer;

        if (_md) {
                md = (void *)_md;
        } else {
                md = (void *)buf;

                fileid_t fileid;
                cid2fid(&fileid, chkid);
                ret = md_getattr((void *)md, &fileid);
                if (ret)
                        GOTO(err_ret, ret);
        }
        
        ret = md_chunk_create(md, chkid->idx, chkinfo);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_chkload(chkinfo_t *chk, const chkid_t *chkid, const nid_t *_peer)
{
        (void) _peer;

        return md_chunk_load(chkid, chk);
}

static int __md_location_check(const diskid_t *id, const diskid_t *id1, int *same)
{
        int ret;
        char buf[MAX_BUF_LEN], buf1[MAX_BUF_LEN];
        ynet_net_info_t *info, *info1;
        uint32_t len;

        info = (void *)buf;
        info1 = (void *)buf1;

        len = MAX_BUF_LEN;
        ret = netable_getinfo(id, info, &len);
        if (ret) {
                GOTO(err_ret, ret);
        }

        len = MAX_BUF_LEN;
        ret = netable_getinfo(id1, info1, &len);
        if (ret) {
                if (ret == ENONET) {
                        ret = EAGAIN;
                        DWARN("peer "DISKID_FORMAT" offline\n", DISKID_ARG(id1));
                        goto err_ret;
                } else
                        GOTO(err_ret, ret);
        }

        if (strcmp(info->nodeid, info1->nodeid) == 0)
                *same = 1;
        else
                *same = 0;

        return 0;
err_ret:
        return ret;
}

static int __md_newdisk(diskid_t *diskid, const chkinfo_t *chkinfo, int idx)
{
        int ret, i, j, found, got, retry = 0;
        diskid_t total_disks[YFS_CHK_REP_MAX];
        diskid_t *newdisk;

        UNIMPLEMENTED(__WARN__);//get tier
retry:
        ret = allocator_new(chkinfo->repnum, mdsconf.chknew_hardend, TIER_SSD, total_disks);
        if (ret) {
                DWARN("require %u, hardend %u ret (%d) %s\n", chkinfo->repnum,
                                mdsconf.chknew_hardend, ret, strerror(ret));
                goto err_ret;
        }

        got = 0;
        for (i = 0; i < (int)chkinfo->repnum; i++) {
                found = 0;
                newdisk = &total_disks[i];
                for (j = 0; j < (int)chkinfo->repnum; ++j) {
                        if (nid_cmp(&chkinfo->diskid[j], newdisk) == 0) {
                                found = 1;
                                break;
                        }

                        if (j != idx && mdsconf.chknew_hardend) {
                                ret = __md_location_check(&chkinfo->diskid[j], newdisk, &found);
                                if (ret) {
                                        if (ret == EAGAIN) {
                                                retry++;
                                                goto retry;
                                        } else if (ret == ENONET) {
                                                continue;
                                        } else
                                                GOTO(err_ret, ret);
                                }

                                if (found)
                                        break;
                        }
                }

                if (!found) {
                        *diskid = total_disks[i];
                        got = 1;
                        break;
                }
        }

        if (got == 0) {
                DWARN("object "OBJID_FORMAT" busy\n", OBJID_ARG(&chkinfo->chkid));
                retry++;
                if (retry < 100) {
                        goto retry;
                } else {
                        ret = ENOSPC;
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

static int __md_newrep(chkinfo_t *chkinfo, int repmin, int flag)
{
        int ret, i, online, newreps, available = 0;
        diskid_t *diskid, newdisk;
        chkid_t *chkid = &chkinfo->chkid;
        chkloc_info_t _info[YFS_CHK_REP_MAX], *info;
        char msg[MAX_BUF_LEN];

        online = 0;
        for (i = 0; i < (int)chkinfo->repnum; i++) {
                diskid = &chkinfo->diskid[i];
                info = &_info[i];
                info->diskid = *diskid;

                ret = network_connect(diskid, NULL, 1, 1);
                if (ret) {
                        DINFO("%s (%d) not online\n", netable_rname_nid(diskid), diskid->id);
                        info->online = 0;
                } else {
                        info->online = 1;
                }

                if (info->online) {
                        if  (info->diskid.status & __S_DIRTY) {
                        } else {
                                available++;
                        }

                        online++;
                }
        }

        if ((int)chkinfo->repnum == online) {
                ret = EEXIST;
                goto err_ret;
        }

        if (available < repmin) {
                ret = EAGAIN;
                DWARN(CHKID_FORMAT" total %u online %u, avaliable %u, require %u\n",
                      CHKID_ARG(&chkinfo->chkid), chkinfo->repnum, online, available, repmin);
                GOTO(err_ret, ret);
        }
        
        if (available == 0) {
                ret = EAGAIN;
                DWARN("chk "CHKID_FORMAT" newrep online %u available %u\n",
                      CHKID_ARG(chkid), online, available);
                goto err_ret;
        }

        if (online == 0) {
                msg[0] = '\0';

                for (i = 0; i < (int)chkinfo->repnum; i++) {
                        sprintf(msg + strlen(msg), DISKID_FORMAT,
                                DISKID_ARG(&chkinfo->diskid[i]));
                }

                ret = EAGAIN;
                DWARN("chk "CHKID_FORMAT" online %u repnum %u:%s\n", CHKID_ARG(chkid), online,
                                chkinfo->repnum, msg);
                goto err_ret;
        } else {
                DINFO("chk "CHKID_FORMAT" online %u\n", CHKID_ARG(chkid), online);
        }

#if 0
        ret = __md_ec_check(&chkinfo->chkid, chkinfo->repnum - available);
        if (ret) {
                DINFO("msg:%s\n", msg);
                GOTO(err_ret, ret);
        }
#endif

        newreps = 0;
        for (i = 0; i < (int)chkinfo->repnum; i++) {
                info = &_info[i];
                diskid = &info->diskid;
                if (info->online == 0) {
                        DBUG("newrep "CHKID_FORMAT", disk "
                             DISKID_FORMAT" not online\n", CHKID_ARG(chkid),
                             DISKID_ARG(diskid));

                        //retry:
                        ret = __md_newdisk(&newdisk, chkinfo, i);
                        if (ret) {
                                if (ret == ENOSPC)
                                        goto err_ret;
                                else
                                        GOTO(err_ret, ret);
                        }

                        if (ynet_nid_cmp(&newdisk, diskid) != 0) {
                                DINFO("chk "CHKID_FORMAT" newrep at "
                                      DISKID_FORMAT"(%s) replace "DISKID_FORMAT"\n", 
                                      CHKID_ARG(&chkinfo->chkid), DISKID_ARG(&newdisk),
                                      netable_rname_nid(&newdisk), 
                                      DISKID_ARG(diskid));

                                ret = rm_push(diskid, -1, chkid);
                                if (ret)
                                        GOTO(err_ret, ret);

                                chkinfo->diskid[i] = newdisk;
                                newreps++;

                                if (flag != NEWREP_UNREG) {
                                        chkinfo->diskid[i].status |= __S_DIRTY;
                                }
                        }
                }
        }

        if (newreps == 0) {
                ret = EEXIST;
                goto err_ret;
        }

#if 0
        if (chkinfo->diskid[chkinfo->master].status & __S_DIRTY) {
                DWARN("chk %llu_v%u[%u] newrep at %llu_v%u(%s) replace %llu_v%u\n",
                                (LLU)chkinfo->chkid.id, chkinfo->chkid.version,
                                chkinfo->chkid.idx, (LLU)newdisk.id,
                                newdisk.version, netable_rname_nid(&newdisk), 
                                (LLU)diskid->id, diskid->version);

                done = 0;
                for (i = 0; i < (int)chkinfo->repnum; i++) {
                        info = &_info[i];
                        diskid = &info->diskid;
                        if (info->online && (diskid->status & __S_DIRTY) == 0) {
                                chkinfo->master = i;
                                done = 1;
                                break;
                        }                
                }

                YASSERT(done == 1);
        }
#endif

        return 0;
err_ret:
        return ret;
}

static void __chkinfo_init(chkinfo_t *chkinfo, const chkid_t *chkid,
                           const diskid_t *disks, const fileinfo_t *md)
{
        chkinfo->chkid = *chkid;
        chkinfo->status = md->status;
        chkinfo->repnum = md->repnum;
        chkinfo->md_version = 0;
        chkinfo->master = 0;
        chkinfo->size = md->split;

        memcpy(chkinfo->diskid, disks, sizeof(*disks) * md->repnum);
}

int md_chunk_create(const fileinfo_t *md, uint64_t idx, chkinfo_t *chkinfo)
{
        int ret;
        diskid_t disk[YFS_CHK_REP_MAX];
        chkid_t chkid;

        UNIMPLEMENTED(__NULL__);//get tier

        ANALYSIS_BEGIN(0);
        
        ret = allocator_new(md->repnum, 0, TIER_SSD, disk);
        if (ret)
                GOTO(err_ret, ret);

        if (md->plugin != PLUGIN_NULL) {
                YASSERT(md->repnum == md->m);
        }
        
        fid2cid(&chkid, &md->fileid, idx);
        __chkinfo_init(chkinfo, &chkid, disk, md);

        ret = chunkop->create(chkinfo);
        if (ret)
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

int md_chunk_load(const chkid_t *chkid, chkinfo_t *chkinfo)
{
        int ret;
        
        ret = chunkop->load(chkid, chkinfo);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

static int __md_chunk_load_fast(const chkid_t *chkid, chkinfo_t *chkinfo)
{
        int ret, count;
        uint32_t i;
        nid_t *nid;

        ret = md_chunk_load(chkid, chkinfo);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        count = 0;
        for (i = 0; i < chkinfo->repnum; i++) {
                nid = &chkinfo->diskid[i];
                
                if (nid->status & __S_DIRTY) {
                        continue;
                }

                ret = network_connect(nid, NULL, 1, 0);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                count++;
        }

        if (count == 0) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __md_chunk_check(chkinfo_t *chkinfo, int repmin)
{
        int ret, count;
        uint32_t i;
        nid_t *nid;

        count = 0;
        for (i = 0; i < chkinfo->repnum; i++) {
                nid = &chkinfo->diskid[i];

                if (nid->status & __S_DIRTY) {
                        continue;
                }
                
                ret = network_connect(nid, NULL, 1, 0);
                if (unlikely(ret)) {
                        nid->status |= __S_DIRTY;
                        continue;
                }

                count++;
        }

        if (count < repmin) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __md_chunk_load_slow(const chkid_t *chkid, chkinfo_t *chkinfo, int repmin)
{
        int ret;

        ret = klock(chkid, 10, 1);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ret = md_chunk_load(chkid, chkinfo);
        if (unlikely(ret))
                GOTO(err_lock, ret);

        ret = __md_chunk_check(chkinfo, repmin);
        if (unlikely(ret))
                GOTO(err_lock, ret);

#if 0
        int count = 0;
        for (int i = 0; i < (int)chkinfo->repnum; i++) {
                //YASSERT(chkinfo->diskid[i].status == 0);
                if (chkinfo->diskid[i].status)
                        count++;
        }
        YASSERT(count < 2);
#endif
        
        ret = chunkop->update(chkinfo);
        if (ret)
                GOTO(err_lock, ret);

        ret = kunlock(chkid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_lock:
        kunlock(chkid);
err_ret:
        return ret;
}

int md_chunk_load_check(const chkid_t *chkid, chkinfo_t *chkinfo, int repmin)
{
        int ret;

        ANALYSIS_BEGIN(0);
        
        ret = __md_chunk_load_fast(chkid, chkinfo);
        if (unlikely(ret)) {
                if (ret == ENOENT) {
                        GOTO(err_ret, ret);
                } else {
                        ret = __md_chunk_load_slow(chkid, chkinfo, repmin);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);
                }
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

int md_chunk_newdisk(const chkid_t *chkid, chkinfo_t *chkinfo, int repmin, int flag)
{
        int ret;

        ret = md_chunk_load(chkid, chkinfo);
        if (ret)
                GOTO(err_ret, ret);
        
        ret = __md_newrep(chkinfo, repmin, flag);
        if (ret) {
                if (ret == EEXIST) {
                        //pass
                } else 
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int md_chunk_update(const chkinfo_t *chkinfo)
{
        int ret;

#if 0
        for (int i = 0; i < (int)chkinfo->repnum; i++) {
                YASSERT(chkinfo->diskid[i].status == 0);
        }
#endif
        
        ret = chunkop->update(chkinfo);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

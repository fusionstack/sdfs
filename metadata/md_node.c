#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSMDC

#include "job_dock.h"
#include "net_global.h"
#include "ylib.h"
#include "md_proto.h"
#include "md_lib.h"
#include "md_db.h"
#include "redis.h"
#include "schedule.h"
#include "attr_queue.h"
#include "dbg.h"

static dirop_t *dirop = &__dirop__;
static inodeop_t *inodeop = &__inodeop__;

static uint64_t __systemvolid__ = -1;

#if 0
static int __md_getsize(md_proto_t *_md)
{
        int ret;
        fileinfo_t *md = (void *)_md;
        chkinfo_t *chkinfo;
        char _chkinfo[CHK_SIZE(YFS_CHK_REP_MAX)];

        if (md->chknum == 0) {
                md->at_size = 0;
                return 0;
        }

        chkid_t chkid;
        chkinfo = (void *)_chkinfo;
        fid2cid(&chkid, &md->fileid, md->chknum - 1);
        ret = chunkop->load(NULL, chkid, chkinfo);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}
#endif

int md_getattr(const volid_t *volid, const fileid_t *fileid, md_proto_t *md)
{
        int ret;

        ANALYSIS_BEGIN(0);

        ret = inodeop->getattr(volid, fileid, md);
        if (ret)
                GOTO(err_ret, ret);

#if ENABLE_ATTR_QUEUE
        if (ng.daemon) {
                attr_queue_update(volid, fileid, md);
        }
#endif

#if 0
        if (mdsconf.size_on_md && S_ISREG(md->at_mode)) {
                ret = __md_getsize(md);
                if (ret)
                        GOTO(err_ret, ret);
        }
#endif
        
        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

int md_system_volid(uint64_t *id)
{
        int ret;
        fileid_t fileid;

        if (__systemvolid__ == (uint64_t)-1) {
                DINFO("load system volid\n");
                
                ret = sdfs_lookupvol(SDFS_SYSTEM_VOL, &fileid);
                if(ret)
                        GOTO(err_ret, ret);

                __systemvolid__ = fileid.volid;
        }

        *id = __systemvolid__;

        return 0;
err_ret:
        return ret;
}

int md_initroot()
{
        int ret;
        setattr_t setattr;
        fileid_t fileid;

        setattr_init(&setattr, -1, -1, NULL, -1, -1, -1);
        ret = md_mkvol(SDFS_SYSTEM_VOL, &setattr, &fileid);
        if (ret) {
                if (ret == EEXIST) {
                        ret = md_lookupvol(SDFS_SYSTEM_VOL, &fileid);
                        if (ret)
                                GOTO(err_ret, ret);
                } else
                        GOTO(err_ret, ret);
        }
        
        ret = md_root_create(fileid.volid);
        if (ret)
                GOTO(err_ret, ret);

        ret = md_root_init();
        if (ret)
                GOTO(err_ret, ret);
        
#if 0
        ret = inodeop->init();
        if (ret)
                GOTO(err_ret, ret);
#endif

        return 0;
err_ret:
        return ret;
}

int md_chmod(const volid_t *volid, const fileid_t *fileid, mode_t mode)
{
        int ret;
        setattr_t setattr;

        setattr_init(&setattr, mode & MODE_MAX, -1, NULL, -1, -1, -1);
        ret = inodeop->setattr(volid, fileid, &setattr, 1);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int md_setattr(const volid_t *volid, const fileid_t *fileid, const setattr_t *setattr, int force)
{
        int ret;

        ret = inodeop->setattr(volid, fileid, setattr, force);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int md_utime(const volid_t *volid, const fileid_t *fileid, const struct timespec *atime,
             const struct timespec *mtime, const struct timespec *ctime)
{
        int ret;
        setattr_t setattr;

        setattr_init(&setattr, -1, -1, NULL, -1, -1, -1);
        setattr_update_time(&setattr,
                            __SET_TO_CLIENT_TIME, atime,
                            __SET_TO_CLIENT_TIME, mtime,
                            __SET_TO_CLIENT_TIME, ctime);

        ret = inodeop->setattr(volid, fileid, &setattr, 1);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_set_wormid(const volid_t *volid, const fileid_t *fileid, uint64_t wormid)
{
        int ret;
        setattr_t setattr;

        setattr_init(&setattr, -1, -1, NULL, -1, -1, -1);
        setattr.wormid.set_it = 1;
        setattr.wormid.val = wormid;

        ret = inodeop->setattr(volid, fileid, &setattr, 1);
        if (ret)
                GOTO(err_ret, ret);
                
        return 0;
err_ret:
        return ret;
}

int md_chown(const volid_t *volid, const fileid_t *fileid, uid_t uid, gid_t gid)
{
        int ret;
        setattr_t setattr;

        setattr_init(&setattr, -1, -1, NULL, uid, gid, -1);

        ret = inodeop->setattr(volid, fileid, &setattr, 1);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_set_quotaid(const volid_t *volid, const fileid_t *fileid, const fileid_t *quotaid)
{
        int ret;
        setattr_t setattr;

        setattr_init(&setattr, -1, -1, NULL, -1, -1, -1);
        setattr.quotaid.set_it = 1;
        setattr.quotaid.val = *quotaid;

        //DINFO("set quotaid:\n", (LLU)quotaid);

        ret = inodeop->setattr(volid, fileid, &setattr, 1);
        if (ret)
                GOTO(err_ret, ret);
                
        return 0;
err_ret:
        return ret;
}

int md_rename(const volid_t *volid, const fileid_t *fparent,
              const char *fname, const fileid_t *tparent, const char *tname)
{
        int ret;
        fileid_t fileid;
        uint32_t type;

        ret = dirop->lookup(volid, fparent, fname, &fileid, &type);
        if (ret)
                GOTO(err_ret, ret);

        if (fileid.id == fileid.volid) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }
        
        ret = dirop->newrec(volid, tparent, tname, &fileid, type, O_EXCL);
        if (ret)
                GOTO(err_ret, ret);

        ret = dirop->unlink(volid, fparent, fname);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_remove(const volid_t *volid, const fileid_t *fileid)
{
        int ret, i, j;
        chkinfo_t *chkinfo;
        chkid_t chkid;
        char _chkinfo[CHK_SIZE(YFS_CHK_REP_MAX)];
        fileinfo_t *md;
        char buf[MAX_BUF_LEN] = {0};

        if (fileid->type != ftype_file) {
                goto out;
        }
        
        md = (void *)buf;
        ret = inodeop->getattr(volid, fileid, (void *)md);
        if (ret)
                GOTO(err_ret, ret);

        chkinfo = (void *)_chkinfo;
        for (i = 0; i < (int)md->chknum; i++) {
                fid2cid(&chkid, &md->fileid, i);
                ret = md_chunk_load(&chkid, chkinfo);
                if (ret)
                        continue;

                for (j = 0; j < (int)chkinfo->repnum; j++) {
                        rm_push(&chkinfo->diskid[i], -1, &chkinfo->chkid);
                }
        }

out:
        ret = inodeop->remove(volid, fileid, NULL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_getxattr(const volid_t *volid, const fileid_t *fileid, const char *name, void *value, size_t *size)
{
        int ret;

        ret = inodeop->getxattr(volid, fileid, name, value, size);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int md_setxattr(const volid_t *volid, const fileid_t *fileid, const char *name,
                const void *value, size_t size, int flags)
{
        int ret;

        ret = inodeop->setxattr(volid, fileid, name, value, size, flags);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int md_removexattr(const volid_t *volid, const fileid_t *fileid, const char *name)
{
        int ret;

        ret = inodeop->removexattr(volid, fileid, name);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int md_listxattr(const volid_t *volid, const fileid_t *fileid, char *list, size_t *size)
{
        int ret;

        ret = inodeop->listxattr(volid, fileid, list, size);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int md_childcount(const volid_t *volid, const fileid_t *fileid, uint64_t *count)
{
        return inodeop->childcount(volid, fileid, count);
}


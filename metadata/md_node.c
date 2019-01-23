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
#include "dbg.h"

static dirop_t *dirop = &__dirop__;
static inodeop_t *inodeop = &__inodeop__;

static uint64_t __systemvolid__ = -1;

int md_getattr(md_proto_t *md, const fileid_t *fileid)
{
        int ret;

        ANALYSIS_BEGIN(0);
        
        ret = inodeop->getattr(fileid, md);
        if (ret)
                GOTO(err_ret, ret);

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

int md_chmod(const fileid_t *fileid, mode_t mode)
{
        int ret;
        setattr_t setattr;

        setattr_init(&setattr, mode & MODE_MAX, -1, NULL, -1, -1, -1);
        ret = inodeop->setattr(fileid, &setattr, 1);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int md_setattr(const fileid_t *fileid, const setattr_t *setattr, int force)
{
        int ret;

        ret = inodeop->setattr(fileid, setattr, force);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int md_utime(const fileid_t *fileid, const struct timespec *atime,
             const struct timespec *mtime, const struct timespec *ctime)
{
        int ret;
        setattr_t setattr;

        setattr_init(&setattr, -1, -1, NULL, -1, -1, -1);
        setattr_update_time(&setattr,
                            __SET_TO_CLIENT_TIME, atime,
                            __SET_TO_CLIENT_TIME, mtime,
                            __SET_TO_CLIENT_TIME, ctime);

        ret = inodeop->setattr(fileid, &setattr, 1);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_set_wormid(const fileid_t *fileid, uint64_t wormid)
{
        int ret;
        setattr_t setattr;

        setattr_init(&setattr, -1, -1, NULL, -1, -1, -1);
        setattr.wormid.set_it = 1;
        setattr.wormid.val = wormid;

        ret = inodeop->setattr(fileid, &setattr, 1);
        if (ret)
                GOTO(err_ret, ret);
                
        return 0;
err_ret:
        return ret;
}

int md_chown(const fileid_t *fileid, uid_t uid, gid_t gid)
{
        int ret;
        setattr_t setattr;

        setattr_init(&setattr, -1, -1, NULL, uid, gid, -1);

        ret = inodeop->setattr(fileid, &setattr, 1);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_set_quotaid(const fileid_t *fileid, const fileid_t *quotaid)
{
        int ret;
        setattr_t setattr;

        setattr_init(&setattr, -1, -1, NULL, -1, -1, -1);
        setattr.quotaid.set_it = 1;
        setattr.quotaid.val = *quotaid;

        //DINFO("set quotaid:\n", (LLU)quotaid);

        ret = inodeop->setattr(fileid, &setattr, 1);
        if (ret)
                GOTO(err_ret, ret);
                
        return 0;
err_ret:
        return ret;
}

int md_rename(const fileid_t *fparent,
              const char *fname, const fileid_t *tparent, const char *tname)
{
        int ret;
        fileid_t fileid;
        uint32_t type;

        ret = dirop->lookup(fparent, fname, &fileid, &type);
        if (ret)
                GOTO(err_ret, ret);

        if (fileid.id == fileid.volid) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }
        
        ret = dirop->newrec(tparent, tname, &fileid, type, O_EXCL);
        if (ret)
                GOTO(err_ret, ret);

        ret = dirop->unlink(fparent, fname);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_remove(const fileid_t *fileid)
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
        ret = inodeop->getattr(fileid, (void *)md);
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
        ret = inodeop->remove(fileid, NULL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_getxattr(const fileid_t *fileid, const char *name, void *value, size_t *size)
{
        int ret;

        ret = inodeop->getxattr(fileid, name, value, size);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int md_setxattr(const fileid_t *fileid, const char *name,
                const void *value, size_t size, int flags)
{
        int ret;

        ret = inodeop->setxattr(fileid, name, value, size, flags);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int md_removexattr(const fileid_t *fileid, const char *name)
{
        int ret;

        ret = inodeop->removexattr(fileid, name);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int md_listxattr(const fileid_t *fileid, char *list, size_t *size)
{
        int ret;

        ret = inodeop->listxattr(fileid, list, size);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int md_childcount(const fileid_t *fileid, uint64_t *count)
{
        return inodeop->childcount(fileid, count);
}


#include <sys/types.h>
#include <dirent.h>
#include <string.h>

#define DBG_SUBSYS S_YFSMDC

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

static dirop_t *dirop = &__dirop__;
static inodeop_t *inodeop = &__inodeop__;

inline static int __md_update_time(const volid_t *volid, const fileid_t *fileid, int at, int mt, int ct)
{
        int ret;
        setattr_t setattr;

        setattr_init(&setattr, -1, -1, NULL, -1, -1, -1);
        setattr_update_time(&setattr,
                            at ? __SET_TO_SERVER_TIME : __DONT_CHANGE, NULL,
                            mt ? __SET_TO_SERVER_TIME : __DONT_CHANGE, NULL,
                            ct ? __SET_TO_SERVER_TIME : __DONT_CHANGE, NULL);

        ret = inodeop->setattr(volid, fileid, &setattr, 0);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __md_create(const volid_t *volid, const fileid_t *parent, const char *name,
                       const setattr_t *setattr, int mode, fileid_t *_fileid)
{
        int ret;
        fileid_t fileid;

        DBUG("create %s @ "CHKID_FORMAT"\n", name, CHKID_ARG(parent));
        
        if (strncmp(name, SDFS_MD_SYSTEM, strlen(SDFS_MD_SYSTEM)) == 0) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }

#if ENABLE_QUOTA
        ret = quota_inode_increase(parent, setattr);
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif

        ret = inodeop->create(volid, parent, setattr, mode, &fileid);
        if (ret)
                GOTO(err_dec, ret);

#if ENABLE_MD_POSIX
        ret = __md_update_time(volid, parent, 0, 1, 1);
        if (ret)
                GOTO(err_dec, ret);
#endif

        ret = dirop->newrec(volid, parent, name, &fileid, mode, O_EXCL);
        if (ret) {
                if (ret == EEXIST) {
                        inodeop->unlink(volid, &fileid, NULL);
                }

                GOTO(err_dec, ret);
        }

        if (_fileid) {
                *_fileid = fileid;
        }
        
        return 0;
err_dec:
#if ENABLE_QUOTA
        quota_inode_decrease(parent, setattr);
#endif
err_ret:
        return ret;
}

int md_create(const volid_t *volid, const fileid_t *parent, const char *name, const setattr_t *setattr, fileid_t *fileid)
{
        int ret;

        ANALYSIS_BEGIN(0);
        
        ret = __md_create(volid, parent, name, setattr, ftype_file, fileid);
        if (ret)
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

int md_mkdir(const volid_t *volid, const fileid_t *parent, const char *name, const setattr_t *setattr, fileid_t *fileid)
{
        int ret;

        ANALYSIS_BEGIN(0);
        
        ret = __md_create(volid, parent, name, setattr, ftype_dir, fileid);
        if (ret)
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

int md_readdir(const volid_t *volid, const fileid_t *fileid, off_t offset, void **de, int *delen)
{
        int ret, len;
        char buf[MAX_BUF_LEN];
        void *ptr;

        len = MAX_BUF_LEN;
        ret = dirop->readdir(volid, fileid, buf, &len, offset);
        if (ret) {
                GOTO(err_ret, ret);
        }

        if (len) {
                ret = ymalloc(&ptr, len);
                if (ret)
                        GOTO(err_ret, ret);
        
                memcpy(ptr, buf, len);
                *de = ptr;
        } else {
                *de = NULL;
        }

        *delen = len;

        return 0;
err_ret:
        return ret;
}

static int __md_redirplus(const volid_t *volid, void *buf, int buflen)
{
        int ret;
        struct dirent *de;
        md_proto_t *md, *pos;
        uint64_t offset = 0;
        char tmp[MAX_BUF_LEN];

        (void) offset;

        md = (void *)tmp;
        dir_for_each(buf, buflen, de, offset) {
                YASSERT(strlen(de->d_name));
                                
                DBUG("name (%s) d_off %llu\n", de->d_name, (LLU)de->d_off);

                if (strcmp(de->d_name, ".") == 0
                    || strcmp(de->d_name, "..") == 0) {
                        offset = de->d_off;
                        continue;
                }

                pos = (void *)de + de->d_reclen - sizeof(md_proto_t);
                YASSERT(de->d_reclen < MAX_NAME_LEN * 2 + sizeof(md_proto_t));
                
                ret = inodeop->getattr(volid, &pos->fileid, md);
                if (ret) {
                        DWARN("load file "CHKID_FORMAT " not found \n",
                              CHKID_ARG(&pos->fileid));
                        memset(pos, 0x0, sizeof(*md));
                        continue;
                }

                DBUG("load file "CHKID_FORMAT " chknum %u, size %llu \n", CHKID_ARG(&md->fileid),
                      md->chknum, md->at_size);
                memcpy(pos, md, sizeof(*md));
        }

        return 0;
}

int md_readdirplus(const volid_t *volid, const fileid_t *fileid, off_t offset,
                    void **de, int *delen)
{
        int ret, len;
        char buf[MAX_BUF_LEN * 4];
        void *ptr;

        len = MAX_BUF_LEN * 4;
        ret = dirop->readdirplus(volid, fileid, buf, &len, offset);
        if (ret) {
                GOTO(err_ret, ret);
        }

        if (len) {
                ret = __md_redirplus(volid, buf, len);
                if (ret)
                        GOTO(err_ret, ret);
                
                ret = ymalloc(&ptr, len);
                if (ret)
                        GOTO(err_ret, ret);

                memcpy(ptr, buf, len);
                *de = ptr;
        } else {
                *de = NULL;
        }
        
        *delen = len;

        return 0;
err_ret:
        return ret;
}

int md_readdirplus_with_filter(const volid_t *volid, const fileid_t *fileid, off_t offset,
                               void **de, int *delen, const filter_t *filter)
{
        int ret, len;
        char buf[MAX_BUF_LEN * 4];
        void *ptr;

        len = MAX_BUF_LEN * 4;
        ret = dirop->readdirplus_filter(volid, fileid, buf, &len, offset, filter);
        if (ret) {
                GOTO(err_ret, ret);
        }

        if (len) {
                ret = __md_redirplus(volid, buf, len);
                if (ret)
                        GOTO(err_ret, ret);
                
                ret = ymalloc(&ptr, len);
                if (ret)
                        GOTO(err_ret, ret);

                memcpy(ptr, buf, len);
                *de = ptr;
        } else {
                *de = NULL;
        }
        
        *delen = len;

        return 0;
err_ret:
        return ret;
}

int md_lookup(const volid_t *volid, fileid_t *fileid, const fileid_t *parent, const char *name)
{
        int ret;
        uint32_t type;
        md_proto_t *md;
        char buf[MAX_BUF_LEN];

        DBUG("lookup %s @ "CHKID_FORMAT"\n", name, CHKID_ARG(parent));
        
        if (strcmp(name, ".") == 0) {
                *fileid = *parent;
                return 0;
        } else if (strcmp(name, "..") == 0) {
                if (parent->type == ftype_vol) {
                        *fileid = *parent;
                        return 0;
                } else {
                        md = (void *)buf;
                        ret = inodeop->getattr(volid, parent, md);
                        if (ret)
                                GOTO(err_ret, ret);
                        
                        *fileid = md->parent;
                }
        } else {
                ret = dirop->lookup(volid, parent, name, fileid, &type);
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int md_rmdir(const volid_t *volid, const fileid_t *parent, const char *name)
{
        int ret;
        uint32_t type;
        uint64_t count;
        fileid_t fileid;

        ret = dirop->lookup(volid, parent, name, &fileid, &type);
        if (ret)
                GOTO(err_ret, ret);

        ret = inodeop->childcount(volid, &fileid, &count);
        if (ret) {
                if (ret == ENOENT) {
                        DWARN(CHKID_FORMAT" not found\n",
                              CHKID_ARG(&fileid));
                } else
                        GOTO(err_ret, ret);
        } else {
                if (count > 0) {//SDFS_MD, SDFS_PARENT
                        ret = ENOTEMPTY;
                        GOTO(err_ret, ret);
                }

                ret = quota_check_dec(&fileid);
                if (ret)
                        GOTO(err_ret, ret);

                ret = inodeop->unlink(volid, &fileid, NULL);
                if (ret) {
                        if (ret == ENOENT) {
                                DWARN(CHKID_FORMAT" not found\n", CHKID_ARG(&fileid));
                        } else
                                GOTO(err_ret, ret);
                }

        }

#if ENABLE_MD_POSIX
        ret = __md_update_time(volid, parent, 0, 1, 1);
        if (ret)
                GOTO(err_ret, ret);
#endif
        
        ret = dirop->unlink(volid, parent, name);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_unlink(const volid_t *volid, const fileid_t *parent, const char *name, md_proto_t *_md)
{
        int ret;
        fileid_t fileid;
        md_proto_t *md;
        char buf[MAX_BUF_LEN];

        ret = md_lookup(volid, &fileid, parent, name);
        if (ret)
                GOTO(err_ret, ret);

        ret = quota_check_dec(&fileid);
        if (ret)
                GOTO(err_ret, ret);

        md = (void *)buf;
        ret = inodeop->unlink(volid, &fileid, md);
        if (ret) {
                if (ret == ENOENT) {
                        DWARN(CHKID_FORMAT" not found\n", CHKID_ARG(&fileid));
                } else
                        GOTO(err_ret, ret);
        }

#if ENABLE_MD_POSIX
        ret = __md_update_time(volid, parent, 0, 1, 1);
        if (ret)
                GOTO(err_ret, ret);

        if (S_ISREG(md->at_mode) && md->at_nlink) {
                ret = __md_update_time(volid, &fileid, 0, 0, 1);
                if (ret)
                        GOTO(err_ret, ret);
        }
#endif

        ret = dirop->unlink(volid, parent, name);
        if (ret)
                GOTO(err_ret, ret);

        memcpy(_md, md, md->md_size);
        
        return 0;
err_ret:
        return ret;
}

int md_link2node(const volid_t *volid, const fileid_t *fileid, const fileid_t *parent,
                  const char *name)
{
        int ret;
        
        ret = inodeop->link(volid, fileid);
        if (ret)
                GOTO(err_ret, ret);

#if ENABLE_MD_POSIX
        ret = __md_update_time(volid, parent, 0, 1, 1);
        if (ret)
                GOTO(err_ret, ret);

        ret = __md_update_time(volid, fileid, 0, 0, 1);
        if (ret)
                GOTO(err_ret, ret);
#endif
        
        ret = dirop->newrec(volid, parent, name, fileid, __S_IFREG, O_EXCL);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int md_symlink(const volid_t *volid, const fileid_t *parent, const char *name, const char *link_target,
               uint32_t mode, uint32_t uid, uint32_t gid)
{
        int ret;
        fileid_t fileid;
        setattr_t setattr;

        setattr_init(&setattr, mode, -1, NULL, uid, gid, -1);
        ret = inodeop->create(volid, parent, &setattr, ftype_symlink, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        ret = inodeop->symlink(volid, &fileid, link_target);
        if (ret)
                GOTO(err_ret, ret);

        ret = dirop->newrec(volid, parent, name, &fileid, __S_IFLNK, O_EXCL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_readlink(const volid_t *volid, const fileid_t *fileid, char *_buf)
{
        return inodeop->readlink(volid, fileid, _buf);
}


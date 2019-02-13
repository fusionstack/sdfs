#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSMDC

#include "job_dock.h"
#include "net_global.h"
#include "ylib.h"
#include "redis_conn.h"
#include "md_proto.h"
#include "md_lib.h"
#include "md_db.h"
#include "md_attr.h"
#include "schedule.h"
#include "dbg.h"

void setattr_update_time(setattr_t *setattr,
                         __time_how ahow,
                         const struct timespec *atime,
                         __time_how mhow,
                         const struct timespec *mtime,
                         __time_how chow,
                         const struct timespec *ctime)
{
        if (atime && ahow == __SET_TO_CLIENT_TIME) {
                setattr->atime.set_it = __SET_TO_CLIENT_TIME;
                setattr->atime.time = *atime;
        } else {
                setattr->atime.set_it = ahow;
        }

        if (mtime && mhow == __SET_TO_CLIENT_TIME) {
                setattr->mtime.set_it = __SET_TO_CLIENT_TIME;
                setattr->mtime.time = *mtime;
        } else {
                setattr->mtime.set_it = mhow;
        }

        if (ctime && chow == __SET_TO_CLIENT_TIME) {
                setattr->ctime.set_it = __SET_TO_CLIENT_TIME;
                setattr->ctime.time = *ctime;
        } else {
                setattr->ctime.set_it = chow;
        }
}

void setattr_init(setattr_t *setattr, uint32_t mode, int replica, const ec_t *ec,
                  int uid, int gid, size_t size)
{
        memset((void *)setattr, 0x0, sizeof(*setattr));

        if (uid != -1) {
                setattr->uid.set_it = 1;
                setattr->uid.val = uid;
        }

        if (gid != -1) {
                setattr->gid.set_it = 1;
                setattr->gid.val = gid;
        }

        if (mode != (uint32_t)-1) {
                setattr->mode.set_it = 1;
                setattr->mode.val = mode;
        }

        if (ec) {
                setattr->ec.set_it = 1;
                setattr->ec.ec = *ec;
                /*
                setattr->ec.ec.plugin = PLUGIN_NULL;
                setattr->ec.ec.tech = TECH_NULL;
                setattr->ec.ec.m = 0;
                setattr->ec.ec.k = 0;
                */
        }

#if 0
        if (atime != -1) {
                setattr->atime.set_it = __SET_TO_CLIENT_TIME;
                clock_gettime(CLOCK_REALTIME, &setattr->atime.time);
        }

        if (mtime != -1) {
                setattr->mtime.set_it = __SET_TO_CLIENT_TIME;
                clock_gettime(CLOCK_REALTIME, &setattr->mtime.time);
        }

        if (ctime != -1) {
                setattr->ctime.set_it = __SET_TO_CLIENT_TIME;
                clock_gettime(CLOCK_REALTIME, &setattr->ctime.time);
        }
#endif
        
        if (replica != -1) {
                setattr->replica.set_it = 1;
                setattr->replica.val = replica;
        }

        if (size != (size_t)-1) {
                setattr->size.set_it = 1;
                setattr->size.size = size;
        }
}

void md_attr_inherit(md_proto_t *md, const md_proto_t *parent, const ec_t *ec, uint32_t mode)
{
        uint32_t repnum;
        
        if (parent) {
                DBUG("parent "FID_FORMAT" repnum %u status %u split %u\n", FID_ARG(&parent->fileid),
                     parent->repnum, parent->status, parent->split);
                YASSERT(parent->repnum);
        }

        if (!parent) {
                md->plugin = PLUGIN_NULL;
                md->tech = TECH_ISA_SSE;
                md->m = 0;
                md->k = 0;
        } else if (!ec || ec->plugin == PLUGIN_NULL) {
                md->plugin = parent->plugin;
                md->tech = parent->tech;
                md->m = parent->m;
                md->k = parent->k;
        } else {
                md->plugin = ec->plugin;
                md->tech = ec->tech;
                md->m = ec->m;
                md->k = ec->k;
        }

        if (parent) {
                YASSERT(parent->repnum);
                md->status = parent->status;
                repnum = parent->repnum;
        } else {
                md->status = 0;
                repnum = gloconf.chunk_rep;
        }

        if (parent && parent->repnum) {
                if (md->plugin == PLUGIN_EC_ISA && S_ISREG(mode)) {
                        md->repnum = md->m;
                } else
                        md->repnum = parent->repnum;
        } else
                md->repnum = repnum;

        if (parent && parent->split) {
                if (md->plugin == PLUGIN_EC_ISA && S_ISREG(mode)) {
                        md->split = md->k * YFS_CHK_LEN_DEF;
                } else
                        md->split = parent->split;
        } else
                md->split = YFS_CHK_LEN_DEF;

        if (parent) {
                md->parent = parent->fileid;
                DBUG("create "CHKID_FORMAT" parent "CHKID_FORMAT"\n",
                      CHKID_ARG(&md->fileid), CHKID_ARG(&md->parent));
        } else {
                md->parent.id = 0;
                md->parent.volid = 0;
        }

        if(parent)
                md->wormid = parent->wormid;
        else
                md->wormid = WORM_FID_NULL;

        if (S_ISREG(mode) && (md->plugin != PLUGIN_NULL)) {
                YASSERT(md->repnum == md->m);
        }
}

void md_attr_update(md_proto_t *md, const setattr_t *setattr)
{
        int changed = 0;

        if (setattr->uid.set_it) {
                md->at_uid = setattr->uid.val;
                changed = 1;
        }

        if (setattr->gid.set_it) {
                md->at_gid = setattr->gid.val;
                changed = 1;
        }

        if (setattr->mode.set_it) {
                DBUG(CHKID_FORMAT" mode %x %x\n", CHKID_ARG(&md->fileid),
                     setattr->mode.val, setattr->mode.val & MODE_MAX);

                md->at_mode = (md->at_mode & (~MODE_MAX)) | (setattr->mode.val & MODE_MAX);
                changed = 1;
        }

        if (setattr->size.set_it) {
                md->at_size = setattr->size.size;
                md->chknum = _get_chknum(md->at_size, md->split);
                changed = 1;
        }

        if (setattr->ec.set_it) {
                md->m = setattr->ec.ec.m;
                md->k = setattr->ec.ec.k;
                md->plugin = setattr->ec.ec.plugin;
                md->tech = setattr->ec.ec.tech;
                changed = 1;
        }

        if (setattr->quotaid.set_it) {
                md->quotaid = setattr->quotaid.val;
                changed = 1;
        }

        if (setattr->wormid.set_it) {
                md->wormid = setattr->wormid.val;
                changed = 1;
        }


        if (setattr->atime.set_it == __SET_TO_SERVER_TIME) {
                clock_gettime(CLOCK_REALTIME, &md->at_atime);
        } else if (setattr->atime.set_it == __SET_TO_CLIENT_TIME) {
                md->at_atime = setattr->atime.time;
        }

        if (setattr->ctime.set_it == __SET_TO_SERVER_TIME) {
                clock_gettime(CLOCK_REALTIME, &md->at_ctime);
        } else if (setattr->ctime.set_it == __SET_TO_CLIENT_TIME) {
                md->at_ctime = setattr->ctime.time;
        } else if (changed) {
                clock_gettime(CLOCK_REALTIME, &md->at_ctime);
        }

        if (setattr->mtime.set_it == __SET_TO_SERVER_TIME) {
                clock_gettime(CLOCK_REALTIME, &md->at_mtime);
        } else if (setattr->mtime.set_it == __SET_TO_CLIENT_TIME) {
                md->at_mtime = setattr->mtime.time;
        }
}

int md_attr_init(md_proto_t *md, const setattr_t *setattr, uint32_t type,
                     const md_proto_t *parent, const fileid_t *fileid)
{
        int ret;
        fileinfo_t *file_md;
        dir_md_t *dir_md;
        symlink_md_t *sym_md;
        uint32_t mode;

        YASSERT(type > (uint32_t)ftype_null && type < (uint32_t)ftype_max);

        DBUG("fileid "FID_FORMAT" type %o\n", FID_ARG(fileid), type);

        memset(md, 0x0, sizeof(*md));
        mode = stype(type);
#if 0
        if (type == ftype_dir || type == ftype_vol || type == ftype_xattr) {
                mode = __S_IFDIR;
        } else {
                mode = __S_IFREG;
        }
#endif
        
        if (S_ISREG(mode)) {
                YASSERT(parent);

                file_md = (void *)md;
                file_md->at_nlink = 1;
                file_md->at_size = 0;
                file_md->md_size = sizeof(fileinfo_t);
        } else if (S_ISDIR(mode)) {
                dir_md = (void *)md;
                dir_md->repnum = gloconf.chunk_rep;
                dir_md->at_nlink = 2;
                dir_md->at_size = sizeof(dir_md_t);
                dir_md->md_size = sizeof(dir_md_t);
        }  else if (S_ISLNK(mode)) {
                YASSERT(parent);

                DBUG("symlink\n");
                sym_md = (void *)md;
                sym_md->at_nlink = 1;
                sym_md->md_size = sizeof(symlink_md_t);
                sym_md->at_size = sym_md->at_size;
        } else {
                DWARN("mode %x\n", mode);
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        md->at_mode = mode;
        md->fileid = *fileid;
        md->md_version = 0;
        md->chknum = 0;
        memset(&md->quotaid, 0x0, sizeof(md->quotaid));
        md_attr_inherit(md, parent, NULL, mode);
        md_attr_update(md, setattr);

        YASSERT(md->at_mode);
        DBUG("fileid "FID_FORMAT" mode %o size %u atime %u mtime %u\n",
                        FID_ARG(fileid), md->at_mode, md->md_size,
                        md->at_atime, md->at_mtime);

        return 0;
err_ret:
        return ret;
}

int md_attr_getid(fileid_t *fileid, const fileid_t *parent, ftype_t type, const volid_t *volid)
{
        int ret;
        uint64_t id;

        (void) parent;
        
        ANALYSIS_BEGIN(0);
        
        ret = md_newid(idtype_fileid, &id);
        if (ret)
                GOTO(err_ret, ret);

        if (volid) {
                fileid->volid = volid->volid;
                fileid->idx = 0;
                fileid->id = id;

                ret = redis_conn_new(volid, &fileid->sharding);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                uint64_t systemvol;
                ret = md_system_volid(&systemvol);
                        
                fileid->volid = systemvol;
                fileid->idx = 0;
                fileid->id = id;
                volid_t _volid = {systemvol, 0};
                ret = redis_conn_new(&_volid, &fileid->sharding);
                if (ret)
                        GOTO(err_ret, ret);
        }

        fileid->type = type;
        fileid->__pad__ = 0;

        DBUG("create "CHKID_FORMAT" @ sharding[%u]\n", CHKID_ARG(fileid), fileid->sharding);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        return 0;
err_ret:
        return ret;
}

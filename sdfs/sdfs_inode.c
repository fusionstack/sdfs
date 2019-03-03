
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>

#define DBG_SUBSYS S_YFSLIB

#include "sdfs_id.h"

#include "md_lib.h"
#include "chk_proto.h"
#include "network.h"
#include "net_global.h"
#include "chk_proto.h"
#include "file_table.h"
#include "job_dock.h"
#include "ylib.h"
#include "net_global.h"
#include "yfs_file.h"
#include "cache.h"
#include "schedule.h"
#include "sdfs_lib.h"
#include "sdfs_chunk.h"
#include "network.h"
#include "yfs_limit.h"
#include "io_analysis.h"
#include "worm_cli_lib.h"
#include "main_loop.h"
#include "posix_acl.h"
#include "flock.h"
#include "xattr.h"
#include "dbg.h"


int sdfs_getattr(sdfs_ctx_t *ctx, const fileid_t *fileid, struct stat *stbuf)
{
        int ret, retry = 0;
        md_proto_t *md;
        char buf[MAX_BUF_LEN];

        md = (void *)buf;

        io_analysis(ANALYSIS_OP_READ, 0);
        DBUG("getattr "FID_FORMAT"\n", FID_ARG(fileid));

        if (fileid->type == ftype_root || fileid->type == ftype_null) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        volid_t volid = {fileid->volid, ctx ? ctx->snapvers : 0};
retry:
        ret = md_getattr(&volid, fileid, md);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        MD2STAT(md, stbuf);

        return 0;
err_ret:
        return ret;
}

int sdfs_rename(sdfs_ctx_t *ctx, const fileid_t *fparent, const char *fname,
                const fileid_t *tparent,
                const char *tname)
{
        int ret, retry = 0;
        fileid_t src_fileid, dist_fileid;

        memset(&src_fileid, 0, sizeof(src_fileid));
        ret = sdfs_lookup(ctx, fparent, fname, &src_fileid);
        if (ret)
                GOTO(err_ret, ret);

#if ENABLE_WORM
        worm_status_t worm_status;
        worm_status = worm_get_status(&src_fileid);
        if (WORM_IN_PROTECT == worm_status)
        {
                ret = EACCES;
                goto err_ret;
        }
#endif

        ret = sdfs_lookup(ctx, tparent, tname, &dist_fileid);
        if (ret) {
                if (ret == ENOENT) {
                        memset(&dist_fileid, 0x0, sizeof(dist_fileid));
                } else
                        GOTO(err_ret, ret);
        }

        volid_t volid = {fparent->volid, ctx ? ctx->snapvers : 0};
retry:
        ret = md_rename(&volid, fparent, fname, tparent, tname);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

#if 1
        if (dist_fileid.type == ftype_file) {
                ret = md_remove(&volid, &dist_fileid);
                if (ret) {
                       DWARN("remove "CHKID_FORMAT" fail\n", CHKID_ARG(&dist_fileid));
                }
        }
#endif

        return 0;
err_ret:
        return ret;
}

int sdfs_chown(sdfs_ctx_t *ctx, const fileid_t *fileid, uid_t uid, gid_t gid)
{
        int ret, retry = 0;

#if ENABLE_WORM
        worm_status_t worm_status;

        worm_status = worm_get_status(fileid);
        if (WORM_IN_PROTECT == worm_status)
        {
                ret = EACCES;
                goto err_ret;
        }
#endif

        volid_t volid = {fileid->volid, ctx ? ctx->snapvers : 0};
retry:
        ret = md_chown(&volid, fileid, uid, gid);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}


int sdfs_utime(sdfs_ctx_t *ctx, const fileid_t *fileid, const struct timespec *atime,
               const struct timespec *mtime, const struct timespec *ctime)
{
        int ret, retry = 0;

        (void) ctx;
        volid_t volid = {fileid->volid, ctx ? ctx->snapvers : 0};
retry:
        ret = md_utime(&volid, fileid, atime, mtime, ctime);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int sdfs_chmod(sdfs_ctx_t *ctx, const fileid_t *fileid, mode_t mode)
{
        int ret, retry = 0;

        (void) ctx;

#if ENABLE_WORM
        worm_status_t worm_status;

        worm_status = worm_get_status(fileid);
        if (WORM_IN_PROTECT == worm_status)
        {
                ret = EACCES;
                goto err_ret;
        }
#endif

        volid_t volid = {fileid->volid, ctx ? ctx->snapvers : 0};
retry:
        ret = md_chmod(&volid, fileid, mode);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int sdfs_childcount(sdfs_ctx_t *ctx, const fileid_t *fileid, uint64_t *count)
{
        int ret, retry = 0;

        (void) ctx;

        volid_t volid = {fileid->volid, ctx ? ctx->snapvers : 0};
retry:
        ret = md_childcount(&volid, fileid, count);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int sdfs_setattr(sdfs_ctx_t *ctx, const fileid_t *fileid, const setattr_t *setattr, int force)
{
        int ret, retry = 0;

        (void) ctx;

#if ENABLE_WORM
        worm_status_t worm_status;

        worm_status = worm_get_status(fileid);
        if (WORM_IN_PROTECT == worm_status)
        {
                ret = EACCES;
                goto err_ret;
        }
#endif

        volid_t volid = {fileid->volid, ctx ? ctx->snapvers : 0};
retry:
        ret = md_setattr(&volid, fileid, setattr, force);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}



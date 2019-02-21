

#include <errno.h>

#define DBG_SUBSYS S_YNFS

#include "xdr_nfs.h"
#include "nfs_args.h"
#include "file_proto.h"
#include "dbg.h"

#define __FALSE -1
#define __TRUE 0

int xdr_dirpath(xdr_t *xdrs, char **dir)
{
        if (__xdr_string(xdrs, dir, MNTPATH_LEN))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_mountstat(xdr_t *xdrs, mount_stat *mntstat)
{
        if (__xdr_enum(xdrs, (enum_t *)mntstat))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_fhandle(xdr_t *xdrs, fhandle *fh)
{
        if (__xdr_bytes(xdrs, (char **)&fh->val, (u_int *)&fh->len, FH_SIZE))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_mountretok(xdr_t *xdrs, mount_retok *mntok)
{
        if (xdr_fhandle(xdrs, &mntok->fhandle))
                return __FALSE;

        if (__xdr_array(xdrs, (char **)&mntok->auth_flavors.val,
                       (uint32_t *)&mntok->auth_flavors.len, ~0, sizeof(int),
                       (__xdrproc_t)__xdr_int))
                return __FALSE;

        return __TRUE;
}
int xdr_mountret(xdr_t *xdrs, mount_ret *mntret)
{
        if (xdr_mountstat(xdrs, &mntret->fhs_status))
                return __FALSE;

        switch (mntret->fhs_status) {
        case MNT_OK:
                if (xdr_mountretok(xdrs, &mntret->u.mountinfo))
                        return __FALSE;
                break;
        default:
                break;
        }

        return __TRUE;
}

int xdr_nfs_fh3(xdr_t *xdrs, nfs_fh3 *fh)
{
        int ret;
        fileid_t *fileid;
        nfh_t *nfh;

        ret = __xdr_bytes(xdrs, (char **)&fh->val, (u_int *)&fh->len, NFS3_FHSIZE);
        if (ret)
                GOTO(err_ret, ret);

        if (xdrs->op != __XDR_FREE) {
                nfh = (void *)fh->val;
                fileid = &nfh->fileid;
                YASSERT(fileid->idx == 0);
                if (fileid->id == 0 || fileid->volid == 0 || fh->len != sizeof(nfh_t)) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

int xdr_getattrargs(xdr_t *xdrs, getattr_args *args)
{
        if (xdr_nfs_fh3(xdrs, &args->obj))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_nfs3_stat(xdr_t *xdrs, nfs3_stat *stat)
{
        if (__xdr_enum(xdrs, (enum_t *)stat))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_ftype(xdr_t *xdrs, nfs3_ftype *type)
{
        if (__xdr_enum(xdrs, (enum_t *)type))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_mode(xdr_t *xdrs, uint32_t *mode)
{
        if (__xdr_uint32(xdrs, mode))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_specdata(xdr_t *xdrs, nfs3_specdata *data)
{
        if ((__xdr_uint32(xdrs, &data->data1))
            || (__xdr_uint32(xdrs, &data->data2)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_time(xdr_t *xdrs, nfs3_time *time)
{
        if ((__xdr_uint32(xdrs, &time->seconds))
            || (__xdr_uint32(xdrs, &time->nseconds)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_fattr(xdr_t *xdrs, nfs3_fattr *attr)
{
        //YASSERT(attr->fsid == 2054);

        if ((xdr_ftype(xdrs, &attr->type))
            || (xdr_mode(xdrs, &attr->mode))
            || (__xdr_uint32(xdrs, &attr->nlink))
            || (__xdr_uint32(xdrs, &attr->uid))
            || (__xdr_uint32(xdrs, &attr->gid))
            || (__xdr_uint64(xdrs, &attr->size))
            || (__xdr_uint64(xdrs, &attr->used))
            || (xdr_specdata(xdrs, &attr->rdev))
            || (__xdr_uint64(xdrs, &attr->fsid))
            || (__xdr_uint64(xdrs, &attr->fileid))
            || (xdr_time(xdrs, &attr->atime))
            || (xdr_time(xdrs, &attr->mtime))
            || (xdr_time(xdrs, &attr->ctime)))
                return __FALSE;

        return __TRUE;
}

int xdr_getattrretok(xdr_t *xdrs, nfs3_fattr *attr)
{
        if (xdr_fattr(xdrs, attr))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_getattrret(xdr_t *xdrs, getattr_ret *ret)
{
        if (xdr_nfs3_stat(xdrs, &ret->status))
                return __FALSE;

        switch (ret->status) {
        case NFS3_OK:
                if (xdr_getattrretok(xdrs, &ret->attr))
                        return __FALSE;
                break;
        default:
                break;
        }

        return __TRUE;
}

int xdr_name(xdr_t *xdrs, char **name)
{
        if (__xdr_string(xdrs, name, ~0))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_nfspath3(xdr_t *xdrs, nfspath3 *path)
{
        if (__xdr_string(xdrs, path, ~0))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_diropargs(xdr_t *xdrs, diropargs3 *args)
{
        if ((xdr_nfs_fh3(xdrs, &args->dir))
            || (xdr_name(xdrs, &args->name)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_lookupargs(xdr_t *xdrs, lookup_args *args)
{
        if (xdr_diropargs(xdrs, (diropargs3 *)args))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_post_op_attr(xdr_t *xdrs, post_op_attr *attr)
{
        int ret;

        ret = __xdr_bool(xdrs, &attr->attr_follow);
        if (ret)
                GOTO(err_ret, ret);

        switch (attr->attr_follow) {
        case TRUE:
                ret = xdr_fattr(xdrs, &attr->attr);
                if (ret)
                        GOTO(err_ret, ret);
                break;
        case FALSE:
                break;
        default:
                ret = EINVAL;

                DERROR("attr_follow %u\n", attr->attr_follow);

                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int xdr_lookupretfail(xdr_t *xdrs, lookup_retfail *fail)
{
        return xdr_post_op_attr(xdrs, &fail->dir_attr);
}

int xdr_lookupretok(xdr_t *xdrs, lookup_retok *ok)
{
        if ((xdr_nfs_fh3(xdrs, &ok->obj))
            || (xdr_post_op_attr(xdrs, &ok->obj_attr))
            || (xdr_post_op_attr(xdrs, &ok->dir_attr)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_lookupret(xdr_t *xdrs, lookup_ret *res)
{
        int ret;
        ret = xdr_nfs3_stat(xdrs, &res->status);
        if (ret)
                GOTO(err_ret, ret);

        switch (res->status) {
        case NFS3_OK:
                ret = xdr_lookupretok(xdrs, &res->u.ok);
                if (ret)
                        GOTO(err_ret, ret);
                break;
        default:
                ret = xdr_lookupretfail(xdrs, &res->u.fail);
                if (ret)
                        GOTO(err_ret, ret);
                break;
        }

        return 0;
err_ret:
        return ret;
}

int xdr_accessargs(xdr_t *xdrs, access_args *args)
{
        if ((xdr_nfs_fh3(xdrs, &args->obj))
            || (__xdr_uint32(xdrs, &args->access)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_accessretok(xdr_t *xdrs, access_retok *ok)
{
        if ((xdr_post_op_attr(xdrs, &ok->obj_attr))
            || (__xdr_uint32(xdrs, &ok->access)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_accessretfail(xdr_t *xdrs, access_retfail *fail)
{
        if (xdr_post_op_attr(xdrs, &fail->obj_attr))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_accessret(xdr_t *xdrs, access_ret *ret)
{
        if (xdr_nfs3_stat(xdrs, &ret->status))
                return __FALSE;

        switch (ret->status) {
        case NFS3_OK:
                if (xdr_accessretok(xdrs, &ret->u.ok))
                        return __FALSE;
                break;
        default:
                if (xdr_accessretfail(xdrs, &ret->u.fail))
                        return __FALSE;
                break;
        }

        return __TRUE;
}

int xdr_readargs(xdr_t *xdrs, read_args *args)
{
        if ((xdr_nfs_fh3(xdrs, &args->file))
            || (__xdr_uint64(xdrs, &args->offset))
            || (__xdr_uint32(xdrs, &args->count)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_readretok(xdr_t *xdrs, read_retok *ok)
{
        if ((xdr_post_op_attr(xdrs, &ok->attr))
            || (__xdr_uint32(xdrs, &ok->count))
            || (__xdr_bool(xdrs, &ok->eof))
            || (__xdr_buffer(xdrs, (char **)&ok->data.val, (u_int *)&ok->data.len,
                           ~0)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_readretfail(xdr_t *xdrs, read_retfail *fail)
{
        if (xdr_post_op_attr(xdrs, &fail->attr))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_readret(xdr_t *xdrs, read_ret *ret)
{
        if (xdr_nfs3_stat(xdrs, &ret->status))
                return __FALSE;

        switch (ret->status) {
        case NFS3_OK:
                if (xdr_readretok(xdrs, &ret->u.ok))
                        return __FALSE;
                break;
        default:
                if (xdr_readretfail(xdrs, &ret->u.fail))
                        return __FALSE;
                break;
        }

        return __TRUE;
}

int xdr_stablehow(xdr_t *xdrs, stable_how *how)
{
        if (__xdr_enum(xdrs, (enum_t *)how))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_writeargs(xdr_t *xdrs, write_args *args)
{
        if ((xdr_nfs_fh3(xdrs, &args->file))
            || (__xdr_uint64(xdrs, &args->offset))
            || (__xdr_uint32(xdrs, &args->count))
            || (xdr_stablehow(xdrs, &args->stable))
            || (__xdr_buffer(xdrs, (char **)&args->data.val,
                             (u_int *)&args->data.len, ~0)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_wcc_attr(xdr_t *xdrs, wcc_attr *attr)
{
        if ((__xdr_uint64(xdrs, &attr->size))
            || (xdr_time(xdrs, &attr->mtime))
            || (xdr_time(xdrs, &attr->ctime)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_preop_attr(xdr_t *xdrs, preop_attr *attr)
{
        int ret;
        if (__xdr_bool(xdrs, &attr->attr_follow)) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        switch (attr->attr_follow) {
        case TRUE:
                if (xdr_wcc_attr(xdrs, &attr->attr)) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
                break;
        case FALSE:
                break;
        default:
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        return __TRUE;
err_ret:
        return __FALSE;
}

int xdr_wccdata(xdr_t *xdrs, wcc_data *wcc)
{
        int ret;

        if (xdr_preop_attr(xdrs, &wcc->before)) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        if (xdr_post_op_attr(xdrs, &wcc->after)) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        return __TRUE;
err_ret:
        return __FALSE;
}

int xdr_writeverf(xdr_t *xdrs, char *verf)
{
        if (__xdr_opaque(xdrs, verf, NFS3_WRITEVERFSIZE))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_writeretok(xdr_t *xdrs, write_retok *ok)
{
        if ((xdr_wccdata(xdrs, &ok->file_wcc))
            || (__xdr_uint32(xdrs, &ok->count))
            || (xdr_stablehow(xdrs, &ok->committed))
            || (xdr_writeverf(xdrs, ok->verf)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_writeretfail(xdr_t *xdrs, write_retfail *fail)
{
        if (xdr_wccdata(xdrs, &fail->file_wcc))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_writeret(xdr_t *xdrs, write_ret *ret)
{
        if (xdr_nfs3_stat(xdrs, &ret->status))
                return __FALSE;

        switch (ret->status) {
        case NFS3_OK:
                if (xdr_writeretok(xdrs, &ret->u.ok))
                        return __FALSE;
                break;
        default:
                if (xdr_writeretfail(xdrs, &ret->u.fail))
                        return __FALSE;
                break;
        }

        return __TRUE;
}

int xdr_createmode(xdr_t *xdrs, create_mode *mode)
{
        if (__xdr_enum(xdrs, (enum_t *)mode))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_set_mode(xdr_t *xdrs, set_mode *mode)
{
        if (__xdr_bool(xdrs, &mode->set_it))
                return __FALSE;

        switch (mode->set_it) {
        case TRUE:
                if (__xdr_uint32(xdrs, &mode->mode))
                        return __FALSE;
                break;
        default:
                break;
        }

        return __TRUE;
}

int xdr_set_uid(xdr_t *xdrs, set_uid *uid)
{
        if (__xdr_bool(xdrs, &uid->set_it))
                return __FALSE;

        switch (uid->set_it) {
        case TRUE:
                if (__xdr_uint32(xdrs, &uid->uid))
                        return __FALSE;
                break;
        default:
                break;
        }

        return __TRUE;
}

int xdr_set_gid(xdr_t *xdrs, set_gid *gid)
{
        if (__xdr_bool(xdrs, &gid->set_it))
                return __FALSE;

        switch (gid->set_it) {
        case TRUE:
                if (__xdr_uint32(xdrs, &gid->gid))
                        return __FALSE;
                break;
        default:
                break;
        }

        return __TRUE;
}

int xdr_set_size(xdr_t *xdrs, set_size *size)
{
        if (__xdr_bool(xdrs, &size->set_it))
                return __FALSE;

        switch (size->set_it) {
        case TRUE:
                if (__xdr_uint64(xdrs, &size->size))
                        return __FALSE;
                break;
        default:
                break;
        }

        return __TRUE;
}

int xdr_time_how(xdr_t *xdrs, time_how *how)
{
        if (__xdr_enum(xdrs, (enum_t *)how))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_set_time(xdr_t *xdrs, set_time *time)
{
        if (xdr_time_how(xdrs, &time->set_it))
                return __FALSE;

        switch (time->set_it) {
        case SET_TO_CLIENT_TIME:
                if (xdr_time(xdrs, &time->time))
                        return __FALSE;
                break;
        default:
                break;
        }

        return __TRUE;
}

int xdr_sattr(xdr_t *xdrs, sattr *attr)
{
        if ((xdr_set_mode(xdrs, &attr->mode))
            || (xdr_set_uid(xdrs, &attr->uid))
            || (xdr_set_gid(xdrs, &attr->gid))
            || (xdr_set_size(xdrs, &attr->size))
            || (xdr_set_time(xdrs, &attr->atime))
            || (xdr_set_time(xdrs, &attr->mtime)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_createverf(xdr_t *xdrs, char *verf)
{
        if (__xdr_opaque(xdrs, verf, NFS3_CREATEVERFSIZE))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_createhow(xdr_t *xdrs, create_how *how)
{
        if (xdr_createmode(xdrs, &how->mode))
                return __FALSE;

        switch (how->mode) {
        case UNCHECKED:
        case GUARDED:
                if (xdr_sattr(xdrs, &how->attr))
                        return __FALSE;
                break;
        case EXCLUSIVE:
                if (xdr_createverf(xdrs, how->verf))
                        return __FALSE;
                break;
        default:
                return __FALSE;
        }

        return __TRUE;
}

int xdr_createargs(xdr_t *xdrs, create_args *args)
{
        if ((xdr_diropargs(xdrs, &args->where))
            || (xdr_createhow(xdrs, &args->how)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_postop_fh(xdr_t *xdrs, postop_fh *fh)
{
        if (__xdr_bool(xdrs, &fh->handle_follows))
                GOTO(err_ret, EINVAL);

        switch (fh->handle_follows) {
        case TRUE:
                if (xdr_nfs_fh3(xdrs, &fh->handle))
                        GOTO(err_ret, EINVAL);
                break;
        case FALSE:
                break;
        default:
                GOTO(err_ret, EINVAL);
        }

        return __TRUE;
err_ret:
        return __FALSE;
}

int xdr_createretok(xdr_t *xdrs, create_retok *ok)
{
        if ((xdr_postop_fh(xdrs, &ok->obj))
            || (xdr_post_op_attr(xdrs, &ok->obj_attr))
            || (xdr_wccdata(xdrs, &ok->dir_wcc)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_createretfail(xdr_t *xdrs, create_retfail *fail)
{
        if (xdr_wccdata(xdrs, &fail->dir_wcc))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_createret(xdr_t *xdrs, create_ret *ret)
{
        if (xdr_nfs3_stat(xdrs, &ret->status))
                return __FALSE;

        switch (ret->status) {
        case NFS3_OK:
                if (xdr_createretok(xdrs, &ret->u.ok))
                        return __FALSE;
                break;
        default:
                if (xdr_createretfail(xdrs, &ret->u.fail))
                        return __FALSE;
                break;
        }

        return __TRUE;
}

int xdr_mkdirargs(xdr_t *xdrs, mkdir_args *args)
{
        if ((xdr_diropargs(xdrs, &args->where))
            || (xdr_sattr(xdrs, &args->attr)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_mkdirretok(xdr_t *xdrs, mkdir_retok *ok)
{
        if ((xdr_postop_fh(xdrs, &ok->obj))
            || (xdr_post_op_attr(xdrs, &ok->obj_attr))
            || (xdr_wccdata(xdrs, &ok->dir_wcc)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_mkdirretfail(xdr_t *xdrs, mkdir_retfail *fail)
{
        if (xdr_wccdata(xdrs, &fail->dir_wcc))
                GOTO(err_ret, EINVAL);

        return __TRUE;
err_ret:
        return __FALSE;
}

int xdr_mkdirret(xdr_t *xdrs, mkdir_ret *ret)
{
        if (xdr_nfs3_stat(xdrs, &ret->status))
                return __FALSE;

        switch (ret->status) {
        case NFS3_OK:
                if (xdr_mkdirretok(xdrs, &ret->u.ok)) {
                        DWARN("mkdir fail\n");
                        return __FALSE; }
                break;
        default:
                if (xdr_mkdirretfail(xdrs, &ret->u.fail)) {
                        DWARN("mkdir fail\n");
                        return __FALSE;}
                break;
        }

        return __TRUE;
}

int xdr_removeargs(xdr_t *xdrs, remove_args *args)
{
        if (xdr_diropargs(xdrs, &args->obj))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_removeret(xdr_t *xdrs, remove_ret *ret)
{
        if ((xdr_nfs3_stat(xdrs, &ret->status))
            || (xdr_wccdata(xdrs, &ret->dir_wcc)))
                return __FALSE;

        return __TRUE;
}

int xdr_LINK3args(xdr_t * xdrs, LINK3args *args)
{
        int ret;

        ret = xdr_nfs_fh3(xdrs, &args->file);
        if (ret)
                GOTO(err_ret, ret);

        ret = xdr_diropargs(xdrs, &args->link);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int xdr_LINK3resok(xdr_t *xdrs, LINK3resok *ok)
{
        int ret;

        ret = xdr_post_op_attr(xdrs, &ok->file_attributes);
        if (ret)
                GOTO(err_ret, ret);

        ret = xdr_wccdata(xdrs, &ok->linkdir_wcc);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int xdr_LINK3resfail(xdr_t *xdrs, LINK3resfail *fail)
{
        int ret;

        ret = xdr_post_op_attr(xdrs, &fail->file_attributes);
        if (ret)
                GOTO(err_ret, ret);

        ret = xdr_wccdata(xdrs, &fail->linkdir_wcc);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int xdr_linkret(xdr_t * xdrs, LINK3res * res)
{
        if (xdr_nfs3_stat(xdrs, &res->status))
                return __FALSE;

        switch (res->status) {
        case NFS3_OK:
                if (xdr_LINK3resok(xdrs, &res->u.ok))
                        return __FALSE;
                break;
        default:
                if (xdr_LINK3resfail(xdrs, &res->u.fail))
                        return __FALSE;
                break;
        }
        return __TRUE;
}

int xdr_renameargs(xdr_t * xdrs, rename_args *objp)
{
    if (xdr_diropargs(xdrs, &objp->from))
        return __FALSE;
    if (xdr_diropargs(xdrs, &objp->to))
        return __FALSE;
    return __TRUE;
}

int xdr_renameretok(xdr_t * xdrs, rename_retok *objp)
{
    if (xdr_wccdata(xdrs, &objp->from))
        return __FALSE;
    if (xdr_wccdata(xdrs, &objp->to))
        return __FALSE;
    return __TRUE;
}

int xdr_renameretfail(xdr_t * xdrs, rename_retfail * objp)
{
    if (xdr_wccdata(xdrs, &objp->from))
        return __FALSE;
    if (xdr_wccdata(xdrs, &objp->to))
        return __FALSE;
    return __TRUE;
}

int xdr_renameret(xdr_t * xdrs, rename_ret * objp)
{
    if (xdr_nfs3_stat(xdrs, &objp->status))
        return __FALSE;
    switch (objp->status) {
        case NFS3_OK:
            if (xdr_renameretok(xdrs, &objp->u.ok))
                return __FALSE;
            break;
        default:
            if (xdr_renameretfail(xdrs, &objp->u.fail))
                return __FALSE;
            break;
    }
    return __TRUE;
}

int xdr_rmdirargs(xdr_t *xdrs, rmdir_args *args)
{
        if (xdr_diropargs(xdrs, &args->obj))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_rmdirret(xdr_t *xdrs, rmdir_ret *ret)
{
        if ((xdr_nfs3_stat(xdrs, &ret->status))
            || (xdr_wccdata(xdrs, &ret->dir_wcc)))
                return __FALSE;

        return __TRUE;
}

int xdr_cookieverf(xdr_t *xdrs, char *verf)
{
        if (__xdr_opaque(xdrs, verf, NFS3_COOKIEVERFSIZE))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_readdirargs(xdr_t *xdrs, readdir_args *args)
{
        if ((xdr_nfs_fh3(xdrs, &args->dir))
            || (__xdr_uint64(xdrs, &args->cookie))
            || (xdr_cookieverf(xdrs, args->cookieverf))
            || (__xdr_uint32(xdrs, &args->count)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_entry(xdr_t *xdrs, entry *en)
{
        if ((__xdr_uint64(xdrs, &en->fileid))
            || (xdr_name(xdrs, &en->name))
            || (__xdr_uint64(xdrs, &en->cookie))
            || (__xdr_pointer(xdrs, (char **)&en->next, sizeof(entry),
                             (__xdrproc_t)xdr_entry)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_dirlist(xdr_t *xdrs, dirlist *list)
{
        if ((__xdr_pointer(xdrs, (char **)&list->entries, sizeof(entry),
                          (__xdrproc_t)xdr_entry))
            || (__xdr_bool(xdrs, &list->eof)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_readdirretok(xdr_t *xdrs, readdir_retok *ok)
{
        if ((xdr_post_op_attr(xdrs, &ok->dir_attr))
            || (xdr_cookieverf(xdrs, ok->cookieverf))
            || (xdr_dirlist(xdrs, &ok->reply)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_readdirretfail(xdr_t *xdrs, readdir_retfail *fail)
{
        if (xdr_post_op_attr(xdrs, &fail->dir_attr))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_readdirret(xdr_t *xdrs, readdir_ret *ret)
{
        if (xdr_nfs3_stat(xdrs, &ret->status))
                return __FALSE;

        switch (ret->status) {
        case NFS3_OK:
                if (xdr_readdirretok(xdrs, &ret->u.ok))
                        return __FALSE;
                break;
        default:
                if (xdr_readdirretfail(xdrs, &ret->u.fail))
                        return __FALSE;
                break;
        }

        return __TRUE;
}

int xdr_readdirplusargs(xdr_t *xdrs, readdirplus_args *args)
{
        if ((xdr_nfs_fh3(xdrs, &args->dir))
            || (__xdr_uint64(xdrs, &args->cookie))
            || (xdr_cookieverf(xdrs, args->cookieverf))
            || (__xdr_uint32(xdrs, &args->dircount))
            || (__xdr_uint32(xdrs, &args->maxcount)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_entryplus(xdr_t *xdrs, entryplus *en)
{
        if ((__xdr_uint64(xdrs, &en->fileid))
            || (xdr_name(xdrs, &en->name))
            || (__xdr_uint64(xdrs, &en->cookie))
            || (xdr_post_op_attr(xdrs, &en->attr))
            || (xdr_postop_fh(xdrs, &en->fh))
            || (__xdr_pointer(xdrs, (char **)&en->next, sizeof(entryplus),
                             (__xdrproc_t)xdr_entryplus)))
                return __FALSE;
        else {
                DBUG("name %s "CHKID_FORMAT", fileid %ju, next %p\n",
                     en->name, CHKID_ARG((chkid_t *)(en->fh.handle.val)),
                      en->fileid, en->next);
                return __TRUE;
        }
}

int xdr_dirlistplus(xdr_t *xdrs, dirlistplus *list)
{
        if ((__xdr_pointer(xdrs, (char **)&list->entries, sizeof(entryplus),
                           (__xdrproc_t)xdr_entryplus))
            || (__xdr_bool(xdrs, &list->eof)))
                return __FALSE;
        else {
                DBUG("eof %u\n", list->eof);
                return __TRUE;
        }
}

int xdr_readdirplusretok(xdr_t *xdrs, readdirplus_retok *ok)
{
        if ((xdr_post_op_attr(xdrs, &ok->dir_attr))
            || (xdr_cookieverf(xdrs, ok->cookieverf))
            || (xdr_dirlistplus(xdrs, &ok->reply)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_readdirplusretfail(xdr_t *xdrs, readdirplus_retfail *fail)
{
        if (xdr_post_op_attr(xdrs, &fail->dir_attr))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_readdirplusret(xdr_t *xdrs, readdirplus_ret *ret)
{
        if (xdr_nfs3_stat(xdrs, &ret->status))
                return __FALSE;

        switch (ret->status) {
        case NFS3_OK:
                if (xdr_readdirplusretok(xdrs, &ret->u.ok))
                        return __FALSE;
                break;
        default:
                if (xdr_readdirplusretfail(xdrs, &ret->u.fail))
                        return __FALSE;
                break;
        }

        return __TRUE;
}

int xdr_fsstatargs(xdr_t *xdrs, fsstat_args *args)
{
        if (xdr_nfs_fh3(xdrs, &args->fsroot))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_fsstatretok(xdr_t *xdrs, fsstat_retok *ok)
{
        if ((xdr_post_op_attr(xdrs, &ok->attr))
            || (__xdr_uint64(xdrs, &ok->tbytes))
            || (__xdr_uint64(xdrs, &ok->fbytes))
            || (__xdr_uint64(xdrs, &ok->abytes))
            || (__xdr_uint64(xdrs, &ok->tfiles))
            || (__xdr_uint64(xdrs, &ok->ffiles))
            || (__xdr_uint64(xdrs, &ok->afiles))
            || (__xdr_uint32(xdrs, &ok->invarsec)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_fsstatretfail(xdr_t *xdrs, fsstat_retfail *fail)
{
        if (xdr_post_op_attr(xdrs, &fail->attr)) {
                DERROR("fail->attr\n");
                return __FALSE;
        } else
                return __TRUE;
}

int xdr_fsstatret(xdr_t *xdrs, fsstat_ret *ret)
{
        if (xdr_nfs3_stat(xdrs, &ret->status)) {
                DERROR("ret->status\n");
                return __FALSE;
        }

        switch (ret->status) {
        case NFS3_OK:
                if (xdr_fsstatretok(xdrs, &ret->u.ok)) {
                        DERROR("ret->u.ok\n");
                        return __FALSE;
                }

                break;
        default:
                if (xdr_fsstatretfail(xdrs, &ret->u.fail)) {
                        DERROR("ret->u.fail\n");
                        return __FALSE;
                }

                break;
        }

        return __TRUE;
}

int xdr_fsinfoargs(xdr_t *xdrs, fsinfo_args *args)
{
        if (xdr_nfs_fh3(xdrs, &args->fsroot))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_fsinforetok(xdr_t *xdrs, fsinfo_retok *ok)
{
        if ((xdr_post_op_attr(xdrs, &ok->obj_attr))
            || (__xdr_uint32(xdrs, &ok->rtmax))
            || (__xdr_uint32(xdrs, &ok->rtpref))
            || (__xdr_uint32(xdrs, &ok->rtmult))
            || (__xdr_uint32(xdrs, &ok->wtmax))
            || (__xdr_uint32(xdrs, &ok->wtpref))
            || (__xdr_uint32(xdrs, &ok->wtmult))
            || (__xdr_uint32(xdrs, &ok->dtpref))
            || (__xdr_uint64(xdrs, &ok->maxfilesize))
            || (xdr_time(xdrs, &ok->time_delta))
            || (__xdr_uint32(xdrs, &ok->properties)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_fsinforetfail(xdr_t *xdrs, fsinfo_retfail *fail)
{
        if (xdr_post_op_attr(xdrs, &fail->obj_attr))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_fsinforet(xdr_t *xdrs, fsinfo_ret *ret)
{
        if (xdr_nfs3_stat(xdrs, &ret->status))
                return __FALSE;

        switch (ret->status) {
        case NFS3_OK:
                if (xdr_fsinforetok(xdrs, &ret->u.ok))
                        return __FALSE;
                break;
        default:
                if (xdr_fsinforetfail(xdrs, &ret->u.fail))
                        return __FALSE;
                break;
        }

        return __TRUE;
}

int xdr_pathconf3_args(xdr_t * xdrs, pathconf3_args * objp)
{
    if (xdr_nfs_fh3(xdrs, &objp->object))
        return __FALSE;

    return __TRUE;
}


int xdr_pathconf3_retok(xdr_t * xdrs, pathconf3_resok * objp)
{
    if (xdr_post_op_attr(xdrs, &objp->obj_attributes))
        return __FALSE;
    if (__xdr_uint32(xdrs, &objp->linkmax))
        return __FALSE;
    if (__xdr_uint32(xdrs, &objp->name_max))
        return __FALSE;
    if (__xdr_bool(xdrs, &objp->no_trunc))
        return __FALSE;
    if (__xdr_bool(xdrs, &objp->chown_restricted))
        return __FALSE;
    if (__xdr_bool(xdrs, &objp->case_insensitive))
        return __FALSE;
    if (__xdr_bool(xdrs, &objp->case_preserving))
        return __FALSE;
    return __TRUE;
}

int xdr_pathconf3_retfail(xdr_t * xdrs, pathconf3_resfail * objp)
{
    if (xdr_post_op_attr(xdrs, &objp->obj_attributes))
        return __FALSE;
    return __TRUE;
}

int xdr_pathconf3_ret(xdr_t * xdrs, pathconf3_res * objp)
{
    if (xdr_nfs3_stat(xdrs, &objp->status))
        return __FALSE;
    switch (objp->status) {
        case NFS3_OK:
            if (xdr_pathconf3_retok(xdrs, &objp->pathconf3_res_u.resok))
                return __FALSE;
            break;
        default:
            if (xdr_pathconf3_retfail(xdrs, &objp->pathconf3_res_u.resfail))
                return __FALSE;
            break;
    }
    return __TRUE;
}

int xdr_commitargs(xdr_t *xdrs, commit_args *args)
{
        if ((xdr_nfs_fh3(xdrs, &args->file))
            || (__xdr_uint64(xdrs, &args->offset))
            || (__xdr_uint32(xdrs, &args->count)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_commitretok(xdr_t *xdrs, commit_retok *ok)
{
        if ((xdr_wccdata(xdrs, &ok->file_wcc))
            || (xdr_writeverf(xdrs, ok->verf)))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_commitretfail(xdr_t *xdrs, commit_retfail *fail)
{
        if (xdr_wccdata(xdrs, &fail->file_wcc))
                return __FALSE;
        else
                return __TRUE;
}

int xdr_commitret(xdr_t *xdrs, commit_ret *ret)
{
        if (xdr_nfs3_stat(xdrs, &ret->status))
                return __FALSE;

        switch (ret->status) {
        case NFS3_OK:
                if (xdr_commitretok(xdrs, &ret->u.ok))
                        return __FALSE;
                break;
        default:
                if (xdr_commitretfail(xdrs, &ret->u.fail))
                        return __FALSE;
                break;
        }

        return __TRUE;
}

int xdr_sattrguard3(xdr_t * xdrs, sattrguard3 * objp)
{
    if (__xdr_bool(xdrs, &objp->check))
        return __FALSE;
    switch (objp->check) {
        case TRUE:
            if (xdr_time(xdrs, &objp->sattrguard3_u.obj_ctime))
                return __FALSE;
            break;
        case FALSE:
            break;
        default:
            return __FALSE;
    }
    return __TRUE;
}

int xdr_setattrargs(xdr_t * xdrs, setattr3_args * args)
{
    if (xdr_nfs_fh3(xdrs, &args->obj))
        return __FALSE;
    if (xdr_sattr(xdrs, &args->new_attributes))
        return __FALSE;
    if (xdr_sattrguard3(xdrs, &args->guard))
        return __FALSE;

    return __TRUE;
}

int xdr_setattr3resok(xdr_t * xdrs, setattr3_resok * objp)
{
    if (xdr_wccdata(xdrs, &objp->obj_wcc))
        return __FALSE;
    return __TRUE;
}

int xdr_setattr3resfail(xdr_t * xdrs, setattr3_resfail * objp)
{
    if (xdr_wccdata(xdrs, &objp->obj_wcc))
        return __FALSE;
    return __TRUE;
}

int xdr_setattrret(xdr_t * xdrs, setattr3_res * objp)
{
    if (xdr_nfs3_stat(xdrs, &objp->status))
        return __FALSE;
    switch (objp->status) {
        case NFS3_OK:
            if (xdr_setattr3resok(xdrs, &objp->setattr3_res_u.resok))
                return __FALSE;
            break;
        default:
            if (xdr_setattr3resfail(xdrs, &objp->setattr3_res_u.resfail))
                return __FALSE;
            break;
    }
    return __TRUE;
}

/* symlink */
int xdr_symlinkresok(xdr_t *xdrs, symlink_retok *ok)
{
        if (xdr_postop_fh(xdrs, &ok->obj)
            || xdr_post_op_attr(xdrs, &ok->obj_attributes)
            || xdr_wccdata(xdrs, &ok->dir_wcc))
                return __FALSE;

        return __TRUE;
}

int xdr_symlinkresfail(xdr_t *xdrs, symlink_retfail *fail)
{
        if (xdr_wccdata(xdrs, &fail->dir_wcc))
                return __FALSE;
        return __TRUE;
}

int xdr_symlinkret(xdr_t * xdrs, symlink_res * res)
{
        if (xdr_nfs3_stat(xdrs, &res->status))
                return __FALSE;

        switch (res->status) {
        case NFS3_OK:
                if (xdr_symlinkresok(xdrs, &res->u.ok))
                        return __FALSE;
                break;
        default:
                if (xdr_symlinkresfail(xdrs, &res->u.fail))
                        return __FALSE;
                break;
        }
        return __TRUE;
}

int xdr_symlinkdata(xdr_t *xdrs, symlinkdata3 *data)
{
        if (xdr_sattr(xdrs, &data->symlink_attributes)
            || xdr_nfspath3(xdrs, &data->symlink_data))
                return __FALSE;

        return __TRUE;
}

int xdr_symlinkargs(xdr_t *xdrs, symlink_args *args)
{
        if ((xdr_diropargs(xdrs, &args->where))
            || (xdr_symlinkdata(xdrs, &args->symlink)))
                return __FALSE;
        else
                return __TRUE;
}

/* mknod */
int xdr_mknodresok(xdr_t *xdrs, mknod_resok *ok)
{
        if (xdr_postop_fh(xdrs, &ok->obj) ||
            xdr_post_op_attr(xdrs, &ok->obj_attributes) ||
            xdr_wccdata(xdrs, &ok->dir_wcc))
                return __FALSE;

        return __TRUE;
}

int xdr_mknodresfail(xdr_t *xdrs, mknod_resfail *fail)
{
        if (xdr_wccdata(xdrs, &fail->dir_wcc))
                return __FALSE;

        return __TRUE;
}

int xdr_mknodret(xdr_t *xdrs, mknod_res *res)
{
        if (xdr_nfs3_stat(xdrs, &res->status))
                return __FALSE;
        switch (res->status) {
        case NFS3_OK:
                if (xdr_mknodresok(xdrs, &res->u.ok))
                        return __FALSE;
                break;
        default:
                if (xdr_mknodresfail(xdrs, &res->u.fail))
                        return __FALSE;
                break;
        }

        return __TRUE;
}

int xdr_devicedata(xdr_t *xdrs, devicedata3 *dev_data)
{
        if (xdr_sattr(xdrs, &dev_data->dev_attributes) ||
            xdr_specdata(xdrs, &dev_data->spec))
                return __FALSE;

        return __TRUE;
}

int xdr_mknoddata(xdr_t *xdrs, mknoddata3 *nod_data)
{
        int ret = __TRUE;
        if (__xdr_enum(xdrs, (enum_t *)&nod_data->type))
                return __FALSE;
        switch (nod_data->type) {
        case NFS3_CHR:
        case NFS3_BLK:
                if (xdr_devicedata(xdrs, &nod_data->mknoddata3_u.device))
                        ret = __FALSE;
                break;
        case NFS3_SOCK:
        case NFS3_FIFO:
                if (xdr_sattr(xdrs, &nod_data->mknoddata3_u.pipe_attributes))
                        ret = __FALSE;
                break;
        default:
                ret = __FALSE;
                break;

        }

        return ret;
}

int xdr_mknodargs(xdr_t *xdrs, mknod_args *args)
{
        if (xdr_diropargs(xdrs, &args->where) ||
            xdr_mknoddata(xdrs, &args->what))
                return __FALSE;
        else
                return __TRUE;
}

/* readlink */
int xdr_readlinkargs(xdr_t *xdrs, readlink_args *args)
{
        if (xdr_nfs_fh3(xdrs, &args->symlink))
                return __FALSE;
        return __TRUE;
}

int xdr_readlinkresok(xdr_t *xdrs, readlink_retok *ok)
{
        if (xdr_post_op_attr(xdrs, &ok->symlink_attributes) ||
            xdr_nfspath3(xdrs, &ok->data))
                return __FALSE;
        return __TRUE;
}

int xdr_readlinkresfail(xdr_t *xdrs, readlink_retfail *fail)
{
        if (xdr_post_op_attr(xdrs, &fail->symlink_attributes))
                return __FALSE;
        return __TRUE;
}

int xdr_readlinkret(xdr_t * xdrs, readlink_res * res)
{
        if (xdr_nfs3_stat(xdrs, &res->status))
                return __FALSE;

        switch (res->status) {
        case NFS3_OK:
                if (xdr_readlinkresok(xdrs, &res->u.ok))
                        return __FALSE;
                break;
        default:
                if (xdr_readlinkresfail(xdrs, &res->u.fail))
                        return __FALSE;
                break;
        }
        return __TRUE;
}

int xdr_groupnode(xdr_t * xdrs, groupnode * objp);
int xdr_exports(xdr_t * xdrs, exports * objp);
int xdr_exportnode(xdr_t * xdrs, exportnode * objp);

int xdr_groups(xdr_t * xdrs, groups * objp)
{
    if (__xdr_pointer
	(xdrs, (char **)objp, sizeof(struct groupnode),
	 (__xdrproc_t) xdr_groupnode))
	return __FALSE;
    return __TRUE;
}

int xdr_groupnode(xdr_t * xdrs, groupnode * objp)
{
    if (xdr_name(xdrs, &objp->gr_name))
	return __FALSE;
    if (xdr_groups(xdrs, &objp->gr_next))
	return __FALSE;
    return __TRUE;
}

int xdr_exports(xdr_t * xdrs, exports * objp)
{
    if (__xdr_pointer
	(xdrs, (char **)objp, sizeof(struct exportnode),
	 (__xdrproc_t) xdr_exportnode))
	return __FALSE;
    return __TRUE;
}

int xdr_exportnode(xdr_t * xdrs, exportnode * objp)
{
    if (xdr_dirpath(xdrs, &objp->ex_dir))
	return __FALSE;
    if (xdr_groups(xdrs, &objp->ex_groups))
	return __FALSE;
    if (xdr_exports(xdrs, &objp->ex_next))
	return __FALSE;
    return __TRUE;
}

int xdr_dump(xdr_t * xdrs,  mountlist *objp);

int xdr_mountbody(xdr_t * xdrs, struct mountbody * objp)
{
    if (xdr_name(xdrs, &objp->ml_hostname))
	return __FALSE;
    if (xdr_dirpath(xdrs, &objp->ml_directory))
	return __FALSE;
    if (xdr_dump(xdrs, &objp->ml_next))
	return __FALSE;
    return __TRUE;
}

int xdr_dump(xdr_t * xdrs, mountlist *objp)
{
    if (__xdr_pointer
	(xdrs, (char **)objp, sizeof(struct mountbody),
	 (__xdrproc_t) xdr_mountbody))
	return __FALSE;
    return __TRUE;
}

inline uint64_t hash_getattr(getattr_args *args)
{
        return fileid_hash((fileid_t *)(args->obj.val));
}

inline uint64_t hash_lookup(lookup_args *args)
{
        return fileid_hash((fileid_t *)(args->dir.val));
}

inline uint64_t hash_access(access_args *args)
{
        return fileid_hash((fileid_t *)(args->obj.val));
}

inline uint64_t hash_read(read_args *args)
{
        return fileid_hash((fileid_t *)(args->file.val));
}

inline uint64_t hash_write(write_args *args)
{
        return fileid_hash((fileid_t *)(args->file.val));
}

inline uint64_t hash_create(create_args *args)
{
        return fileid_hash((fileid_t *)(args->where.dir.val));
}

inline uint64_t hash_mkdir(mkdir_args *args)
{
        return fileid_hash((fileid_t *)(args->where.dir.val));
}

inline uint64_t hash_remove(remove_args *args)
{
#if 0
        return fileid_hash((fileid_t *)(args->obj.dir.val));
#else
        (void) args;
        return 0;
#endif
}

inline uint64_t hash_rmdir(rmdir_args *args)
{
        return fileid_hash((fileid_t *)(args->obj.dir.val));
}

inline uint64_t hash_readdir(readdir_args *args)
{
        return fileid_hash((fileid_t *)(args->dir.val));
}

inline uint64_t hash_readdirplus(readdirplus_args *args)
{
        return fileid_hash((fileid_t *)(args->dir.val));
}

inline uint64_t hash_fsstat(fsstat_args *args)
{
        return fileid_hash((fileid_t *)(args->fsroot.val));
}

inline uint64_t hash_fsinfo(fsinfo_args *args)
{
        return fileid_hash((fileid_t *)(args->fsroot.val));
}

inline uint64_t hash_pathconf3(pathconf3_args *args)
{
        return fileid_hash((fileid_t *)(args->object.val));
}

inline uint64_t hash_setattr(setattr3_args *args)
{
        return fileid_hash((fileid_t *)(args->obj.val));
}



#include <errno.h>

#define DBG_SUBSYS S_YNFS

#include "nfs3.h"
#include "dbg.h"

static int is_stale(int syserr)
{
        if (syserr == ENOTDIR || syserr == ELOOP || syserr == ENOENT
            || syserr == ENAMETOOLONG)
                return 1;
        else
                return 0;
}

nfs3_stat lookup_err(int syserr)
{
        if (syserr == ENOENT)
                return NFS3_ENOENT;
        else if (syserr == EACCES)
                return NFS3_EACCES;
        else if (syserr == ENOTDIR || syserr == ELOOP || syserr == ENAMETOOLONG)
                return NFS3_ESTALE;
        else if (syserr == EINVAL)
                return NFS3_EINVAL;
        else
                return NFS3_EIO;
}

nfs3_stat rename_err(int syserr)
{
    if (syserr == EISDIR)
        return NFS3_EISDIR;
    else if (syserr == ENOSYS)
            return NFS3_ENOTSUPP;
    else if (syserr == EXDEV)
        return NFS3_EXDEV;
    else if (syserr == EEXIST)
        return NFS3_EEXIST;
    else if (syserr == ENOTEMPTY)
        return NFS3_ENOTEMPTY;
    else if (syserr == EINVAL)
        return NFS3_EINVAL;
    else if (syserr == ENOTDIR)
        return NFS3_ENOTDIR;
    else if (syserr == EACCES || syserr == EPERM)
        return NFS3_EACCES;
    else if (syserr == ENOENT)
        return NFS3_ENOENT;
    else if (syserr == ELOOP || syserr == ENAMETOOLONG)
        return NFS3_ESTALE;
    else if (syserr == EROFS)
        return NFS3_EROFS;
    else if (syserr == ENOSPC)
        return NFS3_ENOSPC;
#ifdef EDQUOT
    else if (syserr == EDQUOT)
        return NFS3_EDQUOT;
#endif
    else
        return NFS3_EIO;
}

nfs3_stat read_err(int syserr)
{
        if (syserr == EINVAL)
                return NFS3_EINVAL;
        else if (is_stale(syserr))
                return NFS3_ESTALE;
        else if (syserr == EACCES)
                return NFS3_EACCES;
        else if (syserr == ENXIO || syserr == ENODEV)
                return NFS3_ENXIO;
        else
                return NFS3_EIO;
}

nfs3_stat write_err(int syserr)
{
        if (syserr == EACCES)
                return NFS3_EACCES;
        else if (syserr == EFBIG)
                return NFS3_EFBIG;
        else if (syserr == ENOSPC)
                return NFS3_ENOSPC;
        else if (is_stale(syserr))
                return NFS3_ESTALE;
        else if (syserr == EINVAL)
                return NFS3_EINVAL;
        else if (syserr == EROFS)
                return NFS3_EROFS;
        else if (syserr == EDQUOT)
                return NFS3_EDQUOT;
        else
                return NFS3_EIO;
}

nfs3_stat create_err(int syserr)
{
        if (syserr == EACCES)
                return NFS3_EACCES;
        else if (is_stale(syserr))
                return NFS3_ESTALE;
        else if (syserr == EROFS)
                return NFS3_EROFS;
        else if (syserr == ENOSPC)
                return NFS3_ENOSPC;
        else if (syserr == EEXIST)
                return NFS3_EEXIST;
        else if (syserr == ENOSYS)
                return NFS3_ENOTSUPP;
        else if (syserr == EDQUOT)
                return NFS3_EDQUOT;
        else
                return NFS3_EIO;
}

nfs3_stat mkdir_err(int syserr)
{
        if (syserr == EACCES || syserr == EPERM)
                return NFS3_EACCES;
        else if (is_stale(syserr))
                return NFS3_ESTALE;
        else if (syserr == EROFS)
                return NFS3_EROFS;
        else if (syserr == EEXIST)
                return NFS3_EEXIST;
        else if (syserr == ENOSPC)
                return NFS3_ENOSPC;
        else if (syserr == ENOSYS)
                return NFS3_ENOTSUPP;
        else if (syserr == EINVAL)
                return NFS3_EINVAL;
        else if (syserr == EDQUOT)
                return NFS3_EDQUOT;
        else
                return NFS3_EIO;
}

nfs3_stat setattr_err(int syserr)
{
        if (syserr == EPERM)
                return NFS3_EPERM;
        else if (syserr == EROFS)
                return NFS3_EROFS;
        else if (is_stale(syserr))
                return NFS3_ESTALE;
        else if (syserr == EACCES)
                return NFS3_EACCES;
#ifdef EDQUOT
        else if (syserr == EDQUOT)
                return NFS3_EDQUOT;
#endif
        else if (syserr == EINVAL)
                return NFS3_EINVAL;
        else if (syserr == EFBIG)
                return NFS3_EFBIG;
        else
                return NFS3_EIO;
}

nfs3_stat remove_err(int syserr)
{
        if (syserr == EACCES || syserr == EPERM)
                return NFS3_EACCES;
        else if (syserr == ENOENT)
                return NFS3_ENOENT;
        else if (syserr == ENOTDIR || syserr == ELOOP || syserr == ENAMETOOLONG)
                return NFS3_ESTALE;
        else if (syserr == EINVAL)
                return NFS3_EINVAL;
        else if (syserr == EROFS)
                return NFS3_EROFS;
        else
                return NFS3_EIO;
}

nfs3_stat rmdir_err(int syserr)
{
        if (syserr == ENOTEMPTY)
                return NFS3_ENOTEMPTY;
        else
                return remove_err(syserr);
}

nfs3_stat readdir_err(int syserr)
{
        if (syserr == EPERM)
                return NFS3_EPERM;
        else if (syserr == EACCES)
                return NFS3_EACCES;
        else if (syserr == ENOTDIR)
                return NFS3_ENOTDIR;
        else if (is_stale(syserr))
                return NFS3_ESTALE;
        else if (syserr == EINVAL)
                return NFS3_EINVAL;
        else if (syserr == ENOENT)
                return NFS3_ENOENT;
        else
                return NFS3_EIO;
}

nfs3_stat symlink_err(int syserr)
{
        if (syserr == EPERM)
                return NFS3_EPERM;
        else if (syserr == EACCES)
                return NFS3_EACCES;
        else if (syserr == ENOTDIR)
                return NFS3_ENOTDIR;
        else if (is_stale(syserr))
                return NFS3_ESTALE;
        else if (syserr == EINVAL)
                return NFS3_EINVAL;
        else if (syserr == ENOENT)
                return NFS3_ENOENT;
        else if (syserr == ENOSYS)
                return NFS3_ENOTSUPP;
        else if (syserr == EDQUOT)
                return NFS3_EDQUOT;
        else
                return NFS3_EIO;
}

nfs3_stat mknod_err(int syserr)
{
        if (syserr == EACCES || syserr == EPERM)
                return NFS3_EACCES;
        else if (is_stale(syserr))
                return NFS3_ESTALE;
        else if (syserr == EROFS)
                return NFS3_EROFS;
        else if (syserr == EEXIST)
                return NFS3_EEXIST;
        else if (syserr == ENOSPC)
                return NFS3_ENOSPC;
        else if (syserr == EINVAL)
                return NFS3_EINVAL;
        else if (syserr == ENOSYS)
                return NFS3_ENOTSUPP;
        else if (syserr == EDQUOT)
                return NFS3_EDQUOT;
        else
                return NFS3_EIO;
}

nfs3_stat join(nfs3_stat x, nfs3_stat y)
{
    return (x != NFS3_OK) ? x : y;
}



#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <string.h>

#define DBG_SUBSYS S_YFSLIB

#include "md_lib.h"
#include "rep_proto.h"
#include "ylib.h"
#include "yfscli_conf.h"
#include "yfs_file.h"
#include "net_global.h"
#include "sdfs_lib.h"
#include "file_table.h"
#include "chk_meta.h"
#include "network.h"
#include "dbg.h"
#include "xattr.h"

int yrecycle_fd;

int ly_getattr(const char *path, struct stat *stbuf)
{
        int ret;
        fileid_t fileid;

        YASSERT(path[0] != '\0');

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_getattr(&fileid, stbuf);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int ly_mkdir(const char *path, const ec_t *ec, mode_t mode)
{
        int ret;
        fileid_t parent;
        char name[MAX_NAME_LEN];
        /*add by wangyingjie for uid and gid*/
        uid_t uid = 0;
        gid_t gid = 0;

        (void) ec;
        
        ret = sdfs_splitpath(path, &parent, name);
        if (ret)
                GOTO(err_ret, ret);

        /*add by wangyingjie for uid and gid*/
        uid = geteuid();
        gid = getegid();

        ret = sdfs_mkdir(&parent, name, ec, NULL, mode, uid, gid);
        if (ret)
                GOTO(err_ret, ret);

        DBUG("dir (%s) created\n", path);

        return 0;
err_ret:
        return ret;
}

int ly_chmod(const char *path, mode_t mode)
{
        int ret;
        fileid_t fileid;

        YASSERT(path[0] != '\0');

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_chmod(&fileid, mode);
        if (ret)
                GOTO(err_ret, ret);

        DBUG("chmod (%s) \n", path);

        return 0;
err_ret:
        return ret;
}

int ly_chown(const char *path, uid_t uid, gid_t gid)
{
        int ret;
        fileid_t fileid;

        YASSERT(path[0] != 0);

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_chown(&fileid, uid, gid);
        if (ret)
                GOTO(err_ret, ret);

        DBUG("chown (%s) \n", path);

        return 0;
err_ret:
        return ret;
}

int ly_unlink(const char *path)
{
        int ret;
        fileid_t parent;
        char name[MAX_NAME_LEN];

        ret = sdfs_splitpath(path, &parent, name);
        if (ret) {
                GOTO(err_ret, ret);
        }

        ret = sdfs_unlink(&parent, name);
        if (ret) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int ly_rename(const char *from, const char *to)
{
        int ret, empty = 0;
        char fname[MAX_NAME_LEN], tname[MAX_NAME_LEN];
        fileid_t fparent, tparent;
        fileid_t from_fileid, to_fileid;
        struct stat from_sbuf, to_sbuf;

        YASSERT(from[0] != '\0');
        YASSERT(to[0] != '\0');

        ret = sdfs_splitpath(from, &fparent, fname);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_splitpath(to, &tparent, tname);
        if (ret)
                GOTO(err_ret, ret);

again:
        ret = md_rename(&fparent, fname, &tparent, tname);
        if (ret) {
                if (ret == EPIPE || ret == ETIMEDOUT || ret == ENOTCONN) {
                        ret = network_connect_master();
                        if (ret == 0)
                                goto again;

                        ret = EAGAIN;
                } else if(ret == EEXIST) {
                        ret = sdfs_lookup_recurive(from, &from_fileid);
                        if(ret)
                                GOTO(err_ret, ret);

                        ret = sdfs_lookup_recurive(to, &to_fileid);
                        if(ret)
                                GOTO(err_ret, ret);

                        ret = sdfs_getattr(&from_fileid, &from_sbuf);
                        if(ret)
                                GOTO(err_ret, ret);

                        ret = sdfs_getattr(&to_fileid, &to_sbuf);
                        if(ret)
                                GOTO(err_ret, ret);

                        if(S_ISDIR(from_sbuf.st_mode)) {
                                if(!S_ISDIR(to_sbuf.st_mode)) {
                                        ret = ENOTDIR;
                                        GOTO(err_ret, ret);
                                } else {
                                        uint64_t child;
                                        ret = sdfs_childcount(&to_fileid, &child);
                                        if(ret)
                                                GOTO(err_ret, ret);

                                        empty = !child;
                                        
                                        if(empty) {
                                                ret = sdfs_rmdir(&tparent, tname);
                                                if(ret)
                                                        GOTO(err_ret, ret);

                                                goto again;
                                        } else {
                                                ret = EEXIST;
                                                GOTO(err_ret, ret);
                                        }
                                }
                        } else {
                                if(S_ISDIR(to_sbuf.st_mode)) {
                                        ret = EISDIR;
                                        GOTO(err_ret, ret);
                                } else {
                                        if(fileid_cmp(&from_fileid, &to_fileid)) {
                                                ret = sdfs_unlink(&tparent, tname);
                                                if(ret)
                                                        GOTO(err_ret, ret);

                                                goto again;
                                        }
                                }
                        }
                } else {
                        GOTO(err_ret, ret);
                }

        }

        return 0;
err_ret:
        DWARN("from %s to %s\n", from, to);
        return ret;
}

int ly_rmdir(const char *path)
{
        int ret;
        fileid_t parent;
        char name[MAX_NAME_LEN];

        ret = sdfs_splitpath(path, &parent, name);
        if (ret)
                GOTO(err_ret, ret);

        YASSERT(path[0] != '\0');

        ret = sdfs_rmdir(&parent, name);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}


int ly_statfs(const char *path, struct statvfs *vfs)
{
        int ret;
        fileid_t fileid;

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_statvfs(&fileid, vfs);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int ly_opendir(const char *path)
{
        int ret;
        uint32_t mode;
        fileid_t fileid;
        md_proto_t *md;
        char buf[MAX_BUF_LEN];

        YASSERT(path[0] != '\0');

        md = (void *)buf;

again:
        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret) {
                if (ret == EPIPE || ret == ETIMEDOUT || ret == ENOTCONN) {
                        ret = network_connect_master();
                        if (ret == 0)
                                goto again;

                        ret = EAGAIN;
                } else if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }
        /*
        *BABY
        */
        ret = md_getattr(md, &fileid);
        if (ret) {
                if (ret == EPIPE || ret == ETIMEDOUT || ret == ENOTCONN) {
                        ret = network_connect_master();
                        if (ret == 0)
                                goto again;

                        ret = EAGAIN;
                } else if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        mode = md->at_mode;

        if (!(mode & __S_IFDIR)) {
                ret = ENOTDIR;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int ly_utime(const char *path, uint32_t atime, uint32_t mtime)
{
        int ret;
        fileid_t fileid;
        struct timespec _atime, _mtime;

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        _atime.tv_sec = atime;
        _mtime.tv_sec = mtime;
        ret = sdfs_utime(&fileid, &_atime, &_mtime, &_mtime);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int ly_link2node(const char *path, fileid_t *nodeid)
{
        (void) path;
        (void) nodeid;

        UNIMPLEMENTED(__DUMP__);

        return 0;

#if 0
        int ret;

again:
        ret = md_link2node(path, nodeid);
        if (ret) {
                if (ret == EPIPE || ret == ETIMEDOUT || ret == ENOTCONN) {
                        ret = network_connect_master();
                        if (ret == 0)
                                goto again;

                        ret = EAGAIN;
                }

                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
#endif
}

int ly_setxattr(const char *path, const char *name, const void *value,
                size_t size, int flags)
{
        int ret;
        fileid_t fileid;

        (void) size;
        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);
        ret = sdfs_setxattr(&fileid, name, value, size, flags);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int ly_getxattr(const char *path, const char *name, void *value, size_t *size)
{
        int ret;
        fileid_t fileid;

        YASSERT(size != NULL);
        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_getxattr(&fileid, name, value, size);
        if (ret) {
                if (ret == ENOKEY) {
                        goto err_ret;
                } else {
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

int ly_listxattr(const char *path, char *list, size_t *size)
{
        int ret;
        fileid_t fileid;

        YASSERT(size != NULL);
        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_listxattr(&fileid, list, size);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int ly_removexattr(const char *path, const char *name)
{
        int ret;
        fileid_t fileid;

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_removexattr(&fileid, name);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int ly_setrepnum(const char *path, int repnum)
{
        int ret;
        uint32_t replica;

again:
        replica = (uint32_t)repnum;

        ret = ly_setxattr(path, YFS_XATTR_REPLICA, (const void *)&replica,
                          sizeof(uint32_t), USS_XATTR_DEFAULT);
        if (ret) {
                if (ret == EPIPE || ret == ETIMEDOUT || ret == ENOTCONN) {
                        ret = network_connect_master();
                        if (ret == 0)
                                goto again;

                        ret = EAGAIN;
                }

                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int ly_getrepnum(const char *path)
{
        int ret;
        uint32_t replica;
        size_t size;

again:
        replica = YFS_MIN_REP;
        size = sizeof(uint32_t);

        ret = ly_getxattr(path, YFS_XATTR_REPLICA, (void *)&replica, &size);
        if (ret) {
                if (ret == EPIPE || ret == ETIMEDOUT || ret == ENOTCONN) {
                        ret = network_connect_master();
                        if (ret == 0)
                                goto again;

                        ret = EAGAIN;
                }

                GOTO(err_ret, ret);
        }

        ret = (int)replica;
        return ret;
err_ret:
        return -ret;
}

int ly_setchklen(const char *path, int chklen)
{
        int ret;
        uint32_t chunk;

again:
        chunk = (uint32_t)chklen;

        ret = ly_setxattr(path, YFS_XATTR_CHKLEN, (const void *)&chunk,
                          sizeof(uint32_t), USS_XATTR_DEFAULT);
        if (ret) {
                if (ret == EPIPE || ret == ETIMEDOUT || ret == ENOTCONN) {
                        ret = network_connect_master();
                        if (ret == 0)
                                goto again;

                        ret = EAGAIN;
                }

                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int ly_getchklen(const char *path)
{
        int ret;
        uint32_t chunk;
        size_t size;

again:
        chunk = YFS_CHK_LEN_DEF;
        size = sizeof(uint32_t);

        ret = ly_getxattr(path, YFS_XATTR_CHKLEN, (void *)&chunk, &size);
        if (ret) {
                if (ret == EPIPE || ret == ETIMEDOUT || ret == ENOTCONN) {
                        ret = network_connect_master();
                        if (ret == 0)
                                goto again;

                        ret = EAGAIN;
                }

                GOTO(err_ret, ret);
        }

        ret = (int)chunk;
        return ret;
err_ret:
        return -ret;
}

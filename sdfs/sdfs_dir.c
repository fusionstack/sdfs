#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>

#define DBG_SUBSYS S_YFSLIB

#include "sdfs_id.h"

#include "md_lib.h"
#include "chk_proto.h"
#include "redis_conn.h"
#include "network.h"
#include "net_global.h"
#include "chk_proto.h"
#include "file_table.h"
#include "job_dock.h"
#include "ylib.h"
#include "net_global.h"
#include "yfs_file.h"
#include "cache.h"
#include "sdfs_lib.h"
#include "md_lib.h"
#include "sdfs_chunk.h"
#include "network.h"
#include "yfs_limit.h"
#include "worm_cli_lib.h"
#include "main_loop.h"
#include "posix_acl.h"
#include "flock.h"
#include "xattr.h"
#include "mond_rpc.h"
#include "schedule.h"
#include "io_analysis.h"
#include "dbg.h"

int sdfs_mkdir(sdfs_ctx_t *ctx, const fileid_t *parent, const char *name, const ec_t *ec,
               fileid_t *fileid, uint32_t mode, uint32_t uid, uint32_t gid)
{
        int ret, retry = 0;
        setattr_t setattr;

        (void) ctx;
        
        io_analysis(ANALYSIS_OP_WRITE, 0);
        
        setattr_init(&setattr, mode, -1, ec, uid, gid, -1);
#if 1
        setattr_update_time(&setattr,
                            __SET_TO_SERVER_TIME, NULL,
                            __SET_TO_SERVER_TIME, NULL,
                            __SET_TO_SERVER_TIME, NULL);
#endif

        volid_t volid = {parent->volid, ctx ? ctx->snapvers : 0};
retry:
        ret = md_mkdir(&volid, parent, name, &setattr, fileid);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 10, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        DBUG("dir (%s) created\n", name);

        return 0;
err_ret:
        return ret;
}

int sdfs_lookup_recurive(const char *path, fileid_t *fileid)
{
        int ret, retry = 0;
        char dirname[MAX_PATH_LEN], basename[MAX_NAME_LEN];
        fileid_t parent;

        DBUG("lookup %s\n", path);
        
        if (path[0] == '\0') {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        if (strcmp(path, ROOT_NAME) == 0) {
                memset(fileid, 0x0, sizeof(*fileid));
                fileid->type = ftype_root;
                return 0;
        }

        ret = _path_split2(path, dirname, basename);
        if (ret)
                GOTO(err_ret, ret);

retry:
        if (strcmp(dirname, ROOT_NAME) == 0) {
                ret = md_lookupvol(basename, fileid);
                if (ret) {
                        ret = _errno(ret);
                        if (ret == EAGAIN) {
                                USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                        } else
                                GOTO(err_ret, ret);
                }
        } else {
                ret = sdfs_lookup_recurive(dirname, &parent);
                if (ret) {
                        GOTO(err_ret, ret);
                }

                ret = md_lookup(NULL, fileid, &parent, basename);
                if (ret) {
                        ret = _errno(ret);
                        if (ret == EAGAIN) {
                                USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                        } else
                                GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

int sdfs_mkvol(const char *name, const ec_t *ec, mode_t mode, fileid_t *_fileid)
{
        int ret, retry = 0;
        setattr_t setattr;
        fileid_t fileid;

        io_analysis(ANALYSIS_OP_WRITE, 0);
        
        setattr_init(&setattr, mode, -1, ec, geteuid(), getgid(), -1);
#if 1
        setattr_update_time(&setattr,
                            __SET_TO_SERVER_TIME, NULL,
                            __SET_TO_SERVER_TIME, NULL,
                            __SET_TO_SERVER_TIME, NULL);
#endif
retry:
        ret = md_mkvol(name, &setattr, &fileid);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        if (_fileid) {
                *_fileid = fileid;
        }
        
        return 0;
err_ret:
        return ret;
}

int sdfs_lookupvol(const char *name, fileid_t *fileid)
{
        int ret;

        io_analysis(ANALYSIS_OP_READ, 0);

        ret = md_lookupvol(name, fileid);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

inline static int __etcd_getattr(const char *name, md_proto_t *md)
{
        int ret;
        fileid_t fileid;

        ret = md_lookupvol(name, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        ret = md_getattr(NULL, &fileid, md);
        if (ret)
                GOTO(err_ret, ret);

        DINFO(CHKID_FORMAT" mode %o\n", CHKID_ARG(&md->fileid), md->at_mode);
        
        return 0;
err_ret:
        memset(md, 0x0, sizeof(*md));
        return ret;
}

static int __etcd_listvol__(const char *_key, const etcd_node_t *array,
                            char *buf, int *_buflen, int plus)
{
        int ret, i, buflen, reclen, len;
        struct dirent *de;
        const char *key;
        md_proto_t *md;
        etcd_node_t *node;

        (void) _key;

        de = (void *)buf;
        buflen = *_buflen;
        for (i = 0; i < array->num_node; i++) {
                node = array->nodes[i];
                key = node->key;
                reclen = sizeof(*de) - sizeof(de->d_name) + strlen(key) + 1;
                if (plus)
                        len = reclen + sizeof(md_proto_t);
                else
                        len = reclen;

                if ((void *)de - (void *)buf + len  > buflen) {
                        ret = ENOSPC;
                        GOTO(err_ret, ret);
                }

                strcpy(de->d_name, key);
                de->d_reclen = len;
                de->d_off = 0;
                de->d_type = 0;

                if (plus) {
                        md = (void *)de + reclen;
                        __etcd_getattr(key, md);
                }

                //DBUG("%s : (%s) fileid "CHKID_FORMAT" reclen %u\n", _key,
                //de->d_name, CHKID_ARG(&md->fileid), de->d_reclen);

                de = (void *)de + len;
        }

        *_buflen = (void *)de - (void *)buf;

        return 0;
err_ret:
        return ret;
}

static int __etcd_listvol(void **de, int *delen, int plus)
{
        int ret, buflen;
        etcd_node_t *node = NULL;
        char *buf;

        buflen = MAX_BUF_LEN;

        ret = etcd_list(ETCD_VOLUME, &node);
        if(ret){
                GOTO(err_ret, ret);
        }
        
retry:
        ret = ymalloc((void**)&buf, buflen);
        if (ret)
                GOTO(err_ret, ret);
        
        ret = __etcd_listvol__(ETCD_VOLUME, node, buf, &buflen, plus);
        if(ret) {
                if (ret == ENOSPC) {
                        DWARN("nospace %u\n", buflen);
                        buflen = buflen * 2;
                        yfree((void **)&buf);
                        goto retry;
                } else 
                        GOTO(err_ret, ret);
        }

        free_etcd_node(node);
        *de = buf;
        if (delen) {
                *delen = buflen;
        }

        return 0;
err_ret:
        return ret;
}

int sdfs_readdir1(sdfs_ctx_t *ctx, const fileid_t *fileid, off_t offset, void **de, int *delen)
{
        int ret, retry = 0;

        (void) ctx;
        
        if (offset == 2147483647) {  /* 2GB - 1 */
                *delen = 0;
                return 0;
        }

retry:
        if (fileid->type == ftype_root) {
                ret = __etcd_listvol(de, delen, 0);
        } else {
                volid_t volid = {fileid->volid, ctx ? ctx->snapvers : 0};
                ret = md_readdir(&volid, fileid, offset, de, delen);
        }

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

int sdfs_readdirplus(sdfs_ctx_t *ctx, const fileid_t *fileid, off_t offset, void **de, int *delen)
{
        int ret, retry = 0;

        (void) ctx;

        if (offset == 2147483647) {  /* 2GB - 1 */
                *delen = 0;
                return 0;
        }

        DBUG(""FID_FORMAT"\n", FID_ARG(fileid));

retry:
        if (fileid->type == ftype_root) {
                ret = __etcd_listvol(de, delen, 1);
        } else {
                volid_t volid = {fileid->volid, ctx ? ctx->snapvers : 0};
                ret = md_readdirplus(&volid, fileid, offset, de, delen);
        }

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

int sdfs_readdirplus_with_filter(sdfs_ctx_t *ctx, const fileid_t *fileid, off_t offset,
                                 void **de, int *delen, const filter_t *filter)
{
        int ret, retry = 0;

        (void) ctx;

        if (offset == 2147483647) {  /* 2GB - 1 */
                *delen = 0;
                return 0;
        }

        DBUG(""FID_FORMAT"\n", FID_ARG(fileid));

retry:
        if (fileid->type == ftype_root) {
                ret = __etcd_listvol(de, delen, 1);
        } else {
                volid_t volid = {fileid->volid, ctx ? ctx->snapvers : 0};
                ret = md_readdirplus_with_filter(&volid, fileid, offset, de, delen, filter);
        }

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

int sdfs_rmdir(sdfs_ctx_t *ctx, const fileid_t *parent, const char *name)
{
        int ret, retry = 0;

        (void) ctx;

        io_analysis(ANALYSIS_OP_WRITE, 0);
        
        if (parent->type == ftype_root) {
                return md_rmvol(name);
        }
        
#if ENABLE_WORM
        UNIMPLEMENTED(__WARN__);
        fileid_t fileid;
        worm_t worm;

        memset(&fileid, 0, sizeof(fileid));
        ret = md_lookup(parent, name, &fileid);
        if (ERROR_SUCCESS != ret) {
                goto err_ret;
        }

        ret = worm_get_attr(&fileid, &worm);
        if(ret == 0) {
                DWARN("it is a worm root directory, only allowed to be removed by super user\n");
                ret = EPERM;
                GOTO(err_ret, ret);
        }

        if(ret != ENOKEY) {
                GOTO(err_ret, ret);
        }
#endif

        volid_t volid = {parent->volid, ctx ? ctx->snapvers : 0};
retry:
        ret = md_rmdir(&volid, parent, name);
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

int sdfs_splitpath(const char *path, fileid_t *parent, char *basename)
{
        int ret;
        char dirname[MAX_NAME_LEN];

        _path_split2(path, dirname, basename);
        
        ret = sdfs_lookup_recurive(dirname, parent);
        if (ret) {
                GOTO(err_ret, ret);
        }

        DBUG("name %s path %s parent "FID_FORMAT"\n", basename, path, FID_ARG(parent));

        return 0;
err_ret:
        return ret;
}

int sdfs_lookup(sdfs_ctx_t *ctx, const fileid_t *parent, const char *name, fileid_t *fileid)
{
        int ret, retry = 0;

        (void) ctx;

        io_analysis(ANALYSIS_OP_READ, 0);
        
retry:
        if (parent->type == ftype_root) {
                ret = md_lookupvol(name, fileid);
        } else {
                volid_t volid = {parent->volid, ctx ? ctx->snapvers : 0};
                ret = md_lookup(&volid, fileid, parent, name);
        }
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

int sdfs_statvfs(sdfs_ctx_t *ctx, const fileid_t *fileid, struct statvfs *vfs)
{
        int ret, retry = 0;

        (void) ctx;

        io_analysis(ANALYSIS_OP_READ, 0);
retry:
        ret = mond_rpc_statvfs(net_getnid(), fileid, vfs);
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

int sdfs_symlink(sdfs_ctx_t *ctx, const fileid_t *parent, const char *link_name,
                 const char *link_target, uint32_t mode, uid_t uid, gid_t gid)
{
        int ret, retry = 0;

        (void) ctx;

        io_analysis(ANALYSIS_OP_WRITE, 0);
        volid_t volid = {parent->volid, ctx ? ctx->snapvers : 0};
retry:
        ret = md_symlink(&volid, parent, link_name, link_target, mode, uid, gid);
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

int sdfs_link2node(sdfs_ctx_t *ctx, const fileid_t *old, const fileid_t *parent, const char *name)
{
        int ret, retry = 0;

        (void) ctx;

        io_analysis(ANALYSIS_OP_WRITE, 0);
        volid_t volid = {old->volid, ctx ? ctx->snapvers : 0};
retry:
        ret = md_link2node(&volid, old, parent, name);
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

int sdfs_readlink(sdfs_ctx_t *ctx, const fileid_t *fileid, char *buf, uint32_t *buflen)
{
        int ret, len, retry = 0;
        char link_target[MAX_BUF_LEN];

        (void) ctx;

        io_analysis(ANALYSIS_OP_READ, 0);

        volid_t volid = {fileid->volid, ctx ? ctx->snapvers : 0};
retry:
        ret = md_readlink(&volid, fileid, link_target);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        len = strlen(link_target);
        YASSERT(len < MAX_BUF_LEN);
        link_target[len] = '\0';
        *buflen = len;
        strncpy(buf, link_target, len + 1);

        return 0;
err_ret:
        return ret;
}

int sdfs_unlink(sdfs_ctx_t *ctx, const fileid_t *parent, const char *name)
{
        int ret, retry = 0;
        fileinfo_t *md;
        char buf[MAX_BUF_LEN];

        (void) ctx;

        io_analysis(ANALYSIS_OP_WRITE, 0);
#if ENABLE_WORM
        fileid_t fileid;
        worm_status_t worm_status;

        memset(&fileid, 0, sizeof(fileid));
        ret = md_lookup(&fileid, parent, name);
        if (ret) {
                GOTO(err_ret, ret);
        }

        worm_status = worm_get_status(&fileid);
        if (WORM_IN_PROTECT == worm_status)
        {
                ret = EACCES;
                goto err_ret;
        }
#endif
        volid_t volid = {parent->volid, ctx ? ctx->snapvers : 0};
retry:
        md = (void *)buf;
        ret = md_unlink(&volid, parent, name, (void *)md);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        if (S_ISREG(md->at_mode)) {
#if 1
                ret = md_remove(&volid, &md->fileid);
                if (ret) {
                        DWARN("remove "CHKID_FORMAT" fail\n", CHKID_ARG(&md->fileid));
                }
#endif
        }
        
        return 0;
err_ret:
        return ret;
}

int sdfs_create(sdfs_ctx_t *ctx, const fileid_t *parent, const char *name,
                fileid_t *fileid, uint32_t mode, uint32_t uid, uint32_t gid)
{
        int ret, retry = 0;
        setattr_t setattr;

        (void) ctx;
        
        io_analysis(ANALYSIS_OP_WRITE, 0);
        setattr_init(&setattr, mode, -1, NULL, uid, gid, -1);
#if 1
        setattr_update_time(&setattr,
                            __SET_TO_SERVER_TIME, NULL,
                            __SET_TO_SERVER_TIME, NULL,
                            __SET_TO_SERVER_TIME, NULL);
#endif
        volid_t volid = {parent->volid, ctx ? ctx->snapvers : 0};
retry:
        ret = md_create(&volid, parent, name, &setattr, fileid);
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

static int __sdfs_listvol(dirlist_t **_dirlist)
{
        int ret, i, idx;
        etcd_node_t *array, *node;
        dirlist_t *dir_array;
        __dirlist_t *dir_node;

        ret = etcd_list(ETCD_VOLUME, &array);
        if(ret){
                GOTO(err_ret, ret);
        }
        
        ret = ymalloc((void **)&dir_array, DIRLIST_SIZE(array->num_node));
        if (ret)
                GOTO(err_ret, ret);

        idx = 0;
        for (i = 0; i < array->num_node; i++) {
                dir_node = &dir_array->array[idx];
                node = array->nodes[i];
                dir_node->d_type = __S_IFDIR;
                strcpy(dir_node->name, node->key);

                ret = md_lookupvol(dir_node->name, &dir_node->fileid);
                if (ret) {
                        if (ret == ENOENT) {
                                memset(&dir_node->fileid, 0x0, sizeof(dir_node->fileid));
                                DWARN("volume %s not found\n", dir_node->name);
                                continue;
                        } else
                                GOTO(err_free, ret);
                }

                idx++;
        }
        
        
        free_etcd_node(array);
        DINFO("readdir count %u\n", idx);
        dir_array->count = idx;
        dir_array->cursor = 0;
        dir_array->offset = 0;
        *_dirlist = dir_array;

        return 0;
err_free:
        free_etcd_node(array);
err_ret:
        return ret;
}

int sdfs_dirlist(sdfs_ctx_t *ctx, const dirid_t *dirid, uint32_t count, uint64_t offset, dirlist_t **dirlist)
{
        int ret, retry = 0;

        (void) ctx;
retry:
        if (dirid->type == ftype_root) {
                ret = __sdfs_listvol(dirlist);
        } else {
                volid_t volid = {dirid->volid, ctx ? ctx->snapvers : 0};
                ret = md_dirlist(&volid, dirid, count, offset, dirlist);
        }

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

int sdfs_opendir(sdfs_ctx_t *ctx, const dirid_t *dirid, dirhandler_t **_dirhandler)
{
        int ret;
        struct stat stbuf;
        dirhandler_t *dirhandler;
        
        if (dirid->type != ftype_root) {
                ret = sdfs_getattr(ctx, dirid, &stbuf);
                if (ret)
                        GOTO(err_ret, ret);
                
                if (!S_ISDIR(stbuf.st_mode)) {
                        ret = ENOTDIR;
                        GOTO(err_ret, ret);
                }
        }

        ret = ymalloc((void **)&dirhandler, sizeof(*dirhandler));
        if (ret)
                GOTO(err_ret, ret);
        
        dirhandler->dirid = *dirid;
        memset(&dirhandler->diroff, 0x0, sizeof(dirhandler->diroff));
        dirhandler->dirlist = NULL;

        *_dirhandler = dirhandler;
        
        return 0;
err_ret:
        return ret;
}

void sdfs_closedir(sdfs_ctx_t *ctx, dirhandler_t *dirhandler)
{
        (void) ctx;
        
        if (dirhandler->dirlist) {
                yfree((void **)&dirhandler->dirlist);
        }

        yfree((void **)&dirhandler);
}

int sdfs_readdir(sdfs_ctx_t *ctx, dirhandler_t *dirhandler, struct dirent **_de, fileid_t *_fileid)
{
        int ret, len, retry = 0;
        dirlist_t *dirlist;
        __dirlist_t *node;
        struct dirent *de;
        diroff_t diroff;
        dirid_t *dirid;

        (void) ctx;
        
        DINFO("readdir "CHKID_FORMAT"\n", CHKID_ARG(&dirhandler->dirid));

        diroff = dirhandler->diroff;
        dirid = &dirhandler->dirid;

        if (dirhandler->dirlist) {
                dirlist = dirhandler->dirlist;
                dirlist->cursor = diroff.cursor;
                if (dirlist->cursor == dirlist->count) {
                        DINFO("free seg, total %u offset (%u,%u)\n",
                              dirlist->count, diroff.roff, diroff.cursor);
                        
                        yfree((void **)&dirhandler->dirlist);
                        dirhandler->dirlist = NULL;
                        if (dirid->type == ftype_root) {
                                *_de = NULL;
                                goto out;
                        } else if (diroff.roff == 0) {
                                *_de = NULL;
                                goto out;
                        }
                }
        }
        
        dirlist = dirhandler->dirlist;
        if (dirlist == NULL) {
        retry:
                if (dirid->type == ftype_root) {
                        ret = __sdfs_listvol(&dirlist);
                } else {
                        volid_t volid = {dirid->volid, ctx ? ctx->snapvers : 0};
                        ret = md_dirlist(&volid, dirid, UINT8_MAX / 2, diroff.roff, &dirlist);
                }

                if (ret) {
                        ret = _errno(ret);
                        if (ret == EAGAIN) {
                                USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                        } else
                                GOTO(err_ret, ret);
                }
        }

        if (dirlist->count == 0) {
                *_de = NULL;
                goto out;
        }
        
        de = &dirhandler->de;

        YASSERT(diroff.cursor < dirlist->count);
        static_assert(sizeof(diroff_t) == sizeof(de->d_off), "diroff");
        node = &dirlist->array[diroff.cursor];
        len = sizeof(*de) - sizeof(de->d_name) + strlen(node->name) + 1;

        diroff.cursor++;

        strcpy(de->d_name, node->name);
        memcpy(&de->d_off, &diroff, sizeof(diroff));
        de->d_type = node->d_type;
        de->d_reclen = len;

        if (_fileid) {
                *_fileid = node->fileid;
        }
        
        dirlist->cursor = diroff.cursor;
        diroff.roff = dirlist->offset;
        dirhandler->diroff = diroff;
        dirhandler->dirlist = dirlist;

        DINFO("readdir %s total %u offset (%u,%u)\n",
             node->name, dirlist->count, diroff.roff, diroff.cursor);
        
        *_de = de;

out:
        return 0;
err_ret:
        return ret;
}

long sdfs_telldir(sdfs_ctx_t *ctx, dirhandler_t *dirhandler)
{
        long off;
        static_assert(sizeof(diroff_t) == sizeof(off), "diroff");

        (void) ctx;
        
        io_analysis(ANALYSIS_OP_READ, 0);
        
        DINFO("telldir "CHKID_FORMAT"\n", CHKID_ARG(&dirhandler->dirid));
        
        memcpy(&off, &dirhandler->diroff, sizeof(dirhandler->diroff));

        return off;
}

void sdfs_rewinddir(sdfs_ctx_t *ctx, dirhandler_t *dirhandler)
{
        DINFO("rewinddir "CHKID_FORMAT"\n", CHKID_ARG(&dirhandler->dirid));

        (void) ctx;
        
        io_analysis(ANALYSIS_OP_READ, 0);
        
        if (dirhandler->dirlist) {
                yfree((void **)&dirhandler->dirlist);
                dirhandler->dirlist = NULL;
        }

        memset(&dirhandler->diroff, 0x0, sizeof(dirhandler->diroff));
}


void sdfs_seekdir(sdfs_ctx_t *ctx, dirhandler_t *dirhandler, long loc)
{
        DINFO("seekdir "CHKID_FORMAT"\n", CHKID_ARG(&dirhandler->dirid));

        (void) ctx;
        
        io_analysis(ANALYSIS_OP_READ, 0);
        
        if (dirhandler->dirlist) {
                yfree((void **)&dirhandler->dirlist);
                dirhandler->dirlist = NULL;
        }

        memcpy(&dirhandler->diroff, &loc, sizeof(dirhandler->diroff));
}

static void *__sdfs_ctx_worker(void *arg)
{
        int ret, idx;
        sdfs_ctx_t *ctx = arg;
        char key[MAX_PATH_LEN];
        etcd_session  sess;
        etcd_node_t  *node = NULL;

        snprintf(key, MAX_NAME_LEN, "%s/%s/%s/snapvers", ETCD_ROOT, ETCD_VOLUME, ctx->vol);

        char *host = strdup("localhost:2379");
        sess = etcd_open_str(host);
        if (!sess) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }

        while (ctx->running) {
                ret = etcd_watch(sess, key, &idx, &node, 5);
                if(ret != ETCD_OK){
                        ret = EPERM;
                        GOTO(err_close, ret);
                }

                DINFO("conn watch %s:%s\n", node->key, node->value);
                idx = node->modifiedIndex + 1;
                ctx->snapvers = atol(node->value);

                free_etcd_node(node);
        }

        etcd_close_str(sess);
        free(host);
        yfree((void **)&ctx);
        pthread_exit(NULL);
err_close:
        etcd_close_str(sess);
err_ret:
        free(host);
        yfree((void **)&ctx);
        pthread_exit(NULL);
}


int sdfs_connect(const char *vol, sdfs_ctx_t **_ctx)
{
        int ret;
        sdfs_ctx_t *ctx;
        fileid_t fileid;

        ret = md_lookupvol(vol, &fileid);
        if (ret)
                GOTO(err_ret, ret);
        
        ret = ymalloc((void **)&ctx, sizeof(*ctx));
        if (ret)
                GOTO(err_ret, ret);

        strcpy(ctx->vol, vol);
        ctx->rootid = fileid;
        ctx->snapvers = 0;
        ctx->running = 1;

        ret = sy_thread_create2(__sdfs_ctx_worker, ctx, "ctx_worker");
        if (ret)
                GOTO(err_ret, ret);

        *_ctx = ctx;
        
        return 0;
err_ret:
        return ret;
}
                
void sdfs_disconnect(sdfs_ctx_t *ctx)
{
        ctx->running = 0;
}

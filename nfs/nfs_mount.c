
#include <sys/types.h>
#include <sys/stat.h>
#include <rpc/rpc.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <dirent.h>


#define DBG_SUBSYS S_YNFS

#include "yfs_conf.h"
#include "network.h"

#include "attr.h"
#include "error.h"
#include "job_tracker.h"
#include "job_dock.h"
#include "nfs_job_context.h"
#include "net_global.h"
#include "nfs_events.h"
#include "nfs_conf.h"
#include "nfs_events.h"
#include "nfs_state_machine.h"
#include "readdir.h"
#include "sunrpc_proto.h"
#include "sunrpc_reply.h"
#include "sdfs_lib.h"
#include "xdr_nfs.h"
#include "configure.h"
#include "sdfs_lib.h"
#include "md_lib.h"
#include "yfs_limit.h"
#include "dbg.h"

#define xdr_mountargs xdr_dirpath
#define xdr_umountargs xdr_dirpath

#define __FREE_ARGS(__func__, __request__)              \
        do {                                            \
                xdr_t xdr;                              \
                                                        \
                xdr.op = __XDR_FREE;                    \
                xdr.buf = __request__;                  \
                xdr_##__func__##args(&xdr, args);       \
        } while (0)


struct in_addr __get_remote(const sockid_t *sockid)
{
        struct in_addr sin;

        sin.s_addr = sockid->addr;
        return sin;
}

static int __mount_mnt_auth(mount_ret *res, const char *dpath, const char *remoteip)
{
        int i, ret;

        DINFO("auth\n");
        
        if (!nfsconf.use_export) {
                DBUG("Not use export, skip checking...\n");
                return 0;
        }

        for (i = 0; i < nfsconf.export_size; i++) {
                if (_strcmp(nfsconf.nfs_export[i].path, dpath) == 0) {
                        if (_strcmp(nfsconf.nfs_export[i].ip,
                                                "0.0.0.0") == 0)
                                break;

                        if (_strcmp(nfsconf.nfs_export[i].ip,
                                                remoteip) == 0)
                                break;
                }

        }

        /* no IP or PATH not path in exports, response access error */
        if (i == nfsconf.export_size) {
                ret = EPERM;
                (*res).fhs_status = MNT_EACCES;

                DERROR("%s attempted to mount path (%s) - %s\n",
                                remoteip, dpath, strerror(ret));

                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __mount_null_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                   uid_t uid, gid_t gid, nfsarg_t *arg, buffer_t *buf)
{
        int ret;

        (void) req;
        (void) uid;
        (void) gid;
        (void) arg;
        (void) buf;

        DINFO("null\n");
        
        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK, NULL, NULL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __mount_dir_get(const char *_path, dirid_t *dirid)
{
        int ret;
        char volname[MAX_NAME_LEN], path[MAX_PATH_LEN], *pos;
        shareinfo_t shareinfo;

        ret = sscanf(_path, "/%[^/]", volname);
        if (ret != 1) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        ret = sdfs_share_get(volname, &shareinfo);
        if (ret)
                GOTO(err_ret, ret);

        pos = strstr(_path, volname);
        pos += strlen(volname);

        DINFO("split as (%s,%s)\n", volname, pos);

        if (strlen(pos) == 0) {
                *dirid = shareinfo.dirid;
                goto out;
        } 

        snprintf(path, MAX_PATH_LEN, "%s/%s", shareinfo.path, pos);
        DINFO("real path %s\n", path);

        ret = sdfs_lookup_recurive(path, dirid);
        if (ret)
                GOTO(err_ret, ret);
        
out:
        return 0;
err_ret:
        return ret;
}

static int __mount_mnt_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                           uid_t uid, gid_t gid, nfsarg_t *_args, buffer_t *buf)
{
        int ret = 0;
        static mount_ret res;
        char dpath[MAX_PATH_LEN];
        char remoteip[MAX_NAME_LEN];
        fileid_t fileid;
        static int auth = AUTH_UNIX;
        char **args = &_args->mnt_arg;
        uint32_t mode;
        struct stat stbuf;

        (void) uid;
        (void) gid;
        (void) buf;

        _memset(&res, 0x0, sizeof(mount_ret));
        snprintf(dpath, MAX_PATH_LEN, "%s", *args);
        snprintf(remoteip, MAX_NAME_LEN, "%s", inet_ntoa(__get_remote(sockid)));

        DINFO("mount dir (%s) from %s, socket %d\n", dpath, remoteip, sockid->sd);

        if (strcmp(dpath, "/") == 0) {
                ret = EPERM;
                DWARN("try to mount path (%s)\n", dpath);
                GOTO(err_rep, ret);
        }

        ret = __mount_mnt_auth(&res, dpath, remoteip);
        if(ret)
                GOTO(err_rep, ret);

        ret = __mount_dir_get(dpath, &fileid);
        if (ret) {
                DERROR("%s attempted to mount path (%s) - %s\n",
                                remoteip, dpath, strerror(ret));
                res.fhs_status = MNT_EACCES;
                goto err_rep;
        }

        ret = sdfs_getattr(NULL, &fileid, &stbuf);
        if (ret) {
                res.fhs_status = MNT_EACCES;
                GOTO(err_rep, ret);
        }

        mode = stbuf.st_mode;
        if (!S_ISDIR(mode)) {
                DERROR("%s attempted to mount path (%s) - not a directory, mode %o\n",
                       remoteip, dpath, mode);
                res.fhs_status = MNT_EACCES;
                GOTO(err_rep, ret);
        }

        DBUG("mount ok, res %p\n", &res);

        res.fhs_status = MNT_OK;
        res.u.mountinfo.fhandle.len = sizeof(fileid_t);
        res.u.mountinfo.fhandle.val = (char *)&fileid;
        //res.u.mountinfo.handle_follows = TRUE;
        res.u.mountinfo.auth_flavors.len = 0;
        res.u.mountinfo.auth_flavors.val = &auth;

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK, &res,
                           (xdr_ret_t)xdr_mountret);
        if (ret)
                GOTO(err_ret, ret);

        __FREE_ARGS(mount, buf);

        return 0;
err_rep:
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK, &res,
                     (xdr_ret_t)xdr_mountret);
err_ret:
        __FREE_ARGS(mount, buf);
        return ret;
}

static int __mount_umnt_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                           uid_t uid, gid_t gid, nfsarg_t *_args, buffer_t *buf)
{
        int ret;
        char **args = &_args->umnt_arg;
        char remoteip[MAX_NAME_LEN];

        snprintf(remoteip, MAX_NAME_LEN, "%s", inet_ntoa(__get_remote(sockid)));
        
        (void) uid;
        (void) gid;
        (void) buf;

        DINFO("umount %s %s\n", *args, remoteip);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK, NULL,
                           NULL);
        if (ret)
                GOTO(err_ret, ret);

        __FREE_ARGS(umount, buf);
        
        return 0;
//err_rep:
        //sunrpc_reply(sockid, req, ACCEPT_STATE_OK, NULL,
        //NULL);
err_ret:
        __FREE_ARGS(umount, buf);
        return ret;
}

static int __mount_dump_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                           uid_t uid, gid_t gid, nfsarg_t *_args, buffer_t *buf)
{
        int ret, i, len, count;
        static struct mountbody *dump_list = 0, *resnode;
        void *ptr;
        shareinfo_t *shareinfo, *array;
        const char *addr = "0.0.0.0";

        DINFO("dump\n");
        
        (void) _args;
        (void) uid;
        (void) gid;
        (void) buf;

        ret = sdfs_share_list(SHARE_NFS, &array, &count);
        if (ret)
                GOTO(err_rep, ret);

        if (count == 0) {
                dump_list = NULL;
                goto out;
        }
        
        ret = ymalloc(&ptr, sizeof(struct mountbody) * count);
        if (ret)
                GOTO(err_rep, ret);

        dump_list = ptr;

        for (i = 0; i < count; i++) {
                shareinfo = &array[i];
                resnode = &dump_list[i];

                len = sizeof(shareinfo->share_name);
                YASSERT(len < MAX_PATH_LEN);
                ret = ymalloc(&ptr, len + 1);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);

                _strncpy(ptr, shareinfo->share_name, len);

                resnode->ml_directory = ptr;

                DINFO("share name : %s path : %s hostname : %s\n", shareinfo->share_name,
                      shareinfo->path, shareinfo->hname);
                
                if (strlen(shareinfo->hname)) {
                        addr = shareinfo->hname;
                }
                        
                len = sizeof(addr);
                YASSERT(len < MAX_PATH_LEN);

                ret = ymalloc(&ptr, len);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);

                _strncpy(ptr, addr, len);
                resnode->ml_hostname = ptr;

                if (i < count - 1)
                        resnode->ml_next = &dump_list[i + 1];
                else
                        resnode->ml_next = NULL;
        }

        yfree((void **)&shareinfo);
out:
        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &dump_list, (xdr_ret_t)xdr_dump);
        if (ret)
                GOTO(err_ret, ret);

        //__FREE_ARGS(mount, buf);

        return 0;
err_rep:
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &dump_list, (xdr_ret_t)xdr_dump);
err_ret:
        //__FREE_ARGS(mount, buf);
        return ret;
}

static int __mount_export_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                           uid_t uid, gid_t gid, nfsarg_t *_args, buffer_t *buf)
{
        int ret, i, len, count;
        static struct exportnode *export_list = 0, *resnode;
        void *ptr;
        shareinfo_t *shareinfo, *array;
        const char *addr = "0.0.0.0";

        (void) _args;
        (void) uid;
        (void) gid;
        (void) buf;

        DINFO("export\n");

        ret = sdfs_share_list(SHARE_NFS, &array, &count);
        if (ret)
                GOTO(err_rep, ret);

        if (count == 0) {
                goto out;
        }
        
        ret = ymalloc(&ptr, sizeof(exportnode) * count);
        if (ret)
                GOTO(err_rep, ret);

        export_list = ptr;

        for (i = 0; i < count; i++) {
                shareinfo = &array[i];
                resnode = &export_list[i];

                len = sizeof(shareinfo->share_name);
                YASSERT(len < MAX_PATH_LEN);
                ret = ymalloc(&ptr, len + 1);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);

                _strncpy(ptr, shareinfo->share_name, len);

                resnode->ex_dir = ptr;

                ret = ymalloc(&ptr, sizeof(groupnode));
                if (ret)
                        UNIMPLEMENTED(__DUMP__);

                resnode->ex_groups = ptr;

                if (strlen(shareinfo->hname)) {
                        addr = shareinfo->hname;
                }
                
                len = sizeof(addr);
                YASSERT(len < MAX_PATH_LEN);
                ret = ymalloc(&ptr, len);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);

                _strncpy(ptr, addr, len);

                resnode->ex_groups->gr_name = ptr;
                resnode->ex_groups->gr_next = NULL;

                if (i < count - 1)
                        resnode->ex_next = &export_list[i + 1];
                else
                        resnode->ex_next = NULL;
        }

out:        
        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &export_list, (xdr_ret_t)xdr_exports);
        if (ret)
                GOTO(err_ret, ret);

        //__FREE_ARGS(mount, buf);

        return 0;
err_rep:
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &export_list, (xdr_ret_t)xdr_exports);
err_ret:
        //__FREE_ARGS(mount, buf);
        return ret;
}


int nfs_mount(const sockid_t *sockid, const sunrpc_request_t *req,
              uid_t uid, gid_t gid, buffer_t *buf)
{
        int ret;
        nfs_handler handler = NULL;
        xdr_arg_t xdr_arg = NULL;
        char *name = NULL;
        nfsarg_t nfsarg;
        xdr_t xdr;

        switch (req->procedure) {
        case MNT_NULL:
                handler = __mount_null_svc;
                xdr_arg = (xdr_arg_t)__xdr_void;
                name = "MNT_NULL";
                break;
        case MNT_MNT:
                handler = __mount_mnt_svc;
                xdr_arg = (xdr_arg_t)xdr_dirpath;
                name = "MNT_MNT";
                break;
        case MNT_DUMP:
                handler = __mount_dump_svc;
                xdr_arg = (xdr_arg_t)__xdr_void;
                name = "MNT_DUMP";
                break;
        case MNT_UMNT:
                handler = __mount_umnt_svc;
                xdr_arg = (xdr_arg_t)xdr_dirpath;
                name = "MNT_UMNT";
                break;
        case MNT_UMNTALL:
                handler = NULL;
                xdr_arg = NULL;
                name = "MNT_UMNTALL";
                break;
        case MNT_EXPORT:
                handler = __mount_export_svc;
                xdr_arg = (xdr_arg_t)__xdr_void;
                name = "MNT_EXPORT";
                break;
        default:
                DERROR("unknow procedure\n");
        }

        DBUG("<-------------------new procedure %s------------------>\n", name);

        if (handler == NULL) {
                ret = EINVAL;
                DERROR("error proc %s\n", name);
                GOTO(err_ret, ret);
        }

        xdr.op = __XDR_DECODE;
        xdr.buf = buf;

        if (xdr_arg) {
                if (xdr_arg(&xdr, &nfsarg)) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }
        
        ret = handler(sockid, req, uid, gid, &nfsarg, buf);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

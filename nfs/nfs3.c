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
#include "nfs_events.h"
#include "job_tracker.h"
#include "job_dock.h"
#include "nfs_args.h"
#include "net_global.h"
#include "nfs_conf.h"
#include "nfs_events.h"
#include "nfs3.h"
#include "readdir.h"
#include "sunrpc_proto.h"
#include "sunrpc_reply.h"
#include "sdfs_lib.h"
#include "xdr_nfs.h"
#include "configure.h"
#include "sdfs_lib.h"
#include "core.h"
#include "yfs_limit.h"
#include "dbg.h"

#define __FREE_ARGS(__func__, __request__)              \
        do {                                            \
                xdr_t xdr;                              \
                                                        \
                xdr.op = __XDR_FREE;                    \
                xdr.buf = __request__;                  \
                xdr_##__func__##args(&xdr, args);       \
        } while (0)


/* write verifier */
static char wverf[NFS3_WRITEVERFSIZE];
static uint32_t nfs_read_max_size = NFS_TCPDATA_MAX;
extern nfs_analysis_t nfs_analysis;

int nfs_remove(const fileid_t *parent, const char *name);

/* generate write verifier based on PID and current time */
void regenerate_write_verifier(void)
{
        *(wverf + 0) = (uint32_t) getpid();
        *(wverf + 0) ^= rand();
        *(wverf + 4) = (uint32_t) time(NULL);
}

static void* __nfs_analysis_dump(void *arg)
{
        int ret;
        time_t now;
        char path[MAX_PATH_LEN], buf[MAX_INFO_LEN];

        (void) arg;
        now = time(NULL);

        while (1) {
                sleep(1);
                snprintf(buf, MAX_INFO_LEN,
                                "access: %d\ncreate: %d\ncommit: %d\nfsinfo: %d\nfsstat: %d\ngetattr: %d\nlink: %d\n"
                                "lookup: %d\nmkdir: %d\nmknod: %d\nnull: %d\npathconf: %d\nread: %d\nreaddir: %d\n"
                                "readdirplus:%d\nrename: %d\nremove: %d\nrmdir: %d\nsetattr: %d\nsymlink: %d\nwrite: %d\n"
                                "time:  %u\n",
                                nfs_analysis.access,    nfs_analysis.create,
                                nfs_analysis.commit,    nfs_analysis.fsinfo,
                                nfs_analysis.fsstat,    nfs_analysis.getattr,
                                nfs_analysis.link,      nfs_analysis.lookup,
                                nfs_analysis.mkdir,     nfs_analysis.mknod,
                                nfs_analysis.null,      nfs_analysis.pathconf,
                                nfs_analysis.read,      nfs_analysis.readdir,
                                nfs_analysis.readdirplus,nfs_analysis.rename,
                                nfs_analysis.remove,    nfs_analysis.rmdir,
                                nfs_analysis.setattr,   nfs_analysis.symlink,
                                nfs_analysis.write, (int)now);

                if (strlen(buf)) {
                        snprintf(path, MAX_PATH_LEN, "/dev/shm/sdfs/nfs/ops");
                        ret = _set_value(path, buf, strlen(buf) + 1, O_CREAT | O_TRUNC);
                        if (ret)
                                break;
                }
        }
        return NULL;
}

static int __nfs_op_analysis(int op)
{
        int ret;

        switch (op) {
                case NFS3_NULL:
                        nfs_analysis.lookup++;
                        break;
                case NFS3_MKDIR:
                        nfs_analysis.mkdir++;
                        break;
                case NFS3_MKNOD:
                        nfs_analysis.mknod++;
                        break;
                case NFS3_ACCESS:
                        nfs_analysis.access++;
                        break;
                case NFS3_FSSTAT:
                        nfs_analysis.fsstat++;
                        break;
                case NFS3_FSINFO:
                        nfs_analysis.fsinfo++;
                        break;
                case NFS3_RENAME:
                        nfs_analysis.rename++;
                        break;
                case NFS3_RMDIR:
                        nfs_analysis.rmdir++;
                        break;
                case NFS3_CREATE:
                        nfs_analysis.create++;
                        break;
                case NFS3_WRITE:
                        nfs_analysis.write++;
                        break;
                case NFS3_READ:
                        nfs_analysis.read++;
                        break;
                case NFS3_READDIR:
                        nfs_analysis.readdir++;
                        break;
                case NFS3_READDIRPLUS:
                        nfs_analysis.readdirplus++;
                        break;
                case NFS3_SETATTR:
                        nfs_analysis.setattr++;
                        break;
                case NFS3_GETATTR:
                        nfs_analysis.getattr++;
                        break;
                case NFS3_LINK:
                        nfs_analysis.link++;
                        break;
                case NFS3_SYMLINK:
                        nfs_analysis.symlink++;
                        break;
                case NFS3_READLINK:
                        nfs_analysis.readlink++;
                        break;
                case NFS3_REMOVE:
                        nfs_analysis.remove++;
                        break;
                case NFS3_PATHCONF:
                        nfs_analysis.pathconf++;
                        break;
                case NFS3_COMMIT:
                        nfs_analysis.commit++;
                        break;
                default:
                        DBUG("unknown op code (%d)\n", op);
                        ret = EINVAL;
                        goto err_op;
        }

err_op:
        return ret;
}

int nfs_analysis_init()
{
        pthread_t th;
        pthread_attr_t ta;

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        pthread_create(&th, &ta, __nfs_analysis_dump, NULL);

        DINFO("nfs analysis started...\n");

        return 0;
}

static int __nfs3_null_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                   uid_t uid, gid_t gid, nfsarg_t *arg, buffer_t *buf)
{
        int ret;

        (void) req;
        (void) uid;
        (void) gid;
        (void) arg;
        (void) buf;

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK, NULL, NULL);
        if (ret)
                GOTO(err_ret, ret);

        __nfs_op_analysis(NFS3_NULL);
        
        return 0;
err_ret:
        return ret;
}

static int __nfs3_getattr_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                     uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        getattr_args *args = &_arg->getattr_arg;;
        fileid_t *fileid = (fileid_t *)args->obj.val;
        post_op_attr attr;
        getattr_ret res;
        struct stat stbuf;

        (void) req;
        (void) uid;
        (void) gid;
        (void) buf;

        DBUG("----NFS3---- fileid "FID_FORMAT" len %u\n", FID_ARG(fileid), args->obj.len);

        ret = sdfs_getattr(NULL, fileid, &stbuf);
        if (ret) {
                GOTO(err_rep, ret);
        }

        attr.attr_follow = TRUE;
        get_postopattr_stat(&attr, &stbuf);

        DBUG("fileid "FID_FORMAT" mode %o, time(%u, %u, %u), time (%u, %u, %u)\n",
              FID_ARG(fileid), attr.attr.mode,
              stbuf.st_atime, stbuf.st_mtime, stbuf.st_ctime,
              stbuf.st_atim.tv_sec, stbuf.st_mtim.tv_sec, stbuf.st_ctim.tv_sec);

        res.attr = attr.attr;
        res.status = NFS3_OK;

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_getattrret);
        if (ret)
                GOTO(err_ret, ret);

        __nfs_op_analysis(NFS3_GETATTR);
        __FREE_ARGS(getattr, buf);

        return 0;
err_rep:
        res.status = NFS3_EIO;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_getattrret);
err_ret:
        __FREE_ARGS(getattr, buf);
        return ret;
}

static int __nfs3_setattr_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                     uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        setattr3_args *args = &_arg->setattr3_arg;
        fileid_t *fileid = (fileid_t *)args->obj.val;
        setattr3_res res;
        preop_attr pre;
        post_op_attr attr;
        sattr *attr1;
        const nfs3_time *ctime = NULL;

        (void) req;
        (void) uid;
        (void) gid;
        (void) buf;

        get_preopattr1(fileid, &pre);
        res.setattr3_res_u.resok.obj_wcc.before = pre;
        res.status = NFS3_OK;
        attr1 = &args->new_attributes;

        if (uid) {
                attr1->uid.set_it = TRUE;
                attr1->uid.uid = uid;
        }

        if (gid) {
                attr1->gid.set_it = TRUE;
                attr1->gid.gid = gid;
        }
 
        if (args->guard.check) {
                DBUG("check!!!!!!!!!!!!!!!!!!!!!\n");
                ctime = &args->guard.sattrguard3_u.obj_ctime;
        }
       
        DBUG("----NFS3---- set attr "FID_FORMAT" uid (%u,%u) gid(%u,%u)  mode(0%o,%u)"
              " size (%u,%u) mtime (%u,%u) atime (%u,%u) ctime (%u, %p)\n",
              FID_ARG(fileid),
              attr1->uid.uid, attr1->uid.set_it,
              attr1->gid.gid, attr1->gid.set_it,
              attr1->mode.mode, attr1->mode.set_it,
              attr1->size.size, attr1->size.set_it,
              attr1->mtime.time.seconds, attr1->mtime.set_it, 
              attr1->atime.time.seconds, attr1->atime.set_it,
              ctime ? ctime->seconds : 0, ctime);

        ret = sattr_set(fileid, attr1, ctime);
        if (ret)
                GOTO(err_rep, ret);

        get_postopattr1(fileid, &attr);

        DBUG("increase %d\n", (int)(attr.attr.size - pre.attr.size));

        res.setattr3_res_u.resok.obj_wcc.after = attr;

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_setattrret);
        if (ret)
                GOTO(err_ret, ret);

        __nfs_op_analysis(NFS3_SETATTR);
        __FREE_ARGS(setattr, buf);

        return 0;
err_rep:
        res.status = setattr_err(ret);
        res.setattr3_res_u.resfail.obj_wcc.after.attr_follow = FALSE;
        res.setattr3_res_u.resfail.obj_wcc.before.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_setattrret);
err_ret:
        __FREE_ARGS(setattr, buf);
        return ret;
}

static int __nfs3_lookup_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                             uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        lookup_args *args = &_arg->lookup_arg;
        fileid_t *parent = (fileid_t *)args->dir.val ;
        lookup_ret res;
        fileid_t fileid;

        (void) req;
        (void) uid;
        (void) gid;
        (void) buf;

        ANALYSIS_BEGIN(0);
        
        DBUG("----NFS3---- parent "FID_FORMAT" name %s\n", FID_ARG(parent), args->name);

        if (strlen(args->name) > MAX_NAME_LEN) {
                ret = ENAMETOOLONG;
                GOTO(err_rep, ret);
        }

        if (_strcmp(args->name, ".") == 0 || _strcmp(args->name, "./") == 0) {
                fileid = *parent;
                ret = 0;
                res.status = NFS3_OK;
        } else {
                if (_strcmp(args->name, "..") == 0) {
                        DWARN("lookup parent\n");
                }

                ret = sdfs_lookup(NULL, parent, args->name, &fileid);
                if (ret) {
                        DBUG("lookup %s\n", args->name);
                        GOTO(err_rep, ret);
                }

                if (fileid.volid == 0 || fileid.volid != parent->volid) {
                        ret = EACCES;
                        GOTO(err_rep, ret);
                }
                
                res.status = NFS3_OK;
        }

        res.u.ok.obj.len = sizeof(fileid_t);
        res.u.ok.obj.val = (char *)&fileid;
        get_postopattr1(&fileid, &res.u.ok.obj_attr);
        get_postopattr1(parent, &res.u.ok.dir_attr);

        DBUG("parent "FID_FORMAT" name %s ok\n", FID_ARG(parent), args->name);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_lookupret);
        if (ret)
                GOTO(err_ret, ret);

        __nfs_op_analysis(NFS3_LOOKUP);
        __FREE_ARGS(lookup, buf);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_rep:
        res.status = lookup_err(ret);
        res.u.fail.dir_attr.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_lookupret);
err_ret:
        __FREE_ARGS(lookup, buf);
        return ret;
}

inline static int __nfs3_access(const nfs3_fattr *attr, uid_t uid, gid_t gid)
{
        uint32_t mode, parm, access;

        mode = attr->mode & 0777;

        if (uid == attr->uid) {
                parm = (mode & 0700)  / 0100;
        } else if (gid == attr->gid) {
                parm = (mode & 0070) / 010;
        } else {
                parm = (mode & 0007) / 01;
        }

        DBUG("mode 0%o 0%o\n", mode, parm);
        
        access = 0;
        if (parm & 4) {
                access |= (ACCESS_READ);
        }

        if (parm & 2) {
                access |= (ACCESS_MODIFY | ACCESS_EXTEND);
        }

        if (parm & 1) {
                access |= (ACCESS_EXECUTE);
        }

        if (attr->type == NFS3_DIR) {
                if (access & ACCESS_READ)
                        access |= ACCESS_LOOKUP;
                if (access & ACCESS_MODIFY)
                        access |= ACCESS_DELETE;
                access &= ~ACCESS_EXECUTE;
        }

        return access;
}
static int __nfs3_access_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                             uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret, access = 0;
        post_op_attr post;
        access_args *args = &_arg->access_arg ;
        fileid_t *fileid = (fileid_t *)args->obj.val;
        access_ret res;

        (void) req;
        (void) uid;
        (void) gid;
        (void) buf;

        DBUG("----NFS3---- fileid "FID_FORMAT" len %u\n", FID_ARG(fileid), args->obj.len);

        get_postopattr1(fileid, &post);

#if ENABLE_MD_POSIX
        access = __nfs3_access(&post.attr, uid, gid);
#else
        mode_t mode;
        (void) mode;
        mode = post.attr.mode;

        
        /* owner, group, other, and root are allowed everything */
        access |= ACCESS_READ | ACCESS_MODIFY | ACCESS_EXTEND | ACCESS_EXECUTE;

        /* adjust if directory */
        if (post.attr.type == NFS3_DIR) {
                if (access & ACCESS_READ)
                        access |= ACCESS_LOOKUP;
                if (access & ACCESS_MODIFY)
                        access |= ACCESS_DELETE;
                access &= ~ACCESS_EXECUTE;
        }
#endif

        res.status = NFS3_OK;
        res.u.ok.access = access & args->access;
        res.u.ok.obj_attr = post;

        DBUG("fileid "FID_FORMAT" len %u ok\n", FID_ARG(fileid), args->obj.len);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_accessret);
        if (ret)
                GOTO(err_ret, ret);

        __nfs_op_analysis(NFS3_ACCESS);
        __FREE_ARGS(access, buf);

        return 0;
#if 0
err_rep:
        res.status = NFS3_EIO;
        res.u.fail.obj_attr.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_accessret);
#endif
err_ret:
        __FREE_ARGS(access, buf);
        return ret;
}

static int __nfs3_fsstat_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                             uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        fsstat_ret res;
        struct statvfs svbuf;
        fsstat_args *args = &_arg->fsstat_arg;
        fileid_t *fileid = (fileid_t *)args->fsroot.val;

        (void) req;
        (void) uid;
        (void) gid;
        (void) buf;
        
        /* overlaps with resfail */
        get_postopattr1(fileid, &res.u.ok.attr);

        ret = sdfs_statvfs(NULL, fileid, &svbuf);
        if (ret) {
                DWARN("fileid "FID_FORMAT"\n", FID_ARG(fileid));
                GOTO(err_rep, ret);
        } else {
                res.status = NFS3_OK;
                res.u.ok.tbytes = (uint64_t) svbuf.f_blocks * svbuf.f_bsize;
                res.u.ok.fbytes = (uint64_t) svbuf.f_bfree * svbuf.f_bsize;
                res.u.ok.abytes = (uint64_t) svbuf.f_bavail * svbuf.f_bsize;
                res.u.ok.tfiles = (uint64_t) svbuf.f_files;
                res.u.ok.ffiles = (uint64_t) svbuf.f_ffree;
                res.u.ok.afiles = (uint64_t) svbuf.f_ffree;
                res.u.ok.invarsec = 0;

                DBUG("fbsize:%lu   f_frsize:%lu    f_blocks:%lu    f_bfree:%lu     f_bavail:%lu"
                      "f_files:%lu     f_ffree:%lu     f_favail:%lu    f_fsid:%lu  f_flag:%lu"
                      "f_namemax:%lu\n",
                      svbuf.f_bsize, svbuf.f_frsize, svbuf.f_blocks,
                      svbuf.f_bfree, svbuf.f_bavail, svbuf.f_files,
                      svbuf.f_ffree, svbuf.f_favail, svbuf.f_fsid,
                      svbuf.f_flag, svbuf.f_namemax);
        }

        DBUG("----NFS3---- total:%luG, avail:%luG\n",
              svbuf.f_blocks * svbuf.f_bsize/1024/1024/1024,
              svbuf.f_bavail * svbuf.f_bsize/1024/1024/1024);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_fsstatret);
        if (ret)
                GOTO(err_ret, ret);

        __nfs_op_analysis(NFS3_FSSTAT);
        __FREE_ARGS(fsstat, buf);

        return 0;
err_rep:
        res.status = NFS3_EIO;
        res.u.fail.attr.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_fsstatret);
err_ret:
        __FREE_ARGS(fsstat, buf);
        return ret;
}

static int __nfs3_fsinfo_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                             uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        fsinfo_ret res;
        fsinfo_args *args = &_arg->fsinfo_arg;
        fileid_t *fileid = (fileid_t *)args->fsroot.val;

        (void) req;
        (void) uid;
        (void) gid;
        (void) buf;

        DBUG("----NFS3---- fh len %u\n", args->fsroot.len);

        get_postopattr1(fileid, &res.u.ok.obj_attr);

        res.status = NFS3_OK;
        res.u.ok.rtmax = nfsconf.rsize;
        res.u.ok.rtpref = nfsconf.rsize;
        res.u.ok.rtmult = 4096;
        res.u.ok.wtmax = nfsconf.wsize;
        res.u.ok.wtpref = nfsconf.wsize;
        res.u.ok.wtmult = 4096;
        res.u.ok.dtpref = 4096;
        res.u.ok.maxfilesize = ((LLU)1024 * 1024 * 1024 * 1024 * 10);
        res.u.ok.time_delta.seconds = 1;
        res.u.ok.time_delta.nseconds = 1;
        res.u.ok.properties = NFSINFO_HOMOGENEOUS | NFSINFO_SYMLINK | NFSINFO_LINK;
        //res.u.ok.properties = NFSINFO_HOMOGENEOUS | NFSINFO_LINK;

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_fsinforet);
        if (ret)
                GOTO(err_ret, ret);
        
        __nfs_op_analysis(NFS3_FSINFO);
        __FREE_ARGS(fsinfo, buf);

        return 0;
#if 0
err_rep:
        res.status = NFS3_EIO;
        res.u.fail.obj_attr.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_fsinforet);
#endif
err_ret:
        __FREE_ARGS(fsinfo, buf);
        return ret;
}

static int __nfs3_create(const fileid_t *parent, const char *name, uint32_t cmode, 
                         uid_t uid, gid_t gid, uint32_t fmode,
                         uint32_t mtime, uint32_t atime, fileid_t *fileid)
{
        int ret;

        ret = sdfs_create(NULL, parent, name, fileid, fmode, uid, gid);
        if (ret) {
                if (ret == EEXIST) {
                        ret = sdfs_lookup(NULL, parent, name, fileid);
                        if (ret)
                                GOTO(err_ret, ret);

                        if (cmode == EXCLUSIVE) {
                                struct stat stbuf;
                                ret = sdfs_getattr(NULL, fileid, &stbuf);
                                if (ret)
                                        GOTO(err_ret, ret);

                                if (cmode == EXCLUSIVE) {
                                        YASSERT((stbuf.st_mode & 01777) == EXCLUSIVE);
                                }
                                
                                if ((stbuf.st_mode & 01777) == EXCLUSIVE
                                    &&stbuf.st_mtime == mtime && stbuf.st_atime == atime) {
                                        DBUG("EXCLUSIVE resume "FID_FORMAT"\n",
                                              FID_ARG(fileid));
                                } else {
                                        DWARN("EXCLUSIVE "FID_FORMAT" mtime"
                                              " %u:%u atime %u:%u mode %u\n", FID_ARG(fileid),
                                              stbuf.st_mtime, mtime, stbuf.st_atime, atime,
                                              stbuf.st_mode & 01777);
                                        ret = EEXIST;
                                        GOTO(err_ret, ret);
                                }
                        } else {
                                DBUG("file "FID_FORMAT"/%s exist\n", FID_ARG(parent), name);

                                uint64_t count = 0;
                                ret = sdfs_childcount(NULL, fileid, &count);
                                if (ret)
                                        GOTO(err_ret, ret);

                                if (count) {
                                        ret = EEXIST;
                                        GOTO(err_ret, ret);
                                }
                        }
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __nfs3_create_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                             uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        mode_t flags = O_RDWR | O_CREAT | O_EXCL;
        uint32_t mode, mtime, atime;
        create_args *args = &_arg->create_arg;
        fileid_t *parent = (fileid_t *)args->where.dir.val;
        create_ret res;
        postop_fh *pfh;
        sattr *attr;
        fileid_t fileid;

        ANALYSIS_BEGIN(0);
        
        DBUG("create "FID_FORMAT" %s\n", FID_ARG(parent), args->where.name);
        
        /* GUARDED and EXCLUSIVE maps to unix exclusive create*/
        if (args->how.mode != UNCHECKED)
                flags |= O_EXCL;

        if (args->how.mode != EXCLUSIVE) {
                /* NULL */
        }

        /* XXX need set this value according to file's access
         * mode. but I cann't get it here. maybe require SETATTR
         * proc. maybe ...  -gj
         */

        res.status = NFS3_OK;

        get_preopattr1(parent, &res.u.ok.dir_wcc.before);

        if (args->how.mode == EXCLUSIVE) {
                uint64_t verf;
                uint64_t *tmp;
                uint32_t *tmp1;
                
                tmp = (uint64_t *)args->how.verf;
                verf = *tmp;

                tmp1 = (uint32_t *)args->how.verf;
                atime = *tmp1;
                mtime = *(uint32_t *)&args->how.verf[4];
                uid = 0;
                gid = 0;

                DBUG("----NFS3---- EXCLUSIVE create %s @ "FID_FORMAT" verf %llu"
                      " atime %u, mtime %u\n", args->where.name, FID_ARG(parent),
                      (LLU)verf, atime, mtime);
                mode = EXCLUSIVE;
        } else {
                (void) sattr_tomode(&mode, &args->how.attr);
                attr = &args->how.attr;

                if (attr->uid.set_it == TRUE) {
                        uid = attr->uid.uid;
                } else
                        uid = 0;

                if (attr->gid.set_it == TRUE) {
                        gid = attr->gid.gid;
                } else
                        gid = 0;


                DBUG("----NFS3---- create %s @ "FID_FORMAT" uid (%u,%u) gid(%u,%u)  mode(0%o,%u)"
                      " size (%u,%u) mtime (%u,%u) atime (%u,%u)\n",
                      args->where.name, FID_ARG(parent),
                      attr->uid.uid, attr->uid.set_it,
                      attr->gid.gid, attr->gid.set_it,
                      attr->mode.mode, attr->mode.set_it,
                      attr->size.size, attr->size.set_it,
                      attr->mtime.time.seconds, attr->mtime.set_it, 
                      attr->atime.time.seconds, attr->atime.set_it);

#if 0
                DBUG("mode %u %o uid: %u set(%u) gid: %u set(%u)\n",
                     args->how.attr.mode.set_it, args->how.attr.mode.mode,
                     attr->uid.uid, attr->uid.set_it, attr->gid.gid, attr->gid.set_it);
#endif

                YASSERT(attr->size.set_it != TRUE);
                atime = gettime();
                mtime = atime;
        }

        DBUG("parent "FID_FORMAT" name %s mode %o how %d\n", FID_ARG(parent),
              args->where.name, mode, args->how.mode);

        ret = __nfs3_create(parent, args->where.name, args->how.mode,
                            uid, gid, mode,
                            mtime, atime, &fileid);
        if (ret)
                GOTO(err_rep, ret);
        
        DBUG("fileid "FID_FORMAT"\n", FID_ARG(&fileid));

        res.status = NFS3_OK;

        pfh = &res.u.ok.obj;
        pfh->handle.val = (void *)&fileid;
        pfh->handle.len = sizeof(fileid);
        pfh->handle_follows = TRUE;

        get_postopattr1(&fileid, &res.u.ok.obj_attr);
        get_postopattr1(parent, &res.u.ok.dir_wcc.after);

        DBUG("status %d fhlen %u\n", res.status, pfh->handle.len);
        DBUG("parent "FID_FORMAT" %s exist\n", FID_ARG(parent), args->where.name);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_createret);
        if (ret)
                GOTO(err_ret, ret);
        
        __nfs_op_analysis(NFS3_CREATE);
        __FREE_ARGS(create, buf);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        return 0;
err_rep:
        res.status = create_err(ret);
        res.u.fail.dir_wcc.after.attr_follow = FALSE;
        res.u.fail.dir_wcc.before.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_createret);
err_ret:
        __FREE_ARGS(create, buf);
        return ret;
}

static int __nfs3_pathconf_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                               uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        pathconf3_res res;
        pathconf3_args *args = &_arg->pathconf_arg;
        fileid_t *fileid = (fileid_t *)args->object.val;

        (void) req;
        (void) uid;
        (void) gid;
        (void) buf;

        get_postopattr1(fileid, &res.pathconf3_res_u.resok.obj_attributes);
        res.status = NFS3_OK;
        res.pathconf3_res_u.resok.linkmax = 0xFFFFFFFF;
        res.pathconf3_res_u.resok.name_max = NFS_PATHLEN_MAX;
        res.pathconf3_res_u.resok.no_trunc = TRUE;
        res.pathconf3_res_u.resok.chown_restricted = FALSE;
        res.pathconf3_res_u.resok.case_insensitive = FALSE;
        res.pathconf3_res_u.resok.case_preserving = TRUE;

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_pathconf3_ret);
        if (ret)
                GOTO(err_ret, ret);
        
        __nfs_op_analysis(NFS3_PATHCONF);
        __FREE_ARGS(pathconf3_, buf);

        return 0;
#if 0
err_rep:
        res.status = NFS3_EIO;
        res.pathconf3_res_u.resfail.obj_attributes.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_pathconf3_ret);
#endif
err_ret:
        return ret;
}

static int __nfs3_mkdir(const fileid_t *parent, const char *name,
                        uid_t uid, gid_t gid, uint32_t mode,
                        uint32_t mtime, uint32_t atime, fileid_t *fileid)
{
        int ret;

        (void) mtime;
        (void) atime;
        
        ret = sdfs_mkdir(NULL, parent, name, NULL, fileid, mode, uid, gid);
        if (ret) {
                if (ret == EEXIST) {
                        DBUG("dir "FID_FORMAT" %s exist\n", FID_ARG(parent), name);

                        ret = sdfs_lookup(NULL, parent, name, fileid);
                        if (ret)
                                GOTO(err_ret, ret);

                        uint64_t count = 0;
                        ret = sdfs_childcount(NULL, fileid, &count);
                        if (ret)
                                GOTO(err_ret, ret);

                        if (count) {
                                ret = EEXIST;
                                GOTO(err_ret, ret);
                        }
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __nfs3_mkdir_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                            uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        mkdir_args *args = &_arg->mkdir_arg;
        fileid_t *parent = (fileid_t *)args->where.dir.val;
        fileid_t fileid;
        preop_attr pre;
        post_op_attr post;
        mode_t mode;
        postop_fh *pfh;
        mkdir_ret res;

        ANALYSIS_BEGIN(0);

        DBUG("----NFS3---- parent "FID_FORMAT" %s exist\n", FID_ARG(parent), args->where.name);

        get_preopattr1(parent, &pre);
        (void) sattr_tomode(&mode, &args->attr);

        ret = __nfs3_mkdir(parent, args->where.name, uid, gid, mode, -1, -1, &fileid);
        if (ret)
                GOTO(err_rep, ret);

        pfh = &res.u.ok.obj;
        pfh->handle.val = (void *)&fileid;
        pfh->handle.len = sizeof(fileid_t);
        pfh->handle_follows = TRUE;

        res.status = NFS3_OK;

        get_postopattr1(&fileid, &res.u.ok.obj_attr);
        get_postopattr1(parent, &post);

        /* overlaps with resfail */
        res.u.ok.dir_wcc.before = pre;
        res.u.ok.dir_wcc.after = post;

        DBUG("parent "FID_FORMAT" name: %s ok\n", FID_ARG(parent), args->where.name);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_mkdirret);
        if (ret)
                GOTO(err_ret, ret);

        __nfs_op_analysis(NFS3_MKDIR);
        __FREE_ARGS(mkdir, buf);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_rep:
        res.status = mkdir_err(ret);
        res.u.fail.dir_wcc.before.attr_follow = FALSE;
        res.u.fail.dir_wcc.after.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_mkdirret);
err_ret:
        __FREE_ARGS(mkdir, buf);
        return ret;
}

static int __nfs3_readdir_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                     uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        readdir_ret res;
        readdir_args *args = &_arg->readdir_arg;
        fileid_t *fileid = (fileid_t *)args->dir.val;
        entry *entrys;
        char *patharray;
        void *ptr;

        (void) req;
        (void) uid;
        (void) gid;
        (void) buf;
        
        ret = ymalloc(&ptr, (sizeof(entry) + MAX_PATH_LEN)
                        * MAX_ENTRIES);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        entrys = ptr;
        ptr += sizeof(entry) * MAX_ENTRIES;
        patharray = ptr;

        DBUG("----NFS3---- "FID_FORMAT"\n", FID_ARG(fileid));

        ret = read_dir(NULL, fileid, args->cookie, args->cookieverf,
                        args->count, &res, entrys, patharray);
        if (ret)
                GOTO(err_rep, ret);

        res.status = NFS3_OK;
        get_postopattr1(fileid, &res.u.ok.dir_attr);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_readdirret);
        if (ret)
                GOTO(err_ret, ret);
         
        __nfs_op_analysis(NFS3_READDIR);
        __FREE_ARGS(readdir, buf);

        yfree((void **)&entrys);

        return 0;
err_rep:
        res.status = readdir_err(ret);
        res.u.fail.dir_attr.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_readdirret);
err_ret:
        __FREE_ARGS(readdir, buf);
        yfree((void **)&entrys);
        return ret;
}

static int __nfs3_readdirplus_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                     uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        readdirplus_ret res;
        readdirplus_args *args = &_arg->readdirplus_arg;
        fileid_t *fileid = (fileid_t *)args->dir.val;
        entryplus *entrys;
        char *patharray;
        fileid_t *fharray;
        void *ptr;

        (void) req;
        (void) uid;
        (void) gid;
        (void) buf;

        DBUG("----NFS3---- "FID_FORMAT"\n", FID_ARG(fileid));

        ret = ymalloc(&ptr, (sizeof(entryplus) + sizeof(fileid_t) + MAX_NAME_LEN )
                        * MAX_DIRPLUS_ENTRIES);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        entrys = ptr;
        ptr += sizeof(entryplus) * MAX_DIRPLUS_ENTRIES;
        fharray = ptr;
        ptr += sizeof(fileid_t) * MAX_DIRPLUS_ENTRIES;
        patharray = ptr;

        DBUG("max %ju\n", args->maxcount);
        
        ret = readdirplus(NULL, fileid, args->cookie, args->cookieverf,
                          args->dircount, &res, entrys, patharray,
                          fharray);
        if (ret)
                GOTO(err_rep, ret);

        res.status = NFS3_OK;

        get_postopattr1(fileid, &res.u.ok.dir_attr);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_readdirplusret);
        if (ret)
                GOTO(err_ret, ret);
         
        __nfs_op_analysis(NFS3_READDIRPLUS);
        __FREE_ARGS(readdirplus, buf);

        yfree((void **)&entrys);

        return 0;

err_rep:
        res.status = readdir_err(ret);
        res.u.fail.dir_attr.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_readdirplusret);
err_ret:
        __FREE_ARGS(readdirplus, buf);
        yfree((void **)&entrys);
        return ret;
}

static int __nfs3_rmdir_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                     uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        rmdir_ret res;
        rmdir_args *args = &_arg->rmdir_arg;
        fileid_t *parent = (fileid_t *)args->obj.dir.val;
        fileid_t fileid;

        (void) req;
        (void) uid;
        (void) gid;
        (void) buf;

        DBUG("----NFS3---- parent "FID_FORMAT" name %s\n", FID_ARG(parent), args->obj.name);

        ret = sdfs_lookup(NULL, parent, args->obj.name, &fileid);
        if (ret) {
                if (ret == ENOENT) {
                        goto rmdir_ok;
                } else {
                        DWARN("parent "FID_FORMAT" name %s\n",
                              FID_ARG(&fileid), args->obj.name);
                        GOTO(err_rep, ret);
                }
        }

        get_preopattr1(&fileid, &res.dir_wcc.before);

        ret = sdfs_rmdir(NULL, parent, args->obj.name);
        if (ret) {
                if (ret == ENOENT) {
                        goto rmdir_ok;
                } else {
                        DWARN("parent "FID_FORMAT" name %s\n",
                              FID_ARG(&fileid), args->obj.name);
                        GOTO(err_rep, ret);
                }
        }

rmdir_ok:
        DBUG("rmdir ok parent "FID_FORMAT" name %s\n", FID_ARG(parent), args->obj.name);
        res.status = NFS3_OK;

        /* overlaps with resfail */
        get_postopattr1(parent, &res.dir_wcc.after);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_rmdirret);
        if (ret)
                GOTO(err_ret, ret);
        
        __nfs_op_analysis(NFS3_RMDIR);
        __FREE_ARGS(rmdir, buf);

        return 0;
err_rep:
        res.status = rmdir_err(ret);
        res.dir_wcc.before.attr_follow = FALSE;
        res.dir_wcc.after.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_rmdirret);
err_ret:
        __FREE_ARGS(rmdir, buf);
        return ret;
}

static int __nfs3_readlink_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                     uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        readlink_args *args = &_arg->readlink_arg;
        fileid_t *fileid = (fileid_t *)args->symlink.val;
        readlink_res res;
        char tmp[MAX_BUF_LEN];
        uint32_t buflen = MAX_BUF_LEN;

        (void) req;
        (void) uid;
        (void) gid;
        (void) buf;

        ret = sdfs_readlink(NULL, fileid, tmp, &buflen);
        if (ret)
                GOTO(err_rep, ret);

        /* just a pointer */
        res.status = NFS3_OK;
        res.u.ok.data = tmp;
        get_postopattr1(fileid, &res.u.ok.symlink_attributes);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_readlinkret);
        if (ret)
                GOTO(err_ret, ret);
        
        __nfs_op_analysis(NFS3_READLINK);
        __FREE_ARGS(readlink, buf);

        return 0;
err_rep:
        res.status = NFS3_EIO;
        res.u.fail.symlink_attributes.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_readlinkret);
err_ret:
        __FREE_ARGS(readlink, buf);
        return ret;
}

/**
 * no need to verify target path
 */
static int __nfs3_symlink_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                     uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        symlink_args *args = &_arg->symlink_arg;
        fileid_t *parent = (fileid_t *)args->where.dir.val;
        symlink_res res;
        postop_fh *pfh;
        uint32_t  buflen;
        char tmp[MAX_BUF_LEN];
        uint32_t mode;
        fileid_t fileid;

        (void) req;
        (void) uid;
        (void) gid;

        DBUG("----NFS3---- link %s data %s\n", args->where.name, args->symlink.symlink_data);

        (void) sattr_tomode(&mode, &args->symlink.symlink_attributes);

        ret = sdfs_symlink(NULL, parent, args->where.name, args->symlink.symlink_data,
                           mode, uid, gid);
        if (ret) {
                if (ret == EEXIST) {
                        ret = sdfs_lookup(NULL, parent, args->where.name, &fileid);
                        if (ret)
                                GOTO(err_rep, ret);

                        buflen = MAX_BUF_LEN;

                        ret = sdfs_readlink(NULL, &fileid, tmp, &buflen);
                        if (ret)
                                GOTO(err_rep, ret);

                        if (strcmp(tmp, args->symlink.symlink_data) != 0)
                                GOTO(err_rep, ret);
                } else {
                        DWARN("link %s data %s\n", args->where.name,
                              args->symlink.symlink_data);
                        GOTO(err_rep, ret);
                }
        }

        ret = sdfs_lookup(NULL, parent, args->where.name, &fileid);
        if (ret) {
                DWARN("lookup %s form "FID_FORMAT"\n", args->where.name, FID_ARG(parent));
                GOTO(err_rep, ret);
        }

        res.status = NFS3_OK;

        pfh = &res.u.ok.obj;
        pfh->handle.val = (void *)&fileid;
        pfh->handle.len = sizeof(fileid_t);
        pfh->handle_follows = TRUE;

        res.u.ok.dir_wcc.before.attr_follow = FALSE;
        get_postopattr1(&fileid, &res.u.ok.obj_attributes);
        get_postopattr1(parent, &res.u.ok.dir_wcc.after);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_symlinkret);
        if (ret)
                GOTO(err_ret, ret);
        
        __nfs_op_analysis(NFS3_SYMLINK);
        __FREE_ARGS(symlink, buf);

        return 0;
err_rep:
        res.status = symlink_err(ret);
        res.u.fail.dir_wcc.after.attr_follow = FALSE;
        res.u.fail.dir_wcc.before.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_symlinkret);
err_ret:
        __FREE_ARGS(symlink, buf);
        return ret;
}

static int __nfs3_mknod_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                     uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        mknod_args *args = &_arg->mknod_arg;
        mknod_res res;

        (void) req;
        (void) uid;
        (void) gid;

        ret = ENOSYS;
        GOTO(err_rep, ret);

        __nfs_op_analysis(NFS3_MKNOD);
        __FREE_ARGS(mknod, buf);

err_rep:
        res.status = mknod_err(ret);
        res.u.fail.dir_wcc.after.attr_follow = FALSE;
        res.u.fail.dir_wcc.before.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_mknodargs);
//err_ret:
        __FREE_ARGS(mknod, buf);
        return ret;
}

static int __nfs3_remove_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                     uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        remove_ret res;
        remove_args *args = &_arg->remove_arg;
        fileid_t *parent = (fileid_t *)args->obj.dir.val;;
        fileid_t fileid;

        (void) req;
        (void) uid;
        (void) gid;
        
        DBUG("----NFS3---- parent "FID_FORMAT" name %s\n", FID_ARG(parent),
                        args->obj.name);

        ret = sdfs_lookup(NULL, parent, args->obj.name, &fileid);
        if (ret) {
                if (ret == ENOENT) {
                        goto remove_ok;
                } else {
                        GOTO(err_rep, ret);
                }
        }

        get_preopattr1(&fileid, &res.dir_wcc.before);

#if 1
        ret = nfs_remove(parent, args->obj.name);
        if (ret) {
                GOTO(err_rep, ret);
        }
#else
        ret = sdfs_unlink(NULL, parent, args->obj.name);
        if (ret) {
                if (ret == ENOENT) {
                        goto remove_ok;
                } else {
                        GOTO(err_rep, ret);
                }
        }
#endif

remove_ok:
        DBUG("rmove ok parent "FID_FORMAT" name %s\n",
             FID_ARG(parent), args->obj.name);

        res.status = NFS3_OK;
        get_postopattr1(parent, &res.dir_wcc.after);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_removeret);
        if (ret)
                GOTO(err_ret, ret);
        
        __nfs_op_analysis(NFS3_REMOVE);
        __FREE_ARGS(remove, buf);

        return 0;
err_rep:
        res.status = remove_err(ret);
        res.dir_wcc.after.attr_follow = FALSE;
        res.dir_wcc.before.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_removeret);
err_ret:
        return ret;
}

static int __rename_proc(const fileid_t *fromdir, const char *fromname,
                         const fileid_t *todir, const char *toname)
{
        int ret;
        fileid_t from_fileid, to_fileid;
        struct stat stfrom, stto;

        ret = sdfs_rename(NULL, fromdir, fromname, todir, toname);
        if (0 == ret)
                return ret;
        if (EEXIST != ret)
                GOTO(err_ret, ret);

        ret = sdfs_lookup(NULL, fromdir, fromname, &from_fileid);
        if (ret)
                GOTO(err_ret, ret);
        ret = sdfs_getattr(NULL, &from_fileid, &stfrom);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_lookup(NULL, todir, toname, &to_fileid);
        if (ret)
                GOTO(err_ret, ret);
        ret = sdfs_getattr(NULL, &to_fileid, &stto);
        if (ret)
                GOTO(err_ret, ret);

        if (!S_ISDIR(stfrom.st_mode) && !S_ISDIR(stto.st_mode)) {
                ret = sdfs_unlink(NULL, todir, toname);
                if (ret)
                        GOTO(err_ret, ret);
        }
        else if (S_ISDIR(stfrom.st_mode) && S_ISDIR(stto.st_mode)) {
                uint64_t count = 0;
                ret = sdfs_childcount(NULL, &to_fileid, &count);
                if (ret)
                        GOTO(err_ret, ret);

                if (!count) {
                        ret = sdfs_rmdir(NULL, todir, toname);
                        if (ret)
                                GOTO(err_ret, ret);
                }
                else {
                        ret = EEXIST;
                        GOTO(err_ret, ret);
                }
        } else {
                ret = EINVAL;
                GOTO(err_ret, ret);

        }

        ret = sdfs_rename(NULL, fromdir, fromname, todir, toname);
        if (ret)
                GOTO(err_ret, ret);

        return 0;

err_ret:
        return ret;
}

static int __nfs3_rename_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                     uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        rename_args *args = &_arg->rename_arg;
        const char *from = args->from.name;
        const char *to = args->to.name;
        fileid_t *fromdir = (fileid_t *)args->from.dir.val;
        fileid_t *todir = (fileid_t *)args->to.dir.val;
        rename_ret res;

        (void) req;
        (void) uid;
        (void) gid;

        DBUG("----NFS3---- from "FID_FORMAT" %s to "FID_FORMAT" %s\n", FID_ARG(fromdir),
             from, FID_ARG(todir), to);

        get_preopattr1(fromdir, &res.u.ok.from.before);
        get_preopattr1(todir, &res.u.ok.to.before);

        ret = __rename_proc(fromdir, from, todir, to);
        if (ret)
                GOTO(err_rep, ret);

        res.status = NFS3_OK;

        get_postopattr1(fromdir, &res.u.ok.from.after);
        get_postopattr1(todir, &res.u.ok.to.after);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_renameret);
        if (ret)
                GOTO(err_ret, ret);
        
        
        __nfs_op_analysis(NFS3_RENAME);
        __FREE_ARGS(rename, buf);


        return 0;
err_rep:
        res.status = rename_err(ret);
        res.u.fail.from.after.attr_follow = FALSE;
        res.u.fail.to.after.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_renameret);
err_ret:
        __FREE_ARGS(rename, buf);
        return ret;
}

static int __nfs3_link_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                     uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        LINK3args *args = &_arg->link3arg;
        const fileid_t *parent = (fileid_t *)args->link.dir.val;
        const fileid_t *fileid = (fileid_t *)args->file.val;;
        LINK3res res;

        (void) req;
        (void) uid;
        (void) gid;

        get_preopattr1(parent, &res.u.ok.linkdir_wcc.before);

        DBUG("----NFS3---- parent "FID_FORMAT" name %s\n", FID_ARG(parent), args->link.name);

        //todo uid, gid
        ret = sdfs_link2node(NULL, fileid, parent, args->link.name);
        if (ret)
                GOTO(err_rep, ret);

        res.status = NFS3_OK;

        get_postopattr1(parent, &res.u.ok.linkdir_wcc.after);
        get_postopattr1(fileid, &res.u.ok.file_attributes);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_linkret);
        if (ret)
                GOTO(err_ret, ret);
        
        __nfs_op_analysis(NFS3_LINK);
        __FREE_ARGS(LINK3, buf);


        return 0;
err_rep:
        res.status = NFS3_EIO;
        res.u.fail.linkdir_wcc.after.attr_follow = FALSE;
        res.u.fail.linkdir_wcc.before.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_linkret);
err_ret:
        __FREE_ARGS(LINK3, buf);
        return ret;
}

static int __nfs3_commit_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                     uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        commit_args *args = &_arg->commit_arg;
        fileid_t *fileid = (fileid_t *)args->file.val;
        commit_ret res;

        (void) req;
        (void) uid;
        (void) gid;

        DBUG("----NFS3---- commit "FID_FORMAT" size %u offset %ju\n",
              FID_ARG(fileid), args->count, args->offset);
        
        res.status = NFS3_OK;
        _memcpy(res.u.ok.verf, wverf, NFS3_WRITEVERFSIZE);

        get_preopattr1(fileid, &res.u.ok.file_wcc.before);
        get_postopattr1(fileid, &res.u.ok.file_wcc.after);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_commitret);
        if (ret)
                GOTO(err_ret, ret);

        __nfs_op_analysis(NFS3_COMMIT);
        __FREE_ARGS(commit, buf);

        return 0;
#if 0
err_rep:
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_commitret);
#endif
err_ret:
        __FREE_ARGS(commit, buf);
        return ret;
}


static int __nfs3_read_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                           uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret, eof = 0;
        read_args *args = &_arg->read_arg;
        struct stat stbuf;
        read_ret res;
        fileid_t *fileid = (fileid_t *)args->file.val;
        buffer_t rbuf;

        (void) uid;
        (void) gid;

        ANALYSIS_BEGIN(0);
        
        DBUG("----NFS3---- read "FID_FORMAT" size %u offset %ju\n",
              FID_ARG(fileid), args->count, args->offset);

        ret = sdfs_getattr(NULL, fileid, &stbuf);
        if (unlikely(ret)) {
                ret = (ret == ENOENT) ? ESTALE : ret;
                GOTO(err_rep, ret);
        }

        mbuffer_init(&rbuf, 0);
        if (unlikely(args->offset >= (LLU)stbuf.st_size)) {
                DBUG("read after offset off %llu size %llu fileid "FID_FORMAT"\n",
                     (LLU)args->offset,
                     (LLU)stbuf.st_size, FID_ARG(fileid));

                eof = 1;
                goto read_zero;
        }

        if (args->count + args->offset >= (LLU)stbuf.st_size) {
                DBUG("read offset %llu count %u, file len %llu\n", (LLU)args->offset,
                     args->count, (LLU)stbuf.st_size);
                args->count = stbuf.st_size - args->offset;
                eof = 1;
        } else
                eof = 0;

        if (unlikely(args->count > nfs_read_max_size)) {
                DWARN("request too big %u\n", args->count);
                args->count = nfs_read_max_size;
        }

        ret = sdfs_read(NULL, fileid, &rbuf, args->count, args->offset);
        if (unlikely(ret))
                GOTO(err_rep, ret);

read_zero:
        res.status = NFS3_OK;
        res.u.ok.count = rbuf.len;
        res.u.ok.data.len = rbuf.len;
        res.u.ok.eof = eof;
        res.u.ok.data.val = (void *)&rbuf;
        res.status = NFS3_OK;

#if ENABLE_MD_POSIX
        sattr_utime(fileid, 1, 0, 0);
#endif
        
        /* overlaps with resfail */
        get_postopattr1(fileid, &res.u.ok.attr);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_readret);
        if (ret)
                GOTO(err_ret, ret);
        
        __nfs_op_analysis(NFS3_READ);
        __FREE_ARGS(read, buf);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_rep:
        res.status = read_err(ret);
        res.u.fail.attr.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_readret);
err_ret:
        __FREE_ARGS(read, buf);
        return ret;
}

static int __nfs3_write_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                           uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        write_args *args = &_arg->write_arg;
        write_ret res;
        fileid_t *fileid = (fileid_t *)args->file.val;
        const buffer_t *wbuf = (buffer_t *)args->data.val;
        preop_attr attr;

        (void) uid;
        (void) gid;

        ANALYSIS_BEGIN(0);
        
        get_preopattr1(fileid, &attr);

        DBUG("----NFS3---- write "FID_FORMAT" size %u offset %ju\n",
              FID_ARG(fileid), args->count, args->offset);
        
        if (args->data.len == 0) {
                DWARN("write "FID_FORMAT" off %llu size %u\n",
                      FID_ARG(fileid), (LLU)args->offset, args->data.len);
        } else {
                ret = sdfs_write(NULL, fileid, wbuf, args->data.len, args->offset);
                if (ret)
                        GOTO(err_rep, ret);
        }

        res.status = NFS3_OK;
        res.u.ok.count = args->data.len;
        //res.u.ok.committed = args->stable;
        res.u.ok.committed = FILE_SYNC;
        _memcpy(res.u.ok.verf, wverf, NFS3_WRITEVERFSIZE);

        DBUG("write %u\n", res.u.ok.count);
#if ENABLE_MD_POSIX
        sattr_utime(fileid, 0, 1, 1);
#endif
        
        res.u.ok.file_wcc.before = attr;
        get_postopattr1(fileid, &res.u.ok.file_wcc.after);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_writeret);
        if (ret)
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        __nfs_op_analysis(NFS3_WRITE);
        __FREE_ARGS(write, buf);

        return 0;
err_rep:
        res.status = write_err(ret);
        res.u.fail.file_wcc.before.attr_follow = FALSE;
        res.u.fail.file_wcc.after.attr_follow = FALSE;
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_writeret);
err_ret:
        __FREE_ARGS(write, buf);
        return ret;
}

#if !ENABLE_CO_WORKER

static int __core_handler(va_list ap)
{
        int ret;
        nfs_handler handler = va_arg(ap, nfs_handler);
        const sockid_t *sockid = va_arg(ap, const sockid_t *);
        const sunrpc_request_t *req = va_arg(ap, const sunrpc_request_t *);
        uid_t uid = va_arg(ap, uid_t);
        gid_t gid = va_arg(ap, gid_t);
        nfsarg_t *nfsarg = va_arg(ap, nfsarg_t *);
        buffer_t *buf = va_arg(ap, buffer_t *);

        va_end(ap);

        ret = handler(sockid, req, uid, gid, nfsarg, buf);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}
#endif

int nfs_ver3(const sockid_t *sockid, const sunrpc_request_t *req,
             uid_t uid, gid_t gid, buffer_t *buf)
{
        int ret;
        nfs_handler handler = NULL;
        xdr_arg_t xdr_arg = NULL;
        hash_args_t hash_args = NULL;
        nfsarg_t nfsarg;
        xdr_t xdr;
        const char *name;

        switch (req->procedure) {
        case NFS3_NULL:
                handler = __nfs3_null_svc;
                xdr_arg = NULL;
                name = "nfs_null";
                break;

        case NFS3_GETATTR:
                handler = __nfs3_getattr_svc;
                xdr_arg = (xdr_arg_t)xdr_getattrargs;
                hash_args = (hash_args_t)hash_getattr;
                name = "nfs_getattr";
                break;
        case NFS3_SETATTR:
                handler = __nfs3_setattr_svc;
                xdr_arg = (xdr_arg_t)xdr_setattrargs;
                hash_args = (hash_args_t)hash_setattr;
                name = "nfs_setattr";
                break;
        case NFS3_LOOKUP:
                handler = __nfs3_lookup_svc;
                xdr_arg = (xdr_arg_t)xdr_lookupargs;
                hash_args = (hash_args_t)hash_lookup;
                name = "nfs_lookup";
                break;
        case NFS3_ACCESS:
                handler = __nfs3_access_svc;
                xdr_arg = (xdr_arg_t)xdr_accessargs;
                hash_args = (hash_args_t)hash_access;
                name = "nfs_access";
                break;

        case NFS3_FSSTAT:
                handler = __nfs3_fsstat_svc;
                xdr_arg = (xdr_arg_t)xdr_fsstatargs;
                hash_args = (hash_args_t)hash_fsstat;
                name = "nfs_fsstat";
                break;

        case NFS3_FSINFO:
                handler = __nfs3_fsinfo_svc;
                xdr_arg = (xdr_arg_t)xdr_fsinfoargs;
                hash_args = (hash_args_t)hash_fsinfo;
                name = "nfs_fsinfo";
                break;
        case NFS3_PATHCONF:
                handler = __nfs3_pathconf_svc;
                xdr_arg = (xdr_arg_t)xdr_pathconf3_args;
                hash_args = (hash_args_t)hash_pathconf3;
                name = "nfs_pathconf";
                break;

        case NFS3_CREATE:
                handler = __nfs3_create_svc;
                xdr_arg = (xdr_arg_t)xdr_createargs;
                hash_args = (hash_args_t)hash_create;
                name = "nfs_create";
                break;
        case NFS3_MKDIR:
                handler = __nfs3_mkdir_svc;
                xdr_arg = (xdr_arg_t)xdr_mkdirargs;
                hash_args = (hash_args_t)hash_mkdir;
                name = "nfs_mkdir";
                break;
        case NFS3_READDIR:
                handler = __nfs3_readdir_svc;
                xdr_arg = (xdr_arg_t)xdr_readdirargs;
                hash_args = (hash_args_t)hash_readdir;
                name = "nfs_readdir";
                break;
        case NFS3_READDIRPLUS:
                handler = __nfs3_readdirplus_svc;
                xdr_arg = (xdr_arg_t)xdr_readdirplusargs;
                hash_args = (hash_args_t)hash_readdirplus;
                name = "nfs_readdirplus";
                break;
        case NFS3_RMDIR:
                handler = __nfs3_rmdir_svc;
                xdr_arg = (xdr_arg_t)xdr_rmdirargs;
                hash_args = (hash_args_t)hash_rmdir;
                name = "nfs_rmdir";
                break;
        case NFS3_READLINK:
                handler = __nfs3_readlink_svc;
                xdr_arg = (xdr_arg_t)xdr_readlinkargs;
                name = "nfs_readlink";
                break;

        case NFS3_SYMLINK:
                handler = __nfs3_symlink_svc;
                xdr_arg = (xdr_arg_t)xdr_symlinkargs;
                name = "nfs_symlink";
                break;
        case NFS3_MKNOD:
                handler = __nfs3_mknod_svc;
                xdr_arg = (xdr_arg_t)xdr_mknodargs;
                name = "nfs_mknod";
                break;
        case NFS3_REMOVE:
                handler = __nfs3_remove_svc;
                xdr_arg = (xdr_arg_t)xdr_removeargs;
                hash_args = (hash_args_t)hash_remove;
                name = "nfs_remove";
                break;
        case NFS3_RENAME:
                handler = __nfs3_rename_svc;
                xdr_arg = (xdr_arg_t)xdr_renameargs;
                hash_args = NULL;
                name = "nfs_rename";
                break;
        case NFS3_LINK:
                handler = __nfs3_link_svc;
                xdr_arg = (xdr_arg_t)xdr_LINK3args;
                hash_args = NULL;
                name = "nfs_link";
                break;
        case NFS3_COMMIT:
                handler = __nfs3_commit_svc;
                xdr_arg = (xdr_arg_t)xdr_commitargs;
                hash_args = NULL;
                name = "nfs_commit";
                break;
        case NFS3_READ:
                handler = __nfs3_read_svc;
                xdr_arg = (xdr_arg_t)xdr_readargs;
                hash_args = (hash_args_t)hash_read;
                name = "nfs_read";
                break;
        case NFS3_WRITE:
                handler = __nfs3_write_svc;
                xdr_arg = (xdr_arg_t)xdr_writeargs;
                hash_args = (hash_args_t)hash_write;
                name = "nfs_write";
                break;
        default:
                DERROR("error procedure\n");
        }

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

        DBUG("request %s \n", name);

        (void) hash_args;
        
#if ENABLE_CO_WORKER
        ret = handler(sockid, req, uid, gid, &nfsarg, buf);
        if (ret) {
                if (req->procedure == NFS3_LOOKUP) {
                        if (ret != ENOENT) {
                                DERROR("%s (%d) %s\n", name, ret, strerror(ret));
                        }
                } else {
                        DERROR("%s (%d) %s\n", name, ret, strerror(ret));
                }
                GOTO(err_ret, ret);
        }
#else
        if (hash_args) {
                int hash = hash_args(&nfsarg);

                DBUG("core request %s hash %d\n", name, hash);
                ret = core_request(hash, -1, name, __core_handler, handler,
                                   sockid, req, uid, gid, &nfsarg, buf);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                ret = handler(sockid, req, uid, gid, &nfsarg, buf);
                if (ret)
                        GOTO(err_ret, ret);
        }
#endif

        return 0;
err_ret:
        return ret;
}

/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  Copyright (C) 2011       Sebastian Pipping <sebastian@pipping.org>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.

  gcc -Wall fuseyfs.c `pkg-config fuse --cflags --libs` -o fuseyfs
*/


#define FUSE_USE_VERSION 26
#define HAVE_UTIMENSAT
#define DBG_SUBSYS S_YFUSE

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#include <ctype.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#include "configure.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "network.h"
#include "sdfs_quota.h"

#define FUSE_PATH "/yfuse"

extern int __fence_test_need__;
static struct options{
    const char *dir;
    int foreground;
    int service;
    char *mountpoint;
    int allow_other;
}options;

typedef struct {
        int daemon;
        char dir_to_mount[MAX_PATH_LEN];
        char name[MAX_PATH_LEN];
        char home[MAX_PATH_LEN];
        char mountpoint[MAX_PATH_LEN];
        struct fuse_args args;
} yfuse_args_t;

#define OPTION(t, p)                           \
 { t, offsetof(struct options, p), 1 }

static const struct fuse_opt option_spec[] = {
     OPTION("--dir=%s", dir),
     OPTION("-f", foreground),
     OPTION("--service=%d", service),
     OPTION("--allow_other", allow_other),
     FUSE_OPT_END
};

static int fuse_opt_helper_proc(void *data, const char *arg, int key,
                struct fuse_args *outargs)
{
        int ret;
        struct options *opts = data;

        (void) outargs;

        if(key == FUSE_OPT_KEY_NONOPT){
                if(!opts->mountpoint){
                        char mountpoint[MAX_PATH_LEN];
                        if(realpath(arg, mountpoint) == NULL){
                                ret = errno;
                                GOTO(err_ret, ret);
                        }
                        return fuse_opt_add_opt(&opts->mountpoint, mountpoint);
                }else {
                        DERROR("invalid argument : %s\n", arg);
                        return -1;
                }
        }else {
                return -1;
        }

        return 0;
err_ret:
        return ret;
}

void disable_fence_test()
{
    __fence_test_need__ = 0;
}

static int yfs_fusepath(const char *path, char *newpath)
{
        snprintf(newpath, MAX_PATH_LEN, "%s%s", options.dir, path);
        if (newpath[strlen(newpath)- 1] == '/')
                newpath[strlen(newpath)- 1] = '\0';

        return 0;
}

static int yfs_getattr(const char *_path, struct stat *stbuf)
{
        int ret;
        char path[MAX_PATH_LEN];

        yfs_fusepath(_path, path);
        DBUG("getattr  %s\n", path);
        ret = ly_getattr(path, stbuf);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;
}

static int yfs_access(const char *path, int mask)
{
        /*Check file access permissions*/
        (void) path;
        (void) mask;

        DBUG("access: %s\n", path);

        return 0;
}

static int yfs_readlink(const char *_path, char *buf, size_t size)
{
        /*Read the target of a symbolic link*/

        /*The buffer should be filled with a null terminated string.*/
        /*The buffer size argument includes the space for the terminating null character.*/
        /*If the linkname is too long to fit in the buffer, it should be truncated.*/
        /*The return value should be 0 for success. */
        char path[MAX_PATH_LEN];

        yfs_fusepath(_path, path);
        DBUG("readlink  %s\n", path);

        return ly_readlink(path, buf, &size);
}

static int yfs_readdir(const char *_path, void *buf, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi)
{
        int ret, done = 0;
        int delen;
        struct dirent *de0, *de;
		struct stat st;
        char path[MAX_PATH_LEN];

        (void) fi;

        yfs_fusepath(_path, path);
        DBUG("readdir  %s\n", path);

        while (done == 0) {
                delen = 0;
                ret = ly_readdir(path, offset, (void **)&de0, &delen, 0);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                if (delen == 0) {
                        done = 1;
                        break;
                }

                dir_for_each(de0, delen, de, offset) {
                        if (strlen(de->d_name) == 0) {
                                done = 1;
                                break;
                        }

                        memset(&st, 0, sizeof(st));
                        st.st_ino = de->d_ino;
                        st.st_mode = de->d_type << 12;
                        if (filler(buf, de->d_name, &st, 0))
                            break;

                }

                yfree((void **)&de0);
                if(offset == 0)
                    done = 1;
                if(done)
                    break;
        }

        return 0;
err_ret:
        return -ret;
}

static int yfs_mknod(const char *_path, mode_t mode, dev_t rdev)
{
        int ret;
        char path[MAX_PATH_LEN];

        (void)rdev;

        yfs_fusepath(_path, path);
        if (strcmp(path, FUSE_PATH) == 0)
                goto out;
        DBUG("mknode %s, mode : %o\n", path, mode);
        ret = ly_create(path, mode);
        if (unlikely(ret))
                GOTO(err_ret, ret);

out:
        return 0;
err_ret:
        return -ret;
}

static int yfs_create(const char *_path, mode_t mode, struct fuse_file_info *fi)
{
        int ret;
        char path[MAX_PATH_LEN];

        (void) fi;


        yfs_fusepath(_path, path);
        DBUG("create %s, mode : %o\n", path, mode);

        if (strcmp(path, FUSE_PATH) == 0)
                goto out;

        ret = ly_create(path, mode);
        if (unlikely(ret))
                GOTO(err_ret, ret);

out:
        return 0;
err_ret:
        return -ret;
}

static int yfs_mkdir(const char *_path, mode_t mode)
{
        int ret;
        char path[MAX_PATH_LEN];

        yfs_fusepath(_path, path);
        DBUG("mkdir %s, mode : %o\n", path, mode);

        ret = ly_mkdir(path, NULL, mode);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;
}

static int yfs_opendir(const char *_path, struct fuse_file_info *fi)
{
        char path[MAX_PATH_LEN];

        (void)fi;
        yfs_fusepath(_path, path);
        DBUG("opendir %s\n", path);

        return 0;
}

static int yfs_unlink(const char *_path)
{
        int ret;
        char path[MAX_PATH_LEN];

        yfs_fusepath(_path, path);
        DBUG("unlink %s\n", path);

        ret = ly_unlink(path);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;
}

static int yfs_rmdir(const char *_path)
{
        int ret;
        char path[MAX_PATH_LEN];

        yfs_fusepath(_path, path);
        DBUG("rmdir %s\n", path);

        ret = ly_rmdir(path);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;
}

static int yfs_symlink(const char *_target, const char *_link)
{
        int ret;
        char link[MAX_PATH_LEN];

        DBUG("_target:%s\t_link:%s\n", _target, _link);
        yfs_fusepath(_link, link);

        ret = ly_symlink(_target, link);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;
}

static int yfs_rename(const char *_from, const char *_to)
{
        int ret;
        char from[MAX_PATH_LEN];
        char to[MAX_PATH_LEN];

        yfs_fusepath(_from, from);
        yfs_fusepath(_to, to);
        DBUG("rename from %s to %s\n", _from, _to);

        ret = ly_rename(from, to);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;
}

static int yfs_link(const char *_from, const char *_to)
{
        /*Create a hard link to a file */
        int ret;
        char from[MAX_PATH_LEN];
        char to[MAX_PATH_LEN];

        yfs_fusepath(_from, from);
        yfs_fusepath(_to, to);
        DBUG("link from %s to %s\n", _from, _to);

        ret = ly_link(from, to);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;
}

static int yfs_chmod(const char *_path, mode_t mode)
{
        int ret;
        char path[MAX_PATH_LEN];

        yfs_fusepath(_path, path);
        DBUG("chmod %s, mode : %o\n", path, mode);

        ret = ly_chmod(path, mode);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;
}

static int yfs_chown(const char *_path, uid_t uid, gid_t gid)
{
        int ret;
        char path[MAX_PATH_LEN];

        yfs_fusepath(_path, path);
        DBUG("chown %s\n", path);

        ret = ly_chown(path, uid, gid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;
}

static int yfs_truncate(const char *_path, off_t size)
{
        int ret;
        char path[MAX_PATH_LEN];

        yfs_fusepath(_path, path);
        DBUG("truncate %s\n", path);

        ret = ly_truncate(path, size);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;
}

#ifdef HAVE_UTIMENSAT
static int yfs_utimens(const char *_path, const struct timespec ts[2])
{
        int ret;
        char path[MAX_PATH_LEN];
        uint32_t atime, mtime;

        (void) ts;

        yfs_fusepath(_path, path);
        atime =  (ts[0].tv_sec == 0) ? time(NULL) : ts[0].tv_sec;
        mtime =  (ts[1].tv_sec == 0) ? time(NULL) : ts[1].tv_sec;
        DBUG("path: %s, atime : %d, mtime : %d\n", path, atime, mtime);

        ret = ly_utime(path, atime, mtime);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;
}
#endif

static int yfs_open(const char *_path, struct fuse_file_info *fi)
{
        int ret;
        char path[MAX_PATH_LEN];

        (void) fi;

        yfs_fusepath(_path, path);
        DBUG("open %s\n", path);

        ret = ly_open(path);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;
}

static int yfs_read(const char *_path, char *buf, size_t size, off_t offset,
                struct fuse_file_info *fi)
{
        (void) fi;
        char path[MAX_PATH_LEN];

        yfs_fusepath(_path, path);
        DBUG("read %s\n", path);
        YASSERT(path[0] != 0);

        return ly_read(path, buf, size, offset);
}

static int yfs_write(const char *_path, const char *buf, size_t size,
                off_t offset, struct fuse_file_info *fi)
{
        (void) fi;
        char path[MAX_PATH_LEN];

        yfs_fusepath(_path, path);
        YASSERT(path[0] != 0);

        DBUG("write : %s, size:%lu\toffset:%llu\n", path, size, (LLU)offset);

        return ly_write(path, buf, size, offset);
}

static int yfs_statfs(const char *_path, struct statvfs *stbuf)
{
        int ret;
        char path[MAX_PATH_LEN];

        yfs_fusepath(_path, path);
        DBUG("statfs %s\n", path);

        ret = ly_statfs(path, stbuf);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;
}

static int yfs_release(const char *path, struct fuse_file_info *fi)
{
        /* Just a stub.	 This method is optional and can safely be left
           unimplemented */

        (void) path;
        (void) fi;

        DBUG("release %s\n", path);
        return 0;
}

static int yfs_fsync(const char *path, int isdatasync,
                struct fuse_file_info *fi)
{
        /* Just a stub.	 This method is optional and can safely be left
           unimplemented */

        (void) path;
        (void) isdatasync;
        (void) fi;

        DBUG("fsync %s\n", path);
        return 0;
}

#ifdef HAVE_POSIX_FALLOCATE
static int yfs_fallocate(const char *path, int mode,
                off_t offset, off_t length, struct fuse_file_info *fi)
{
        (void) path;
        (void) mode;
        (void) offset;
        (void) length;
        (void) fi;

        DBUG("fallocate %s\n", path);
        return 0;
}
#endif

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int yfs_setxattr(const char *_path, const char *name, const void *value,
                        size_t size, int flags)
{
        int ret;
        char path[MAX_PATH_LEN];

        (void) flags;

        yfs_fusepath(_path, path);
        DBUG("setxattr %s\n", path);

        ret = ly_setxattr(path, name, value, size, flags);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;
}

static int yfs_getxattr(const char *_path, const char *name, char *value,
                size_t size)
{
        int ret;
        char path[MAX_PATH_LEN];

        yfs_fusepath(_path, path);
        DBUG("getxattr %s\n", path);

        ret = ly_getxattr(path, name, value, size);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return -ret;

}

static int yfs_listxattr(const char *path, char *list, size_t size)
{
        (void) path;
        (void) list;
        (void) size;

        UNIMPLEMENTED(__DUMP__);
}

static int yfs_removexattr(const char *path, const char *name)
{
        (void) path;
        (void) name;

        UNIMPLEMENTED(__DUMP__);
}
#endif /* HAVE_SETXATTR */

static struct fuse_operations yfs_oper = {
        .getattr	= yfs_getattr,
        .access		= yfs_access,
        .readlink	= yfs_readlink,
        .readdir	= yfs_readdir,
        .mknod		= yfs_mknod,
        .mkdir		= yfs_mkdir,
        .opendir    = yfs_opendir,
        .symlink	= yfs_symlink,
        .unlink		= yfs_unlink,
        .rmdir		= yfs_rmdir,
        .rename		= yfs_rename,
        .link		= yfs_link,
        .chmod		= yfs_chmod,
        .chown		= yfs_chown,
        .truncate	= yfs_truncate,
#ifdef HAVE_UTIMENSAT
        .utimens	= yfs_utimens,
#endif
        .open		= yfs_open,
        .read		= yfs_read,
        .create     = yfs_create,
        .write		= yfs_write,
        .statfs		= yfs_statfs,
        .release	= yfs_release,
        .fsync		= yfs_fsync,
#ifdef HAVE_POSIX_FALLOCATE
        .fallocate	= yfs_fallocate,
#endif
#ifdef HAVE_SETXATTR
        .setxattr	= yfs_setxattr,
        .getxattr	= yfs_getxattr,
        .listxattr	= yfs_listxattr,
        .removexattr	= yfs_removexattr,
#endif
};

void usage()
{
    fprintf(stderr, "usage: uss_fuse [-f][--allow_other] --dir=remotedir  mountpoint  --service=nums\n");
    fprintf(stderr, "-f             foreground operation\n");
    fprintf(stderr, "--dir          specify the directory to be mounted\n");
    fprintf(stderr, "--service      specify the fuse service no\n");
    fprintf(stderr, "--allow_other  optional, allow access by all users\n");
}

int is_dir_exist(const char *path)
{
        int ret;
        fileid_t fileid;

        ret = sdfs_lookup_recurive(path, &fileid);
        if(ret)
                return 0;
        return 1;
}

int check_is_mounted(const char *mountpoint)
{
        int ret;
        char line[MAX_LINE_LEN];
        FILE *fp = fopen("/proc/mounts", "r");
        if(fp == NULL){
                ret = errno;
                GOTO(err_ret, ret);
        }

        while(fgets(line, MAX_LINE_LEN, fp) != NULL){
                //already mounted
                if(strstr(line, mountpoint) != NULL){
                        ret = EEXIST;
                        GOTO(err_ret, ret);
                }
        }
        if(fp)
                fclose(fp);

        return 0;
err_ret:
        if(fp){fclose(fp);}
        return ret;
}

int try_unmount_fuse(const char *mountpoint)
{
        int ret;
        char cmd[MAX_LINE_LEN], progname[MAX_LINE_LEN];
        struct stat stat_buf;

        snprintf(progname, MAX_LINE_LEN, "/usr/local/bin/fusermount");
        ret = stat(progname, &stat_buf);
        if(ret == ENOENT){
                DWARN("fusermount does not exist\n");
                GOTO(err_ret, ret);
        }

        snprintf(cmd, MAX_LINE_LEN, "%s -u %s", progname, mountpoint);

        ret = _popen(cmd);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int check_argument(struct fuse_args *args, char *dir, char *mountpoint, int *service, int *foreground)
{
        if(fuse_opt_parse(args, &options, option_spec, fuse_opt_helper_proc) == -1){
                DERROR("fuse parse argument failed\n");
                usage();
                return -1;
        }

        if(!options.dir){
                DERROR("--dir must be specified\n");
                usage();
                return -1;
        }

        if(!options.foreground){
                *foreground = 1;
        }else{
                *foreground = 0;
        }

        if(!options.service){
                DERROR("--service must be specified or service no must start with non zero digit\n");
                usage();
                return -1;
        }

        if(fuse_opt_add_arg(args, "-f") == -1){
                DERROR("fuse allocate args failed\n");
                return -1;
        }

        if(fuse_opt_add_arg(args, "-d") == -1){
                DERROR("fuse allocate args failed\n");
                return -1;
        }

        if(fuse_opt_add_arg(args, "-obig_writes,max_write=1048576") == -1){
                DERROR("fuse allocate args failed\n");
                return -1;
        }

        if(options.allow_other) {
                if(fuse_opt_add_arg(args, "-oallow_other") == -1){
                        DERROR("fuse allocate args failed\n");
                        return -1;
                }
        }

        if(strcmp(options.dir, "/") == 0){
                DERROR("/ is not allowed to be mounted\n");
                return -1;
        }

        if(isdigit(options.service)){
                DERROR("service number must be a digit\n");
                return -1;
        }

        *service = options.service;
        strcpy(dir, options.dir);

        if(options.mountpoint){
                if(fuse_opt_add_arg(args, options.mountpoint) == -1){
                        DERROR("fuse allocate args failed\n");
                        return -1;
                }
                strcpy(mountpoint, options.mountpoint);
        }

        //DINFO("mountpoint is %s\n", options.mountpoint);

        return 0;
}

int yfuse_srv(void *argvs)
{
        int ret, daemon;
        yfuse_args_t *yfuse_args;
        struct fuse_args args;
        char path[MAX_PATH_LEN];

        yfuse_args = argvs;
        daemon = yfuse_args->daemon;
        args = yfuse_args->args;

        snprintf(path, MAX_NAME_LEN, "%s/status/status.pid", yfuse_args->home);
        ret = daemon_pid(path);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init(daemon, yfuse_args->name, -1);
        if (ret)
                GOTO(err_ret, ret);

        ret = network_connect_master();
        if (ret){
                GOTO(err_ret, ret);
        }

        if(!is_dir_exist(yfuse_args->dir_to_mount)){
                DERROR("%s is not found\n", yfuse_args->dir_to_mount);
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        ret = fuse_main(args.argc, args.argv, &yfs_oper, NULL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret, service, daemon;
        struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
        char home[MAX_PATH_LEN], name[MAX_NAME_LEN], dir_to_mount[MAX_PATH_LEN], mountpoint[MAX_PATH_LEN];
        yfuse_args_t yfuse_args;


        disable_fence_test();

        ret = check_argument(&args, dir_to_mount, mountpoint, &service, &daemon);
        if(ret)
                GOTO(err_ret, ret);

        sprintf(name, "fuse/%d", service);
        //if -f is specified, damon=1
        ret = ly_prep(daemon, name, -1);
        if (ret)
                GOTO(err_ret, ret);

        sprintf(home, "%s/%s", gloconf.workdir, name);
        yfuse_args.daemon = daemon;
        strcpy(yfuse_args.home, home);
        strcpy(yfuse_args.name, name);
        strcpy(yfuse_args.dir_to_mount, dir_to_mount);
        strcpy(yfuse_args.mountpoint, mountpoint);
        yfuse_args.args = args;

        ret = ly_run(home, yfuse_srv, &yfuse_args);
        if(ret)
                GOTO(err_ret, ret);

        (void) ylog_destroy();

        return 0;
err_ret:
        return ret;
}

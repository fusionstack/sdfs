/*
 * =====================================================================================
 *
 *       Filename:  snap_rollback.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  03/17/2011 09:58:40 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */
#if 1
#include "ylib.h"
#include "dbg.h"
#endif
#include <sys/mount.h>
#include <linux/fs.h>
#include <sys/vfs.h>
#include <uuid/uuid.h>
#include <stdint.h>
#include <stddef.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define EXT3_SUPER_MAGIC     0xEF53
#define REISERFS_SUPER_MAGIC 0x52654973
#define XFS_SUPER_MAGIC      0x58465342
#define JFS_SUPER_MAGIC 0x3153464a

#define DIRTY_PATH       "/sysy/yfs/mds/1/dirty"

void usage(const char *prog)
{
        printf("*** snap_rollback\n"
               "Usage:\n"
               "\t%s [-h]\n"
               "\t%s absolute_lvm_path  absolute_lvm_mountpoint  absolute_lvmsnap_path\n"
               , prog, prog);
}

int _run(const char *path, ...)
{
        int ret, i, son, status;
        va_list ap;
        char *argv[128];
        struct stat stbuf;

        ret = stat(path, &stbuf);
        if (ret < 0) {
                ret = errno;
                goto err_ret;
        }

        son = fork();

        switch (son) {
        case -1:
                ret = errno;
                goto err_ret;
        case 0:
                va_start(ap, path);

                for (i = 0; i < 128; i++) {
                        argv[i] = va_arg(ap, char *);
                        if (argv[i] == NULL)
                                break;
                }

                va_end(ap);

                ret = execv(path, argv);
                if (ret) {
                        ret = errno;
                        goto err_ret;
                }

                break;
        default:
                ret = wait(&status);

                if (WIFexitED(status)) {
                        ret = WexitSTATUS(status);
                        if (ret) {
                                if (ret == ENOENT)
                                        goto err_ret;
                                else
                                        goto err_ret;
                        }
                } else {
                }
        }

        return 0;
err_ret:
        return ret;
}


int main(int argc, char *argv[])
{
        int ret;
        uuid_t uid;
        long f_type;
        int fd;

        char lvm[1024];
        char snap[1024];
        char buf[1024];
        uint64_t offset;
        (void)f_type;

        if (argc == 4) {
                memset(lvm, 0x00, 1024);
                memset(snap, 0x00, 1024);
                uuid_generate(uid);
                uuid_unparse(uid, lvm);
                uuid_generate(uid);
                uuid_unparse(uid, snap);

                memset(buf, 0x00, 1024);
                sprintf(buf, "/%s/%s","var", lvm);
                ret = mkdir(buf, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
                if (ret) {
                        printf("why path %s  exist!\n", buf);
                        goto err_ret;
                }
        
                memset(buf, 0x00, 1024);
                sprintf(buf,"/%s/%s","var", snap);
                memcpy(snap, buf, 1024);
                ret = mkdir(buf, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
                if (ret) {
                        printf("why path %s  exist!\n", buf);
                        goto err_ret;
                }
                printf("0k SNAP path %s\n", snap);
#if 0
                struct statfs vfs;
                ret = statfs(argv[2], &vfs);
                if (ret == -1) {
                        goto err_ret;
                }
                f_type = vfs.f_type;
                if (f_type !=  REISERFS_SUPER_MAGIC) {
                        printf("wrong filesystem\n");
                        goto err_ret;
                }
                ret = mount(argv[3], snap, "ext2", MS_DIRSYNC, NULL);
                if (ret) {
                        printf("mount error\n");
                        goto err_mount;
                }
#endif
                printf("begin mount %s\n", argv[3]);
                ret = _run(0, "/bin/mount","mount",argv[3],snap,NULL);
                if (ret) {
                        printf("mount error, change the snap");
                        goto err_mount;
                }

                ret = _run(0, "/usr/bin/rsync","rsync",snap, argv[2],NULL);
                if (ret) {
                        printf("rsync error, change the snap");
                        goto err_mount;
                }
#ifdef __x86_64__
                ret = sscanf(strstr(argv[3],"snap"), "snap_%lu", &offset);
#else
                ret = sscanf(strstr(argv[3],"snap"), "snap_%llu", &offset);
#endif
                if (ret!=1) {
                        printf("sscanf error\n");
                        goto err_mount;
                }
                fd = open(DIRTY_PATH, O_CREAT|O_TRUNC|O_RDWR);
                if (fd == -1) {
                        printf("open error\n");
                        goto err_mount;
                }
                memset(buf, 0x00, 1024);
#ifdef __x86_64__
                sprintf(buf,"%lu_%lu", offset, offset);
#else
                sprintf(buf,"%llu_%llu", offset, offset);
#endif
                ret = write(fd, buf, 1024);
                if (ret== -1)  {
                        printf("error %s %d\n", strerror(ret),ret);
                        goto err_sync;
                }
                close(fd);
                ret = _run(0, "/bin/umount","umount", snap, NULL);
                if (ret) {
                        printf("umount %s error, manaul\n", snap);
                        goto err_mount;
                }
                sleep(10);
                rmdir(snap);
                return 0;
        } else {
                usage(argv[0]);
        }
        return 0;
err_sync:
        close(fd);
err_mount:
        umount(snap);
        rmdir(snap);
err_ret:
        exit(-1);
}

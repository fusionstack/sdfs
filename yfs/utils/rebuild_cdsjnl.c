

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#include "chk_meta.h"
#include "ylib.h"
#include "job_dock.h"
#include "jnl_proto.h"
#include "dbg.h"

static jnl_handle_t jnl;

void usage(char *prog)
{
        printf("%s: -n cds number\n", prog);
}

static int __rebuild(char *chk_path)
{
        int fd, ret;
        uint32_t buflen;
        chkmeta2_t chkmd;
        chkjnl_t chkjnl;
        int64_t offset;

        DINFO("chk %s\n", chk_path);

        fd = open(chk_path, O_RDONLY);
        if (fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        buflen = sizeof(chkmeta2_t);
        ret = _pread(fd, (void*)&chkmd, buflen, 0);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_fd, ret);
        }

        close(fd);

        _memset(&chkjnl, 0x0, sizeof(chkjnl_t));

        chkjnl.op = CHKOP_WRITE;
        chkjnl.increase = chkmd.chklen;
        chkjnl.chkid = chkmd.chkid;

        offset = jnl_append(&jnl, (const char *)&chkjnl, sizeof(chkjnl_t),
                            NULL, 1, NULL);
        if (offset < 0) {
                ret = -offset;
                GOTO(err_ret, ret);
        }

        return 0;
err_fd:
        close(fd);
err_ret:
        return ret;
}

static int __walkdir(char *path)
{
        int ret;
        struct stat statbuf;
        struct dirent *dirp;
        char childpath[MAX_PATH_LEN];
        DIR *dp;
        char *tmp;

        ret = lstat(path, &statbuf);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        if (S_ISDIR(statbuf.st_mode) == 0) { //Is not dir
                ret = __rebuild(path);
                if (ret)
                        GOTO(err_ret, ret);

                return 0;
        }

        printf("Enter dir: %s\n", path);
        dp = opendir(path);
        if (dp == NULL) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        while ((dirp = readdir(dp)) != NULL) {
                if (strcmp(dirp->d_name, ".") == 0
                    || strcmp(dirp->d_name, "..") == 0) 
                        continue;

                tmp = strrchr(dirp->d_name, '.');

                if (tmp && strcmp(tmp, ".tmp") == 0)
                    continue;

                snprintf(childpath, MAX_PATH_LEN,
                                "%s/%s", path, dirp->d_name);

                ret = __walkdir(childpath);
                if (ret)
                        GOTO(err_dir, ret);
        }

        closedir(dp);

        return 0;
err_dir:
        closedir(dp);
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret;
        int diskno;
        char c_opt;
        char chkdir[MAX_PATH_LEN], path[MAX_PATH_LEN];

        while ((c_opt = getopt(argc, argv, "n:h")) > 0)
                switch (c_opt) {
                case 'n':
                        diskno = atoi(optarg);
                        break;
                case 'h':
                default:
                        usage(argv[1]);
                        exit(1);
                }

        sprintf(path, "/sysy/yfs/cds/%d/chkinfo/jnl/", diskno);
        sprintf(chkdir, "/sysy/yfs/cds/%d/ychunk", diskno);

        ret = jobdock_init(NULL, NULL, NULL);
        if (ret)
                GOTO(err_ret, ret);

        ret = jobtracker_create(&jobtracker, gloconf.yfs_jobtracker, "default");
        if (ret)
                GOTO(err_ret, ret);

        ret = jnl_open(path, &jnl, O_RDWR);
        if (ret)
                GOTO(err_ret, ret);

        ret = __walkdir(chkdir);
        if (ret)
                GOTO(err_jnl, ret);

        jnl_close(&jnl);

        return 0;
err_jnl:
        jnl_close(&jnl);
err_ret:
        return ret;
}

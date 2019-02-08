/*
 * sysutil.c
 *
 * Routines to make the libc/syscall API more pleasant to use. Long term,
 * more libc/syscalls will go in here to reduce the number of .c files with
 * dependencies on libc or syscalls.
 */


#include <sys/mman.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <unistd.h>
#include <netdb.h>
#include <time.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <sys/vfs.h>
#include <ustat.h>
#include <openssl/sha.h>
#include <stdarg.h>
#include <ctype.h>
#include <sys/wait.h>
#include <dirent.h>
#include <linux/aio_abi.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/syscall.h>
#include <libgen.h>
#include <execinfo.h>
#include <linux/fs.h>
#include <fnmatch.h>
#include <sys/types.h>
#include <regex.h>

#define DBG_SUBSYS S_LIBYLIB

#include "sdfs_conf.h"
#include "sysutil.h"
#include "configure.h"
#include "adt.h"
#include "sdfs_id.h"
#include "sdfs_id.h"
#include "ylib.h"
#include "analysis.h"
#include "dbg.h"

int __fence_test_need__ = 0;

static void __ppath(char *ppath, const char *path)
{
        char *c;
        strcpy(ppath, path);

        c = strrchr(ppath, '/');
        if (c)
                *c = '\0';
        else
                strcpy(ppath, "/");
}

int _open_for_samba(const char *path, int flag, mode_t mode)
{
        int ret, fd;
        char ppath[MAX_NAME_LEN];

retry:
        fd = open(path, flag, mode);
        if (fd < 0) {
                ret = errno;
                if (ret == ENOENT) {
                        __ppath(ppath, path);

                        ret = _mkdir(ppath, mode);
                        if (ret) {
                                if (ret == EEXIST)
                                        goto retry;
                                else
                                        GOTO(err_ret, ret);
                        }

                        chmod(ppath, mode);
                        goto retry;
                } else if (ret == EEXIST)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        return fd;
err_ret:
        return -ret;
}


int _set_value_for_samba(const char *path, const char *value, int size, int flag)
{
        int ret, fd;

retry:
        fd = open(path, O_WRONLY | flag, 0777);
        if (fd < 0) {
                ret = errno;
                if ((flag & O_CREAT) && ret == ENOENT) {
                        ret = path_validate_for_samba(path, 0, YLIB_DIRCREATE);
                        if (ret)
                                GOTO(err_ret, ret);

                        goto retry;
                }

                if (ret == EEXIST)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        if (value) {
                ret = _write(fd, value, size);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }
        }

        close(fd);

        return 0;
err_ret:
        return ret;
}

int is_digit_str(const char *str)
{
        int i;
        int len;

        if (NULL == str)
                return 0;

        len = strlen(str);
        if (0 == len)
                return 0;

        for (i = 0; i < len; i++) {
                if (!isdigit(str[i])) {
                        return 0;
                }
        }

        return 1;
}

time_t gettime()
{
        return time(NULL);
}

int _gettimeofday(struct timeval *tv, struct timezone *tz)
{
        return gettimeofday(tv, tz);
}

int date_check(const char *date_time)
{
        int ret;
        int year, mon, day, hour, min, sec;
        char days_of_mon[16] = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

        ret = sscanf(date_time, "%d-%d-%d %d:%d:%d",
                        &year, &mon, &day, &hour, &min, &sec);

        if (ret != 6) {
                ret = EINVAL;
                goto err_ret;
        }

        if (year < 0
                        || mon > 12 || mon < 0
                        || day > days_of_mon[mon] || day < 0
                        || hour > 23 || hour < 0
                        || min > 59 || min < 0
                        || sec > 59 || sec < 0) {
                ret = EINVAL;
                goto err_ret;
        }

        return 0;
err_ret:
        return ret;
}


bool is_valid_name (const char *name)
{
    if (!*name || !((*name >= 'a' && *name <= 'z') || *name == '_'))
        return false;

    while (*++name) {
        if (!((*name >= 'a' && *name <= 'z') ||
              (*name >= '0' && *name <= '9') ||
              *name == '_' || *name == '-' ||
              (*name == '$' && *(name + 1) == '\0')))
            return false;
    }

    return true;
}

void str2time(const char *date_time, uint32_t *unix_time)
{
        struct tm tm_time;

        strptime(date_time, "%Y-%m-%d %H:%M:%S", &tm_time);
        *unix_time = mktime(&tm_time);
}

int pattern_ismatch(const char *pattern, const char *compare, int *match)
{
        int ret;

        if (pattern == NULL || pattern[0] == '\0') {
                *match = 1;
                goto out;
        }

        ret = fnmatch(pattern, compare, FNM_PERIOD);
        if(ret == REG_NOMATCH){
                *match = 0;
                goto out;
        } else if(ret) {
                DERROR("fnmatch fail, ret:%d, errmsg:%s\n", ret, strerror(ret));
                goto err_ret;
        }

        *match = 1;
out:
        return 0;
err_ret:
        return ret;
}

bool is_valid_password(const char *name)
{
        int i;
        char tmp;

        if (!*name)
                return false;

        for (i = 0; i < (int)strlen(name); i++) {
                tmp = *(name + i);
                if (!((tmp >= 'a' && tmp <= 'z') ||
                      (tmp >= '0' && tmp <= '9') ||
                      (tmp >= 'A' && tmp <= 'Z') ||
                      tmp == '_' || tmp == '-' ||
                      (tmp == '$' && *(name + i + 1) == '\0')))
                        return false;
        }

        return true;
}

int _popen(const char *cmd)
{
        int ret;
        FILE* file;
        char buf[MAX_BUF_LEN];

        if (!cmd) {
                ret = EPERM;
                DERROR("cmd was null\n");
                GOTO(err_ret, ret);
        }

        file = popen(cmd, "r");
        if (!file) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }

        DINFO("cmd %s\n", cmd);
        while(fgets(buf, MAX_BUF_LEN, file) != NULL) {
                DINFO("%s\n", buf);
        }

        ret = pclose(file);
        if (ret < 0) {//wait4() error
                ret = errno;
                GOTO(err_ret, ret);
        } else if(WIFEXITED(ret)) {//child exit normally
                if(WEXITSTATUS(ret) != 0){//child return value
                        DERROR("cmd [%s], child return code[%d]\n",
                                        cmd, WEXITSTATUS(ret));
                        GOTO(err_ret, ret);
                }
        } else {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

inline void exit_handler(int sig)
{
        DWARN("got signal %d, exiting\n", sig);

        srv_running = 0;
}

int select_popen_fd(int fd, long timeout)
{
        int ret, maxfd;
        fd_set rfds;
        struct timeval tv;

        FD_ZERO(&rfds);
        FD_SET(fd, &rfds);

        maxfd = fd + 1;
        tv.tv_sec = timeout;
        tv.tv_usec = 0;

        ret = select(maxfd, &rfds, NULL, NULL, &tv);
        if(ret == -1) {
                return -1;
        } else if(ret == 0) {
                return -1;
        } else {
                return 0;
        }
}

int sy_is_mountpoint(const char *path, long f_type)
{
        int ret, ismp = 0;
        char parent[MAX_PATH_LEN];
        struct stat st1, st2;
        struct statfs vfs;

        ret = statfs(path, &vfs);
        if (ret == -1) {
                ret = errno;
                GOTO(out, ret);
        }

        if (vfs.f_type != f_type) {
                DBUG("File system type dismatched: %s [%llu/%llu].\n",
                      path, (LLU)vfs.f_type, (LLU)f_type);
                ret = EINVAL;
                goto out;
        }

        ret = stat(path, &st1);
        if (ret == -1) {
                ret = errno;
                GOTO(out, ret);
        }

        snprintf(parent, MAX_PATH_LEN, "%s/..", path);
        ret = stat(parent, &st2);
        if (ret == -1) {
                ret = errno;
                GOTO(out, ret);
        }

        // DINFO("%lu %lu\n", st1.st_dev, st2.st_dev);
        if (st1.st_dev != st2.st_dev)
                ismp = 1;
        else
                DWARN("%s and %s have the same st_dev.\n", path, parent);

out:
        if (!ismp)
                DBUG("%s is not a mountpoint.\n", path);

        return ismp;
}

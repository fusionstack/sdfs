

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "ylib.h"
#include "fnotify.h"
#include "ylog.h"
#include "dbg.h"

/*
 * open the error log
 *
 * we have 3 possilities:
 * - stderr (default)
 * - syslog
 * - logfile
 */

ylog_t *ylog = NULL;

ylog_t __ylog__;

int ylog_init(logmode_t logmode, const char *file)
{
        int ret, fd;

        if (ylog == NULL) {
                fd = -1;

                if (logmode == YLOG_FILE && file) {
                        ret = sy_rwlock_init(&__ylog__.lock, NULL);
                        if (ret) {
                                fprintf(stderr, "ylog init %u", ret);
                                goto err_ret;
                        }

                        __ylog__.count = 0;
                        __ylog__.time = 0;

                        fd = _open(file, O_APPEND | O_CREAT | O_WRONLY, 0644);
                        if (fd == -1) {
                                ret = errno;
                                fprintf(stderr, "open(%s, ...) ret (%d) %s\n", file, ret,
                                       strerror(ret));
                                goto err_ret;
                        }
                } else if (logmode == YLOG_STDERR)
                        fd = 2;

                __ylog__.logfd = fd;
                __ylog__.logmode = logmode;
                ylog = &__ylog__;
        }

        return 0;
err_ret:
        return ret;
}

int ylog_destroy(void)
{
        if (ylog) {
                if (ylog->logmode == YLOG_FILE && ylog->logfd != -1)
                        (void) sy_close(ylog->logfd);

                //yfree((void **)&ylog);
        }

        return 0;
}

int ylog_write(logtype_t type, const char *_msg)
{
        int ret;
        //time_t now;
        //char msg_buf[MAX_BUF_LEN];

        (void) type;
        
        if (ylog && ylog->logmode == YLOG_FILE && ylog->logfd != -1) {
#if 0
                ret = sy_rwlock_wrlock(&ylog->lock);
                if (ret) {
                        fprintf(stderr, "ylog write error %u", ret);
                        EXIT(ret);
                }

                ylog->count++;

                if (ylog->count % 100 == 0) {
                        now = time(NULL);
                        if (now - ylog->time < 10) {
                                snprintf(msg_buf, MAX_BUF_LEN,
                                         "%lu:%s:%s( ):%4d:%6lu:%6lu: WARNING: too many log, sleep 2, count %u, diff %u\n",
                                         (unsigned long)time(NULL),            
                                         __FILE__, __FUNCTION__, __LINE__, 
                                         (unsigned long)getpid( ),
                                         (unsigned long)pthread_self( ), ylog->count, (int)(now - ylog->time));

                                ret = write(ylog->logfd, msg_buf, strlen(msg_buf));
                                if (ret < 0) {
                                        ret = errno;
                                        fprintf(stderr, "ylog write error %u", ret);
                                        EXIT(ret);
                                }

                                //sleep(2);
                        }

                        ylog->time = time(NULL);
                }

                sy_rwlock_unlock(&ylog->lock);
#endif

                ret = write(ylog->logfd, _msg, strlen(_msg));
                if (ret < 0) {
                        ret = errno;
                        fprintf(stderr, "ylog write error %u", ret);
                        EXIT(ret);
                }
        } else
                fprintf(stderr, "%s", _msg);

        return 0;
}

int yroc_create(const char *filename, int *fd)
{
        DIR * rootdir;
        int fddir;
        int fdnew;
        int ret = -1;
        char status[] = "status", dir[MAX_PATH_LEN];

        if (!filename || !strlen(filename))
                GOTO(err_ret, errno);

        snprintf(dir, sizeof(dir), "%s/%s", YROC_ROOT, filename);

        ret = path_validate(dir, YLIB_ISDIR, YLIB_DIRCREATE);
        if (ret)
                GOTO(err_ret, ret);

        rootdir = opendir(dir);
        if (NULL == rootdir) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        fddir = dirfd(rootdir);
        if (-1 == fddir) {
                ret = errno;
                GOTO(close_fddir, ret);
        }

        fdnew = openat(fddir, status, O_CREAT | O_WRONLY, FILEMODE);
        if (-1 == fdnew) {
                ret = errno;
                GOTO(close_fdnew, ret);
        }

        *fd = fdnew;

        close(fddir);
        closedir(rootdir);
        return 0;
close_fdnew:
        close(fdnew);
close_fddir:
        close(fddir);
err_ret:
        return ret;
}

int yroc_write(int fd, const void * buffer, size_t count)
{
        int ret = -1;


        if (!count || !buffer)
                return 0;

        lseek(fd, 0 , SEEK_SET);

        while (count > 0) {
                ssize_t bytes_write;

                bytes_write = write(fd, buffer, count);
                if (-1 == bytes_write) {
                        if (EINTR == errno)
                                continue;
                        else
                                GOTO(err_ret, errno);
                }

                count -= bytes_write;
        }

        ret = 0;
err_ret:
        return ret;
}

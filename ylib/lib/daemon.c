

#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/resource.h>
#ifndef __CYGWIN__
#include <sys/prctl.h>
#endif
#include <sys/ioctl.h>
#include <sys/file.h>
#include <unistd.h>
#include <stdlib.h>
#include <signal.h>
#include <locale.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>

#define DBG_SUBSYS S_LIBYLIB

#include "configure.h"
#include "sysutil.h"
#include "ylib.h"
#include "dbg.h"
#include "ylog.h"

uint64_t nofile_cur;
uint64_t nofile_max;

#define YFS_PID_DIR "/dev/shm/uss/pid"

int is_daemon = -1;

int get_nodeid(uuid_t id, const char *nodeid_file)
{
        int ret, fd, retry = 0;
        uint32_t len;
        struct stat stbuf;
        char uuid[128];
        
        ret = path_validate(nodeid_file, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (ret)
                GOTO(err_ret, ret);

        /* avoid multiple copies */
        fd = open(nodeid_file, O_CREAT | O_RDWR, 0640);
        if (fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

retry:
        ret = flock(fd, LOCK_EX | LOCK_NB);
        if (ret == -1) {
                ret = errno;
                if (ret == EAGAIN) {
                        if (retry < 100) {
                                DWARN("retry lock\n");
                                retry++;
                                usleep(100000);
                                goto retry;
                        }
                } else {
                        GOTO(err_ret, ret);
                }
        }

        ret = fstat(fd, &stbuf);
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        if (stbuf.st_size == 0) {
                uuid_generate(id);
                ret = _write(fd, (char *)id, sizeof(uuid_t));
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_fd, ret);
                }
        } else {
                len = sizeof(uuid_t);
                ret = _read(fd, (char *)id, len);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_fd, ret);
                }
        }

        uuid_unparse(id, uuid);

        DINFO("get nodeid %s\n", uuid);

        ret = flock(fd, LOCK_UN);
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = sy_close(fd);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_fd:
        (void) sy_close(fd);
err_ret:
        return ret;
}

int daemonlize(int daemon, int maxcore, char *chr_path, int preservefd, int64_t _maxopenfile)
{
        int ret, fd, i;
        int64_t maxopenfile;
        struct rlimit rlim, rlim_new;

        maxopenfile = _maxopenfile == -1 ? MAX_OPEN_FILE : _maxopenfile;

        maxcore = gloconf.maxcore;

        if (maxcore) {
                DINFO("maxcore on!\n");

                /*
                 * first try raising to infinity; if that fails, try bringing
                 * the soft limit to the hard.
                 */
                ret = getrlimit(RLIMIT_CORE, &rlim);
                if (ret == 0) {
                        rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;

                        ret = setrlimit(RLIMIT_CORE, &rlim_new);
                        if (ret == -1) {
                                ret = errno;
                                DWARN("%d - %s\n", ret, strerror(ret));

                                /* failed. try raising just to the old max */
                                rlim_new.rlim_cur = rlim_new.rlim_max
                                                  = rlim.rlim_max;

                                ret = setrlimit(RLIMIT_CORE, &rlim_new);
                                if (ret == -1) {
                                        ret = errno;
                                        DWARN("%d - %s\n", ret, strerror(ret));
                                }
                        }
                } else {        /* -1 */
                        ret = errno;
                        DWARN("%d - %s\n", ret, strerror(ret));
                }

                /*
                 * getrlimit again to see what we ended up with. only fail if
                 * the soft limit ends up 0, because then no core files will
                 * be created at all.
                 */

                 ret = getrlimit(RLIMIT_CORE, &rlim);
                 if (ret == -1 || rlim.rlim_cur == 0) {
                        if (ret == -1)
                                ret = errno;
                        else
                                ret = EPERM;

                        GOTO(err_ret, ret);
                 }
        }

        rlim_new.rlim_cur = maxopenfile;
        rlim_new.rlim_max = maxopenfile;  /* XXX max open file num  */
        ret = setrlimit(RLIMIT_NOFILE, &rlim_new);
        if (ret == -1) {
                ret = errno;
                DWARN("%d - %s\n", ret, strerror(ret));
        }

        ret = getrlimit(RLIMIT_NOFILE, &rlim_new);
        if (ret == -1) {
                ret = errno;
                DWARN("%d - %s\n", ret, strerror(ret));
        }

        nofile_cur = rlim_new.rlim_cur;
        nofile_max = rlim_new.rlim_max;

        DBUG("max %llu\n", (LLU)rlim_new.rlim_max);

        srandom(getpid());

        /* set stderr non-buffering (for running under, say, daemontools) */
        setbuf(stderr, NULL);

        /* read zone info now, in case we chroot() */
        tzset();

        /* for nice %b handling in strfime() */
        setlocale(LC_TIME, "C");

        /* enable core dump */
        prctl(PR_SET_DUMPABLE, 1, 0, 0, 0);

        /* set signal handler */
        signal(SIGPIPE, SIG_IGN);

        /* set Umask */
        (void) umask(022);

        if (daemon) {
                /* set signal handler */
                //if (signal(SIGTERM, termination_handler) == SIG_IGN)
                signal(SIGTERM, SIG_IGN);
                signal(SIGHUP, SIG_IGN);
                signal(SIGINT, SIG_IGN);

                /* operate in background */
                ret = fork();
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                } else if (ret > 0)          /* parent, exit */
		{
#ifndef __CYGWIN__
			exit(0);//需要把退出码改为0，退出码为1，测试脚本直接退出
#endif
		}

                /*
                 * be the process group leader
                 * even if we don't daemonize, we still want to disown our
                 * parent process
                 */
                ret = setsid();
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        /* close inherited descriptors */
        if (daemon) {
                for (i = getdtablesize() - 1; i >= 0; --i) {
                        if (i != preservefd)
                                sy_close_failok(i);
                }
        }

        /* open standard descriptors */
        fd = open("/dev/null", O_RDWR);        /* stdin */
        if (fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }
        ret = dup(fd);                        /* stdout */
        ret = dup(fd);                        /* stderr */

        /*
         * detach from controlling terminal
         * and set process group if needed
         */
        fd = open("/dev/tty", O_RDWR);
        if (fd != -1) {
                if (getpgrp() != getpid()) {
                        (void) ioctl(fd, TIOCNOTTY, 0);
                        (void) setpgid(0, getpid());
                }
                (void) sy_close(fd);
        }

        /* move to a safe and known directory */
        if (chr_path)
                ret = chdir(chr_path);
        else
                ret = chdir("/");


        return 0;
err_ret:
        return ret;
}

int daemon_pid(const char *path)
{
        int ret, fd;
        char pid[128];

        ret = path_validate(path, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (ret)
                GOTO(err_ret, ret);

        /* record process ID */
        fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0640);
        if (fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        snprintf(pid, 128, "%d\n", getpid());

        DBUG("get pid %s\n", pid);

        ret = _write(fd, pid, _strlen(pid));
        if (ret < 0) {
                ret = -ret;
                GOTO(err_fd, ret);
        }

        close(fd);

        return 0;
err_fd:
        close(fd);
err_ret:
        return ret;
}



int daemon_lock(const char *key)
{
        int ret, fd, retry = 0, flags;

        DINFO("lock %s\n", key);

        ret = path_validate(key, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (ret)
                GOTO(err_ret, ret);

        /* avoid multiple copies */
        fd = open(key, O_CREAT | O_RDONLY, 0640);
        if (fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        flags = fcntl(fd, F_GETFL, 0);
        if (flags < 0 ) {
                ret = errno;
                GOTO(err_fd, ret);
        }

        ret = fcntl(fd, F_SETFL, flags | FD_CLOEXEC);
        if (ret < 0) {
                ret = errno;
                GOTO(err_fd, ret);
        }

retry:
        ret = flock(fd, LOCK_EX | LOCK_NB);
        if (ret == -1) {
                ret = errno;
                if (ret == EWOULDBLOCK) {
                        if (retry < 5) {
                                DINFO("lock %s fail\n", key);
                                sleep(1);
                                retry++;
                                goto retry;
                        } else {
                                DWARN("lock %s fail\n", key);
                                GOTO(err_fd, ret);
                        }
                } else
                        GOTO(err_fd, ret);
        }

        return fd;
err_fd:
        close(fd);
err_ret:
        return -ret;
}

int daemon_update(const char *key, const char *value)
{
        int ret, fd;
        char path[MAX_PATH_LEN];

        snprintf(path, MAX_PATH_LEN, "%s", key);

        ret = path_validate(path, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (ret)
                GOTO(err_ret, ret);

        fd = open(key, O_RDWR, 0640);
        if (fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = flock(fd, LOCK_SH | LOCK_NB);
        if (ret == -1) {
                ret = errno;
                if (ret == EWOULDBLOCK) {
                        DBUG("lock complete\n");
                } else
                        GOTO(err_fd, ret);
        } else {
                YASSERT(0);
        }

        ret = ftruncate(fd, 0);
        if (ret)
                GOTO(err_fd, ret);

        ret = _write(fd, value, strlen(value));
        if (ret < 0) {
                ret = -ret;
                GOTO(err_fd, ret);
        }

        ret = fsync(fd);
        if (ret < 0) {
                ret = errno;
                GOTO(err_fd, ret);
        }

        close(fd);

        return 0;
err_fd:
        close(fd);
err_ret:
        return ret;
}

int lock_pid(const char *name, int seq)
{
        int ret, fd;
        char pid[MAX_PATH_LEN], lockpath[MAX_PATH_LEN], pidpath[MAX_PATH_LEN];

        if (seq) {
                snprintf(lockpath, MAX_PATH_LEN, "%s/%s/%u/%s/lock",
                         gloconf.workdir, name, seq, YFS_STATUS_PRE);

                snprintf(pidpath, MAX_PATH_LEN, "%s/%s.%u", YFS_PID_DIR, name, seq);
        } else {
                snprintf(lockpath, MAX_PATH_LEN, "%s/%s/%slock",
                         gloconf.workdir, name, YFS_STATUS_PRE);

                snprintf(pidpath, MAX_PATH_LEN, "%s/%s", YFS_PID_DIR, name);
        }

        DINFO("lock %s pid %s\n", lockpath, pidpath);

        ret = path_validate(lockpath, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (ret)
                GOTO(err_ret, ret);

        ret = path_validate(pidpath, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (ret)
                GOTO(err_ret, ret);

        /* avoid multiple copies */
        fd = open(lockpath, O_CREAT | O_RDONLY, 0640);
        if (fd == -1) {
                ret = errno;

                GOTO(err_ret, ret);
        }

        ret = flock(fd, LOCK_EX | LOCK_NB);
        if (ret == -1) {
                ret = errno;

                DWARN("lock %s fail\n", lockpath);

                GOTO(err_ret, ret);
        }

        /* record process ID */
        fd = open(pidpath, O_WRONLY | O_CREAT | O_EXCL | O_TRUNC, 0640);
        if (fd == -1) {
                struct stat st;

                ret = errno;

                if (ret != EEXIST)
                        GOTO(err_ret, ret);

                ret = stat(pidpath, &st);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                if (!S_ISREG(st.st_mode)) {
                        ret = EINVAL;

                        switch (st.st_mode & S_IFMT) {
                        case S_IFBLK:  DERROR("block device\n");     break;
                        case S_IFCHR:  DERROR("character device\n"); break;
                        case S_IFDIR:  DERROR("directory\n");        break;
                        case S_IFIFO:  DERROR("FIFO/pipe\n");        break;
                        case S_IFLNK:  DERROR("symlink\n");          break;
                        case S_IFREG:  DERROR("regular file\n");     break;
                        case S_IFSOCK: DERROR("socket\n");           break;
                        default:       DERROR("unknown?\n");         break;
                        }

                        GOTO(err_ret, ret);
                }

                fd = open(pidpath, O_WRONLY | O_CREAT | O_TRUNC, 0640);
                if (fd == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        (void)snprintf(pid, MAX_PATH_LEN, "%6d", getpid( ));

        DBUG("get pid %s\n", pid);

        ret = _write(fd, pid, _strlen(pid));
        if (ret < 0) {
                ret = -ret;
                GOTO(err_fd, ret);
        }

        ret = sy_close(fd);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_fd:
        (void) sy_close(fd);
err_ret:
        return ret;
}

int server_run(int daemon, const char *lockname, int seq, int (*server)(void *), void *args)
{
        int ret, son, status;

        while (srv_running && daemon) {
                son = fork();

                switch (son) {
                case -1:
                        ret = errno;
                        GOTO(err_ret, ret);
                case 0:
                        ret = lock_pid(lockname, seq);
                        if (ret) {
                                DWARN("%s already running\n", lockname);
                                goto out;
                        }

                        ret = server(args);
                        if (ret) {
                                DERROR("service start fail, exit\n");
                                FATAL_EXIT("");
                        }

                        goto out;
                        break;
                default:
                        while (srv_running) {
                                ret = wait(&status);
                                if (ret == son)
                                        break;

                                ret = errno;
                                DERROR("Monitor: %d\n", ret);
                        }

                        if (WIFEXITED(status)) {
                                ret = WEXITSTATUS(status);
                                if (ret == FATAL_RETVAL) {
                                        DBUG("Monitor: exit monitor\n");
                                        goto out;
                                } else {
                                        DBUG("Monitor: worker exited normally %d\n",
                                             WEXITSTATUS(status));
                                }

                                break;
                        } else if (WIFSIGNALED(status)) {
                                DERROR("Monitor: worker exited on signal %d, runing %u\n"
                                       " restarting...\n", WTERMSIG(status), srv_running);
                        } else {
                                DERROR("Monitor: worker exited (stopped?) %d\n"
                                       " restarting...\n", status);
                        }
                }

                if (daemon == 0)
                        break;
        }

        if (!daemon) {
                ret = lock_pid(lockname, seq);
                if (ret) {
                        DWARN("%s already running\n", lockname);
                        goto out;
                }

                ret = server(args);
                if (ret)
                        GOTO(err_ret, ret);
        }

out:
        return FATAL_RETVAL;
err_ret:
        return ret;
}

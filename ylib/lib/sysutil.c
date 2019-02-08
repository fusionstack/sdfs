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

int srv_running = 1;
int rdma_running = 1;

#undef MAXSIZE_LOGFILE
#define BACKTRACE_SIZE (1024 * 8)

int sy_peek(int sd, char *buf, uint32_t buflen)
{
        int ret;
        uint32_t toread;

        ret = ioctl(sd, FIONREAD, &toread);
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        toread = toread < buflen ? toread : buflen;

        ret = recv(sd, buf, toread, MSG_PEEK);
        if (ret == -1) {
                ret = errno;
                DERROR("peek errno %d\n", ret);
                GOTO(err_ret, ret);
        }

        if (ret == 0) {
                ret = ECONNRESET;
                goto err_ret;
        }

        return ret;
err_ret:
        return -ret;
}

int sy_close(int fd)
{
        int ret;

        while (1) {
                ret = close(fd);
                if (ret == -1) {
                        ret = errno;

                        if(ret == EINTR)
                                continue;

                        GOTO(err_ret, ret);
                } else
                        break;
        }

        return 0;
err_ret:
        return ret;
}

void sy_close_failok(int fd)
{
        (void) close(fd);
}

void sy_msleep(uint32_t milisec)
{
        int ret;
        time_t sec;
        struct timespec req = {0, 0};

        sec = (int) (milisec/1000);
        milisec = milisec - (sec * 1000);

        req.tv_sec = sec;
        req.tv_nsec = milisec * 1000000L;

        while ((ret = nanosleep(&req, &req)) == -1) {
                ret = errno;
                DWARN("(%d) %s\n", ret, strerror(ret));

                continue;
        }
}

int sy_isidle()
{
        int ret;

        double one, five, fifteen;
        int active, total, last;
        FILE *fp;

        fp = fopen("/proc/loadavg", "r");
        if (fp == NULL) {
                DERROR("/proc/loadavg not existed.\n");
                return 0;
        }

        ret = fscanf(fp, "%lf %lf %lf %d/%d %d", &one, &five, &fifteen,
                     &active, &total, &last);
        YASSERT( ret == 6);
        fclose(fp);

        if (one > 1.0)
                return 0;

        return 1;
}

inline void *_memset(void *s, int c, size_t n)
{
        return memset(s, c, n);
}

inline void *_memmove(void *dest, const void *src, size_t n)
{
        return memmove(dest, src, n);
}

inline void *_memcpy(void *dest, const void *src, size_t n)
{
        return memcpy(dest, src, n);
}

inline ssize_t _splice(int fd_in, loff_t *off_in, int fd_out,
                      loff_t *off_out, size_t len, unsigned int flags)
{
        int ret;

        while (1) {
                ret = splice(fd_in, off_in, fd_out, off_out, len, flags); 
                if (ret == -1) {
                        ret = errno;
                        if (ret == EINTR) {
                                DERROR("interrupted");
                                continue;
                        } else
                                GOTO(err_ret, ret);
                }

                break;
        }

        return ret;
err_ret:
        return -ret;
}

/**
 * @timeout specifies the minimum number of milliseconds that epoll_wait() will block
 */
inline int _epoll_wait(int epfd, struct epoll_event *events,
                       int maxevents, int timeout)
{
        int ret, nfds;

        while (1) {
                nfds = epoll_wait(epfd, events, maxevents, timeout);
                if (nfds == -1) {
                        ret = errno;
                        if (ret == EINTR) {
                                DBUG("file recv loop interrupted by signal\n");
                                continue;
                        } else
                                GOTO(err_ret, ret);
                }

                break;
        }

        return nfds;
err_ret:
        return -ret;
}

inline ssize_t _read(int fd, void *buf, size_t count)
{
        int ret;

        while (1) {
                ret = read(fd, buf, count);
                if (ret == -1) {
                        ret = errno;
                        if (ret == EINTR)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }

                break;
        }

        return ret;
err_ret:
        return -ret;
}

inline ssize_t _write(int fd, const void *buf, size_t count)
{
        int ret;

        while (1) {
                ret = write(fd, buf, count);
                if (ret == -1) {
                        ret = errno;

                        if (ret == EINTR)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }

                break;
        }

        return ret;
err_ret:
        return -ret;
}

inline ssize_t _pread(int fd, void *buf, size_t count, off_t offset)
{
        int ret;

        while (1) {
                ret = pread(fd, buf, count, offset);
                if (ret == -1) {
                        ret = errno;

                        if (ret == EINTR)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }

                break;
        }

        return ret;
err_ret:
        return -ret;
}

inline ssize_t _pwrite(int fd, const void *buf, size_t count, off_t offset)
{
        int ret;

        while (1) {
                ret = pwrite(fd, buf, count, offset);
                if (ret == -1) {
                        ret = errno;

                        if (ret == EINTR)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }

                break;
        }

        return ret;
err_ret:
        return -ret;
}

inline ssize_t _send(int sockfd, const void *buf, size_t len, int flags)
{
        int ret;

        ANALYSIS_BEGIN(0);

        while (1) {
                ret = send(sockfd, buf, len, flags);
                if (ret == -1) {
                        ret = errno;

                        if (ret == EINTR) {
                                DERROR("interrupted");
                                continue;
                        } else if (ret == EAGAIN || ret == ECONNREFUSED
                                 || ret == EHOSTUNREACH)
                                goto err_ret;
                        else
                                GOTO(err_ret, ret);
                }

                break;
        }

        ANALYSIS_END(0, 5000, NULL);

        return ret;
err_ret:
        return -ret;
}

inline ssize_t _recv(int sockfd, void *buf, size_t len, int flags)
{
        int ret;

        while (1) {
                ret = recv(sockfd, buf, len, flags);
                if (ret == -1) {
                        ret = errno;

                        if (ret == EINTR) {
                                DERROR("interrupted");
                                continue;
                        } else
                                goto err_ret;
                }

                break;
        }

        if (ret == 0) {
                ret = ECONNRESET;
                goto err_ret;
        }

        return ret;
err_ret:
        return -ret;
}

inline ssize_t _recvmsg(int sockfd, struct msghdr *msg, int flags)
{
        int ret;

        while (1) {
                ret = recvmsg(sockfd, msg, flags);
                if (ret == -1) {
                        ret = errno;
                        if (ret == EINTR) {
                                DERROR("interrupted");
                                continue;
                        } else
                                goto err_ret;
                }

                break;
        }

        if (ret == 0) {
                ret = ECONNRESET;
                goto err_ret;
        }

        return ret;
err_ret:
        return -ret;
}

inline ssize_t _sendmsg(int sockfd, struct msghdr *msg, int flags)
{
        int ret;

        while (1) {
                ret = sendmsg(sockfd, msg, flags);
                if (ret == -1) {
                        ret = errno;
                        if (ret == EINTR) {
                                DERROR("interrupted");
                                continue;
                        } else
                                goto err_ret;
                }

                break;
        }

#if 0
        if (ret == 0) {
                ret = ECONNRESET;
                goto err_ret;
        }
#endif

        return ret;
err_ret:
        return -ret;
}


inline size_t _strlen(const char *s)
{
        if (s == NULL)
                return 0;
        return strlen(s);
}

inline char *_strcpy(char *dest, const char *src)
{
        return strcpy(dest, src);
}

inline char *_strncpy(char *dest, const char *src, size_t n)
{
        return strncpy(dest, src, n);
}

#if 0
inline int _gettimeofday(struct timeval *tv, struct timezone *tz)
{
        return gettimeofday(tv, tz);
}
#endif

inline int _strcmp(const char *s1, const char *s2)
{
        return strcmp(s1, s2);
}

inline int _epoll_ctl(int epfd, int op, int fd, struct epoll_event *event)
{
        return epoll_ctl(epfd, op, fd, event);
}

inline int _posix_memalign(void **memptr, size_t alignment, size_t size)
{
        return posix_memalign(memptr, alignment, size);
}

inline ssize_t _writev(int fd, const struct iovec *iov, int iovcnt)
{
        int ret;

        while (1) {
                ret = writev(fd, iov, iovcnt);
                if (ret == -1) {
                        ret = errno;

                        if (ret == EINTR)
                                continue;
                        else if (ret == EAGAIN)
                                goto err_ret;
                        else
                                GOTO(err_ret, ret);
                }

                break;
        }

        return ret;
err_ret:
        return -ret;
}

inline ssize_t _readv(int fd, const struct iovec *iov, int iovcnt)
{
        int ret;

        while (1) {
                ret = readv(fd, iov, iovcnt);
                if (ret == -1) {
                        ret = errno;

                        if (ret == EINTR)
                                continue;
                        else if (ret == EAGAIN)
                                goto err_ret;
                        else
                                GOTO(err_ret, ret);
                }

                break;
        }

        return ret;
err_ret:
        return -ret;
}

inline ssize_t _sendfile(int out_fd, int in_fd, off_t *offset, size_t count)
{
        int ret;

        while (1) {
                ret = sendfile(out_fd, in_fd, offset, count);
                if (ret == -1) {
                        ret = errno;

                        if (ret == EINTR)
                                continue;
                        else if (ret == EAGAIN)
                                goto err_ret;
                        else
                                GOTO(err_ret, ret);
                }

                break;
        }

        return ret;
err_ret:
        return -ret;
}

inline int _sem_wait(sem_t *sem)
{
        int ret;

        while (1) {
                ret = sem_wait(sem);
                if (unlikely(ret)) {
                        ret = errno;
                        if (ret == EINTR)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }

                break;
        }

        return 0;
err_ret:
        return ret;
}

inline int _sem_timedwait(sem_t *sem, const struct timespec *abs_timeout)
{
        int ret;

        while (1) {
                ret = sem_timedwait(sem, abs_timeout);
                if (unlikely(ret)) {
                        ret = errno;
                        if (ret == ETIMEDOUT)
                                goto err_ret;
                        else if (ret == EINTR)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }

                break;
        }

        return 0;
err_ret:
        return ret;
}

inline int _sem_timedwait1(sem_t *sem, int tmo)
{
        int ret;
        struct timespec ts;

        YASSERT(tmo > 0);

        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += tmo;

        while (1) {
                ret = sem_timedwait(sem, &ts);
                if (unlikely(ret)) {
                        ret = errno;
                        if (ret == ETIMEDOUT)
                                goto err_ret;
                        else if (ret == EINTR)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }

                break;
        }

        return 0;
err_ret:
        return ret;
}

/* 
 *para path should as /xxx/xxx
 *there should only has file type DT_REG
 *or DT_DIR in path, or will crash
 * */
int _delete_path(const char *path)
{
	char tmp[MAX_PATH_LEN] = {0};
	DIR  *dir;
	struct dirent *ptr;
	int ret;
        struct stat stbuf;
	/*fstat(path,&stat) 
	 *S_ISDIR(stat.st_mode);
	 * */
	dir = opendir(path);
	if (!dir) {
		if (errno == ENOTDIR)
			unlink(path);
		return 0;
	}

	while((ptr = readdir(dir))!=NULL) {
		if (strcmp(ptr->d_name,".")==0 
		    ||strcmp(ptr->d_name,"..")==0)
			continue;

		sprintf(tmp,"%s/%s",path, ptr->d_name);

                ret = stat(tmp, &stbuf);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                if (S_ISREG(stbuf.st_mode)) {
			ret = unlink(tmp);
                        if (ret < 0) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }

                } else if (S_ISDIR(stbuf.st_mode)) {
			_delete_path(tmp);
                }
	}

	closedir(dir);
	ret = rmdir(path);
	if (ret < 0){
		ret = errno;
		GOTO(err_ret, ret);
	}

	return 0;
err_ret:
	return ret;
}

int _unlink(const char *path, const char *context)
{
        DINFO("path %s context %s\n", path, context);
        return unlink(path);
}

int _sha1_file(int fd, uint64_t offset, uint32_t size, unsigned char *md)
{
        int ret;
        SHA_CTX c;
        char buf[MAX_BUF_LEN];
        int64_t off, left, cp, end;

        ret = SHA1_Init(&c);
        if (ret == 0) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

#if 0
        struct stat stbuf;
        ret = fstat(fd, &stbuf);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        if (offset >= (uint64_t)stbuf.st_size) {
                DWARN("offset %llu size %llu\n", (LLU)offset, (LLU)stbuf.st_size);
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        end = offset + size < (uint64_t)stbuf.st_size ? offset + size : (uint64_t)stbuf.st_size;
#endif
        end = offset + size;


        for (off = offset; off < end; off += MAX_BUF_LEN) {
                left = end - off;

                cp = left < MAX_BUF_LEN ? left : MAX_BUF_LEN;

                ret = _pread(fd, buf, cp, off);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                ret = SHA1_Update(&c, buf, cp);
                if (ret == 0) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }

        ret = SHA1_Final(md, &c);
        if (ret == 0) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

void _sha1_print(char *str, const unsigned char *md)
{
        const uint32_t *x = (void *)md;

        sprintf(str, "%x%x%x%x%x", x[0], x[1], x[2], x[3], x[4]);
}

int _hex_print(char *output, int outlen, const void *input, const int inlen)
{
        int ret, i;

        if (outlen < inlen * 2 + 1) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        for (i = 0; i < inlen; i++) {
                sprintf(output + i * 2, "%02x", *((unsigned char *)input + i));
        }

        *(output + i * 2) = '\0';

        return 0;
err_ret:
        return ret;
}

int _hex_println(char *output, int outlen, const void *input, const int inlen)
{
        int ret, i;

        /*
         * format:
         * 00000000: 1300 0000 00f0 ed1e 1552 0500 c984 8c82
         * 00000010: 0000 c701 6673 6432 2020 2020 0000 0000
         */
        if (outlen < _ceil(inlen, 16) * 50) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        output[0] = '\0';
        for (i = 0; i < inlen; i++) {
                if (i % 16 == 0) {
                        sprintf(output + strlen(output), "%08x:", i);
                }

                if (i % 2 == 0) {
                        sprintf(output + strlen(output), " ");
                }

                sprintf(output + strlen(output), "%02x", *((unsigned char *)input + i));

                if ((i + 1) % 16 == 0 || i + 1 == inlen) {
                        sprintf(output + strlen(output), "\n");
                }
        }

        return 0;
err_ret:
        return ret;
}

int64_t _time_used(const struct timeval *prev, const struct timeval *now)
{
        return ((LLU)now->tv_sec - (LLU)prev->tv_sec) * 1000 * 1000
                +  (now->tv_usec - prev->tv_usec);
}

int64_t _time_used2(const struct timeval *prev)
{
        int ret;
        struct timeval now;

        ret = gettimeofday(&now, NULL);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        return _time_used(prev, &now);
}

int64_t _time_used1(const struct timespec *prev, const struct timespec *now)
{
        return ((LLU)now->tv_sec - (LLU)prev->tv_sec) * 1000 * 1000 * 1000
                +  (now->tv_nsec - prev->tv_nsec);
}

int _run(int nonblock, const char *path, ...)
{
        int ret, i, son, status;
        va_list ap;
        char *argv[128];
        struct stat stbuf;

        ret = stat(path, &stbuf);
        if (ret < 0) {
                ret = errno;
                DWARN("stat %s not ret %d\n", path, ret);
                GOTO(err_ret, ret);
        }

        son = fork();

        switch (son) {
        case -1:
                ret = errno;
                GOTO(err_ret, ret);
        case 0:
                va_start(ap, path);

                for (i = 0; i < 128; i++) {
                        argv[i] = va_arg(ap, char *);
                        if (argv[i] == NULL)
                                break;
                }

                va_end(ap);

                for (i = getdtablesize() - 1; i >= 0; --i) {
                        sy_close_failok(i);
                }

                ret = execv(path, argv);
                if (unlikely(ret)) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                break;
        default:
                if (nonblock)
                        return 0;

                ret = wait(&status);

                if (WIFEXITED(status)) {
                        ret = WEXITSTATUS(status);
                        if (unlikely(ret)) {
                                if (ret == ENOENT || ret == ENONET)
                                        goto err_ret;
                                else
                                        GOTO(err_ret, ret);
                        }
                } else {
                }
        }

        return 0;
err_ret:
        return ret;
}

int _set_value_off(const char *path, const char *value, int size, off_t offset, int flag)
{
        int ret, fd;

        //YASSERT(memcmp("/tmp/lich/netinfo", path, strlen("/tmp/lich/netinfo")));

retry:
        fd = open(path, O_WRONLY | flag, 0644);
        if (fd < 0) {
                ret = errno;
                if ((flag & O_CREAT) && ret == ENOENT) {
                        ret = path_validate(path, 0, YLIB_DIRCREATE);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);

                        goto retry;
                }

                if (ret == EEXIST)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        if (value) {
                ret = _pwrite(fd, value, size, offset);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                if (flag & O_SYNC)
                        fsync(fd);
        }

        close(fd);

        return 0;
err_ret:
        return ret;
}

int _set_value(const char *path, const char *value, int size, int flag)
{
        return _set_value_off(path, value, size, 0, flag);
}

int _get_value(const char *path, char *value, int buflen)
{
        int ret, fd;
        struct stat stbuf;

        ret = stat(path, &stbuf);
        if (unlikely(ret)) {
                ret = errno;
                if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        if (buflen < stbuf.st_size) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        fd = open(path, O_RDONLY);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = _read(fd, value, buflen);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_fd, ret);
        }

        close(fd);

        return ret;
err_fd:
        close(fd);
err_ret:
        return -ret;
}

int __disk_alloc1(int fd, int size)
{
        int ret;
        char *buf;
        int left, offset, count;

        ret = posix_memalign((void **)&buf, SDFS_BLOCK_SIZE, BIG_BUF_LEN);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        memset(buf, 0x0, BIG_BUF_LEN);

        left = size;
        offset = 0;
        while(left) {
                count = left < BIG_BUF_LEN ? left : BIG_BUF_LEN;

                ret = _pwrite(fd, buf, count, offset);
                if (ret < 0) {
                        ret = errno;
                        YASSERT(0);
                        GOTO(err_free, ret);
                }

                left -= ret;
                offset += ret;
        }

        yfree((void**)&buf);

        return 0;
err_free:
        yfree((void**)&buf);
err_ret:
        return ret;
}

int _disk_alloc(int fd, int size)
{
        int ret, msize;
        void *addr;

        msize = (size / SYS_PAGE_SIZE) * SYS_PAGE_SIZE;
        if (size % SYS_PAGE_SIZE) {
                msize += SYS_PAGE_SIZE;
        }

        ret = ftruncate(fd, msize);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        addr = mmap(0, msize, PROT_WRITE, MAP_SHARED | MAP_NONBLOCK,
                    fd, 0);
        if (addr == MAP_FAILED) {
                ret = errno;
                if (ret == EACCES) {
			DWARN("access fail\n");
                        return __disk_alloc1(fd, msize);
                } else
                        GOTO(err_ret, ret);
        }

        memset(addr, 0x0, msize);

        ret = msync(addr, msize, MS_ASYNC);
        if (ret < 0) {
                ret = errno;
                GOTO(err_map, ret);
        }

        munmap(addr, msize);

        ret = fsync(fd);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return 0;
err_map:
        munmap(addr, msize);
err_ret:
        return ret;
}

int _fallocate(int fd, size_t size)
{
        int ret;

        ret = fallocate(fd, 0, 0, size);
        if (ret < 0) {
                ret = errno;
                if (ret == ENOSYS || ret == EOPNOTSUPP) {
                        ret = _disk_alloc(fd, size);
                        if (unlikely(ret)) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

void __ppath(char *ppath, const char *path)
{
        char *c;
        strcpy(ppath, path);

        c = strrchr(ppath, '/');
        if (c)
                *c = '\0';
        else
                strcpy(ppath, "/");
}

int _mkdir(const char *path, mode_t mode)
{
        int ret;
        char ppath[MAX_NAME_LEN];

retry:
        ret = mkdir(path, mode);
        if (ret < 0) {
                ret = errno;
                if (ret == ENOENT) {
                        __ppath(ppath, path);

                        ret = _mkdir(ppath, mode);
                        if (unlikely(ret)) {
                                if (ret == EEXIST)
                                        goto retry;
                                else
                                        GOTO(err_ret, ret);
                        }

                        goto retry;
                } else if (ret == EEXIST)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        DBUG("mkdir %s\n", path);

        return 0;
err_ret:
        return ret;
}

int _open(const char *path, int flag, mode_t mode)
{
        int ret, fd;
        char ppath[MAX_NAME_LEN];

        YASSERT(strlen(path));

retry:
        fd = open(path, flag, mode);
        if (fd < 0) {
                ret = errno;
                if ((flag & O_CREAT) && ret == ENOENT) {
                        __ppath(ppath, path);

                        ret = _mkdir(ppath, 0755);
                        if (unlikely(ret)) {
                                if (ret == EEXIST)
                                        goto retry;
                                else
                                        GOTO(err_ret, ret);
                        }

                        goto retry;
                } else if (ret == EEXIST || ret == EINVAL)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        return fd;
err_ret:
        return -ret;
}

int _dir_iterator(const char *path,
                  int (*callback)(const char *parent, const char *name, void *opaque),
                  void *opaque)
{
        int ret;
        DIR *dir;
        struct dirent debuf, *de;

        dir = opendir(path);
        if (dir == NULL) {
                ret = errno;
                if (ret == ENOENT)
                        goto err_ret;
                else {
                        DWARN("path %s\n", path);
                        GOTO(err_ret, ret);
                }
        }

        while (1) {
                ret = readdir_r(dir, &debuf, &de);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_dir, ret);
                }

                if (de == NULL)
                        break;

                if (strcmp(de->d_name, ".") == 0
                    || strcmp(de->d_name, "..") == 0)
                        continue;

                ret = callback(path, de->d_name, opaque);
                if (unlikely(ret))
                        GOTO(err_dir, ret);
        }

        closedir(dir);

        return 0;
err_dir:
        closedir(dir);
err_ret:
        return ret;
}

const char *_inet_ntoa(uint32_t addr)
{
        struct in_addr sin;

        sin.s_addr = addr;

        return inet_ntoa(sin);
}

int _inet_addr(struct sockaddr *sin, const char *host)
{
        int ret, herrno = 0;
        struct hostent  hostbuf, *result;
        char buf[MAX_BUF_LEN];

        if (AF_INET == sin->sa_family) {
                ret = gethostbyname_r(host, &hostbuf, buf, sizeof(buf),  &result, &herrno);
        } else if (AF_INET6 == sin->sa_family) {
                ret = gethostbyname2_r(host, AF_INET6, &hostbuf, buf, sizeof(buf),  &result, &herrno);
        } else {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }
        if (unlikely(ret)) {
                ret = errno;
                if (ret == EALREADY || ret == EAGAIN) {
                        DERROR("connect addr %s\n", host);
                        ret = EAGAIN;
                        GOTO(err_ret, ret);
                } else
                        GOTO(err_ret, ret);
        }

        if (result) {
                if (AF_INET == sin->sa_family) {
                        _memcpy(&((struct sockaddr_in *)sin)->sin_addr, result->h_addr, result->h_length);
                } else {
                        _memcpy(&((struct sockaddr_in6 *)sin)->sin6_addr, result->h_addr, result->h_length);
                }
        } else {
                ret = ENONET;
                DWARN("connect addr %s ret (%u) %s\n", host, ret, strerror(ret));
                goto err_ret;
        }

        return 0;
err_ret:
        return ret;
}

const char *_inet_ntop(const struct sockaddr *addr)
{
        static char buf[INET6_ADDRSTRLEN];

        if (AF_INET == addr->sa_family) {
                return inet_ntop(AF_INET, &((struct sockaddr_in *)addr)->sin_addr, buf, sizeof(buf));
        } else if (AF_INET6 == addr->sa_family) {
                return inet_ntop(AF_INET6, &((struct sockaddr_in6 *)addr)->sin6_addr, buf, sizeof(buf));
        } else {
                return NULL;
        }
}

int _disk_dfree(const char *path, uint64_t *size, uint64_t *free)
{
        int ret;
        struct statvfs fsbuf;

        ret = statvfs(path, &fsbuf);
        if (ret == -1) {
                ret = errno;
                DERROR("statvfs(%s, ...) ret (%d) %s\n", path, ret,
                       strerror(ret));
                GOTO(err_ret, ret);
        }

        *free = (LLU)fsbuf.f_bsize * fsbuf.f_bavail;
        *size = (LLU)fsbuf.f_bsize * fsbuf.f_blocks;

        return 0;
err_ret:
        return ret;
}

static int __diskmd_getblocksize(const char *path, uint64_t *size)
{
        int ret, fd;

        fd = open(path, O_RDONLY);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = ioctl(fd, BLKGETSIZE64, size);
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

int _disk_dfree_link(const char *path, uint64_t *disk_size)
{
        int ret;
        char disk_path[PATH_MAX];
        char buf[MAX_PATH_LEN];
        struct stat stbuf;
        char *tmp;

        ret = lstat(path, &stbuf);
        if (ret < 0) {
                ret = errno;
                goto err_ret;
        }

        while (S_ISLNK(stbuf.st_mode)) {
                memset(buf, 0, sizeof(buf));

                ret = readlink(path, buf, sizeof(buf));
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                tmp = realpath(buf, disk_path);
                (void) tmp;

                ret = lstat(disk_path, &stbuf);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        if (S_ISREG(stbuf.st_mode)) {
                if (!gloconf.solomode) {
                        ret = EIO;
                        GOTO(err_ret, ret);
                } else {
                        *disk_size = stbuf.st_size;
                }
        } else if (S_ISBLK(stbuf.st_mode)) {
                ret = __diskmd_getblocksize(disk_path, disk_size);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

        } else {
                UNIMPLEMENTED(__DUMP__);

                *disk_size = 0;
        }

        return 0;
err_ret:
        return ret;
}

void _splithost(char *_addr, char *_port, const char *buf, int default_port)
{
        int len, i;
        char *port;

        port = strchr(buf, '/');
        YASSERT(port);
        memcpy(_addr, buf, port - buf);
        _addr[port - buf] = '\0';
        snprintf(_port, MAX_LINE_LEN, "%d", default_port + atoi(port + 1));

        len = strlen(port) - 1;
        for (i = 0; i < len; i++) {
                YASSERT(isdigit(port[i + 1]));
        }

        DBUG("addr %s port %s\n", _addr, _port);
}

long int _random()
{
        struct timeval tv;

        gettimeofday(&tv, NULL);

        srandom(tv.tv_usec + getpid());

        return random();
}

long int _random_range(int from, int to)
{
        int s;

        YASSERT(to > from);

        s = _random();

        return s % (to - from) + from;
}

void *_opaque_encode(void *buf, uint32_t *len, ...)
{
        void *pos, *value;
        va_list ap;
        uint32_t valuelen, total;

        va_start(ap, len);

        pos = buf;
        total = 0;
        while (1) {
                value = va_arg(ap, char *);
                valuelen = va_arg(ap, uint32_t);

                DBUG("encode %s len %u\n", (char *)value, valuelen);

                if (value == NULL)
                        break;

                memcpy(pos, &valuelen, sizeof(valuelen));
                pos += sizeof(valuelen);

                if (valuelen) {
                        memcpy(pos, value, valuelen);
                } else {
                }

                pos += valuelen;

                total += (sizeof(valuelen) + valuelen);
        }

        va_end(ap);

        *len = total;

        YASSERT(total <= MAX_MSG_SIZE);

        return buf + total;
}

void *_opaque_encode1(void *buf, int buflen,  uint32_t *len, ...)
{
        void *pos, *value;
        va_list ap;
        uint32_t valuelen, total;

        va_start(ap, len);

        pos = buf;
        total = 0;
        while (1) {
                value = va_arg(ap, char *);
                valuelen = va_arg(ap, uint32_t);

                DBUG("encode %s len %u\n", (char *)value, valuelen);

                if (value == NULL)
                        break;

                YASSERT(valuelen);
                memcpy(pos, &valuelen, sizeof(valuelen));
                pos += sizeof(valuelen);

                if (valuelen) {
                        memcpy(pos, value, valuelen);
                } else {
                }

                pos += valuelen;

                total += (sizeof(valuelen) + valuelen);
        }

        va_end(ap);

        *len = total;

        YASSERT((int)total <= buflen);

        return buf + total;
}

const void *_opaque_decode(const void *buf, uint32_t len, ...)
{
        const void *pos;
        const void **value;
        va_list ap;
        uint32_t *_valuelen, valuelen;

        va_start(ap, len);

        pos = buf;
        while (pos < buf + len) {
                value = va_arg(ap, const void **);
                _valuelen = va_arg(ap, uint32_t *);

                if (value == NULL)
                        break;

                memcpy(&valuelen, pos, sizeof(valuelen));
                pos += sizeof(valuelen);
                //memcpy(value, pos, valuelen);

                if (valuelen == 0) {
                        *value = NULL;
                } else {
                        *value = pos;
                }

                pos += valuelen;

                if (_valuelen)
                        *_valuelen = valuelen;

                //DBUG("decode %s len %u\n", (char *)value, valuelen);
        }

        va_end(ap);

        return pos;
}

pid_t _gettid(void)
{
        return syscall(SYS_gettid);
}

int _path_split(const char *path, char *_namespace, char *_bucket,
                char *_object, char *_chunk)
{
        int ret;
        char tmp[MAX_BUF_LEN];
        char namespace[MAX_NAME_LEN], bucket[MAX_NAME_LEN],
                object[MAX_NAME_LEN], chunk[MAX_NAME_LEN];

        namespace[0] = '\0';
        bucket[0] = '\0';
        object[0] = '\0';
        chunk[0] = '\0';
        tmp[0] = '\0';

        if (strcmp(path, "/") == 0) {
                return 0;
        }

        ret = sscanf(path, "/%[^/]/%[^/]/%[^/]/%[^/]/%[^/]", namespace,
                     bucket, object, chunk, tmp);

        DBUG("path '%s' ret %d namespace '%s' bucket '%s' object '%s' tmp '%s'\n",
             path, ret, namespace, bucket, object, tmp);

        if (ret > 4) {
                ret = EINVAL;
                goto err_ret;
        }

        if (_namespace)
                strcpy(_namespace, namespace);

        if (_bucket)
                strcpy(_bucket, bucket);

        if (_object)
                strcpy(_object, object);

        if (_chunk)
                strcpy(_chunk, chunk);

        return ret;
err_ret:
        return -ret;
}

int _path_split1(const char *path, char *parent, char *name)
{
        int ret;
        char namespace[MAX_NAME_LEN], bucket[MAX_NAME_LEN],
                object[MAX_NAME_LEN], chunk[MAX_NAME_LEN];

        ret = _path_split(path, namespace, bucket, object, chunk);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        if (ret == 0) {
                strcpy(parent, "/");
                name[0] = '\0';
        } else if (ret == 1) {
                strcpy(parent, "/");
                strcpy(name, namespace);
        } else if (ret == 2) {
                snprintf(parent, MAX_PATH_LEN, "/%s", namespace);
                strcpy(name, bucket);
        } else if (ret == 3) {
                snprintf(parent, MAX_PATH_LEN, "/%s/%s", namespace, bucket);
                strcpy(name, object);
        } else if (ret == 4) {
                snprintf(parent, MAX_PATH_LEN, "/%s/%s/%s", namespace, bucket, object);
                strcpy(name, chunk);
        }

        return 0;
err_ret:
        return ret;
}

static inline int _valid_char(char ch, int include_dot)
{
        return ((ch >= 'a' && ch <= 'z') ||
                        (ch >= 'A' && ch <= 'Z') ||
                        (ch >= '0' && ch <= '9') ||
                        (ch == '-') || (ch == '_') ||
                        (ch == ':') ||
                        (ch == '.' && include_dot));
}

static inline int _valid_name(const char *name)
{
        int ret;

        if (!strcmp(name, ".") || !strcmp(name, "..")) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

#if 0
        char *tmp = (char *)name;
        while (*tmp) {
                if (!_valid_char(*tmp, 1)) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }

                tmp++;
        }
#endif
        return 0;
err_ret:
        return ret;
}

static inline int _valid_path(const char *path)
{
        int ret;
        char buf[MAX_PATH_LEN];
        char *s;

        if (path[0] != '/') {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        if (strlen(path) > MAX_PATH_LEN) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        strcpy(buf, path);

        s = strtok(buf, "/");
        while(s) {
                ret = _valid_name(s);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                s = strtok(NULL, "/");
        }

        return 0;
err_ret:
        return ret;
}

static inline void print_err(const char *name, int arg)
{
        if (name)
                fprintf(stderr, "\x1b[1;31minvalid char '%c' in %s, only 'a-z', '0-9', "
                        " '-', ':' is allowed\x1b[0m\n", arg, name);
}

static inline int _valid_iscsi_char(char ch)
{
        return ((ch >= 'a' && ch <= 'z') ||
                        (ch >= '0' && ch <= '9') ||
                        (ch == '-') ||
                        (ch == ':') ||
                        (ch == '.'));
}

static inline int _valid_iscsi_name(const char *name)
{
        int ret;
        char *tmp = (char *)name;

        if (!strcmp(name, ".") || !strcmp(name, "..")) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        while (*tmp) {
                if (!_valid_iscsi_char(*tmp)) {
                        print_err(name, *tmp);
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }

                tmp++;
        }

        return 0;
err_ret:
        return ret;
}

int _valid_pool(const char *path)
{
        return _valid_iscsi_name(path);
}

#if 0
int _valid_iscsi_path(const char *path)
{
        int ret, count = 0;
        char buf[MAX_PATH_LEN];
        char *s;

        if (strncmp(path, ISCSI_ROOT, strlen(ISCSI_ROOT))) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        strcpy(buf, path + strlen(ISCSI_ROOT));

        if (strlen(sanconf.iqn) +  strlen(buf)> ISCSI_IQN_NAME_MAX) {
                fprintf(stderr, "\x1b[1;31minvalid name length %ld, pool + volume maximum length is %ld\x1b[0m\n",
                        strlen(buf) - 2, ISCSI_TGT_NAME_MAX - strlen(sanconf.iqn) - 2);
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        s = strtok(buf, "/");
        while(s) {
                if (strlen(sanconf.iqn) + strlen(":") +  strlen(s) + strlen(".x") > ISCSI_IQN_NAME_MAX) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }

                ret = _valid_iscsi_name(s);
                if (ret)
                        GOTO(err_ret, ret);

                count++;
                s = strtok(NULL, "/");
        }

        if (count > 2) {
                fprintf(stderr, "\x1b[1;31minvalid directory depth %d, depth must less than %d\x1b[0m\n",
                        count, 2);
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}
#endif

int _path_split2(const char *path, char *parent, char *name)
{
        int ret;
        char buf[MAX_PATH_LEN];

        ret = _valid_path(path);
        if (unlikely(ret)) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        strcpy(buf, path);
        if (parent)
                strcpy(parent, dirname(buf));

        strcpy(buf, path);
        if (name)
                strcpy(name, basename(buf));

        return 0;
err_ret:
        return ret;
}

void _align(uint64_t *_size, uint64_t *_offset, int align)
{
        uint64_t offset, size, end;

        if (_offset) {
                offset = (*_offset / align) * align;
                end = _ceil(*_offset + *_size, align) * align;
                size = end - offset;

                *_offset = offset;
                *_size = size;
        } else {
                *_size = _ceil(*_size, align) * align;
        }
}

void _str_split(char *from, char split, char *to[], int *_count)
{
        int max, i;
        char *pos;

        if (from[strlen(from) - 1] == ',')
                from[strlen(from) - 1] = '\0';

        pos = from;
        max = *_count;
        to[0] = pos;
        for (i = 1; i < max; i++) {
                pos = strchr(pos, split);
                if (pos) {
                        pos[0] = '\0';
                        pos++;
                } else {
                        break;
                }

                to[i] = pos;
        }

        *_count = i;
}

void _ch_replace(char *str, char c1, char c2)
{
        uint32_t i;

        for (i = 0; i < strlen(str); i++) {
                if (str[i] == c1)
                        str[i] = c2;
        }
        
        return ;
}

int _str_isdigit(const char *str)
{
        int i, len;

        len = strlen(str);
        for (i = 0; i < len; i++) {
                if (!isdigit(str[i]))
                        return 0;
        }

        return 1;
}

int _str_lastchr(const char *str)
{
        return str[strlen(str) - 1];
}

int _set_text(const char *path, const char *value, int size, int flag)
{
        int ret, fd;
        char buf[MAX_BUF_LEN], path_tmp[MAX_BUF_LEN] = {}, _uuid[MAX_NAME_LEN];
        uuid_t uuid;

        uuid_generate(uuid);
        uuid_unparse(uuid, _uuid);

        snprintf(path_tmp, MAX_BUF_LEN, "%s.%s", path, _uuid);
retry:
        fd = open(path_tmp, O_WRONLY | flag, 0644);
        if (fd < 0) {
                ret = errno;
                if ((flag & O_CREAT) && ret == ENOENT) {
                        ret = path_validate(path_tmp, 0, YLIB_DIRCREATE);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);

                        goto retry;
                }

                if (ret == EEXIST)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        if (value) {
                if (value[size - 1] != '\n') {
                        strcpy(buf, value);
                        buf[size] = '\n';
                        ret = _write(fd, buf, size + 1);
                        if (ret < 0) {
                                ret = -ret;
                                GOTO(err_ret, ret);
                        }
                } else {
                        ret = _write(fd, value, size);
                        if (ret < 0) {
                                ret = -ret;
                                GOTO(err_ret, ret);
                        }
                }
        }

        ret = fsync(fd);
        if (ret < 0) {
                ret = errno;
                GOTO(err_fd, ret);
        }

        close(fd);

        /**
         * @todo 使用fnotify的情况下，会触发del事件
         */
        ret = rename(path_tmp, path);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return 0;
err_fd:
        close(fd);
err_ret:
        return ret;
}

int _set_text_direct(const char *path, const char *value, int size, int flag)
{
        int ret, fd;
        char buf[MAX_BUF_LEN];

retry:
        fd = open(path, O_WRONLY | flag, 0644);
        if (fd < 0) {
                ret = errno;
                if ((flag & O_CREAT) && ret == ENOENT) {
                        ret = path_validate(path, 0, YLIB_DIRCREATE);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);

                        goto retry;
                }

                if (ret == EEXIST)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        if (value) {
                if (value[size - 1] != '\n') {
                        strcpy(buf, value);
                        buf[size] = '\n';
                        ret = _write(fd, buf, size + 1);
                        if (ret < 0) {
                                ret = -ret;
                                GOTO(err_ret, ret);
                        }
                } else {
                        ret = _write(fd, value, size);
                        if (ret < 0) {
                                ret = -ret;
                                GOTO(err_ret, ret);
                        }
                }
        }

        close(fd);

        return 0;
err_ret:
        return ret;
}

int _file_exist(const char *path)
{
	int ret;
        struct stat stbuf;

        ret = stat(path, &stbuf);
        if (unlikely(ret)) {
                ret = errno;
                if (ret == ENOENT)
                        goto err_ret;
        }

	return 0;
err_ret:
        return -ret;
}

int _is_dir(const char *path)
{
        int ret, result = 0;
        struct stat stbuf;

        ret = stat(path, &stbuf);
        if (unlikely(ret)) {
                goto out;
        }

        if (stbuf.st_mode & S_IFDIR) {
                result = 1;
        }

out:
        return result;
}

int _file_writezero(const char *path, off_t off, size_t size)
{
        int ret, fd;

        fd = open(path, O_RDWR | O_SYNC, 0);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = _file_writezero2(fd, off, size);
        if (unlikely(ret)) {
                GOTO(err_fd, ret);
        }

        close(fd);
        return 0;
err_fd:
        close(fd);
err_ret:
        return ret;
}

int _file_writezero2(int fd, off_t off, size_t size)
{
        int ret;
        void *buf;

        if (fd < 0 || off < 0 || size <= 0) {
                return 0;
        }

        ret = ymalloc((void **)&buf, size);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        memset(buf, 0x0, size);

        ret = pwrite(fd, buf, size, off);
        if (ret < 0) {
                ret = errno;
                GOTO(err_free, ret);
        }

        yfree((void **)&buf);
        return 0;
err_free:
        yfree((void **)&buf);
err_ret:
        return ret;
}

int _mkstemp(char *path)
{
        int ret, fd;

        ret = path_validate(path, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        fd = mkstemp(path);
        unlink(path);

        return fd;
}

int _get_text(const char *path, char *value, int buflen)
{
        int ret, fd;
        struct stat stbuf;

        ret = stat(path, &stbuf);
        if (unlikely(ret)) {
                ret = errno;
                if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        if (buflen < stbuf.st_size) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        fd = open(path, O_RDONLY);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = _read(fd, value, buflen);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        close(fd);

        if (ret > 0) {
                int i;
                for (i=ret-1; i>=0; i--) {
                        if (value[i] != '\0' && value[i] != '\n') {
                                break;
                        } else if (value[i] == '\n') {
                                value[i] = '\0';
                        }
                }

                if (i < 0) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
                return i + 1;
        } else
                return 0;
err_ret:
        return -ret;
}

int _lock_file(const char *key, int flag)
{
        int ret, fd, flags;

        ret = path_validate(key, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (unlikely(ret))
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

        ret = flock(fd, flag);
        if (ret == -1) {
                ret = errno;
                if (ret == EWOULDBLOCK) {
                        goto err_fd;
                } else
                        GOTO(err_fd, ret);
        }

        return fd;
err_fd:
        close(fd);
err_ret:
        return -ret;
}

void _replace(char *new, const char *old, char from, char to)
{
        char *tmp;
        strcpy(new, old);
        tmp = strchr(new, from);
        if (tmp)
                *tmp = to;
}

int _get_timeout()
{
        int ret, timeout;
        double load[3];

        return gloconf.rpc_timeout;

        ret = getloadavg(load, 3);
        if (ret < 0) {
                ret = errno;
                DWARN("(%u) %s\n", ret, strerror(ret));

                return gloconf.rpc_timeout;
        } else {
                timeout = gloconf.rpc_timeout * (load[0] + 1);
                DBUG("load %f %f %f timeout %u\n", load[0], load[1], load[2], timeout);
                return timeout;
        }
}

inline int _get_rpc_timeout()
{
        return gloconf.rpc_timeout < 10 ? 10 : gloconf.rpc_timeout;
}

inline int _get_long_rpc_timeout()
{
        return gloconf.rpc_timeout < 30 ? 30 : gloconf.rpc_timeout;
}

int _startswith(const char *str1, const char *starts) {
        size_t i;
        for (i = 0; i < strlen(starts); i++) {
                if (str1[i] != starts[i]) {
                        return 0;
                }
        }

        return 1;
}

int seqlist_new(seqhead_t **_ent, int size, uint64_t version)
{
        int ret;
        seqhead_t *ent;

        ret = ymalloc((void**)&ent, sizeof(*ent) + size);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ent->version = version;
        *_ent = ent;

        return 0;
err_ret:
        return ret;
}

void _seqlist_add(seqlist_t *list, seqhead_t *ent)
{
        int found = 0;
        seqhead_t *tmp;
        struct list_head *pos;

#if 0
        if (list->max != 0 && list->count + 1 > list->max) {
                ret = ECANCELED;
		DINFO("count %u max %u\n", list->max, list->count);
                GOTO(err_ret, ret);
        }
#endif

        list_for_each_prev(pos, &list->list) {
                tmp = (void *)pos;

                if (tmp->version < ent->version) {
                        found = 1;
                        list_add(&ent->hook, pos);
                        break;
                } else if (tmp->version == ent->version) {
                        UNIMPLEMENTED(__DUMP__);
                }
        }

        if (found == 0) {
                list_add(&ent->hook, &list->list);
        }
}

void _seqlist_pop(seqlist_t *list, uint64_t version, time_t ltime, func1_t func)
{
        seqhead_t *tmp;
        struct list_head *pos, *n;
        int  retval;

        list_for_each_safe(pos, n, &list->list) {
                tmp = (void *)pos;

                if (ltime != 0 && tmp->ltime != ltime) {
                        retval = ESTALE;
                        list_del(&tmp->hook);
                        func(tmp, &retval);
                        continue;
                }

                if (version != (uint64_t)-1 && tmp->version == version + 1) {
                        retval = 0;
                        version = tmp->version;
                        list_del(pos);
                        func(pos, &retval);
                        continue;
                }
        }
}

void _seqlist_init(seqlist_t *list)
{
        INIT_LIST_HEAD(&list->list);
}

int _errno(int ret)
{
        int i;
        static int errlist[] = {EAGAIN, EREMCHG, ENONET, ETIMEDOUT, ETIME, EBUSY, ECONNREFUSED,
                                ENOLCK, ENOSYS, ECANCELED, ESTALE, ECONNRESET, EADDRNOTAVAIL, EPERM};
        static int count = sizeof(errlist) / sizeof(int);

        for (i = 0; i < count; i++) {
                if (errlist[i] == ret)
                        return EAGAIN;
        }

        return ret;
}

int _errno_net(int ret)
{
        int i;
        static int errlist[] = {ENONET, ETIMEDOUT, ETIME, ECONNRESET, EHOSTUNREACH, EPIPE,  ECONNREFUSED};
        static int count = sizeof(errlist) / sizeof(int);

        for (i = 0; i < count; i++) {
                if (errlist[i] == ret)
                        return ENONET;
        }

        return ret;
}

#define MAX_BACKTRACE 40
void _backtrace_caller(const char *name, int start, int end)
{
        int ret;
        void* array[MAX_BACKTRACE] = {0};
        uint32_t size = 0;
        char **strframe = NULL;
        uint32_t i = 0;
        char *buf, tmp[MAX_MSG_SIZE], time_buf[MAX_MSG_SIZE];
        time_t now = gettime();
        struct tm t;
        unsigned long pid = (unsigned long)getpid();
        unsigned long tid = (unsigned long)__gettid();

        size = backtrace(array, MAX_BACKTRACE);
        strframe = (char **)backtrace_symbols(array, size);
        strftime(time_buf, MAX_MSG_SIZE, "%F %T", localtime_safe(&now, &t));

        ret = ymalloc((void **)&buf, BACKTRACE_SIZE);
        if (unlikely(ret))
                return;

        buf[0] = '\0';
        if (name)
                sprintf(tmp, "%s backtrace:\n", name);
        else
                sprintf(tmp, "backtrace:\n");

        if (strlen(buf) + strlen(tmp) < BACKTRACE_SIZE) {
                sprintf(buf + strlen(buf), "%s", tmp);
        }

        end = _min(end, (int)size);
        for (i = start; (int)i < end; i++) {
                sprintf(tmp, "%s/%ld %ld/%ld frame[%d]: %s\n",
                                time_buf, now, pid, tid, i, strframe[i]);
                if (strlen(buf) + strlen(tmp) > BACKTRACE_SIZE) {
                        break;
                }

                sprintf(buf + strlen(buf), "%s", tmp);
        }

        DINFO1(BACKTRACE_SIZE, "%s", buf);
        //yfree((void **)&buf);
        free((void *)buf);

        if (strframe) {
                free(strframe);
                strframe = NULL;
        }
}

void _backtrace(const char *name)
{
        /* skip _backtrace & _backtrace_caller, so begin 2 */
        _backtrace_caller(name, 2, MAX_BACKTRACE - 2);
}

void _backtrace1(const char *name, int start, int end)
{
        _backtrace_caller(name, start, start + end);
}

inline void get_uuid(char *uuid)
{
        uuid_t _uuid;

        uuid_generate(_uuid);
        uuid_unparse(_uuid, uuid);
}

int is_zero_one_char(const char *_char)
{
        if (strcmp(_char, "0") == 0 || strcmp(_char, "1") == 0) {
                return 1;
        }

        return 0;
}

int sy_thread_create(thread_func fn, void *arg)
{
        int ret;
        pthread_t th;
        pthread_attr_t ta;

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        ret = pthread_create(&th, &ta, fn, arg);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int sy_thread_create2(thread_func fn, void *arg, const char *name) {
        int ret;

        ret = sy_thread_create(fn, arg);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DINFO("thread %s started\n", name);
        return 0;
err_ret:
        return ret;
}

void list_free(struct list_head *head, int (*free_fn)(void **)) {
        struct list_head *pos, *n;

        list_for_each_safe(pos, n, head) {
                list_del_init(pos);
                if (free_fn) {
                        free_fn((void **)&pos);
                }
        }
}

#if 0
void chkid_generator_init(generator_t *gen, uint32_t chknum, uint32_t step)
{
        gen->chknum = chknum;
        gen->step = step;
        gen->cursor = 0;
        gen->idx = 0;
}

int chkid_generator(generator_t *gen, uint32_t *_idx)
{
        // printf("cursor %u idx %u\n", gen->cursor, gen->idx);

        if (gen->cursor >= gen->chknum) {
                gen->idx++;
                gen->cursor = gen->idx;
        }

        if (gen->idx >= gen->step || gen->idx >= gen->chknum)
                return ENOENT;

        *_idx = gen->cursor;
        gen->cursor += gen->step;

        YASSERT(*_idx < gen->chknum);
        return 0;
}
#endif

//just for compatible, will be removed
uint64_t fastrandom()
{
        return _random();
}

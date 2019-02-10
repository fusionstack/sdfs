#ifndef __SYSUTILS_H__
#define __SYSUTILS_H__

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <semaphore.h>
#include <netinet/in.h>

#include "sdfs_list.h"
#include "adt.h"
//#include "mem_cache.h"
//#include "ylib.h"

#define _ceil(size, align) ((size) % (align) == 0 ? (size) / (align) : (size) / (align) + 1)
#define _min(x, y) ((x) < (y) ? (x) : (y))
#define _max(x, y) ((x) > (y) ? (x) : (y))

/* align only for 2 ** x */
#define _align_down(a, size)    ((a) & (~((size) - 1)) )
#define _align_up(a, size)      (((a) + (size) - 1) & (~((size) - 1)))

/* align for any type */
#define round_up(x, y) (((x) % (y) == 0)? (x) : ((x) + (y)) / (y) * (y))
#define round_down(x, y) (((x) / (y)) * (y))

#ifndef offsetof
#define offsetof(t, m) ((size_t) &((t *)0)->m)
#endif

#ifndef container_of
#define container_of(ptr, type, member) ({ \
		typeof(((const type *)0)->member)(*__mptr) = (ptr); \
		(type *)((char *)__mptr - offsetof(type, member)); })
#endif

typedef void *(*thread_func)(void *);

typedef struct {
        struct list_head  hook;
        uint64_t version;
        time_t ltime;
} seqhead_t;

typedef struct {
        struct list_head list;
} seqlist_t;

extern int sy_copy(char *src, char *dst);

extern void *_memset(void *s, int c, size_t n);
extern void *_memmove(void *dest, const void *src, size_t n);
extern void *_memcpy(void *dest, const void *src, size_t n);

extern ssize_t _splice(int fd_in, loff_t *off_in, int fd_out,
                       loff_t *off_out, size_t len, unsigned int flags);

extern int _epoll_wait(int epfd, struct epoll_event *events,
                       int maxevents, int timeout);
extern int _epoll_ctl(int epfd, int op, int fd, struct epoll_event *event);

extern ssize_t _read(int fd, void *buf, size_t count);
extern ssize_t _write(int fd, const void *buf, size_t count);

extern ssize_t _pread(int fd, void *buf, size_t count, off_t offset);
extern ssize_t _pwrite(int fd, const void *buf, size_t count, off_t offset);

extern ssize_t _send(int sockfd, const void *buf, size_t len, int flags);
extern ssize_t _recv(int sockfd, void *buf, size_t len, int flags);

extern ssize_t _recvmsg(int sockfd, struct msghdr *msg, int flags);
extern ssize_t _sendmsg(int sockfd, struct msghdr *msg, int flags);

extern size_t _strlen(const char *s);
extern char *_strcpy(char *dest, const char *src);
extern char *_strncpy(char *dest, const char *src, size_t n);
extern int _strcmp(const char *s1, const char *s2);

extern int _posix_memalign(void **memptr, size_t alignment, size_t size);

extern ssize_t _readv(int fd, const struct iovec *iov, int iovcnt);
extern ssize_t _writev(int fd, const struct iovec *iov, int iovcnt);
extern ssize_t _sendfile(int out_fd, int in_fd, off_t *offset, size_t count);

extern int _sem_wait(sem_t *sem);
extern int _sem_timedwait(sem_t *sem, const struct timespec *abs_timeout);
extern int _sem_timedwait1(sem_t *sem, int tmo);

void _sha1_print(char *str, const unsigned char *md);

int _hex_print(char *output, int outlen, const void *input, const int inlen);
int _hex_println(char *output, int outlen, const void *input, const int inlen);

int _run(int nonblock, const char *path, ...);

int _set_value(const char *path, const char *value, int size, int flag);
int _set_value_off(const char *path, const char *value, int size, off_t offset, int flag);
int _get_value(const char *path, char *value, int buflen);
int _set_text(const char *path, const char *value, int size, int flag);
int _set_text_direct(const char *path, const char *value, int size, int flag);
int _get_text(const char *path, char *value, int buflen);

const char *_inet_ntoa(uint32_t addr);
int _inet_addr(struct sockaddr *sin, const char *host);
const char *_inet_ntop(const struct sockaddr *addr);

int _disk_alloc(int fd, int size);
int _disk_dfree(const char *path, uint64_t *size, uint64_t *free);
int _disk_dfree_link(const char *path, uint64_t *size);

int _fallocate(int fd, size_t size);

void _splithost(char *_addr, char *_port, const char *buf, int default_port);

long int _random(void);
long int _random_range(int from, int to);

void *_opaque_encode1(void *buf, int buflen,  uint32_t *len, ...);
void *_opaque_encode(void *buf, uint32_t *len, ...);
const void *_opaque_decode(const void *buf, uint32_t len, ...);

pid_t _gettid(void);

int _valid_pool(const char *path);

int _valid_iscsi_path(const char *path);
void _align(uint64_t *_size, uint64_t *_offset, int align);
void _str_split(char *from, char split, char *to[], int *_count);
void _ch_replace(char *str, char c1, char c2);
int _str_isdigit(const char *str);
int _str_lastchr(const char *str);
void _replace(char *new, const char *old, char from, char to);

int _startswith(const char *str1, const char *starts);
void _seqlist_add(seqlist_t *list, seqhead_t *ent);
void _seqlist_init(seqlist_t *list);
void _seqlist_pop(seqlist_t *list, uint64_t version, time_t ltime, func1_t func);

int _errno(int ret);
int _errno_net(int ret);

void _backtrace(const char *name);
void _backtrace1(const char *name, int start, int count);

void base64_encode(const char* input, int len, char *output);
void base64_decode(const char* input, int *len, char *output);

int _gettimeofday(struct timeval *tv, struct timezone *tz);
int64_t _time_used1(const struct timespec *prev, const struct timespec *now);
int64_t _time_used(const struct timeval *prev, const struct timeval *now);
int64_t _time_used2(const struct timeval *prev);

int _get_timeout(void);
int _get_rpc_timeout();
int _get_long_rpc_timeout();

time_t gettime();
struct tm *localtime_safe(time_t *_time, struct tm *tm_time);
void  gettime_refresh();
int gettime_private_init();

extern void get_uuid(char *uuid);
int is_zero_one_char(const char *_char);

void list_free(struct list_head *head, int (*free_fn)(void **));
uint64_t fastrandom();
int eventfd_poll(int fd, int tmo, uint64_t *event);

// dir and file {{

int _open(const char *path, int flag, mode_t mode);

int _file_exist(const char *path);
int _is_dir(const char *path);

int _file_writezero(const char *path, off_t off, size_t size);
int _file_writezero2(int fd, off_t off, size_t size);

int _lock_file(const char *key, int flag);
int _sha1_file(int fd, uint64_t offset, uint32_t size, unsigned char *md);

int _mkstemp(char *path);

int _mkdir(const char *path, mode_t mode);
int _delete_path(const char *path);
int _unlink(const char *path, const char *context);

int _dir_iterator(const char *path,
                  int (*callback)(const char *parent, const char *name, void *opaque),
                  void *opaque);

int _path_split(const char *path, char *namespace, char *bucket,
                char *object, char *_chunk);
int _path_split1(const char *path, char *parent, char *name);
int _path_split2(const char *path, char *parent, char *name);

int sy_thread_create2(thread_func fn, void *arg, const char *name);
int sy_thread_create(thread_func fn, void *arg);

// }}

static inline void itorange(int *value, int __min, int __max)
{
        if (*value < __min)
                *value = __min;
        if (*value > __max)
                *value = __max;
}

#endif



#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include <dirent.h>
#include <ctype.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "configure.h"
#include "jnl_proto.h"
#include "ylib.h"
#include "job_dock.h"
#include "dbg.h"

typedef struct {
        int idx;
        int offset;
        int size;
        char *buf;
} jnl_seg_t;

typedef struct {
        sem_t sem;
        int   ret;
} __block_t;

#define MAX_SEG 10
#define MAX_QUEUE (1024 * 100)

extern jobtracker_t *jobtracker;

#if 1

int __jnl_pread(jnl_handle_t *jnl, void *buf, int _size, int64_t _off)
{
        int ret, fd, no, soff, ssize, size;
        char path[MAX_PATH_LEN];
        int64_t off;

        size = _size;
        off = _off;
        while (size) {
                soff = off % MAX_JNL_LEN;
                ssize = (soff + size) < MAX_JNL_LEN ? size
                        : (MAX_JNL_LEN - soff);
                no = off / MAX_JNL_LEN;

                snprintf(path, MAX_PATH_LEN, "%s/%d", jnl->home, no);

                fd = open(path, O_RDONLY);
                if (fd == -1) {
                        ret = errno;
                        DERROR("open(%s,...) ret (%d) %s\n", path, ret,
                               strerror(ret));
                        GOTO(err_ret, ret);
                }

                ret = _pread(fd, buf + (off - _off), ssize, soff);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_fd, ret);
                }

                close(fd);

                off += ret;
                size -= ret;

                if (ret < ssize) {
                        break;
                }
        }

        return _size - size;
err_fd:
        close(fd);
err_ret:
        return -ret;
}

static uint64_t __jnl_seek(jnl_handle_t *jnl, uint64_t offset)
{
        int ret;
        char buf[MAX_BUF_LEN];
        jnl_head_t *head;

        ret = __jnl_pread(jnl, buf, MAX_BUF_LEN, offset);
        if (ret < 0) {
                ret = -ret;
                UNIMPLEMENTED(__DUMP__);
        }

        head = (void *)buf;
        while ((void *)head - (void *)buf < MAX_BUF_LEN) {
                if (head->magic == YFS_MAGIC) {
                        DBUG("diff %llu\n", (LLU)((void *)head - (void *)buf));
                        return offset + (void *)head - (void *)buf;
                }

                head = (void *)head + 1;
        }

        UNIMPLEMENTED(__DUMP__);

        return 0;
}

static int __jnl_check(jnl_handle_t *jnl, int trunc)
{
        int ret, max, buflen, left = 0;
        char buf[MAX_BUF_LEN];
        int64_t off;
        jnl_head_t *head;
        uint32_t crc;

        max = jnl->jnlmaxno;

        if (max < 3) {
                off = 0;
        } else {
                off = __jnl_seek(jnl, (int64_t)(max - 3) * MAX_JNL_LEN);
        }

        while (1) {
                ret = __jnl_pread(jnl, buf, MAX_BUF_LEN, off);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                if (ret == 0)
                        break;

                buflen = ret;
                left = ret;
                head = (void *)buf;

                DBUG("got %u\n", ret);

                while (left && (unsigned)left > sizeof(jnl_head_t)
                       && head->len + sizeof(jnl_head_t) <= (unsigned)left) {
                        if (((head->len == 0 && head->magic == 0)
                             || head->offset != off + (void *)head - (void *)buf) && trunc) {
                                DWARN("bad jnl %s offset %llu:%llu head len %u magic %x\n",
                                      jnl->home, (LLU)head->offset,
                                      (LLU)off + buflen - left, head->len, head->magic);

                                off += (buflen - left); 
                                EXIT(EIO);
                        }

                        DBUG("offset %llu:%llu\n", (LLU)head->offset,
                             (LLU)(off + (void *)head - (void *)buf));

                        YASSERT(head->len);

                        crc = crc32_sum(&head->buf, head->len);

                        DBUG("off %llu\n", (LLU)off + ((void *)head - (void *)buf));

                        if (head->magic != YFS_MAGIC || crc != head->crc) {
                                off += (buflen - left);
                                DWARN("offset %llu left %u bad jnl\n", (LLU)off, left);
                                EXIT(EIO);
                        }

                        DBUG("left %u len %u\n", left, head->len);

                        left -= head->len + sizeof(jnl_head_t);
                        head = (void *)head + head->len + sizeof(jnl_head_t);
                }

                off += (buflen - left);

#if 0
                if (buflen == left && jnl->prev_offset - off == left) {
                        break;
                }
#endif
        }

        DBUG("off %llu %llu left %u\n", (LLU)jnl->offset, (LLU)off, left);

        return 0;
err_ret:
        return ret;
}

int __jnl_next(jnl_handle_t *jnl)
{
        int ret, no, fd;
        char path[MAX_PATH_LEN];

        no = jnl->jnlmaxno + 1;

        snprintf(path, MAX_PATH_LEN, "%s/%d", jnl->home, no);

        fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0644);
        if (fd == -1) {
                ret = errno;
                DERROR("open(%s,...) ret (%d) %s\n", path, ret,
                       strerror(ret));
                GOTO(err_ret, ret);
        }

        (void) sy_close(jnl->fd);

        jnl->jnlmaxno = no;
        jnl->fd = fd;

        return 0;
err_ret:
        return ret;
}

int __jnl_write(jnl_handle_t *jnl, const char *_buf, size_t _size, off_t _offset, int flag)
{
        int ret, seg_count, i, fd;
        jnl_seg_t segs[MAX_SEG], *seg;
        char path[MAX_PATH_LEN], *buf;
        off_t offset;
        size_t size;

        //flag == 0;

        buf = (void *)_buf;
        offset = _offset;
        size = _size;

retry:
        seg_count = 0;
        for (i = 0; i < MAX_SEG; i++) {
                seg = &segs[i];

                seg->idx = offset / MAX_JNL_LEN;
                seg->offset = offset % MAX_JNL_LEN;
                if (i == 0) {
                        seg->size = (seg->idx + 1) * MAX_JNL_LEN - offset;
                        seg->size = (size_t)seg->size < size ? (size_t)seg->size : size;
                } else
                        seg->size = size < MAX_JNL_LEN ? size : MAX_JNL_LEN;

                seg->buf = buf;
                buf += seg->size;
                offset += seg->size;
                size -= seg->size;
                seg_count++;
                if (size == 0)
                        break;
        }

        YASSERT((size_t)(buf - _buf) == _size);
        YASSERT(size == 0);

        ret = sy_rwlock_wrlock(&jnl->rwlock);
        if (ret)
                GOTO(err_ret, ret);

        for (i = 0; i < seg_count; i++) {
                seg = &segs[i];

                YASSERT(seg->offset + seg->size <= MAX_JNL_LEN);

                if (seg->idx < jnl->jnlmaxno) {
                        snprintf(path, MAX_PATH_LEN, "%s/%u", jnl->home,
                                 seg->idx);

                        fd = open(path, O_RDWR);
                        if (fd < 0) {
                                ret = errno;
                                DWARN("idx %u\n", seg->idx);
                                GOTO(err_lock, ret);
                        }
                } else if (seg->idx > jnl->jnlmaxno) {
                        ret = __jnl_next(jnl);
                        if (ret)
                                GOTO(err_lock, ret);

                        fd = jnl->fd;
                } else
                        fd = jnl->fd;

                ret = _pwrite(fd, seg->buf, seg->size, seg->offset);
                if (ret < 0) {
                        ret = -ret;
                        DERROR("write ret (%d) %s\n", ret,
                               strerror(ret));

                        EXIT(ret);
                }

                if (flag & O_SYNC) {
                        ret = fsync(fd);
                        if (ret < 0) {
                                ret = errno;
                                DERROR("fsync ret (%d) %s\n", ret,
                                       strerror(ret));
                                EXIT(ret);
                        }
                }

                if (fd != jnl->fd)
                        close(fd);
        }

        sy_rwlock_unlock(&jnl->rwlock);

        if (size > 0)
                goto retry;

        return 0;
#if 0
err_fd:
        if (fd != jnl->fd)
                close(fd);
#endif
err_lock:
        sy_rwlock_unlock(&jnl->rwlock);
err_ret:
        return ret;
}

int jnl_open(const char *path, jnl_handle_t *jnl, int flag)
{
        int ret, no, fd;
        char jpath[MAX_PATH_LEN];
        DIR *dir;
        struct dirent *de;
        struct stat stbuf;
        off_t offset;

#if 0
        ret = path_validate(path, 1, 1);
        if (ret)
                GOTO(err_ret, ret);
#else
        ret = _mkdir(path, 0644);
        if (ret) {
                if (ret == EEXIST) {
                } else
                        GOTO(err_ret, ret);
        }
#endif

        strcpy(jnl->home, path);

        dir = opendir(jnl->home);
        if (dir == NULL) {
                ret = errno;
                DERROR("opendir(%s) ret (%d) %s\n", jnl->home, ret,
                       strerror(ret));
                GOTO(err_ret, ret);
        }

        no = 0;
        while ((de = readdir(dir)) != NULL) 
                if (isdigit(de->d_name[0])) {
                        if (atoi(de->d_name) > no)
                                no = atoi(de->d_name);
                }

        (void) closedir(dir);

        snprintf(jpath, MAX_PATH_LEN, "%s/%d", jnl->home, no);
        
        fd = open(jpath, O_CREAT | O_RDWR, 0644);
        if (fd == -1) {
                ret = errno;
                DERROR("open(%s,...) ret (%d) %s\n", jpath, ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        ret = fstat(fd, &stbuf);
        if (ret == -1) {
                ret = errno;
                GOTO(err_fd, ret);
        }

        offset = stbuf.st_size;

        ret = sy_rwlock_init(&jnl->rwlock, NULL);
        if (ret)
                GOTO(err_fd, ret);

        jnl->jnlmaxno = no;
        jnl->fd = fd;
        jnl->offset = (int64_t)no * MAX_JNL_LEN + offset;
        jnl->flag = flag;
        jnl->running = 1;

        YASSERT(jnl->offset >= 0);

        ret = __jnl_check(jnl, 1);
        if (ret)
                GOTO(err_ret, ret);

        ret = sy_spin_init(&jnl->lock);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_fd:
        (void) sy_close(fd);
err_ret:
        return ret;
}

int __jnl_setinfo(jnl_handle_t *jnl, int no, int64_t offset)
{
        int ret, fd;
        char path[MAX_PATH_LEN], buf[MAX_BUF_LEN];

        YASSERT((no + 1) * MAX_JNL_LEN >= offset);
        YASSERT((no) * MAX_JNL_LEN < offset);

        snprintf(path, MAX_PATH_LEN, "%s/%d.info", jnl->home, 0);

        fd = open(path, O_CREAT | O_RDWR | O_EXCL, 0644);
        if (fd == -1) {
                ret = errno;
                DERROR("open(%s,...) ret (%d) %s\n", path, ret,
                       strerror(ret));
                GOTO(err_ret, ret);
        }

        snprintf(buf, MAX_BUF_LEN, "last:%llu\n", (LLU)offset);

        ret = _write(fd, buf, strlen(buf));
        if (ret)
                GOTO(err_fd, ret);

        close(fd);

        return 0;
err_fd:
        close(fd);
err_ret:
        return ret;
}

int jnl_pread(jnl_handle_t *jnl, void *buf, int _size, int64_t _off)
{
        return __jnl_pread(jnl, buf, _size, _off);
}

int jnl_close(jnl_handle_t *jnl)
{
        char path[MAX_PATH_LEN];

        if (jnl->running == 0) {
                return 0;
        }

        jnl->running = 0;

        snprintf(path, MAX_PATH_LEN, "%s/dirty", jnl->home);
        unlink(path);

        sy_rwlock_destroy(&jnl->rwlock);
        _memset(jnl->home, 0x0, MAX_PATH_LEN);
        (void) sy_close(jnl->fd);
        jnl->fd = -1;
        jnl->offset = -1;
        jnl->jnlmaxno = -1;

        return 0;
}

int jnl_iterator_buf(const char *buf, int len, int64_t offset, 
                     int (*callback)(const void *, int len, int64_t, void *),
                     void *arg, int *_left, int *eof)
{
        int ret, left;
        uint32_t crc;
        jnl_head_t *head;

        head = (void *)buf;
        left = len;

        *eof = 0;

        while (left && (unsigned)left > sizeof(jnl_head_t)
               && (head->len + sizeof(jnl_head_t)) <= (unsigned)left) {
                if (head->offset != offset) {
                        *eof = 1;
                        break;
                }

                YASSERT(head->len);

                crc = crc32_sum(&head->buf, head->len);

                YASSERT(head->magic == YFS_MAGIC);
                YASSERT(crc == head->crc);

                DBUG("offset %llu\n", (LLU)head->offset);

                ret = callback(head->buf, head->len, head->offset, arg);
                if (ret)
                        GOTO(err_ret, ret);

                left -= head->len + sizeof(jnl_head_t);
                offset += head->len + sizeof(jnl_head_t);
                head = (void *)head + head->len + sizeof(jnl_head_t);
        }

        if (_left)
                *_left = left;

        return 0;
err_ret:
        return ret;
}

int jnl_iterator(jnl_handle_t *jnl, int64_t _off,
                 int (*callback)(const void *, int len, int64_t, void *),
                 void *arg)
{
        int ret, len, left, eof;
        char buf[MAX_BUF_LEN];
        jnl_head_t *head __attribute((unused));
        int64_t off;

        off = _off;

        while (off < jnl->offset) {
                ret = jnl_pread(jnl, buf, MAX_BUF_LEN, off);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                len = ret;
                head = (void *)buf;

                ret = jnl_iterator_buf(buf, len, off, callback, arg, &left, &eof);
                if (ret)
                        GOTO(err_ret, ret);

                off += (len - left);

                if (eof)
                        break;
        }

        return 0;
err_ret:
        return ret;
}

int jnl_append_prep(jnl_handle_t *jnl, uint32_t _len, uint64_t *offset)
{
        int ret, len;

        len = _len + sizeof(jnl_head_t);

        DBUG("jnl len %u\n", len);

        ret = sy_spin_lock(&jnl->lock);
        if (ret)
                GOTO(err_ret, ret);

        *offset = jnl->offset;
        jnl->offset += len;

        sy_spin_unlock(&jnl->lock);

        return 0;
err_ret:
        return ret;
}

int jnl_append1(jnl_handle_t *jnl, const char *_buf, uint32_t _size)
{
        int ret;
        uint64_t offset;

        ret = jnl_append_prep(jnl, _size, &offset);
        if (ret)
                GOTO(err_ret, ret);

        ret = jnl_write(jnl, _buf, _size, offset);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int jnl_write(jnl_handle_t *jnl, const char *_buf, uint32_t _size, uint64_t offset)
{
        int ret, len;
        uint64_t max;
        char buf[MAX_BUF_LEN];
        jnl_head_t *head;

        len = _size + sizeof(jnl_head_t);
        max = len + offset;

        DBUG("jnl len %u\n", len);

        if (len > MAX_BUF_LEN) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        head = (void *)buf;
        memcpy(head->buf, _buf, _size);
        head->magic = YFS_MAGIC;
        head->len = _size;
        head->__pad__ = 0;
        //head->status = iocb->status;
        head->offset = offset;
        head->version = 0;
        head->crc = crc32_sum(head->buf, _size);

        ret = sy_spin_lock(&jnl->lock);
        if (ret)
                GOTO(err_ret, ret);

        jnl->offset = (uint64_t)jnl->offset > max ? (uint64_t)jnl->offset : max;

        sy_spin_unlock(&jnl->lock);

        ret = __jnl_write(jnl, (void *)buf, len, offset, jnl->flag);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int jnl_pwrite(jnl_handle_t *jnl, const char *buf, uint32_t len, uint64_t offset)
{
        int ret;
        uint64_t max;

        max = len + offset;

        DBUG("jnl len %u\n", len);

        ret = sy_spin_lock(&jnl->lock);
        if (ret)
                GOTO(err_ret, ret);

        jnl->offset = (uint64_t)jnl->offset > max ? (uint64_t)jnl->offset : max;

        sy_spin_unlock(&jnl->lock);

        ret = __jnl_write(jnl, (void *)buf, len, offset, jnl->flag);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

#endif

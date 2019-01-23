

#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "ylib.h"
#include "msgqueue.h"

typedef struct {
        uint32_t roff;
} seg_meta_t;

typedef msgqueue_msg_t msg_t;

#define QUEUE_CHECK(__queue__)                                      \
        do {                                                        \
                uint32_t __i__;                                     \
                msgqueue_seg_t *__seg__;                            \
                for (__i__ = 0; __i__ <= __queue__->idx; __i__++) { \
                        __seg__ = &__queue__->seg[__i__];           \
                        if (__seg__->fd != -1)                      \
                                YASSERT(__seg__->roff < __seg__->woff); \
                }                                                   \
        } while (0);

static inline int __msgqueue_init(msgqueue_t *queue, const char *path, const ynet_net_nid_t *nid)
{
        int ret, i;
        msgqueue_seg_t *seg;

        ret = path_validate(path, 1, 1);
        if (ret)
                GOTO(err_ret, ret);

        memset(queue, 0x0, sizeof(msgqueue_t));

        for (i = 0; i < MSGQUEUE_SEG_COUNT_MAX; i++) {
                seg = &queue->seg[i];

                seg->fd = -1;
                seg->woff = 0;
                seg->roff = 0;
        }

        queue->nid = *nid;

        snprintf(queue->home, MAX_PATH_LEN, "%s/%llu/", path, (LLU)nid->id);

        sy_rwlock_init(&queue->rwlock, NULL);

        return 0;
err_ret:
        return ret;
}

int msgqueue_init(msgqueue_t *queue, const char *path, const nid_t *nid)
{
        int ret;

        YASSERT(nid->id);

        ret = __msgqueue_init(queue, path, nid);
        if (ret)
                GOTO(err_ret, ret);

        ret = mkdir(queue->home, 0777);
        if (ret) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int msgqueue_load(msgqueue_t *queue, const char *path, const nid_t *nid)
{
        int ret, count, max, i, j;
        DIR *dir;
        struct dirent *de;
        char path1[MAX_PATH_LEN], path2[MAX_PATH_LEN], path_err[MAX_PATH_LEN];
        seg_meta_t seg_meta;
        struct stat stbuf;
        msgqueue_seg_t *seg;

        ret = __msgqueue_init(queue, path, nid);
        if (ret)
                GOTO(err_ret, ret);

retry:
        dir = opendir(queue->home);
        if (dir == NULL) {
                ret = errno;

                if (ret == ENOENT) {
                        ret = mkdir(queue->home, 0777);
                        if (ret) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }

                        goto out;
                } else
                        GOTO(err_ret, ret);
        }

        count = 0;
        max = 0;
        while ((de = readdir(dir)) != NULL) {
                if (isdigit(de->d_name[0])) {
                        if (atoi(de->d_name) > max)
                                max = atoi(de->d_name);

                        count++;
                }
        }

        (void) closedir(dir);

        j = 0;
        for (i = 0; i < count; i++) {
                while (1) {
                        YASSERT(j <= max);

                        snprintf(path2, MAX_PATH_LEN, "%s/%d", queue->home, j);

                        ret = stat(path2, &stbuf);
                        if (ret < 0) {
                                ret = errno;
                                if (ret == ENOENT) {
                                        j++;
                                        continue;
                                }

                                GOTO(err_ret, ret);
                        }

                        if ((LLU)stbuf.st_size <= (LLU)sizeof(seg_meta_t)) {
                                snprintf(path_err, MAX_PATH_LEN, "%s/error.%d", queue->home, j);

                                DERROR("invalid file %s, move to %s\n", path2, path_err);

                                rename(path2, path_err);

                                goto retry;
                        }

                        if (stbuf.st_size != MSGQUEUE_SEG_LEN + sizeof(seg_meta_t)
                            && i != count -1) {
                                YASSERT(0);
                        }

                        break;
                }

                if (i != j) {
                        snprintf(path1, MAX_PATH_LEN, "%s/%d", queue->home, i);

                        DWARN("rname from %s to %s\n", path2, path1);

                        ret = rename(path2, path1);
                        if (ret < 0) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }
                }

                j++;
        }

        for (i = 0; i < count; i++) {
                seg = &queue->seg[i];

                snprintf(path1, MAX_PATH_LEN, "%s/%d", queue->home, i);

                ret = open(path1, O_RDWR);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                seg->fd = ret;

                if (i == 0) {
                        ret = _pread(seg->fd, &seg_meta, sizeof(seg_meta_t), 0);
                        if (ret < 0) {
                                ret = -ret;
                                GOTO(err_ret, ret);
                        }

                        seg->roff = seg_meta.roff;
                }

                if (i == count - 1) {
                        ret = fstat(seg->fd, &stbuf);
                        if (ret < 0) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }

                        seg->woff = stbuf.st_size - sizeof(seg_meta_t);
                } else
                        seg->woff = MSGQUEUE_SEG_LEN;

                queue->idx = i;
        }

out:
        QUEUE_CHECK(queue);

        return 0;
err_ret:
        return ret;
}

static inline int __msgqueue_newseg(msgqueue_seg_t *seg, const char *home, uint32_t idx)
{
        int ret;
        char path[MAX_PATH_LEN];

        YASSERT(seg->fd == -1);

        snprintf(path, MAX_PATH_LEN, "%s/%u", home, idx);

        ret = open(path, O_RDWR | O_CREAT, 0644);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        seg->fd = ret;
        seg->woff = 0;
        seg->roff = 0;

        return 0;
err_ret:
        return ret;
}

int msgqueue_push(msgqueue_t *queue, const void *_msg, uint32_t len)
{
        int ret;
        msgqueue_seg_t *seg;
        uint32_t left, cp, msglen;
        msg_t *msg;
        char buf[MAX_BUF_LEN];

        YASSERT(len <= MAX_BUF_LEN - sizeof(msg_t));

        if (queue->idx >= MSGQUEUE_SEG_COUNT_MAX) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        ret = sy_rwlock_wrlock(&queue->rwlock);
        if (ret)
                GOTO(err_ret, ret);

        QUEUE_CHECK(queue);

        seg = &queue->seg[queue->idx];

        if (seg->fd == -1) {
                ret = __msgqueue_newseg(seg, queue->home, queue->idx);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);
        }

        msglen = sizeof(msg_t) + len;

        if (queue->idx == MSGQUEUE_SEG_COUNT_MAX - 1
            && seg->woff + msglen > MSGQUEUE_SEG_LEN) {
                ret = ENOSPC;
                GOTO(err_lock, ret);
        }

        msg = (void *)buf;

        msg->len = len;
        msg->crc = crc32_sum(_msg, len);
        memcpy(msg->buf, _msg, len);

        left = MSGQUEUE_SEG_LEN - seg->woff;

        cp = left < msglen ? left : msglen;

        ret = _pwrite(seg->fd, buf, cp, seg->woff + sizeof(seg_meta_t));
        if (ret < 0) {
                ret = -ret;
                GOTO(err_lock, ret);
        }

        seg->woff += cp;

        if (seg->woff == MSGQUEUE_SEG_LEN) {
                queue->idx++;

                if (msglen > cp) {
                        seg = &queue->seg[queue->idx];

                        ret = __msgqueue_newseg(seg, queue->home, queue->idx);
                        if (ret)
                                UNIMPLEMENTED(__DUMP__);

                        ret = _pwrite(seg->fd, buf + cp, msglen - cp,
                                      seg->woff + sizeof(seg_meta_t));
                        if (ret < 0) {
                                ret = -ret;
                                GOTO(err_lock, ret);
                        }

                        seg->woff += msglen - cp;
                }
        }

        QUEUE_CHECK(queue);

        sy_rwlock_unlock(&queue->rwlock);

        return 0;
err_lock:
        sy_rwlock_unlock(&queue->rwlock);
err_ret:
        return ret;
}

static inline void __msgqueue_freeseg(msgqueue_seg_t *seg, const char *home, uint32_t idx)
{
        char path[MAX_PATH_LEN];

        snprintf(path, MAX_PATH_LEN, "%s/%u", home, idx);

        close(seg->fd);
        unlink(path);

        seg->fd = -1;
        seg->woff = 0;
        seg->roff = 0;
}

static inline int __msgqueue_get(msgqueue_t *queue, void *msg, uint32_t *len)
{
        int ret;
        uint32_t left, cp, msglen, i, total, buflen;
        msgqueue_seg_t *seg;

        total = queue->idx * MSGQUEUE_SEG_LEN
                + queue->seg[queue->idx].woff - queue->seg[0].roff;

        if (total == 0) {
                *len = 0;
                goto out;
        }

        left = *len;
        buflen = *len;

        for (i = 0; i <= queue->idx; i++) {
                seg = &queue->seg[i];
                msglen = seg->woff - seg->roff;
                cp = left < msglen ? left : msglen;

                ret = _pread(seg->fd, msg + (buflen - left), cp,
                             seg->roff + sizeof(seg_meta_t));
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                left -= cp;

                if (left == 0)
                        break;
        }

        *len = buflen - left;

out:
        return 0;
err_ret:
        return ret;
}

int msgqueue_get(msgqueue_t *queue, void *msg, uint32_t len)
{
        int ret;
        uint32_t buflen;

        buflen = len;

        ret = sy_rwlock_wrlock(&queue->rwlock);
        if (ret)
                GOTO(err_ret, ret);

        QUEUE_CHECK(queue);

        ret = __msgqueue_get(queue, msg, &buflen);
        if (ret)
                GOTO(err_lock, ret);

        QUEUE_CHECK(queue);

        sy_rwlock_unlock(&queue->rwlock);

        return buflen;
err_lock:
        sy_rwlock_unlock(&queue->rwlock);
err_ret:
        return -ret;
}

int msgqueue_pop(msgqueue_t *queue, void *msg, uint32_t len)
{
        int ret;
        uint32_t count, total, i, off0, buflen;
        msgqueue_seg_t *seg;
        char path1[MAX_PATH_LEN], path2[MAX_PATH_LEN];
        seg_meta_t seg_meta;

        ret = sy_rwlock_wrlock(&queue->rwlock);
        if (ret)
                GOTO(err_ret, ret);

        QUEUE_CHECK(queue);

        buflen = len;

        if (msg) {
                ret = __msgqueue_get(queue, msg, &buflen);
                if (ret)
                        GOTO(err_lock, ret);
        }

        total = queue->idx * MSGQUEUE_SEG_LEN
                + queue->seg[queue->idx].woff - queue->seg[0].roff;

        if (total == 0)
                goto out;

        buflen = total < buflen ? total : buflen;

        count = (buflen + queue->seg[0].roff) / MSGQUEUE_SEG_LEN;

        if (count) {
                off0 = queue->seg[0].roff;

                for (i = 0; i < count; i++) {
                        seg = &queue->seg[i];

                        __msgqueue_freeseg(seg, queue->home, i);
                }

                for (i = count; i <= queue->idx; i++) {
                        seg = &queue->seg[i];
                        queue->seg[i - count] = *seg;

                        snprintf(path1, MAX_PATH_LEN, "%s/%u", queue->home, i);
                        snprintf(path2, MAX_PATH_LEN, "%s/%u", queue->home, i - count);

                        ret = rename(path1, path2);
                        if (ret < 0) {
                                ret = errno;
                                GOTO(err_lock, ret);
                        }

                        seg->fd = -1;
                        seg->woff = 0;
                        seg->roff = 0;
                }

                queue->idx -= count;

                queue->seg[0].roff = (off0 + len - count * MSGQUEUE_SEG_LEN);
        } else
                queue->seg[0].roff += buflen;

        if (queue->seg[0].roff == queue->seg[0].woff) { //last seg
                YASSERT(queue->idx == 0);

                __msgqueue_freeseg(&queue->seg[0], queue->home, 0);
        }

        if (queue->seg[0].fd != -1) {
                seg_meta.roff = queue->seg[0].roff;

                ret = _pwrite(queue->seg[0].fd, &seg_meta, sizeof(seg_meta_t), 0);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_lock, ret);
                }
        }

        QUEUE_CHECK(queue);

out:
        sy_rwlock_unlock(&queue->rwlock);

        return buflen;
err_lock:
        sy_rwlock_unlock(&queue->rwlock);
err_ret:
        return -ret;
}

int msgqueue_empty(msgqueue_t *queue)
{
        int total;

        total = queue->idx * MSGQUEUE_SEG_LEN
                + queue->seg[queue->idx].woff - queue->seg[0].roff;

        return !total;
}

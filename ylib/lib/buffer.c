

#include <errno.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/socket.h>

#define DBG_SUBSYS S_LIBYLIB

#include "align.h"
#include "adt.h"
#include "configure.h"
#include "sysutil.h"
#include "sdfs_buffer.h"
#include "pipe_pool.h"
#include "ylib.h"
#include "job.h"
#include "schedule.h"
#include "shm.h"
#include "dbg.h"


#define MAX_SEG BUFFER_MAX_SEG

//#define SEND_SPLICE
//#define BUFFER_POOL_DEBUG
#ifdef BUFFER_POOL_DEBUG
#define BUFFER_POOL_CHECK(buf)                                               \
        do {                                                            \
                int __len = 0;                                     \
                struct list_head *pos; \
                list_for_each(pos, &buf->list) {                        \
                        __len ++;                            \
                }                                                       \
                if (__len != (buf)->len) {                              \
                        DERROR("__len %u (buf)->len %u\n", __len, (buf)->len); \
                        YASSERT(0);                                     \
                }                                                       \
        } while (0)

#else
#define BUFFER_POOL_CHECK(buf)
#endif

mpool_t head_pool;
mpool_t page_pool;

typedef struct {
        sy_spinlock_t lock;
        struct list_head list;
        int len;
        int inited;
        int max;
        sem_t sem;
} buffer_pool_t;

typedef enum {
        USEABLE_BEGIN,
        USEABLE_END,
} pool_flag_t;

#define BUFFER_POOL_LEN (PAGE_SIZE * 2)

static buffer_pool_t *buffer_pool;

static int __buffer_pool_alloc(struct list_head *list, int count)
{
        int ret, i;
        void *ptr;
        seg_t *seg;

        for (i = 0; i < count; i++) {
                ret = ymalign(&ptr, BUFFER_POOL_LEN, BUFFER_POOL_LEN);
                if (ret )
                        GOTO(err_ret, ret);

                ret = ymalloc((void **)&seg, sizeof(seg_t));
                if (ret )
                        GOTO(err_ret, ret);

                seg->ptr = ptr;
                seg->type = BUFFER_POOL;
                seg->len = PAGE_SIZE;
                list_add_tail(&seg->hook, list);
        }

        return 0;
err_ret:
        return ret;
}

inline static int __buffer_pool_get(struct list_head *list, int need)
{
        int ret, count;
        seg_t *seg;
        struct list_head *pos, *n;

        YASSERT(buffer_pool->inited);

        count = 0;
        ret = sy_spin_lock(&buffer_pool->lock);
        if (ret)
                GOTO(err_ret, ret);

        BUFFER_POOL_CHECK(buffer_pool);
        DBUG("pool len %u need %u\n", buffer_pool->len, need);
        list_for_each_safe(pos, n, &buffer_pool->list) {
                seg = (void *)pos;
                list_del(&seg->hook);
                seg->len = PAGE_SIZE;
                DBUG("remove seg\n");

                list_add_tail(&seg->hook, list);
                buffer_pool->len--;

                count++;

                if (count == need)
                        break;
        }

        DBUG("pool len %u remove %u\n", buffer_pool->len, count);
        BUFFER_POOL_CHECK(buffer_pool);
        sy_spin_unlock(&buffer_pool->lock);

        if (buffer_pool->len < buffer_pool->max / 2)
                sem_post(&buffer_pool->sem);

        if (count < need) {
                ret = __buffer_pool_alloc(list, need - count);
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static void __buffer_pool_switch(seg_t *seg)
{
        int align;

        align = (LLU)seg->ptr % BUFFER_POOL_LEN;
        YASSERT(align == 0 || align == BUFFER_POOL_LEN / 2);

        if (align == 0) {
                DBUG("use end %u\n", align);
                seg->ptr = seg->ptr + BUFFER_POOL_LEN / 2;
        } else {
                DBUG("use begin %u\n", align);
                seg->ptr = seg->ptr - BUFFER_POOL_LEN / 2;
        }
}

static int  __buffer_pool_put(struct list_head *list, int count)
{
        int ret;

        YASSERT(buffer_pool);
        YASSERT(buffer_pool->inited);

        ret = sy_spin_lock(&buffer_pool->lock);
        if (ret)
                GOTO(err_ret, ret);

        BUFFER_POOL_CHECK(buffer_pool);
        if (buffer_pool->len + count > buffer_pool->max * 3)
                DWARN("max recycle, max %u current %u\n", buffer_pool->max, buffer_pool->len);

        list_splice_tail(list, &buffer_pool->list);
        buffer_pool->len += count;

        BUFFER_POOL_CHECK(buffer_pool);
        sy_spin_unlock(&buffer_pool->lock);
        DBUG("pool len %u\n", buffer_pool->len);

        return 0;
err_ret:
        return ret;
}

int __buffer_new(void **ptr, int size)
{
        int ret;

        YASSERT(size <= PAGE_SIZE);

#ifdef SEND_SPLICE
        if (size == PAGE_SIZE) {
                DWARN("malloc big mem\n");
        }

        ret = ymalloc(ptr, size);
        if (ret)
                GOTO(err_ret, ret);
#else
        if (size % SDFS_BLOCK_SIZE == 0) {
                ret = mpool_get(&page_pool, ptr);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                ret = ymalloc(ptr, size);
                if (ret)
                        GOTO(err_ret, ret);
        }
#endif

        return 0;
err_ret:
        return ret;
}

int __buffer_free(void *ptr, int size)
{
        YASSERT(size <= PAGE_SIZE);

        if (size == PAGE_SIZE) {
                DBUG("malloc big mem\n");
        }

        yfree((void **)&ptr);
        
        return 0;
}

static inline seg_t *__memseg_alloc(uint32_t size)
{
        int ret;
        seg_t *seg;

        ret = mpool_get(&head_pool, (void **)&seg);
        if (unlikely(ret))
                return NULL;

        ret = __buffer_new((void **)&(seg->ptr), size);
        if (unlikely(ret)) {
                yfree((void **)&seg);
                return NULL;
        }

        seg->len = size;
        seg->type = BUFFER_RW;

        return seg;
}

static inline void __memseg_free(seg_t *seg)
{
        int ret;
        struct list_head list;

        if (seg->type == BUFFER_RW) {
                __buffer_free(seg->ptr, seg->len);
                mpool_put(&head_pool, seg);
        } else if (seg->type == BUFFER_POOL) {
                DBUG("free big mem %u\n", seg->len);
                __buffer_pool_switch(seg);
                INIT_LIST_HEAD(&list);
                list_add_tail(&seg->hook, &list);
                ret = __buffer_pool_put(&list, 1);
                if (ret)
                        YASSERT(0);
        }
}

static int __spliceseg_alloc(seg_t **_seg, int len)
{
        int ret;
        seg_t *seg;

        ret = mpool_get(&head_pool, (void **)&seg);
        if (ret)
                GOTO(err_ret, ret);

        seg->len = len;
        seg->type = BUFFER_SPLICE;

        ret = pipe(seg->pipe);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        *_seg = seg;

        return 0;
err_ret:
        return ret;
}

static  void __spliceseg_free(seg_t *seg)
{
        close(seg->pipe[0]);
        close(seg->pipe[1]);
        mpool_put(&head_pool, seg);
}

int mbuffer_get(const buffer_t *buf, void *dist, uint32_t len)
{
        struct list_head *pos;
        seg_t *seg;
        uint32_t left, cp;

        YASSERT(buf->len >= len);

        BUFFER_CHECK(buf);

        left = len;

        list_for_each(pos, &buf->list) {
                seg = (seg_t *)pos;
                cp = left < seg->len ? left : seg->len;
                _memcpy(dist + (len - left), seg->ptr, cp);

                left -= cp;
                if (left == 0)
                        break;
        }

        BUFFER_CHECK(buf);

        YASSERT(left == 0);

        return 0;
}

int mbuffer_popmsg(buffer_t *buf, void *dist, uint32_t len)
{
        struct list_head *pos, *n;
        seg_t *seg;
        uint32_t left, cp;

        //ANALYSIS_BEGIN(0);

        BUFFER_CHECK(buf);

        left = len;

        YASSERT(dist);
        YASSERT(buf->len >= len);

        list_for_each_safe(pos, n, &buf->list) {
                seg = (seg_t *)pos;
                cp = left < seg->len ? left : seg->len;

                _memcpy(dist + (len - left), seg->ptr, cp);

                switch (seg->type) {
                case BUFFER_RW:
                case BUFFER_POOL:
                        if (cp < seg->len) {
                                memmove(seg->ptr, seg->ptr + cp, seg->len - cp);
                                seg->len -= cp;
                        } else {
                                list_del(pos);
                                __memseg_free(seg);
                        }

                        break;
                case BUFFER_SPLICE:
                        UNIMPLEMENTED(__DUMP__);
                default:
                        YASSERT(0);
                }

                left -= cp;
                if (left == 0)
                        break;
        }

        YASSERT(left == 0);

        buf->len -= len;

        BUFFER_CHECK(buf);

        //ANALYSIS_END(0, 100, "popmsg");

        return 0;
}

int mbuffer_appendmem(buffer_t *buf, const void *src, uint32_t len)
{
        int ret;
        seg_t *seg;

        DBUG("append len %u\n", len);

        YASSERT(len <= PAGE_SIZE);

        BUFFER_CHECK(buf);

        if (list_empty(&buf->list)) {
                seg = __memseg_alloc(len);
                if (seg == NULL) {
                        ret = ENOMEM;
                        GOTO(err_ret, ret);
                }

                list_add_tail(&seg->hook, &buf->list);

                _memcpy(seg->ptr, src, len);
        } else {
                seg = __memseg_alloc(len);
                if (seg == NULL) {
                        ret = ENOMEM;
                        GOTO(err_ret, ret);
                }

                list_add_tail(&seg->hook, &buf->list);

                _memcpy(seg->ptr, src, len);
        }

        buf->len += len;

        BUFFER_CHECK(buf);

        return 0;
err_ret:
        return ret;
}

static void  __buffer_free1(buffer_t *buf)
{
        int ret, count;
        seg_t *seg;
        struct list_head *pos, *n;

        count = 0;
        list_for_each_safe(pos, n, &buf->list) {
                seg = (seg_t *)pos;

                switch (seg->type) {
                case BUFFER_RW:
                        list_del(pos);
                        __memseg_free(seg);
                        break;
                case BUFFER_SPLICE:
                        list_del(pos);
                        __spliceseg_free(seg);
                        break;
                case BUFFER_POOL:
                        count++;
                        __buffer_pool_switch(seg);
                        break;
                default:
                        YASSERT(0);
                }
        }

        if (count) {
                ret = __buffer_pool_put(&buf->list, count);
                if (ret)
                        YASSERT(0);
        }

        buf->len = 0;
}

static int __buffer_sendrw(buffer_t *buf, int sd)
{
        int ret, count, sent;
        struct iovec iov[Y_MSG_MAX / 4096 + 1];
        struct msghdr msg;
        struct list_head *pos, *n;
        seg_t *seg;

        YASSERT(buf->tee_status == BUFFER_TEE_NULL);

        if (buf->len == 0) {
                return 0;
        }

        sent = 0;

        count = 0;
        list_for_each_safe(pos, n, &buf->list) {
                seg = (seg_t *)pos;

                YASSERT(seg->type == BUFFER_RW || seg->type == BUFFER_POOL);

                iov[count].iov_base = seg->ptr;
                iov[count].iov_len = seg->len;

                count++;
                YASSERT(count <= Y_MSG_MAX / 4096 + 1);
        }

        memset(&msg, 0x0, sizeof(msg));
        msg.msg_iov = iov;
        msg.msg_iovlen = count;

        ANALYSIS_BEGIN(0);

        ret = sendmsg(sd, &msg, MSG_DONTWAIT);
        if (ret < 0) {
                ret = errno;
                if (ret == EAGAIN)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        ANALYSIS_END(0, 2000, "send");

        DBUG("sent %u\n", ret);

        sent = ret;
        buf->len -= sent;

        list_for_each_safe(pos, n, &buf->list) {
                seg = (seg_t *)pos;

                if (sent >= (int)seg->len) {
                        sent -= seg->len;
                        list_del(&seg->hook);
                        __memseg_free(seg);
                } else {
                        memmove(seg->ptr, seg->ptr + sent, seg->len - sent);
                        seg->len -= sent;
                        sent = 0;
                }

                if (sent == 0)
                        break;
        }

        YASSERT(sent == 0);

        BUFFER_CHECK(buf);

        if (buf->len) {
                return EAGAIN;
        }

        return 0;
err_ret:
        if(ret != EAGAIN) {
                __buffer_free1(buf);
        }
        return ret;
}

int mbuffer_send(buffer_t *buf, int sd, int *eagain)
{
        int ret, seg_count, sent, flag, done = 0, cp = 0;
        struct list_head *pos, *n;
        seg_t *seg;

        BUFFER_CHECK(buf);

        ANALYSIS_BEGIN(0);

        if (buf->tee_status == BUFFER_TEE_NULL && buf->len > 4096) {
                ret = __buffer_sendrw(buf, sd);
                if (ret == EAGAIN) {
                        *eagain = 1;
                        return 0;
                }

                return ret;
        }

        *eagain = 0;
        seg_count = 0;
        sent = 0;
        list_for_each_safe(pos, n, &buf->list) {
                seg = (seg_t *)pos;

                switch (seg->type) {
                case BUFFER_POOL:
                        DWARN("send big mem\n");
                case BUFFER_RW:
                        if (seg->hook.next == &buf->list)
                                flag = MSG_NOSIGNAL;
                        else
                                flag = MSG_NOSIGNAL | MSG_MORE;

                        if (seg_count != 0) {
                                DBUG("send unaligned seg %u buf %u seg count %u\n",  seg->len, buf->len, seg_count);
                        }

                        ret = _send(sd, seg->ptr, seg->len, flag);
                        if (unlikely(ret < 0)) {
                                ret = -ret;
                                if (ret == EAGAIN) {
                                        cp = 0;
                                        done = 1;
                                        *eagain = 1;
                                } else
                                        GOTO(err_ret, ret);
                        } else
                                cp = ret;

                        YASSERT(cp <= (int)seg->len);

                        if (cp != (int)seg->len) {
                                memmove(seg->ptr, seg->ptr + cp, seg->len - cp);
                                seg->len -= cp;
                                done = 1;
                                *eagain = 1;
                        } else {
                                list_del(&seg->hook);
                                __memseg_free(seg);
                        }

                        break;
                case BUFFER_SPLICE:
                        if (seg->hook.next == &buf->list)
                                flag = SPLICE_F_NONBLOCK;
                        else
                                flag = SPLICE_F_NONBLOCK | SPLICE_F_MORE;

                        ret = splice(seg->pipe[0], NULL, sd, NULL, seg->len, flag);
                        if (ret < 0) {
                                ret = errno;
                                if (ret == EAGAIN) {
                                        cp = 0;
                                        done = 1;
                                        *eagain = 1;
                                } else
                                        GOTO(err_ret, ret);
                        } else
                                cp = ret;

                        YASSERT(cp <= (int)seg->len);

                        if (cp != (int)seg->len) {
                                seg->len -= cp;
                                done = 1;
                                *eagain = 1;
                        } else {
                                list_del(&seg->hook);
                                __spliceseg_free(seg);
                        }

                        break;
                default:
                        YASSERT(0);
                }

                sent += cp;

                if (done == 1)
                        break;

                seg_count++;
        }

        buf->len -= sent;

        BUFFER_CHECK(buf);

        if (buf->len) {
                return EAGAIN;
        }

        ANALYSIS_END(0, 1000, "send");

        return 0;
err_ret:
        __buffer_free1(buf);
        return ret;
}

static int __mbuffer_tee_trans0(seg_t **_seg, seg_t *segs, int count)
{
        int ret, i, got;
        seg_t *seg, *newseg;
        struct iovec iov[PIPE_SIZE / PAGE_SIZE];

        YASSERT(count);

        for(i = 0; i < count; i++) {
                seg = &segs[i];

                if ((LLU)seg->ptr % PAGE_SIZE == 0 && seg->len % PAGE_SIZE == 0) { /*aligned seg*/
                        iov[i].iov_base = seg->ptr;
                        iov[i].iov_len = PAGE_SIZE;

                        if (i  == PIPE_SIZE / PAGE_SIZE)
                                break;
                } else {
                        break;
                }
        }

        got = i;

        if (got == 0) {/*unaligned seg*/
                seg = &segs[0];

                DBUG("len %u\n", seg->len);

                newseg = __memseg_alloc(seg->len);
                if (newseg == NULL) {
                        ret = ENOMEM;
                        GOTO(err_ret, ret);
                }

                memcpy(newseg->ptr, seg->ptr, seg->len);
                got = 1;
        } else {
                ret = __spliceseg_alloc(&newseg, got * PAGE_SIZE);
                if (ret)
                        GOTO(err_ret, ret);

                ret = vmsplice(newseg->pipe[1], iov, got, SPLICE_F_GIFT);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                YASSERT(ret == (int)newseg->len);
        }

        *_seg = newseg;
        
        return got;
err_ret:
        return -ret;
}

static int  __list2array(seg_t *_seg, int *_seg_count, struct list_head *list)
{
        int ret, seg_count;
        struct list_head *pos;
        seg_t *seg;
 
        seg_count = 0;
        list_for_each(pos, list) {
                seg = (seg_t *)pos;

                _seg[seg_count] = *seg;
                seg_count++;

                if (seg_count > MAX_SEG) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }

        *_seg_count = seg_count;

        return 0;
err_ret:
        return ret;
}

static int __mbuffer_tee0(buffer_t *dist,  buffer_t *src)
{
        int ret, count, seg_count, seg_idx;
        seg_t *seg = NULL, segs[MAX_SEG];

        YASSERT(src->tee_status == BUFFER_TEE_NULL);
        BUFFER_CHECK(src);

        mbuffer_init(dist, 0);

        ret = __list2array(segs, &seg_count, &src->list);
        if (ret)
                GOTO(err_ret, ret);

        ANALYSIS_BEGIN(0);

        seg_idx = 0;
        while (seg_idx < seg_count) {
                count = __mbuffer_tee_trans0(&seg, &segs[seg_idx], seg_count - seg_idx);
                if (count < 0) {
                        ret = -count;
                        GOTO(err_ret, ret);
                }

                seg_idx+= count;

                list_add_tail(&seg->hook, &dist->list);
        }

        ANALYSIS_END(0, 300, "tee0");

        src->tee_status = BUFFER_TEE_UNSUPPORT;
        dist->tee_status = BUFFER_TEE_PIPE;
        dist->len = src->len;

        BUFFER_CHECK(dist);

        return 0;
err_ret:
        return ret;
}

static int __buffer_clean(seg_t *newseg, int left)
{
        int ret, fd;

        //清除已经tee到的数据
        fd = open("/dev/null", O_WRONLY);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        while (left>0) {
                ret = splice(newseg->pipe[0], NULL, fd, NULL, left, SPLICE_F_MOVE);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_close, ret);
                }

                if (ret == 0) {
                        break;
                }

                left -= ret;
        }

        close(fd);

        return 0;
err_close:
        close(fd);
err_ret:
        return ret;
}

static int __mbuffer_tee_trans1(seg_t **_seg, seg_t *seg)
{
        int ret, retry, retry_max, slp;
        seg_t *newseg;

        if (seg->type == BUFFER_RW) {/*unaligned seg*/
                newseg = __memseg_alloc(seg->len);
                if (newseg == NULL) {
                        ret = ENOMEM;
                        GOTO(err_ret, ret);
                }

                DBUG("cp in tee len %u\n", seg->len);

                memcpy(newseg->ptr, seg->ptr, seg->len);
        } else if (seg->type == BUFFER_SPLICE) {
                ret = __spliceseg_alloc(&newseg, seg->len);
                if (ret)
                        GOTO(err_ret, ret);

                retry = 0;
                retry_max = 30;
                slp = 100; //微秒
retry:
                ret = tee(seg->pipe[0], newseg->pipe[1],  seg->len, 0);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                if (ret != (int)seg->len) {
                        DWARN("tee %d, seg->len: %d\n", ret, (int)seg->len);
                        ret = __buffer_clean(newseg, ret);
                        if (ret) {
                                GOTO(err_ret, ret);
                        }

                        USLEEP_RETRY(err_tee, EAGAIN, retry, retry, retry_max, slp);
                }

                DBUG("pipe %u len %u %u\n", newseg->pipe[1], ret, newseg->len);

                YASSERT(ret == (int)seg->len);
        } else
                YASSERT(0);

        *_seg = newseg;
        
        return 0;
err_tee:
        YASSERT(0);
err_ret:
        return ret;
}

static int __mbuffer_tee1(buffer_t *dist, buffer_t *src)
{ 
        int ret;
        struct list_head *pos;
        seg_t *seg = NULL;

        ANALYSIS_BEGIN(0);

        YASSERT(src->tee_status == BUFFER_TEE_PIPE);
        BUFFER_CHECK(src);

        mbuffer_init(dist, 0);

        list_for_each(pos, &src->list) {
                ret = __mbuffer_tee_trans1(&seg, (seg_t *)pos);
                if (ret)
                        GOTO(err_ret, ret);

                list_add_tail(&seg->hook, &dist->list);
        }

        dist->tee_status = BUFFER_TEE_PIPE;
        dist->len = src->len;

        BUFFER_CHECK(dist);

        ANALYSIS_END(0, 300, "tee1");

        return 0;
err_ret:
        return ret;
}

int mbuffer_tee(buffer_t *dist, buffer_t *src)
{
        int ret;

#if 1
        return mbuffer_clone(dist, src);
#endif

        switch (src->tee_status) {
        case BUFFER_TEE_NULL:
                ret = __mbuffer_tee0(dist, src);
                if (ret)
                        GOTO(err_ret, ret);

                break;
        case BUFFER_TEE_PIPE:
                ret = __mbuffer_tee1(dist, src);
                if (ret)
                        GOTO(err_ret, ret);

                break;
        case BUFFER_TEE_UNSUPPORT:
                ret = EPERM;
                GOTO(err_ret, ret);
        default:
                YASSERT(0);
        }

        YASSERT(dist->len == src->len);

        return 0;
err_ret:
        return ret;
}

void mbuffer_merge(buffer_t *dist, buffer_t *src)
{

        BUFFER_CHECK(src);
        BUFFER_CHECK(dist);

        dist->len += src->len;
        src->len = 0;
        list_splice_tail_init(&src->list, &dist->list);

        if (likely(dist->tee_status == BUFFER_TEE_NULL))
                dist->tee_status = src->tee_status;

        BUFFER_CHECK(dist);
}


int mbuffer_init(buffer_t *buf, int size)
{
        seg_t *seg;

        YASSERT(size >= 0 || size < (1024 * 1024 * 100));

        /*_memset(buf, 0x0, sizeof(buffer_t));*/
        INIT_LIST_HEAD(&buf->list);
        buf->tee_status = BUFFER_TEE_NULL;
        buf->len = 0;

#ifdef SEND_SPLICE
        int ret, seg_count, tail;
        if (size >= SDFS_BLOCK_SIZE) {
                tail = size % PAGE_SIZE;
                seg_count = size / PAGE_SIZE;

                if (tail)
                        seg_count++;

                INIT_LIST_HEAD(&buf->list);
                ret = __buffer_pool_get(&buf->list, seg_count);
                if (ret)
                        YASSERT(0);

                if (tail) {
                        seg = (void *)buf->list.prev;
                        seg->len = tail;
                }
        } else if (size) {
                seg = __memseg_alloc(size);
                if (seg == NULL) {
                        YASSERT(0);
                }

                list_add_tail(&seg->hook, &buf->list);
        }
#else
        int left, cp;
        left = size;
        while (left > 0) {
                cp = left < PAGE_SIZE ? left : PAGE_SIZE;

                seg = __memseg_alloc(cp);
                if (seg == NULL) {
                        YASSERT(0);
                }

                list_add_tail(&seg->hook, &buf->list);
                left -= cp;
        }
#endif
        buf->len = size;

        BUFFER_CHECK(buf);

        return 0;
}

void mbuffer_free(buffer_t *buf)
{
        BUFFER_CHECK(buf);

        if (buf->len == 0)
                return;

        __buffer_free1(buf);
}

int mbuffer_pop(buffer_t *buf, buffer_t *newbuf, uint32_t len)
{
        int ret;
        struct list_head *pos, *n;
        seg_t *seg;
        uint32_t left, cp;

        BUFFER_CHECK(buf);

        YASSERT(len <= buf->len);

        left = len;

        list_for_each_safe(pos, n, &buf->list) {
                seg = (seg_t *)pos;
                cp = left < seg->len ? left : seg->len;

                switch (seg->type) {
                case BUFFER_POOL:
                        DWARN("pop big mem\n");
                case BUFFER_RW:
                        if (cp < seg->len) {
                                DBUG("pop %u from %u\n", cp, seg->len);

                                if (newbuf) {
                                        ret = mbuffer_appendmem(newbuf, seg->ptr, cp);
                                        if (ret)
                                                GOTO(err_ret, ret);
                                }

                                memmove(seg->ptr, seg->ptr + cp, seg->len - cp);
                                seg->len -= cp;
                        } else {
                                list_del(pos);

                                YASSERT(cp == seg->len);
                                if (newbuf) {
                                        ret = mbuffer_apply(newbuf, seg->ptr, cp);
                                        if (ret)
                                                GOTO(err_ret, ret);
                                } else {
                                        __buffer_free(seg->ptr, seg->len);
                                }

                                mpool_put(&head_pool, seg);
                        }

                        break;
                case BUFFER_SPLICE:
                        UNIMPLEMENTED(__DUMP__);
                default:
                        YASSERT(0);
                }

                left -= cp;
                if (left == 0)
                        break;
        }

        YASSERT(left == 0);

        buf->len -= len;

        BUFFER_CHECK(buf);
        if (newbuf) {
                BUFFER_CHECK(newbuf);
        }

        return 0;
err_ret:
        return ret;
}

//copy a long mem into buffer_t.
int mbuffer_copy(buffer_t *buf, const char *srcmem, int size)
{
        int ret, left, offset, cp;

        YASSERT(size >= 0);
        BUFFER_CHECK(buf);

        left = size;
        offset = 0;
        while (left > 0) {
                cp = left < PAGE_SIZE ? left : PAGE_SIZE;

                ret = mbuffer_appendmem(buf, srcmem + offset, cp);
                if (ret)
                        GOTO(err_ret, ret);

                left -= cp;
                offset += cp;
        }

        BUFFER_CHECK(buf);

        return 0;
err_ret:
        return ret;
}

int __buffer_clone(buffer_t *newbuf, const buffer_t *buf)
{
        int ret;
        struct list_head *pos;
        seg_t *seg;

        BUFFER_CHECK(buf);

        mbuffer_init(newbuf, 0);

        list_for_each(pos, &buf->list) {
                seg = (seg_t *)pos;

                YASSERT(seg->type == BUFFER_RW || seg->type == BUFFER_POOL);

                ret = mbuffer_appendmem(newbuf, seg->ptr, seg->len);
                if (ret)
                        GOTO(err_ret, ret);
        }

        BUFFER_CHECK(newbuf);
        BUFFER_CHECK(buf);

        return 0;
err_ret:
        return ret;
}

int mbuffer_clone(buffer_t *newbuf, buffer_t *buf)
{

        return __buffer_clone(newbuf, (const buffer_t *)buf);
}

//暂时实现reference为clone
void mbuffer_reference(buffer_t *dist, const buffer_t *src)
{
        int ret;
        struct list_head *pos, *n;
        seg_t *seg;

        list_for_each_safe(pos, n, &src->list) {
                seg = (seg_t *)pos;

                switch (seg->type) {
                case BUFFER_POOL:
                        DWARN("pop big mem\n");
                case BUFFER_RW:
                        ret = mbuffer_appendmem(dist, seg->ptr, seg->len);
                        if (ret)
                                GOTO(err_ret, ret);

                        break;
                case BUFFER_SPLICE:
                        UNIMPLEMENTED(__DUMP__);
                default:
                        YASSERT(0);
                }
        }

        return ;
err_ret:
        YASSERT(0);
}

/*size is the length from the begin of buffer*/
uint32_t mbuffer_crc(const buffer_t *buf, uint32_t offset, uint32_t size)
{
        uint32_t crcode, soff, count, left, step, crc;
        struct list_head *pos;
        seg_t *seg;

        YASSERT(size <= buf->len);

        BUFFER_CHECK(buf);

        crc32_init(crcode);
        count = 0;
        
        left = size;

        list_for_each(pos, &buf->list) {
                seg = (seg_t *)pos;

                if (seg->len + count < offset) {
                        count += seg->len;
                        continue;
                }

                if (count < offset) {
                        soff = offset - count;
                        count += seg->len;
                } else
                        soff = 0;

                step = (seg->len - soff) < left ? (seg->len - soff) : left;

                DBUG("crc off %u size %u (%s) left %u\n", soff, step,
                      (char *)seg->ptr + soff, left);

                crc32_stream(&crcode, seg->ptr + soff, step);

                left -= step;

                if (left == 0)
                        break;
        }

        crc = crc32_stream_finish(crcode);

        DBUG("len %u off %u crc %x\n", buf->len, offset, crc);

        BUFFER_CHECK(buf);

        return crc;
}

int mbuffer_appendzero(buffer_t *buf, int size)
{
        int ret, left, offset, cp;
        char srcmem[PAGE_SIZE];

        YASSERT(size >= 0);
        BUFFER_CHECK(buf);

        memset(srcmem, 0x0, PAGE_SIZE);

        left = size;
        offset = 0;
        while (left > 0) {
                cp = left < PAGE_SIZE ? left : PAGE_SIZE;

                ret = mbuffer_appendmem(buf, srcmem + offset, cp);
                if (ret)
                        GOTO(err_ret, ret);

                left -= cp;
        }

        BUFFER_CHECK(buf);

        return 0;
err_ret:
        return ret;
}

int mbuffer_writefile(const buffer_t *buf, int fd, uint64_t offset, uint64_t count)
{
        int ret;
        struct list_head *pos;
        seg_t *seg;
        uint64_t left, cp, off;

        YASSERT(buf->len >= count);

        DBUG("write fd %u off %llu size %llu\n", fd, (LLU)offset, (LLU)count);

        BUFFER_CHECK(buf);

        left = count;
        off = offset;

        list_for_each(pos, &buf->list) {
                if (left == 0)
                        break;

                seg = (seg_t *)pos;

                cp = left < seg->len ? left : seg->len;

                ret = _pwrite(fd, seg->ptr, cp, off);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                off += ret;
                left -= ret;
        }

        if (off - offset != count) {
                ret = EIO;
                GOTO(err_ret, ret);
        }

        BUFFER_CHECK(buf);

        return 0;
err_ret:
        return ret;
}

inline void *mbuffer_head(const buffer_t *buf)
{
        BUFFER_CHECK(buf);

        YASSERT(buf->len);

        if (list_empty(&buf->list))
                return NULL;

        return ((seg_t *)buf->list.next)->ptr;
}

int mbuffer_droptail(buffer_t *buf, uint32_t len)
{
        uint32_t drop, seg_len;
        seg_t *seg;

        YASSERT(buf->len >= len);

        if (len == 0)
                return 0;

        drop = len;

retry:
        seg = (void *)buf->list.prev;

        if (seg->len > drop) {
                seg->len -= drop;
        } else {
                list_del(&seg->hook);

                seg_len = seg->len;
                if (seg->type == BUFFER_RW || seg->type == BUFFER_POOL)
                        __memseg_free(seg);
                else
                        UNIMPLEMENTED(__DUMP__);

                if (seg_len < drop) {
                        drop -= seg_len;
                        goto retry;
                }
        }

        buf->len -= len;

        return 0;
}

int mbuffer_apply(buffer_t *buf, void *mem, int size)
{
        int ret;
        seg_t *seg;

        ret = mpool_get(&head_pool, (void **)&seg);
        //ret = ymalloc((void **)&seg, sizeof(seg_t));
        if (ret)
                GOTO(err_ret, ret);

        seg->ptr = mem;
        seg->len = size;
        seg->type = BUFFER_RW;

        list_add_tail(&seg->hook, &buf->list);
        buf->len += size;

        return 0;
err_ret:
        return ret;
}

int mbuffer_aligned(buffer_t *buf, int size)
{
        struct list_head *pos;
        seg_t *seg;

        list_for_each(pos, &buf->list) {
                seg = (seg_t *)pos;

                if ((int)seg->len % size || (LLU)(seg->ptr)  % size) {
                        return 0;
                }
        }

        return 1;
}

int mbuffer_trans(struct iovec *_iov, int *iov_count, const buffer_t *buf)
{
        int max, seg_count, size;
        struct list_head *pos;
        seg_t *seg;
 
        max = *iov_count;
        size = 0;
        seg_count = 0;

        list_for_each(pos, &buf->list) {
                if (seg_count == max) {
                        break;
                }

                seg = (seg_t *)pos;
                YASSERT(seg->ptr);
                _iov[seg_count].iov_len = seg->len;
                _iov[seg_count].iov_base = seg->ptr;

                seg_count++;
                size += seg->len;
        }

        *iov_count = seg_count;
        return size;
}

int mbuffer_apply1(buffer_t *buf, seg_t *_seg)
{
        int ret;
        seg_t *seg;

        DBUG("apply seg type %u len %u\n", _seg->type, _seg->len);

        YASSERT(_seg->type == BUFFER_RW);
        YASSERT(_seg->len);

        ret = mpool_get(&head_pool, (void **)&seg);
        //ret = ymalloc((void **)&seg, sizeof(seg_t));
        if (ret)
                GOTO(err_ret, ret);

        *seg = *_seg;
        list_add_tail(&seg->hook, &buf->list);
        buf->len += _seg->len;

        return 0;
err_ret:
        return ret;
}

void mbuffer_pop1(struct iovec *iov, buffer_t *buf, uint32_t len)
{

        int i;
        struct list_head *pos, *n;
        seg_t *seg;
        uint32_t left;

        YASSERT(len % PAGE_SIZE == 0);
        YASSERT(len <= buf->len);
        BUFFER_CHECK(buf);

        left = len;
        i = 0;
        list_for_each_safe(pos, n, &buf->list) {
                seg = (seg_t *)pos;

                YASSERT(seg->len == PAGE_SIZE);

                iov[i].iov_base = seg->ptr;
                iov[i].iov_len = PAGE_SIZE;

                DBUG("page %u size %u\n", i, PAGE_SIZE);

                list_del(&seg->hook);

                left -= PAGE_SIZE;
                buf->len -= PAGE_SIZE;

                mpool_put(&head_pool, seg);
                if (left == 0)
                        break;

                i++;
        }
}

#if 0
int mbuffer_share(buffer_t *dist, const buffer_t *src)
{
        int ret;
        seg_t *seg;
        struct list_head *pos;

        BUFFER_CHECK(src);
        BUFFER_CHECK(dist);

        list_for_each(pos, &src->list) {
                seg = __seg_share((void *)pos);

                if (seg == NULL) {
                        ret = ENOMEM;
                        GOTO(err_ret, ret);
                }

                list_add_tail(&seg->hook, &dist->list);
        }

        dist->len += src->len;

        BUFFER_CHECK(dist);

        return 0;
err_ret:
        return ret;
}
#endif

inline static void *__buffer_pool_worker(void *arg)
{
        int ret, count;
        buffer_pool_t *buffer_pool;
        struct list_head list;

        buffer_pool = arg;

        while (1) {
                ret = _sem_wait(&buffer_pool->sem);
                if (ret)
                        GOTO(err_ret, ret);

                DINFO("sem posted\n");
                while (1) {
                        ret = sy_spin_lock(&buffer_pool->lock);
                        if (ret)
                                GOTO(err_ret, ret);

                        if (buffer_pool->len >= buffer_pool->max) {
                                sy_spin_unlock(&buffer_pool->lock);
                                DINFO("buffer_pool done len %u %u\n", buffer_pool->len, buffer_pool->max);
                                break;
                        }

                        count = buffer_pool->max - buffer_pool->len;

                        sy_spin_unlock(&buffer_pool->lock);

                        INIT_LIST_HEAD(&list);
                        ret = __buffer_pool_alloc(&list, count);
                        if (ret)
                                GOTO(err_ret, ret);

                        ret = __buffer_pool_put(&list, count);
                        if (ret )
                                GOTO(err_ret, ret);
                }
        }

        return NULL;
err_ret:
        YASSERT(0);
        return NULL;
}

#ifdef SEND_SPLICE
int buffer_pool_init(int max)
{
        int ret, len;
        pthread_t th;
        pthread_attr_t ta;

        YASSERT(buffer_pool == NULL);

        ret = ymalloc((void **)&buffer_pool, sizeof(*buffer_pool));
        if (ret)
                GOTO(err_ret, ret);

        len =  sizeof(seg_t);

        ret = mpool_init(&head_pool, 0, len, max);
        if (ret)
                GOTO(err_free, ret);
        
        YASSERT(buffer_pool->inited == 0);
        ret = sy_spin_init(&buffer_pool->lock);
        if (ret)
                GOTO(err_free, ret);

        INIT_LIST_HEAD(&buffer_pool->list);
        buffer_pool->len = 0;
        buffer_pool->max = max;
        buffer_pool->inited = 1;

        ret = sem_init(&buffer_pool->sem, 0, 0);
        if (ret)
                GOTO(err_free, ret);

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);
        //pthread_attr_setstacksize(&ta, 1<<21);

        ret = pthread_create(&th, &ta, __buffer_pool_worker, buffer_pool);
        if (ret)
                GOTO(err_free, ret);

        sem_post(&buffer_pool->sem);

        return 0;
err_free:
        yfree((void **)&buffer_pool);
err_ret:
        return ret;
}
#else
int buffer_pool_init(int max)
{
        int ret;

        ret = mpool_init(&head_pool, 0, sizeof(seg_t), max);
        if (ret)
                GOTO(err_ret, ret);

        ret = mpool_init(&page_pool, PAGE_SIZE_4K, PAGE_SIZE, max);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}
#endif

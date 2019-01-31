#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/socket.h>

#define DBG_SUBSYS S_LIBYLIB

#include "adt.h"
#include "configure.h"
#include "sysutil.h"
#include "sdfs_buffer.h"
#include "ylib.h"
#include "mem_cache.h"
#include "analysis.h"
#include "mem_hugepage.h"
#include "dbg.h"

int use_memcache = 0;

static void __seg_free(seg_t *seg)
{
        if (seg->is_attach) {
                if (seg->base_ptr) {
                        yfree(&seg->base_ptr);
                }
        } else {
                if (seg->use_memcache) {
                        YASSERT(seg->handler.idx >= 0);
                        mem_hugepage_deref(&seg->handler);
                } else {
                        YASSERT(seg->handler.idx == -1);
                        YASSERT(seg->handler.pool == NULL);
                        yfree((void **)&seg->base_ptr);
                }
        }
        mem_cache_free(MEM_CACHE_64, seg);
}

static seg_t *__seg_alloc__(uint32_t size, int is_attach)
{
        seg_t *seg;

        if (!is_attach) {
                YASSERT(size <= BUFFER_SEG_SIZE);
        }

#ifdef HAVE_STATIC_ASSERT
        static_assert(sizeof(*seg)  < sizeof(mem_cache64_t), "seg_t");
#endif

        seg = mem_cache_calloc(MEM_CACHE_64, 1);
        if (!seg)
                return NULL;

        seg->type = BUFFER_RW;
        seg->handler.idx = -1;
        seg->handler.ptr = NULL;
        seg->len = size;
        seg->is_attach = is_attach;
        seg->use_memcache = 0;
        seg->base_ptr = NULL;
        DBUG("ptr %p %u %u %d\n", seg->handler.ptr, seg->len, size, seg->handler.idx);
        YASSERT(seg->len == size && seg->handler.idx == -1);

        return seg;
}

static inline void __seg_add_tail(buffer_t *buf, seg_t *seg)
{
        YASSERT(seg->len);
        list_add_tail(&seg->hook, &buf->list);
        buf->len += seg->len;
}

static inline void __seg_add(buffer_t *buf, seg_t *seg)
{
        YASSERT(seg->len);
        list_add(&seg->hook, &buf->list);
        buf->len += seg->len;
}

static seg_t *__seg_alloc(uint32_t size)
{
        int ret;
        seg_t *seg;

        seg = __seg_alloc__(size, 0);
        if (!seg)
                return NULL;

        if (use_memcache) {
                seg->use_memcache = 1;
                ret = mem_hugepage_new(size, &seg->handler);
                if (unlikely(ret)) {
                        mem_cache_free(MEM_CACHE_64, seg);
                        return NULL;
                }
        
                DBUG("ptr %p %u\n", seg->handler.ptr, seg->len);
                YASSERT(seg->handler.ptr);
                YASSERT(seg->handler.idx != -1);
                YASSERT(seg->len == size);
        } else {
                ret = ymalloc((void **)&seg->base_ptr, size);
                if (unlikely(ret)) {
                        mem_cache_free(MEM_CACHE_64, seg);
                        return NULL;
                }

                seg->use_memcache = 0;
                seg->handler.pool = NULL;
                seg->handler.idx = -1;
                seg->handler.ptr = seg->base_ptr;
        }

        return seg;
}

static seg_t *__seg_share(seg_t *src)
{
        seg_t *seg;

        YASSERT(use_memcache);
        // TODO 托管的内存，不采用引用计数，由应用层自行处理
        seg = __seg_alloc__(src->len, src->is_attach);
        if (!seg)
                return NULL;

        seg->use_memcache = 1;
        seg->handler = src->handler;
        if (!src->is_attach) {
                YASSERT(seg->handler.idx >= 0);
                mem_hugepage_ref(&seg->handler);
        }

        return seg;
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
                _memcpy(dist + (len - left), seg->handler.ptr, cp);

                left -= cp;
                if (left == 0)
                        break;
        }

        BUFFER_CHECK(buf);

        YASSERT(left == 0);

        return 0;
}

int mbuffer_get1(const buffer_t *buf, void *dist, uint32_t offset, uint32_t len)
{
        uint32_t buf_off = 0, seg_off, dist_off = 0;
        struct list_head *pos;
        seg_t *seg;
        uint32_t left, cp;

        YASSERT(buf->len >= offset + len);

        BUFFER_CHECK(buf);

        left = len;

        list_for_each(pos, &buf->list) {
                seg = (seg_t *)pos;

                if (buf_off + seg->len <= offset) {
                        cp = 0;
                } else if (buf_off < offset && buf_off + seg->len <= offset + len) {
                        seg_off = offset - buf_off;
                        cp = seg->len - seg_off;
                        _memcpy(dist + dist_off, seg->handler.ptr + seg_off, cp);
                } else if (buf_off < offset && buf_off + seg->len > offset + len) {
                        seg_off = offset - buf_off;
                        cp = len;
                        _memcpy(dist + dist_off, seg->handler.ptr + seg_off, cp);
                } else if (buf_off >= offset && buf_off + seg->len <= offset + len) {
                        cp = seg->len;
                        _memcpy(dist + dist_off, seg->handler.ptr, cp);
                } else if (buf_off >= offset && buf_off + seg->len> offset + len) {
                        cp = (offset + len) - buf_off;
                        _memcpy(dist + dist_off, seg->handler.ptr, cp);
                } else if (buf_off >= offset + len) {
                        cp = 0;
                } else {
                        YASSERT(0);
                }

                buf_off += seg->len;
                dist_off += cp;
                left -= cp;

                if (left == 0)
                        break;
        }

        BUFFER_CHECK(buf);

        YASSERT(left == 0);

        return 0;
}

int mbuffer_copy1(buffer_t *buf, void *src, uint32_t offset, uint32_t len)
{
        uint32_t buf_off = 0, seg_off, src_off = 0;
        struct list_head *pos;
        seg_t *seg;
        uint32_t left, cp;

        YASSERT(buf->len >= offset + len);

        BUFFER_CHECK(buf);

        left = len;

        list_for_each(pos, &buf->list) {
                seg = (seg_t *)pos;

                if (buf_off + seg->len <= offset) {
                        cp = 0;
                } else if (buf_off < offset && buf_off + seg->len <= offset + len) {
                        seg_off = offset - buf_off;
                        cp = seg->len - seg_off;
                        _memcpy(seg->handler.ptr + seg_off, src + src_off, cp);
                } else if (buf_off < offset && buf_off + seg->len > offset + len) {
                        seg_off = offset - buf_off;
                        cp = len;
                        _memcpy(seg->handler.ptr + seg_off, src + src_off, cp);
                } else if (buf_off >= offset && buf_off + seg->len <= offset + len) {
                        cp = seg->len;
                        _memcpy(seg->handler.ptr, src + src_off, cp);
                } else if (buf_off >= offset && buf_off + seg->len> offset + len) {
                        cp = (offset + len) - buf_off;
                        _memcpy(seg->handler.ptr, src + src_off, cp);
                } else if (buf_off >= offset + len) {
                        cp = 0;
                } else {
                        YASSERT(0);
                }

                buf_off += seg->len;
                src_off += cp;
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

                _memcpy(dist + (len - left), seg->handler.ptr, cp);

                switch (seg->type) {
                case BUFFER_RW:
                        if (cp < seg->len) {
                                seg->handler.ptr = seg->handler.ptr + cp;
                                seg->len -= cp;
                        } else {
                                list_del(pos);
                                __seg_free(seg);
                        }

                        break;
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

int mbuffer_attach(buffer_t *buf, void *src, uint32_t len, void *base_ptr)
{
        int ret;
        seg_t *seg;

        seg = __seg_alloc__(len, 1);
        if (seg == NULL) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        seg->use_memcache = 0;
        seg->handler.ptr = src;
        seg->base_ptr = base_ptr;

        __seg_add_tail(buf, seg);

        BUFFER_CHECK(buf);

        return 0;
err_ret:
        return ret;
}

/**
 * @todo len == 0
 *
 * @param buf
 * @param src
 * @param len len > 0
 * @param tail
 * @return
 */
static int __mbuffer_appendmem(buffer_t *buf, const void *src, uint32_t len, int tail)
{
        int ret;
        seg_t *seg;

        DBUG("append len %u\n", len);

        YASSERT(len <= BUFFER_SEG_SIZE);

        BUFFER_CHECK(buf);

        seg = __seg_alloc(len);
        if (seg == NULL) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        _memcpy(seg->handler.ptr, src, len);

        if (tail) {
                __seg_add_tail(buf, seg);
        } else {
                __seg_add(buf, seg);
        }

        BUFFER_CHECK(buf);

        return 0;
err_ret:
        return ret;
}

int mbuffer_appendmem(buffer_t *buf, const void *src, uint32_t len) {
        return __mbuffer_appendmem(buf, src, len, 1);
}

int mbuffer_appendmem_head(buffer_t *buf, const void *src, uint32_t len) {
        return __mbuffer_appendmem(buf, src, len, 0);
}

static void  __buffer_free1(buffer_t *buf)
{
        int count;
        seg_t *seg;
        struct list_head *pos, *n;

        (void) count;
        count = 0;
        list_for_each_safe(pos, n, &buf->list) {
                seg = (seg_t *)pos;

                switch (seg->type) {
                case BUFFER_RW:
                        list_del(pos);
                        __seg_free(seg);
                        break;
                default:
                        YASSERT(0);
                }
        }

        buf->len = 0;
}

void mbuffer_merge(buffer_t *dist, buffer_t *src)
{
        BUFFER_CHECK(src);
        BUFFER_CHECK(dist);

        dist->len += src->len;
        src->len = 0;
        list_splice_tail_init(&src->list, &dist->list);

        BUFFER_CHECK(dist);
}

static void __mbuffer_reference_clone(buffer_t *dist, const buffer_t *src)
{
        int ret;
        struct list_head *pos, *n;
        seg_t *seg;

        list_for_each_safe(pos, n, &src->list) {
                seg = (seg_t *)pos;

                switch (seg->type) {
                case BUFFER_RW:
                        ret = mbuffer_appendmem(dist, seg->handler.ptr, seg->len);
                        if (ret)
                                GOTO(err_ret, ret);

                        break;
                default:
                        YASSERT(0);
                }
        }

        return ;
err_ret:
        YASSERT(0);
}

void mbuffer_reference(buffer_t *dist, const buffer_t *src)
{
        seg_t *seg;
        struct list_head *pos;

        if (use_memcache == 0) {
                return __mbuffer_reference_clone(dist, src);
        }
        
        BUFFER_CHECK(src);
        BUFFER_CHECK(dist);

        list_for_each(pos, &src->list) {
                seg = __seg_share((void *)pos);

                if (seg == NULL) {
                        UNIMPLEMENTED(__DUMP__);
                }

                __seg_add_tail(dist, seg);
        }

        //YASSERT(dist->len == src->len);
        //dist->len += src->len;

        BUFFER_CHECK(dist);
}

int mbuffer_init(buffer_t *buf, int size)
{
        seg_t *seg;

        YASSERT(size >= 0 || size < (1024 * 1024 * 100));

        _memset(buf, 0x0, sizeof(buffer_t));
        INIT_LIST_HEAD(&buf->list);

        ANALYSIS_BEGIN(0);

        int left, cp;
        left = size;
        while (left > 0) {
                cp = left < BUFFER_SEG_SIZE ? left : BUFFER_SEG_SIZE;

                seg = __seg_alloc(cp);
                if (seg == NULL) {
                        YASSERT(0);
                }

                __seg_add_tail(buf, seg);
                left -= cp;
        }

        BUFFER_CHECK(buf);

        ANALYSIS_END(0, 1000 * 100, NULL);

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
                case BUFFER_RW:
                        if (cp < seg->len) {
                                DBUG("pop %u from %u\n", cp, seg->len);

                                if (newbuf) {
                                        ret = mbuffer_appendmem(newbuf, seg->handler.ptr, cp);
                                        if (unlikely(ret))
                                                GOTO(err_ret, ret);
                                }

                                seg->handler.ptr = seg->handler.ptr + cp;
                                seg->len -= cp;
                        } else {
                                list_del(pos);

                                YASSERT(cp == seg->len);
                                if (newbuf) {
                                        __seg_add_tail(newbuf, seg);
                                } else {
                                        __seg_free(seg);
                                }
                        }

                        break;
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
                cp = left < BUFFER_SEG_SIZE ? left : BUFFER_SEG_SIZE;

                ret = mbuffer_appendmem(buf, srcmem + offset, cp);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                left -= cp;
                offset += cp;
        }

        BUFFER_CHECK(buf);

        return 0;
err_ret:
        return ret;
}

#if 1
void mbuffer_clone(buffer_t *newbuf, const buffer_t *buf)
{
        int ret;
        struct list_head *pos;
        seg_t *seg;

        BUFFER_CHECK(buf);

        mbuffer_init(newbuf, 0);

        list_for_each(pos, &buf->list) {
                seg = (seg_t *)pos;

                YASSERT(seg->type == BUFFER_RW);

                ret = mbuffer_appendmem(newbuf, seg->handler.ptr, seg->len);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);
        }

        BUFFER_CHECK(newbuf);
        BUFFER_CHECK(buf);
}

#else
inline static void __mbuffer_iov_copy(seg_t *seg, struct iovec **_iov, int *_iov_count)
{
        int iov_count, left, count;
        struct iovec *iov = *_iov;
        void *ptr;

        iov_count = *_iov_count;
        YASSERT(iov_count > 0);
        left = seg->len;
        ptr = seg->handler.ptr;
        iov = *_iov;
        while (left) {
                count = left < (int)iov->iov_len ? left : (int)iov->iov_len;
                memcpy(ptr, iov->iov_base, count);
                ptr += count;
                left -= count;
                if (count == (int)iov->iov_len) {
                        iov++;
                        iov_count--;
                        YASSERT(iov_count >= 0);
                } else {
                        iov->iov_base += count;
                        iov->iov_len -= count;
                        YASSERT(left == 0);
                }
        }

        *_iov = iov;
        *_iov_count = iov_count;
}

void mbuffer_clone(buffer_t *newbuf, const buffer_t *buf)
{
        int ret, iov_count;
        uint32_t len;
        struct list_head *pos;
        seg_t *seg;
        struct iovec iov_array[LICH_IOV_MAX * 2], *iov_buf = NULL, *iov;

        //YASSERT(buf->len % LICH_BLOCK_SIZE == 0);
        BUFFER_CHECK(buf);

        iov_count = LICH_IOV_MAX * 2;
        ret = mbuffer_trans(iov_array, &iov_count, buf);
        if (ret != (int)buf->len) {
                ret = ymalloc((void **)&iov_buf, BUF_SIZE_16K);
                if (unlikely(ret))
                        YASSERT(0);

                iov_count = BUF_SIZE_16K / sizeof(struct iovec);
                ret = mbuffer_trans(iov_buf, &iov_count, buf);
                YASSERT(ret == (int)buf->len);

                iov = iov_buf;
        } else {
                iov = iov_array;
        }

        mbuffer_init(newbuf, buf->len);
        len = 0;
        list_for_each(pos, &newbuf->list) {
                seg = (seg_t *)pos;

                __mbuffer_iov_copy(seg, &iov, &iov_count);

                len+= seg->len;

#if 0
                uint32_t crc1, crc2;
                crc1 = mbuffer_crc(buf, 0, len);
                crc2 = mbuffer_crc(newbuf, 0, len);
                YASSERT(crc1 == crc2);
#endif
        }

        BUFFER_CHECK(newbuf);
        BUFFER_CHECK(buf);
        YASSERT(len == newbuf->len);
        if (iov_buf)
                yfree((void **)&iov_buf);


        uint32_t crc1 = mbuffer_crc(buf, 0, buf->len);
        uint32_t crc2 = mbuffer_crc(newbuf, 0, newbuf->len);
        YASSERT(crc1 == crc2);
}
#endif

int mbuffer_part_clone(const buffer_t *buf, uint32_t offset, int size, buffer_t *dist)
{

        int tmp_size = 0;
        int w_size = 0;
        uint32_t left = 0;
        struct list_head *pos;
        seg_t *seg;


        left = size;

        list_for_each(pos, &buf->list) {

                seg = (seg_t *)pos;
                YASSERT(seg->handler.ptr);

                if (offset >= seg->len) {
                        offset -= seg->len;
                        continue;
                }

                if(left < seg->len - offset ){
                	w_size = left;
                }else{
                	w_size = seg->len - offset;
                }

                mbuffer_appendmem(dist, seg->handler.ptr + offset, w_size);
                tmp_size += w_size;
                offset = 0;
                left -= w_size;

                if(left == 0){
                	break;
                }

        }

        return tmp_size;
}

int mbuffer_append(buffer_t *newbuf, const buffer_t *buf)
{
        int ret, left, offset, cp;
        char *srcmem;

        BUFFER_CHECK(newbuf);
        BUFFER_CHECK(buf);

        ret = ymalloc((void **)&srcmem, BUFFER_SEG_SIZE);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        left = buf->len;
        offset = 0;
        while (left > 0) {
                cp = left < BUFFER_SEG_SIZE ? left : BUFFER_SEG_SIZE;

                mbuffer_get1(buf, srcmem, offset, cp);

                ret = mbuffer_appendmem(newbuf, srcmem, cp);
                if (unlikely(ret))
                        GOTO(err_free, ret);

                left -= cp;
                offset += cp;
        }

        BUFFER_CHECK(newbuf);
        BUFFER_CHECK(buf);

        yfree((void **)&srcmem);
        return 0;
err_free:
        yfree((void **)&srcmem);
err_ret:
        return ret;
}

uint32_t mbuffer_crc_stream(uint32_t *crcode, const buffer_t *buf, uint32_t offset, uint32_t size)
{
        uint32_t soff, count, left, step;
        struct list_head *pos;
        seg_t *seg;

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
                      (char *)seg->handler.ptr + soff, left);

                crc32_stream(crcode, seg->handler.ptr + soff, step);

                left -= step;

                if (left == 0)
                        break;
        }

        return 0;
}

/*size is the length from the begin of buffer*/
uint32_t mbuffer_crc(const buffer_t *buf, uint32_t offset, uint32_t size)
{
        uint32_t crcode, crc;

        YASSERT(size <= buf->len);

        BUFFER_CHECK(buf);

        crc32_init(crcode);
        mbuffer_crc_stream(&crcode, buf, offset, size);
        crc = crc32_stream_finish(crcode);

        DBUG("len %u off %u crc %x\n", buf->len, offset, crc);

        BUFFER_CHECK(buf);

        return crc;
}

int mbuffer_appendzero(buffer_t *buf, int size)
{
        int left, cp;
        seg_t *seg;

        YASSERT(size >= 0);
        BUFFER_CHECK(buf);

        left = size;
        while (left > 0) {
                cp = left < BUFFER_SEG_SIZE ? left : BUFFER_SEG_SIZE;

                seg = __seg_alloc(cp);
                YASSERT(seg);
                memset(seg->handler.ptr, 0x0, cp);
                __seg_add_tail(buf, seg);
                left -= cp;
        }

        BUFFER_CHECK(buf);

        return 0;
}

int mbuffer_writefile(const buffer_t *buf, int fd, uint64_t offset)
{
        int ret, count, size;
        uint32_t trans_off = 0;
        struct iovec iov[(BIG_BUF_LEN * 2 / BUFFER_SEG_SIZE) * 2];

        //DBUG("write fd %u off %llu size %llu\n", fd, (LLU)offset, (LLU)count);

        //YASSERT(buf->len <= BIG_BUF_LEN * 2);

        BUFFER_CHECK(buf);

        while (trans_off < buf->len) {
                count = (BIG_BUF_LEN * 2 / BUFFER_SEG_SIZE) * 2;
                size = mbuffer_trans1(iov, &count, trans_off, buf);

                ret = pwritev(fd, iov, count, offset + trans_off);
                if (unlikely(ret < 0)) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                trans_off += size;
        }

        YASSERT(trans_off == buf->len);

        return 0;
err_ret:
        return ret;
}

void *mbuffer_head(buffer_t *buf)
{
        BUFFER_CHECK(buf);

        YASSERT(buf->len);

        if (list_empty(&buf->list))
                return NULL;

        return ((seg_t *)buf->list.next)->handler.ptr;
}

int mbuffer_trans(struct iovec *_iov, int *iov_count, const buffer_t *buf)
{
        int seg_count, max, size = 0;
        struct list_head *pos;
        seg_t *seg;

        max = *iov_count;
        seg_count = 0;
        list_for_each(pos, &buf->list) {
                if (seg_count == max) {
                        break;
                }

                seg = (seg_t *)pos;
                YASSERT(seg->handler.ptr);

                _iov[seg_count].iov_len = seg->len;
                _iov[seg_count].iov_base = seg->handler.ptr;
                seg_count++;
                size += seg->len;
        }

        *iov_count = seg_count;
        //YASSERT(size == (int)buf->len);
        return size;
}

int mbuffer_trans1(struct iovec *_iov, int *iov_count, uint32_t offset, const buffer_t *buf)
{
        int seg_count, max, size = 0;
        struct list_head *pos;
        seg_t *seg;

        max = *iov_count;
        seg_count = 0;
        list_for_each(pos, &buf->list) {
                if (seg_count == max) {
                        break;
                }

                seg = (seg_t *)pos;
                YASSERT(seg->handler.ptr);

                if (offset >= seg->len) {
                        offset -= seg->len;
                        continue;
                }

                _iov[seg_count].iov_len = seg->len - offset;
                _iov[seg_count].iov_base = seg->handler.ptr + offset;
                seg_count++;
                size += seg->len - offset;
                offset = 0;
        }

        *iov_count = seg_count;
        return size;
}

void mbuffer_trans2(struct iovec *_iov, int *iov_count, uint32_t offset, uint32_t size, const buffer_t *buf)
{
        int seg_count;
        uint32_t rest;
        struct list_head *pos;
        seg_t *seg;

        seg_count = 0;
        list_for_each(pos, &buf->list) {
                seg = (seg_t *)pos;
                YASSERT(seg->handler.ptr);

                if (offset >= seg->len) {
                        offset -= seg->len;
                        continue;
                }

                rest = seg->len - offset;
                _iov[seg_count].iov_len = rest < size ? rest : size;
                _iov[seg_count].iov_base = seg->handler.ptr + offset;
                size -= _iov[seg_count].iov_len;
                offset = 0;
                seg_count++;

                if (!size) {
                        break;
                }
        }

        YASSERT(!size);

        *iov_count = seg_count;
}

int mbuffer_compress2(buffer_t *buf, uint32_t max_seg_len)
{
        int ret;
        uint32_t idx1 = 0, idx2 = 0, len = 0, left, seg_left;
        struct list_head *pos, *n, list;
        seg_t *seg1, *seg2;

        BUFFER_CHECK(buf);

        INIT_LIST_HEAD(&list);

        left = buf->len;
        list_for_each_safe(pos, n, &buf->list) {
                seg1 = (seg_t *)pos;
                seg_left = seg1->len;

                YASSERT(seg1->handler.ptr);

                while (seg_left) {
                        if (idx2 == 0) {
                                seg2 = __seg_alloc(_min(left, max_seg_len));
                                if (seg2 == NULL) {
                                        ret = ENOMEM;
                                        GOTO(err_ret, ret);
                                }
                                list_add_tail(&seg2->hook, &list);
                        }

                        len = _min(seg1->len - idx1, seg2->len - idx2);

                        _memcpy(seg2->handler.ptr + idx2, seg1->handler.ptr + idx1, len);

                        idx1 += len;
                        idx2 += len;

                        if (idx1 == seg1->len) {
                                idx1 = 0;
                                list_del(pos);
                                __seg_free(seg1);
                        }
                        if (idx2 == seg2->len) {
                                idx2 = 0;
                        }

                        left -= len;
                        seg_left -= len;
                }
        }

        list_splice_init(&list, &buf->list);

        BUFFER_CHECK(buf);

        return 0;
err_ret:
        return ret;
}

int mbuffer_iszero(const buffer_t *buf)
{
        uint32_t i;
        struct list_head *pos, *n;
        seg_t *seg;

        list_for_each_safe(pos, n, &buf->list) {
                seg = (seg_t *)pos;

                for (i = 0; i < seg->len; i++) {
                        if (*(char *)(seg->handler.ptr + i) != 0)
                                return 0;
                }
        }

        return 1;
}

inline int mbuffer_segcount(const buffer_t *buf)
{
        return list_size((struct list_head *)&buf->list);
}

int mbuffer_ncompare(const buffer_t *buf, uint32_t offset, const buffer_t *cmp, uint32_t size)
{
        int ret = 0, left, len;
        uint32_t idx1 = 0, idx2 = 0, count = 0;
        struct list_head *pos1, *pos2;
        seg_t *seg1, *seg2;

        pos1 = buf->list.next;
        pos2 = cmp->list.next;
        left = size;
        while(left) {
                seg1 = (seg_t *)pos1;
                seg2 = (seg_t *)pos2;

                if (seg1->len + count < offset) {
                        count += seg1->len;
                        pos1 = pos1->next;
                        continue;
                }

                if (count < offset) {
                        idx1 = offset - count;
                        count += idx1;
                }

                len = _min(seg1->len - idx1, seg2->len - idx2);
                len = _min(len, left);

                ret = memcmp(seg1->handler.ptr + idx1, seg2->handler.ptr + idx2, len);
                if (ret)
                        return ret;

                idx1 += len;
                idx2 += len;

                if (idx1 == seg1->len) {
                        idx1 = 0;
                        pos1 = pos1->next;
                }
                if (idx2 == seg2->len) {
                        idx2 = 0;
                        pos2 = pos2->next;
                }

                left -= len;
                if (pos1 == &buf->list || pos2 == &cmp->list) {
                        return !(left == 0);
                }
        }

        return ret;
}

int mbuffer_compress(buffer_t *buf) {
        return mbuffer_compress2(buf, BUFFER_SEG_SIZE);
}

int mbuffer_compare(const buffer_t *buf, const buffer_t *cmp)
{
        if (buf->len != cmp->len) {
                return 1;
        }

        return mbuffer_ncompare(buf, 0, cmp, buf->len);
}

int mbuffer_ncmp(const buffer_t *buf, uint32_t offset, const char *cmp, uint32_t size)
{
        int ret = 0, left, len;
        uint32_t idx1 = 0, idx2 = 0, count = 0;
        struct list_head *pos;
        seg_t *seg;

        pos = buf->list.next;
        left = size;
        while(left) {
                seg = (seg_t *)pos;

                if (seg->len + count < offset) {
                        count += seg->len;
                        pos = pos->next;
                        continue;
                }

                if (count < offset) {
                        idx1 = offset - count;
                        count += idx1;
                }

                len = _min(seg->len - idx1, size - idx2);
                len = _min(len, left);

                ret = memcmp(seg->handler.ptr + idx1, cmp + idx2, len);
                if (ret)
                        return ret;

                idx1 += len;
                idx2 += len;

                if (idx1 == seg->len) {
                        idx1 = 0;
                        pos = pos->next;
                }

                left -= len;
                if (pos == &buf->list || idx2 == size) {
                        return !(left == 0);
                }
        }

        return ret;
}

int mbuffer_find(const buffer_t *buf, const char *find, uint32_t size)
{
        int idx;
        uint32_t i;

        if (buf->len < size)
                return -1;

        idx = -1;
        for (i = 0 ; i <= buf->len - size; i++) {
                if (!mbuffer_ncmp(buf, i, find, size)) {
                        idx = i;
                        break;
                }
        }

        return idx;
}

int mbuffer_rfind(const buffer_t *buf, const char *find, uint32_t size)
{
        int idx;
        uint32_t i;

        if (buf->len < size)
                return -1;

        idx = -1;
        for (i = 0 ; i <= buf->len - size; i++) {
                if (!mbuffer_ncmp(buf, buf->len - size - i, find, size)) {
                        idx = i;
                        break;
                }
        }

        return idx;
}

int mbuffer_dump(const buffer_t *buf, uint32_t len, const char *s)
{
        int ret;
        uint32_t tmplen, hexlen;
        char *tmp, *hex;

        tmplen = _min(len, buf->len);
        hexlen = _ceil(tmplen, 16) * 50;

        ret = ymalloc((void **)&tmp, tmplen);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&hex, hexlen);
        if (unlikely(ret))
                GOTO(err_free, ret);

        mbuffer_get(buf, tmp, tmplen);
        _hex_println(hex, hexlen, tmp, tmplen);

        if (s) {
                DINFO("%s buffer dump:%s\n", s, hex);
        } else {
                DINFO("buffer dump:%s\n", hex);
        }

        yfree((void **)&tmp);
        yfree((void **)&hex);

        return ret;
err_free:
        yfree((void **)&tmp);
err_ret:
        return ret;
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
                if (seg->type == BUFFER_RW)
                        __seg_free(seg);
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

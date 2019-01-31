#ifndef __BUFFER_H__
#define __BUFFER_H__

#include <sys/uio.h>
#include <sys/ioctl.h>
#include <stdint.h>
#include <errno.h>

#include "sdfs_list.h"

#define Y_PAGE_SIZE  (PAGE_SIZE)
#define Y_BLOCK_MAX  (524288 * 2) /*persistence data, never alter it*/
#define Y_MSG_MAX  (SHM_MAX)

#if 1
#define BUFFER_DEBUG
#endif

typedef enum {
        FREE_JOB = 1,
        KEEP_JOB = 2,
} mbuffer_op_t;

typedef enum {
        BUFFER_NONE = 0,
        BUFFER_RW   = 1,
} buffer_type_t;

#pragma pack(4)

typedef struct {
        void *pool;
        void *ptr;
        int idx;
} mem_handler_t;

typedef struct {
        struct list_head hook;
        buffer_type_t type;
        uint32_t len;
        mem_handler_t handler;
        char is_attach;
        char use_memcache;
        void *base_ptr;
} seg_t;

#pragma pack()

typedef struct {
        uint32_t len;
        struct list_head list;
} buffer_t;

#ifdef BUFFER_DEBUG
#define BUFFER_CHECK(buf)                                               \
        do {                                                            \
                uint32_t __len = 0;                                     \
                seg_t *__seg;                                           \
                struct list_head *pos;                                  \
                list_for_each(pos, &buf->list) {                        \
                        __seg = (seg_t *)pos;                           \
                        YASSERT(__seg->type == BUFFER_RW);              \
                        __len += __seg->len;                            \
                }                                                       \
                if (__len != (buf)->len) {                              \
                        DERROR("__len %u (buf)->len %u\n", __len, (buf)->len); \
                        YASSERT(0);                                     \
                }                                                       \
        } while (0)

#else
#define BUFFER_CHECK(buf)
#endif

#define USEC()                                                \
        do {                                                  \
                struct timeval __tv__;                        \
                gettimeofday(&__tv__, NULL);                  \
                DWARN("USEC %llu\n", (LLU)__tv__.tv_usec);    \
        } while (0);

int mbuffer_init(buffer_t *pack, int size);
void mbuffer_free(buffer_t *pack);

int mbuffer_attach(buffer_t *buf, void *src, uint32_t len, void *base_ptr);

int mbuffer_appendmem(buffer_t *buf, const void *src, uint32_t len);

int mbuffer_appendzero(buffer_t *buf, int size);

int mbuffer_appendmem_head(buffer_t *buf, const void *src, uint32_t len);

int mbuffer_copy(buffer_t *buf, const char *srcmem, int size);
int mbuffer_copy1(buffer_t *pack, void *buf, uint32_t offset, uint32_t len);

int mbuffer_popmsg(buffer_t *pack, void *buf, uint32_t len);

int mbuffer_pop(buffer_t *buf, buffer_t *newbuf, uint32_t len);

void mbuffer_merge(buffer_t *dist, buffer_t *src);
void mbuffer_reference(buffer_t *dist, const buffer_t *src);
void mbuffer_clone(buffer_t *dist, const buffer_t *src);
int mbuffer_append(buffer_t *dist, const buffer_t *src);

uint32_t mbuffer_crc_stream(uint32_t *crcode, const buffer_t *buf, uint32_t offset, uint32_t size);
uint32_t mbuffer_crc(const buffer_t *buf, uint32_t _off, uint32_t size);
extern int mbuffer_appendzero(buffer_t *buf, int size);
extern int mbuffer_writefile(const buffer_t *buf, int fd, uint64_t offset);

void *mbuffer_head(buffer_t *buf);
int mbuffer_get(const buffer_t *pack, void *buf, uint32_t len);
int mbuffer_get1(const buffer_t *pack, void *buf, uint32_t offset, uint32_t len);

uint32_t mbuffer_crc(const buffer_t *buf, uint32_t _off, uint32_t size);
int mbuffer_writefile(const buffer_t *buf, int fd, uint64_t offset);

int mbuffer_trans(struct iovec *_iov, int *iov_count, const buffer_t *buf);
int mbuffer_trans1(struct iovec *_iov, int *iov_count, uint32_t offset, const buffer_t *buf);
void mbuffer_trans2(struct iovec *_iov, int *iov_count, uint32_t offset, uint32_t size, const buffer_t *buf);

int mbuffer_segcount(const buffer_t *buf);
int mbuffer_iszero(const buffer_t *buf);
int mbuffer_ncompare(const buffer_t *buf, uint32_t offset, const buffer_t *cmp, uint32_t size);

int mbuffer_compress(buffer_t *buf);
int mbuffer_compress2(buffer_t *buf, uint32_t max_seg_len);

int mbuffer_compare(const buffer_t *buf, const buffer_t *cmp);
int mbuffer_ncmp(const buffer_t *buf, uint32_t offset, const char *cmp, uint32_t size);
int mbuffer_find(const buffer_t *buf, const char *find, uint32_t size);
int mbuffer_rfind(const buffer_t *buf, const char *find, uint32_t size);
int mbuffer_dump(const buffer_t *buf, uint32_t len, const char *s);

int mbuffer_part_clone(const buffer_t *buf, uint32_t offset, int size, buffer_t *dist);

int mbuffer_droptail(buffer_t *buf, uint32_t len);

#endif

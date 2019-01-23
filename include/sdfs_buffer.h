#ifndef __BUFFER_H__
#define __BUFFER_H__

#include <sys/uio.h>
#include <sys/ioctl.h>
#include <stdint.h>
#include <errno.h>

#include "sdfs_list.h"
#include "sdfs_conf.h"

#define Y_PAGE_SIZE  (PAGE_SIZE)
#define Y_BLOCK_MAX  (524288 * 2) /*persistence data, never alter it*/
#define Y_MSG_MAX  (SHM_MAX)

#define BUFFER_MAX_SEG 512

#if 1
#define BUFFER_DEBUG
#endif

typedef enum {
        FREE_JOB = 1,
        KEEP_JOB = 2,
} mbuffer_op_t;

typedef enum {
        BUFFER_NONE = 0,
        BUFFER_RW = 1,
        BUFFER_POOL = 2,
        BUFFER_SPLICE = 3,
} buffer_type_t;

typedef struct {
        struct list_head hook;
        buffer_type_t type;
        uint32_t len;
        void *ptr;
        int pipe[2];
} seg_t;

typedef enum {
        BUFFER_TEE_NULL = 0,
        BUFFER_TEE_UNSUPPORT,
        BUFFER_TEE_PIPE,
} mbuffer_tee_status_t;

typedef struct {
        uint32_t len;
        int tee_status;
        struct list_head list;
} buffer_t;

extern int mbuffer_get(const buffer_t *pack, void *buf, uint32_t len);
extern int mbuffer_popmsg(buffer_t *pack, void *buf, uint32_t len);
extern int mbuffer_appendmem(buffer_t *buf, const void *src, uint32_t len);
extern int mbuffer_pop(buffer_t *buf, buffer_t *newbuf, uint32_t len);
extern void mbuffer_merge(buffer_t *dist, buffer_t *src);
extern void mbuffer_reference(buffer_t *dist, const buffer_t *src);
extern void mbuffer_free(buffer_t *pack);
extern int mbuffer_init(buffer_t *pack, int size);
extern int mbuffer_copy(buffer_t *buf, const char *srcmem, int size);
extern int mbuffer_clone(buffer_t *dist, buffer_t *src);
uint32_t mbuffer_crc(const buffer_t *buf, uint32_t _off, uint32_t size);
extern int mbuffer_appendzero(buffer_t *buf, int size);
extern int mbuffer_writefile(const buffer_t *buf, int fd, uint64_t offset, uint64_t count);
extern void *mbuffer_head(const buffer_t *buf);
int mbuffer_droptail(buffer_t *buf, uint32_t len);
int mbuffer_apply(buffer_t *buf, void *mem, int size);
int mbuffer_aligned(buffer_t *buf, int size);
int mbuffer_trans(struct iovec *_iov, int *iov_count, const buffer_t *buf);
int mbuffer_send_prep(buffer_t *buf);
int mbuffer_apply1(buffer_t *buf, seg_t *_seg);
void mbuffer_pop1(struct iovec *iov, buffer_t *buf, uint32_t len);
int mbuffer_tee(buffer_t *dist, buffer_t *src);
int mbuffer_send(buffer_t *buf, int sd, int *eagain);
int buffer_pool_init(int max);

#endif

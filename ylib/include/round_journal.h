#ifndef __ROUND_JOURNAL_H__
#define __ROUND_JOURNAL_H__

#include <sys/uio.h>
#ifndef __CYGWIN__
#include <libaio.h>
#endif

#include "job.h"

#define RJNL_MAX_COUNT 40

#define RJNL_BUF_LEN 8192

typedef struct {
        uint32_t magic;
        uint32_t headlen;
        uint32_t datalen;
        char buf[0];
} rjnl_head_t;

typedef struct {
#ifndef __CYGWIN__
	struct iocb iocb;
#endif
        void *head;
} rjnl_iocb_t; 

typedef struct {
        int fd;
        int ref;
        time_t last_use;
        int off;
        int seq;
        uint64_t ref_total;
        uint64_t unref_total;
} rjnl_seg_t;

typedef struct {
        int idx;
        int seq;
} rjnl_handle_t;

typedef struct {
#ifndef __CYGWIN__
	io_context_t ctx;
#endif
	int count;
        rjnl_iocb_t *queue;
        sem_t sem;
} rjnl_queue_t;

typedef struct {
        int idx;
        int count;
        int max_size;
        rjnl_seg_t array[RJNL_MAX_COUNT];
} rjnl_file_t;

typedef struct {
        rjnl_queue_t queue;
        rjnl_file_t file;
        int inited;
        sy_spinlock_t lock;
        int seq; /*handle sequence*/
        sem_t *sem;
        char path[MAX_PATH_LEN];
} rjnl_t;

int rjnl_init(const char *_path, int count, int size, rjnl_t *jnl, int *dirty, const char *lock);
int rjnl_append(rjnl_t *jnl, const void *op, int oplen, const buffer_t *buf, rjnl_handle_t *idx, job_t *job);
//int rjnl_append(rjnl_t *jnl, const struct iovec *iov, int iovcnt, rjnl_handle_t *idx, job_t *job);
int rjnl_release(rjnl_t *jnl, rjnl_handle_t *idx);
int rjnl_release1(rjnl_t *jnl, int idx);
int rjnl_load(rjnl_t *jnl, int (*callback)(void *, void *, int, void *, int), void *arg);
void rjnl_destroy(rjnl_t *jnl);
int rjnl_newref(rjnl_t *jnl, int idx);
void rjnl_dump(rjnl_t *jnl);

#endif

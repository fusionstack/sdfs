#ifndef __JNL_PROTO_H__
#define __JNL_PROTO_H__

#include <unistd.h>
#include <pthread.h>

#include "sdfs_conf.h"
#include "job.h"
#include "sdfs_buffer.h"
#include "ylock.h"

#define MAX_JNL_LEN (8 * 1024 * 1024)

#define JNL_BUF_LEN BIG_BUF_LEN

typedef struct {
        int len;
        uint16_t status;
        uint16_t mversion;
        uint64_t offset;
        void *buf;
        job_t *job;
} jnl_iocb_t;

typedef struct {
        char home[MAX_PATH_LEN];
        sy_rwlock_t rwlock;
        sy_spinlock_t lock;
        int jnlmaxno;
        int running;
        int fd;
        int flag;
        int64_t offset;
        uint64_t version;
        uint16_t mversion;
} jnl_handle_t;

#pragma pack(8)

typedef struct {
        uint32_t magic;
        uint16_t __pad__;
        uint16_t status;
} jnl_status_t;

typedef struct {
        uint32_t magic;
        uint32_t __pad__;
        uint16_t len;
        uint16_t version;
        uint32_t crc;
        int64_t offset;
        char buf[0];
} jnl_head_t;

#pragma pack()

#if 1
/* journal.c */
int64_t jnl_append(jnl_handle_t *, const char *buf, uint32_t len, job_t *job,
                   int commit, uint64_t *version);
int jnl_open(const char *path, jnl_handle_t *, int flag);
int jnl_close(jnl_handle_t *);
int jnl_pread(jnl_handle_t *jnl, void *buf, int size, int64_t off);
int jnl_iterator(jnl_handle_t *jnl, int64_t _off,
                 int (*callback)(const void *, int len, int64_t, void *),
                 void *arg);
int jnl_iterator_buf(const char *buf, int len, int64_t offset,
                     int (*callback)(const void *, int len, int64_t, void *),
                     void *arg, int *_left, int *eof);
int jnl_append_prep(jnl_handle_t *jnl, uint32_t _len, uint64_t *offset);
int jnl_append1(jnl_handle_t *jnl, const char *_buf, uint32_t _size);
int jnl_write(jnl_handle_t *jnl, const char *_buf, uint32_t _size, uint64_t offset);
int jnl_pwrite(jnl_handle_t *jnl, const char *buf, uint32_t len, uint64_t offset);

#endif
#endif

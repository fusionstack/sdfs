#ifndef __DISK_H__
#define __DISK_H__

#include <stdint.h>
#include <semaphore.h>

#include "chk_meta.h"
#include "file_proto.h"
#include "cd_proto.h"
#include "ynet_rpc.h"
#include "sdfs_conf.h"
#include "sdfs_buffer.h"

#define DISKIO_OP_READ IO_CMD_PREADV
#define DISKIO_OP_WRITE IO_CMD_PWRITEV
#define DISKIO_OP_ALLOC 16

#define DISK_SEG (Y_BLOCK_MAX)
#define MAX_IO 256

#define __DISKIO_UNDEF__              0x00000000
#define __DISKIO_META__              0x00000001
#define __DISKIO_SKIP__              0x00000002

#ifdef __CENTOS5__
#define MAX_SUBMIT  ((DISK_SEG / PAGE_SIZE) * MAX_IO / 8)
#else
#define MAX_SUBMIT  MAX_IO
#endif

typedef struct {
        chkid_t id;
        job_t *job;
        //struct iovec buf[Y_MSG_MAX / PAGE_SIZE + 1]];
        struct iovec *buf;
        uint64_t version;
        uint32_t status;
        int ref;
        int count;
        int op;
        int offset;
        int size;
        int idx;
        int free;
} diskio_t;

typedef struct {
        int hit;
        time_t last;
} disk_seg_hit_t;

#define HIT_CONSTANT ((LLU)60 * 60 * 24 * 7 * 3) /*three week*/

int disk_unlink(const chkid_t *chkid, int size);
int disk_create(const chkid_t *chkid, int size);
int disk_sha1(const chkid_t *chkid, char *md);
int disk_join(const diskid_t *diskid, struct statvfs *fsbuf);
int disk_read_raw(const chkid_t *id, char *buf, int len, int off);
int disk_write_raw(const chkid_t *id, const char *buf, int len, int off);
int disk_write_raw1(const chkid_t *id, const buffer_t *buf, int len, int off);
int disk_io_submit(const diskio_t **diskios, int count, int hash);
int disk_replace(const chkid_t *chkid, const char *path);
int disk_init(const char *home, uint64_t _max_object);
int disk_getsize(const chkid_t *id, int *size);
int disk_setvalue(const chkid_t *id, const char *key, const char *value);
int disk_getvalue(const chkid_t *id, const char *key, char *value);
int disk_rebalance(int count);
int disk_getlevel(const chkid_t *id, int *level, int *max);
int disk_truncate(const chkid_t *id, int size);
int disk_levelcount();
int disk_analysis();
int disk_statvfs(struct statvfs *_stbuf);
void disk_dumpref();
int disk_get_syncfd(const chkid_t *chkid, int level, int *_fd);

int disk_unlink_leveldb(const chkid_t *chkid, int size);
int disk_sha1_leveldb(const chkid_t *chkid, char *md);
int disk_create_leveldb(const chkid_t *chkid, int size);
int disk_io_submit_leveldb(const diskio_t **diskios, int count);
int disk_read_raw_leveldb(const chkid_t *chkid, char *_buf, int len, int off);
int disk_write_raw_leveldb(const chkid_t *chkid,
                const char *_buf, int len, int off);
int disk_write_raw1_leveldb(const chkid_t *chkid, const buffer_t *_buf, int len, int off);
int disk_replace_leveldb(const chkid_t *chkid, const char *path);
int disk_getsize_leveldb(const chkid_t *chkid, int *size);
int disk_getvalue_leveldb(const chkid_t *id, const char *key, char *value);
int disk_setvalue_leveldb(const chkid_t *id, const char *key, const char *value);


int disk_mt_io_submit(const diskio_t **_diskios, int count, int hash);
int disk_mt_init();

#endif

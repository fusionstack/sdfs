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

int disk_join(const diskid_t *diskid, struct statvfs *fsbuf);
int disk_init(const char *home, uint64_t _max_object);
int disk_statvfs(struct statvfs *_stbuf);

#endif

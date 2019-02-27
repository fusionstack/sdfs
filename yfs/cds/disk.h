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

#define MAX_SUBMIT  256

int disk_join(const diskid_t *diskid, struct statvfs *fsbuf);
int disk_init(const char *home, uint64_t _max_object);
int disk_statvfs(struct statvfs *_stbuf);

#endif

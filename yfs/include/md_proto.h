#ifndef __MD_PROTO_H__
#define __MD_PROTO_H__

#include <sys/statvfs.h>
#include <stdint.h>

#include "adt.h"
#include "chk_proto.h"
#include "disk_proto.h"
#include "file_proto.h"
#include "node_proto.h"
#include "net_proto.h"
#include "yfs_md.h"
#include "sdfs_lib.h"
#include "dbg.h"
#include "sdfs_share.h"
#include "user.h"
#include "group.h"
#include "flock.h"
#include "sdfs_worm.h"

typedef struct {
        uint64_t size;
        uint32_t volid;
        uint32_t __pad__;
} volrept_t;

typedef struct {
        uint32_t volreptnum;
        uint32_t __pad__;
        volrept_t volrept[0];/*chkjnl_t already aligned*/
} volinfo_t;

#define NEWREP_NORMAL 0
#define NEWREP_UNREG 1
#define NEWREP_FORCE 2
#endif

#ifndef __YFS_MD_H__
#define __YFS_MD_H__

#include <stdint.h>

#include "ylib.h"
#include "chk_proto.h"
#include "net_proto.h"
#include "disk_proto.h"
#include "dbg.h"

#define __S_PREALLOC                           0x00000001
#define __S_DIRTY                        0x00000002
#define __S_WRITEBACK                          0x00000004


#define ATTR_ERASURE_CODE   "erasure_code"
#define ATTR_WRITEBACK   "writeback"
#define ATTR_IS_TARGET   "iscsi.is_target"
#define ATTR_PREALLOC   "prealloc"
#define ATTR_CHKREP     "repnum"
#define ATTR_SPLIT     "split"
#define ATTR_QUOTAID "quotaid"
#define ATTR_TRUE "true"
#define ATTR_FALSE "false"
#define ATTR_LVM_READ_BURST_MAX  "throt_read_burst_max"
#define ATTR_LVM_READ_BURST_TIME "throt_read_burst_time"
#define ATTR_LVM_READ_AVG        "throt_read_avg"
#define ATTR_LVM_WRITE_BURST_MAX  "throt_write_burst_max"
#define ATTR_LVM_WRITE_BURST_TIME "throt_write_burst_time"
#define ATTR_LVM_WRITE_AVG        "throt_write_avg"

#pragma pack(8)

#define __MD__ \
        uint32_t md_size;                       \
        uint32_t at_mode;                       \
        uint64_t md_version;                    \
        uint64_t wormid;                        \
        fileid_t quotaid;                       \
        fileid_t fileid;                        \
        fileid_t parent;                        \
        uint32_t at_uid;                        \
        uint32_t at_gid;                        \
        uint32_t at_nlink;                      \
        struct timespec at_atime;                      \
        struct timespec at_mtime;                      \
        struct timespec at_ctime;                      \
        struct timespec at_btime;                      \
        uint64_t at_blocks;                     \
        uint64_t at_size;                       \
        uint8_t status;                        \
        uint8_t repnum;                        \
        uint32_t split;                         \
        uint32_t chknum;                        \
        uint8_t m:5;                            \
        uint8_t plugin:3;                       \
        uint8_t k:5;                            \
        uint8_t tech:3;                         \
        uint64_t __pad__[10];                     

typedef struct {
        chkid_t chkid;
        uint64_t md_version;
        uint8_t status;
        uint32_t repnum;
        uint32_t size;
        uint32_t master;
        diskid_t diskid[0];
} chkinfo_t;

/*size:40*/
typedef struct {
        __MD__
} dir_md_t;

typedef struct {
        __MD__
} fileinfo_t;

typedef struct {
        __MD__
        char name[0];
} symlink_md_t;

typedef struct {
        __MD__
} md_proto_t;

typedef struct {
        crc_t crc;
        fileid_t id;
} namei_disk_t;

#pragma pack()

#if 1
#define MD2STAT(f, y) \
do { \
        (y)->st_mode    = (uint32_t)(f)->at_mode;               \
        (y)->st_dev     = (uint32_t)(f)->fileid.volid;          \
        (y)->st_ino     = (uint64_t)(f)->fileid.id;             \
        (y)->st_nlink   = (uint32_t)(f)->at_nlink;              \
        (y)->st_uid     = (uint32_t)(f)->at_uid;                \
        (y)->st_gid     = (uint32_t)(f)->at_gid;                \
        (y)->st_rdev    = (uint32_t) 0;                         \
        (y)->st_size    = (uint64_t)(f)->at_size;               \
        (y)->st_blksize = (uint32_t)FAKE_BLOCK;                 \
        (y)->st_blocks  = (uint64_t)(f)->at_size / FAKE_BLOCK;  \
        (y)->st_atim   = (f)->at_atime;                         \
        (y)->st_mtim   = (f)->at_mtime;                         \
        (y)->st_ctim   = (f)->at_ctime;                         \
} while (0)

#else
#define MD2STAT(f, y) \
do { \
        (y)->st_mode    = (uint32_t)(f)->at_mode;               \
        (y)->st_dev     = (uint32_t)(f)->fileid.volid;          \
        (y)->st_ino     = (uint64_t)(f)->fileid.id;             \
        (y)->st_nlink   = (uint32_t)(f)->at_nlink;              \
        (y)->st_uid     = (uint32_t)(f)->at_uid;                \
        (y)->st_gid     = (uint32_t)(f)->at_gid;                \
        (y)->st_rdev    = (uint32_t) 0;                         \
        (y)->st_size    = (uint64_t)(f)->at_size;               \
        (y)->st_blksize = (uint32_t)FAKE_BLOCK;                 \
        (y)->st_blocks  = (uint64_t)(f)->at_size / FAKE_BLOCK;  \
        (y)->st_atime   = (uint32_t)(f)->at_atime.tv_sec;       \
        (y)->st_mtime   = (uint32_t)(f)->at_mtime.tv_sec;       \
        (y)->st_ctime   = (uint32_t)(f)->at_ctime.tv_sec;       \
        (y)->st_atime.tv_nsec   = (uint32_t)(f)->at_atime.tv_nsec;  \
        (y)->st_mtime.tv_nsec   = (uint32_t)(f)->at_mtime.tv_nsec;  \
        (y)->st_ctime.tv_nsec   = (uint32_t)(f)->at_ctime.tv_nsec;  \
} while (0)
#endif

#define CHK_SIZE(__repnum__) (sizeof(chkinfo_t) + sizeof(diskid_t) * (__repnum__))

#define MAX_SUB_FILES 99999999

static inline uint32_t _get_chknum(uint64_t length, uint32_t split)
{
        if (length % split)
                return length / split + 1;
        else
                return length / split;

}

#pragma pack(8)
#pragma pack()

#endif /* __YFS_MD_H__ */

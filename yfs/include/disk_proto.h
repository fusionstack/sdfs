#ifndef __DISK_PROTO_H__
#define __DISK_PROTO_H__

#include <stdint.h>

//#include "chk_proto.h"
#include "sdfs_id.h"
#include "dbg.h"

typedef enum {
        DISK_STAT_FREE          = 0,
        DISK_STAT_NORMAL        = 1,
        DISK_STAT_OVERLOAD      = 2,
        DISK_STAT_DEAD          = 3,
} disk_stat_t;

#define YFS_CHK_AVAIL_MIN 300

#define DISKSTAT(t) \
( \
  ((((t)->ds_bsize * (t)->ds_bavail) > (mdsconf.disk_keep)) \
   && ((t)->ds_ffree > YFS_CHK_AVAIL_MIN)) \
? DISK_STAT_NORMAL : DISK_STAT_OVERLOAD)

#define DISKFREE(t) \
( \
  ((((t)->ds_bsize * (t)->ds_bavail) > (mdsconf.disk_keep)) \
   && ((t)->ds_ffree > YFS_CHK_AVAIL_MIN)) ? 1 : 0)

#if 1

typedef nid_t diskid_t;

#else

typedef struct {
        uint64_t id;
        uint32_t version;
        uint32_t dirty;
} diskid_t;

#endif

typedef struct {
        diskid_t    diskid;
        uint32_t    sockaddr;
        uint32_t    sockport;
        disk_stat_t diskstat;
        uint64_t    disktotal;
        uint64_t    diskfree;
        uint64_t    load;
        char        rname[32];
} diskping_stat_t;

/* disk id 0 means no that disk, all disk id begins from 1 */
#define DISKID_NULL 0
#define DISKID_FROM 1

#define DISKVER_NULL 0

typedef struct {
        diskid_t diskid;
} diskinfo_img_file_t;

/* same to struct statvfs (man statvfs) */
typedef struct {
        uint32_t ds_bsize;      /* file system block size */
        uint32_t ds_frsize;     /* fragment size */
        uint64_t ds_blocks;     /* size of fs in ds_frsize units */ 
        uint64_t ds_bfree;      /* free blocks */
        uint64_t ds_bavail;     /* free blocks for non-root */
        uint64_t ds_files;      /* inodes */
        uint64_t ds_ffree;      /* free inodes */
        uint64_t ds_favail;     /* free inodes for non-root */
        uint32_t ds_fsid;       /* file system ID */
        uint32_t ds_flag;       /* mount flags */
        uint32_t ds_namemax;    /* maximum filename length */
        uint32_t __pad__;
} diskinfo_stat_t;

#define DUMP_DISKSTAT(ds) \
do { \
        DINFO(" \
diskinfo stat:\n \
bsize %u, frsize %u, blocks %llu, bfree %llu, bavail %llu,\
files %llu, ffree %llu, favail %llu, fsid %u, flag %u, namemax %u\n", \
        (ds)->ds_bsize,  \
        (ds)->ds_frsize, \
        (LLU)(ds)->ds_blocks, \
        (LLU)(ds)->ds_bfree, \
        (LLU)(ds)->ds_bavail, \
        (LLU)(ds)->ds_files, \
        (LLU)(ds)->ds_ffree, \
        (LLU)(ds)->ds_favail, (ds)->ds_fsid, \
        (ds)->ds_flag, (ds)->ds_namemax); \
} while(0)

#define FSTAT2DISKSTAT(v, d) \
do { \
        (d)->ds_bsize = (v)->f_bsize; \
        (d)->ds_frsize = (v)->f_frsize; \
        (d)->ds_blocks = (v)->f_blocks; \
        (d)->ds_bfree = (v)->f_bfree; \
        (d)->ds_bavail = (v)->f_bavail; \
        (d)->ds_files = (v)->f_files; \
        (d)->ds_ffree = (v)->f_ffree; \
        (d)->ds_favail = (v)->f_favail; \
        (d)->ds_fsid = (v)->f_fsid; \
        (d)->ds_flag = (v)->f_flag; \
        (d)->ds_namemax = (v)->f_namemax; \
} while (0)

#define DISKSTAT2FSTAT(d, v) \
do { \
        (v)->f_bsize = (d)->ds_bsize; \
        (v)->f_frsize = (d)->ds_frsize; \
        (v)->f_blocks = (d)->ds_blocks; \
        (v)->f_bfree = (d)->ds_bfree; \
        (v)->f_bavail = (d)->ds_bavail; \
        (v)->f_files = (d)->ds_files; \
        (v)->f_ffree = (d)->ds_ffree; \
        (v)->f_favail = (d)->ds_favail; \
        (v)->f_fsid = (d)->ds_fsid; \
        (v)->f_flag = (d)->ds_flag; \
        (v)->f_namemax = (d)->ds_namemax; \
} while (0)

#define DUMP_VFSTAT(vs) \
do { \
        DBUG(" \
 vfs stat:\n \
 bsize %lu, frsize %lu, blocks %llu, bfree %llu, bavail %llu,\
 files %llu, ffree %llu, favail %llu,\
 fsid %lu, flag %lu, namemax %lu\n", \
             (vs)->f_bsize, (vs)->f_frsize, (vs)->f_blocks, \
             (vs)->f_bfree, (vs)->f_bavail, (vs)->f_files, \
             (vs)->f_ffree, (vs)->f_favail, (vs)->f_fsid, \
             (vs)->f_flag, (vs)->f_namemax); \
} while(0)

typedef struct {
        uint64_t ds_bsize;
        int64_t ds_bfree;
        int64_t ds_bavail;
        int64_t ds_ffree;
        int64_t ds_favail;
} diskinfo_stat_diff_t;

#define STAT_ISDIFF(df) \
 (((df)->ds_bfree != 0) || ((df)->ds_bavail != 0) || ((df)->ds_ffree != 0) \
  || ((df)->ds_favail != 0))

#define YDISK_BLK_SIZE (1024 * 96)

#endif

#ifndef __YFSCDS_CONF_H__
#define __YFSCDS_CONF_H__

#include "yfs_conf.h"

#if 1
#define YFS_CDS_DIR_LCK_PRE "/dev/shm/sdfs/yfs/cds/lock"
#else
#define YFS_CDS_DIR_LCK_PRE "/sysy/yfs/cds/lock"
#endif

#define CDS_HOTSUM_ROOT "/dev/shm/sdfs/cds"
#define CDS_NETINFO_ROOT "/dev/shm/sdfs/cds"

#define YFS_CDS_LOGFILE "/var/log/yfs_cds_%d.log"

//#define YFS_CDS_DIR_DISK_PRE SDFS_HOME"/cds"
#define YFS_CDS_DIR_MEM_PRE "/dev/shm/sdfs/yfs/cds/chk"
#define YFS_CDS_EXT3_FSID 0xEF53UL /* EXT3_SUPER_MAGIC (man statfs) */

#define YFS_DISKINFO_FILE "diskinfo"
#define YFS_CDS_DIR_JNL_PRE "/jnl"
#define YFS_CDS_WATCHDOG_FILE "watchdog"

#define YFS_LASTSCAN_TIME   "lastscan"
#define YFS_SCAN_INTERVAL   (30 * 24 * 3600)
#define YFS_ROBOT_INTERVAL  (6)

#define CDS_QLEN 256

#define YFS_CDS_VER_MAGIC   0xface0001

#define YFS_CDS_WRITE_UNSTABLE 0

#if 1

#define TIME_BEGIN(__time__)                    \
        struct timeval __begin__##__time__;     \
        gettimeofday(&__begin__##__time__, NULL);

#define TIME_END(__time__)                      \
        struct timeval __end__##__time__;       \
        gettimeofday(&__end__##__time__, NULL);           \
        __time__.count.tv_sec += (__end__##__time__.tv_sec - __begin__##__time__.tv_sec); \
        __time__.count.tv_usec += (__end__##__time__.tv_usec - __begin__##__time__.tv_usec); \
        __time__.count.tv_sec += __time__.count.tv_usec / (1000 * 1000); \
        __time__.count.tv_usec = __time__.count.tv_usec % (1000 * 1000);

#define TIME_DUMP(__time__)                                             \
        DINFO("used %llu.%llu\n",                                       \
              (LLU)__time__.count.tv_sec,  (LLU)__time__.count.tv_usec);

#else

#define TIME_INIT(__time__)
#define TIME_BEGIN(__time__)
#define TIME_END(__time__) 
#define TIME_DUMP(__time__)

#endif

#if 0
#define CHUNK_WRITEBACK
#endif

#endif

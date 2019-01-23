#ifndef __YFSMDS_CONF_H__
#define __YFSMDS_CONF_H__

#include "yfs_conf.h"
#include "job_tracker.h"
#include "yfscli_conf.h"

#define MDS_QLEN 256
#define MDS_EPOLL_SIZE 1024

#define MDS_SHADOW_HOST "mds_shadow"
//#define MDS_SHADOW_SERVICE 10092
#define MDS_SHADOW_QLEN 256

#define MDS_NODEID_PATHLEVEL 5

#define MDS_SHADOW_STOPCHECK_INTERVAL 3

#define YFS_MDS_LOGFILE "/var/log/yfs_mds_%d.log"

#define YFS_MDS_DIR_DISK_PRE "mond"
#define YFS_MDS_DIR_JNL_PRE "jnl"
#define YFS_MDS_DIR_LVM_PRE "lvm"
#define YFS_MDS_DIR_DEL_PRE "deleted"

#if 1
/* seems shm doesn't support flock(2) */

#define YFS_MDS_DIR_LCK_PRE "/dev/shm/uss/yfs/mds/lock"
#else
#define YFS_MDS_DIR_LCK_PRE "/sysy/yfs/mds/lock"
#endif

#define YFS_XATTR_REPLICA "replica"
#define YFS_XATTR_CHKLEN  "chklen"

#endif

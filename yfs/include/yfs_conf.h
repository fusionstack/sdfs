#ifndef __YFS_CONF_H__
#define __YFS_CONF_H__

#include "sdfs_conf.h"

//#define YFS_META_VERSION "apple (2.1.0)\n"
#define YFS_META_VERSION "meta (2017Q3)\n"

#define FAKE_BLOCK 4096

#if 0
#define	__S_IFVOL	0400000	/* volume.  */
#define	__S_IFXATTR	0800000	/* volume.  */
#endif

#define MAX_READDIR_ENTRIES 100
#define MDS_SHADOW_MAX 5
#define YFS_MAX_RETRY 200

#define KEY_MDS_MASTER   "mds_master"
#define KEY_MDS_INFO "mds_info"
#define KEY_MDS_LIST "mds_list"
#define C60_LOCK_TMO 30
#define MDS_RESTART_TMO 5

#define OBJECT_ITAB_SIZE (1024 * 1024 * 1024 * 2LL)

typedef enum {
        DAEMON_MDS,
        DAEMON_CDS,
        DAEMON_ISCSI,
} daemon_type_t;

#define DISK_TIMEOUT 60


#if 1

#define DISK_O_FLAG O_DIRECT

#else

#define DISK_O_FLAG O_DSYNC

#endif

#endif

#ifndef __YFSCLI_CONF_H__
#define __YFSCLI_CONF_H__

#include "yfs_conf.h"

#define YFS_CDC_DIR_MEM_PRE "/dev/shm/sdfs/yfs/cdc"

#define CDC_QLEN 256

#define YFS_XATTR_REPLICA "replica"
#define YFS_XATTR_CHKLEN  "chklen"

#define NR_FILETABLE 8192 * 4

#define CACHE_LIST_LEN 32

#endif

#ifndef __NFS_CONF_H__
#define __NFS_CONF_H__

#include "sdfs_conf.h"
#include "yfscli_conf.h"

typedef enum {
        ACCEPT_STATE_OK, /* 0 */
        ACCEPT_STATE_ERROR, /* 1 */
} acceptstate_t;

#define NFS_PATHLEN_MAX MAX_PATH_LEN

#define FH_CACHE_MAX_ENTRY (NR_FILETABLE * 2)
#define STAT_CACHE_MAX_ENTRY (NR_FILETABLE / 4)
#define FD_CACHE_MAX_ENTRY (NR_FILETABLE / 2)

//need by yproc
#define YNFS_STATE "ynfs"

// stat cache timeout
#define STAT_CACHE_EXPIRE EXPIRED_TIME

#endif

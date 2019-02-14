#include <sys/types.h>
#include <stdint.h>
#include <dirent.h>

#define DBG_SUBSYS S_YNFS

#include "adt.h"
#include "error.h"
#include "readdir.h"
#include "nfs_conf.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "network.h"
#include "yfs_md.h"
#include "dbg.h"

typedef struct {
        int fd;
        uint32_t sessid;
        volid_t volid;
        char path[MAX_PATH_LEN];
        char host[MAX_PATH_LEN];
} nfs_session_t;

int nfs_session_create(int fd, const char *path, const char *host, uint32_t *sessid)
{
        
}

int nfs_session_destroy(int fd, const char *path)
{
        
}

int nfs_session_get(int fd, int sessid, volid_t *volid)
{
        
}

int nfs_session_init()
{
}

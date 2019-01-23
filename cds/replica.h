#ifndef __REPLICA_IO_H__
#define __REPLICA_IO_H__

#include "sdfs_lib.h"
#include "ylib.h"
#include "net_global.h"

static inline void chkid2path(const chkid_t *chkid, char *path)
{
        char cpath[MAX_PATH_LEN];

        (void) cascade_id2path(cpath, MAX_PATH_LEN, chkid->id);

        (void) snprintf(path, MAX_PATH_LEN, "%s/volume/%ju/%ju/%s/%u.chunk",
                        ng.home, chkid->volid, chkid->snapvers, cpath, chkid->idx);

}

int IO_FUNC replica_read(const io_t *io, buffer_t *buf);
int IO_FUNC replica_write(const io_t *io, const buffer_t *buf);
int replica_init();


#endif

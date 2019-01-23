

#include <sys/statvfs.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSMDS

#include "chk_proto.h"
#include "disk_proto.h"
#include "diskpool.h"
#include "node_proto.h"
#include "nodepool.h"
#include "sdfs_id.h"
#include "dbg.h"

int mds_diskdead(const ynet_net_nid_t *nid, uuid_t *nodeid)
{
        int ret;
        (void) nodeid;

        DBUG("mark peer dead "DISKID_FORMAT"\n", DISKID_ARG(nid));

        (void) nodepool_diskdead(nid, TIER_ALL);

        ret = diskpool_dead(nid);
        if (ret)
                goto err_ret;

        return 0;
err_ret:
        return ret;
}

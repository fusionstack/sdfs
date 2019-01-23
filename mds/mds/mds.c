#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSMDS

#include "yfs_conf.h"
#include "file_proto.h"
#include "md_proto.h"
#include "ylib.h"
#include "yfsmds_conf.h"
#include "mds.h"
#include "md.h"
#include "dbg.h"
#include "sdfs_worm.h"
#include "xattr.h"

mds_info_t mds_info;

#pragma pack(8)

typedef struct {
        fileid_t fileid;
        uint32_t __pad__;
        uint32_t volid;
} res_lvcreate_t;

typedef struct {
        chkid_t chkid;
        uint64_t begin;
        uint64_t end;
        ynet_net_nid_t tenant;
        uint32_t rep_count;
        uint32_t __pad__;
        ynet_net_nid_t reps[0];
} res_lease_t;

typedef struct {
        uint32_t count;
        uint32_t __pad__;
        diskid_t diskid[0];
} res_newrep_t;

#pragma pack()

extern disk_stat_t getdiskstat(const diskinfo_stat_t *diskstat);

#if 0
int mds_diskjoin(void *_req, void *_rep, uint32_t *buflen, int *jnl)
{
        int ret;
        const mdp_diskjoin_req_t *req;
        disk_stat_t diskstat;

        req = _req;
        (void) _rep;

        ret = diskpool_join(&req->diskid, &req->stat);
        if (ret) {
                if (ret == EAGAIN)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        diskstat = getdiskstat(&req->stat);

        if (diskstat == DISK_STAT_FREE) {
                ret = nodepool_addisk(&req->nodeid, &req->diskid, req->tier);
                if (ret)
                        GOTO(err_ret, ret);
        }

        *jnl = 0;
        *buflen = 0;

        return 0;
err_ret:
        return ret;
}

int mds_diskget(void *_req, void *_rep, uint32_t *buflen, int *jnl)
{
        int ret, i;
        mdp_diskget_req_t *req;
        char buf[MAX_BUF_LEN];
        net_handle_t *array;
        diskid_t *diskid;

        req = _req;
        (void) _rep;

        DINFO("get disk repnum %u, tier %u\n", req->repnum, req->tier);

        array = (void *)buf;
        ret = nodepool_get(req->repnum, array, req->hardend, req->tier);
        if (ret) {
                GOTO(err_ret, ret);
        }

        diskid = _rep;
        for (i = 0; i < req->repnum; i++) {
                diskid[i] = array[i].u.nid;
        }
        
        DINFO("get disk repnum %u, tier %u ok\n", req->repnum, req->tier);

        *jnl = 0;
        *buflen = sizeof(diskid_t) * req->repnum;

        return 0;
err_ret:
        return ret;
}

int mds_diskhb(void *_req, void *_rep, uint32_t *buflen, int *jnl)
{
        int ret;
        const mdp_hb_req_t *req;
        //mdp_hb_rep_t *rep;

        (void) _rep;
        req = _req;
        //rep = _rep;

        ret = diskpool_hb(&req->diskid, req->tier, &req->diff, req->info.volreptnum,
                          req->info.volrept, &req->nodeid);
        if (ret)
                GOTO(err_ret, ret);

#if 0
        ret = primer_peer(rep->shadow, &rep->count);
        if (ret)
                GOTO(err_ret, ret);
#endif

        *jnl = 0;
        //*buflen = sizeof(*rep) + sizeof(ynet_net_nid_t) * rep->count;
        *buflen = 0;

        return 0;
err_ret:
        return ret;
}

int mds_msgget(void *_req, void *_rep, uint32_t *buflen, int *jnl)
{
        int ret;
        mdp_msg_req_t *req;
        mdp_msg_rep_t *rep;

        req = _req;
        rep = _rep;

        ret = netable_msgget(&req->nid, rep->buf, req->len);
        if (ret < 0) {
                ret = -ret;
                if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        rep->len = ret;
        *buflen = ret + sizeof(*rep);
        *jnl = 0;

        return 0;
err_ret:
        return ret;
}

int mds_msgpop(void *_req, void *_rep, uint32_t *buflen, int *jnl)
{
        int ret;
        mdp_msg_req_t *req;
        mdp_msg_rep_t *rep;

        req = _req;
        rep = _rep;

        ret = netable_msgpop(&req->nid, rep->buf, req->len);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        YASSERT(ret <= (int)req->len);

        rep->len = ret;
        *buflen = ret + sizeof(*rep);
        *jnl = 1;

        return 0;
err_ret:
        return ret;
}

#endif

#ifndef __NET_GLOBAL_H__
#define __NET_GLOBAL_H__

#include <uuid/uuid.h>
#include "sdevent.h"
#include "sdfs_conf.h"
#include "ylib.h"

#define YNET_PORT_NULL ((uint32_t)-1)


typedef enum {
    ROLE_NULL = 0,
    ROLE_MDS,
    ROLE_MAX,
} role_t;

typedef struct {
        int inited;
        net_proto_t op;
        net_handle_t mds_nh;
        net_handle_t pasv_nh;
        ynet_net_nid_t local_nid;

        net_handle_t master;
        uuid_t nodeid;
        char name[MAX_PATH_LEN];
        char home[MAX_PATH_LEN];
        uint32_t seq; /*local seq*/
        uint32_t port;
        int live;
        uint32_t uptime;
        uint32_t xmitbuf;
        time_t info_time;
        char info_local[MAX_INFO_LEN];
        uint64_t nid_sequence;
        int daemon;
        role_t role;
        uint32_t master_magic;
} net_global_t;

/*init in net_lib.c*/

extern net_global_t ng;

static inline int net_isnull(const nid_t *nid)
{
        if (nid == NULL)
            return 1;

        if (nid->id == 0)
                return 1;
        else
                return 0;
}

static inline const nid_t *net_getnid()
{
        if (net_isnull(&ng.local_nid))
                DBUG("nid is null\n");
        return &ng.local_nid;
}

static inline void net_setnid(const nid_t *nid)
{
        ng.local_nid = *nid;
}

static inline const nid_t *net_getadmin()
{
        return &ng.master.u.nid;
}

static inline int net_getadmin1(nid_t *nid)
{
        int ret;

        if (net_isnull(&ng.master.u.nid)) {
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }

        *nid = ng.master.u.nid;

        return 0;
err_ret:
        return ret;
}

static inline void net_setadmin(const nid_t *nid)
{
        DBUG("set master as "NID_FORMAT"\n", NID_ARG(nid));

        id2nh(&ng.master, nid);
        ng.mds_nh = ng.master;
}

static inline int net_islocal(const nid_t *nid)
{
        if (net_isnull(nid))
                return 0;

        if (nid_cmp(nid, net_getnid()) == 0)
                return 1;
        else
                return 0;
}

//todo 用net_isnull 替代
static inline int is_null(const ynet_net_nid_t *nid)
{
        return net_isnull(nid);
}

//todo 用net_islocal 替代
static inline int is_local(const ynet_net_nid_t *nid)
{
        return net_islocal(nid);
}

static inline uint64_t net_getnodeid(void)
{
        return ng.local_nid.id;
}
#endif

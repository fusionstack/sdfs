#include <sys/types.h>
#include <string.h>
#include <stdint.h>
#include <dirent.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSMDS

#include "dir.h"
#include "net_global.h"
#include "redis.h"
#include "md.h"
#include "md_db.h"
#include "dbg.h"

#define CHKINFO_SIZE(__repnum__) (sizeof(chkinfo_t) + sizeof(diskid_t) * __repnum__)

static int __chunk_create(const chkinfo_t *chkinfo)
{
        int ret;
        const chkid_t *chkid = &chkinfo->chkid;
        fileid_t fileid;
        char key[MAX_NAME_LEN];

        cid2fid(&fileid, chkid);

        snprintf(key, MAX_NAME_LEN, "%u", chkid->idx);
        ret = hset(&fileid, key, chkinfo, CHKINFO_SIZE(chkinfo->repnum), O_EXCL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __chunk_load(const chkid_t *chkid, chkinfo_t *chkinfo)
{
        int ret;
        size_t len;
        fileid_t fileid;
        char key[MAX_NAME_LEN];

        cid2fid(&fileid, chkid);

        snprintf(key, MAX_NAME_LEN, "%u", chkid->idx);
        len = CHKINFO_SIZE(YFS_CHK_REP_MAX);
        ret = pipeline_hget(&fileid, key, (char *)chkinfo, &len);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __chunk_update(const chkinfo_t *chkinfo)
{
        int ret;
        const chkid_t *chkid = &chkinfo->chkid;
        fileid_t fileid;
        char key[MAX_NAME_LEN];

        cid2fid(&fileid, chkid);

        snprintf(key, MAX_NAME_LEN, "%u", chkid->idx);
        ret = hset(&fileid, key, chkinfo, CHKINFO_SIZE(chkinfo->repnum), 0);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

chunkop_t __chunkop__ = {
        .create = __chunk_create,
        .load = __chunk_load,
        .update = __chunk_update,
};

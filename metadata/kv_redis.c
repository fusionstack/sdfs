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


static int __kv_get(root_type_t type, const char *key, void *value, size_t *len)
{
        int ret;
        fileid_t fileid;

        fileid = *md_root_getid(type);

        ret = hget(NULL, &fileid, key, value, len);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __kv_create(root_type_t type, const char *key, const void *value, size_t len)
{
        int ret;
        fileid_t fileid;

        fileid = *md_root_getid(type);

        ret = hset(NULL, &fileid, key, value, len, O_EXCL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}


static int __kv_update(root_type_t type, const char *key, const void *value, size_t len)
{
        int ret;
        fileid_t fileid;

        fileid = *md_root_getid(type);

        ret = hset(NULL, &fileid, key, value, len, 0);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __kv_remove(root_type_t type, const char *key)
{
        int ret;
        fileid_t fileid;

        fileid = *md_root_getid(type);

        ret = hdel(NULL, &fileid, key);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static redisReply *__kv_scan(root_type_t type, const char *match, uint64_t offset)
{
        fileid_t fileid;

        fileid = *md_root_getid(type);

        return hscan(NULL, &fileid, match, offset, -1);
}

static int __kv_iter(root_type_t type, const char *match, func2_t func, void *ctx)
{
        int ret;
        fileid_t fileid;

        fileid = *md_root_getid(type);

        ret = hiter(NULL, &fileid, match, func, ctx);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __kv_lock(root_type_t type)
{
        int ret;
        fileid_t fileid;

        fileid = *md_root_getid(type);

        ret = klock(NULL, &fileid, 10, 1);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __kv_unlock(root_type_t type)
{
        int ret;
        fileid_t fileid;

        fileid = *md_root_getid(type);

        ret = kunlock(NULL, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}


kvop_t __kvop__ = {
        .create = __kv_create,
        .get = __kv_get,
        .update = __kv_update,
        .lock = __kv_lock,
        .unlock = __kv_unlock,
        .remove = __kv_remove,
        .scan = __kv_scan,
        .iter = __kv_iter,
};

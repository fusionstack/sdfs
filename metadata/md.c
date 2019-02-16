#include <sys/types.h>
#include <string.h>

#define DBG_SUBSYS S_YFSMDC

#include "net_global.h"
#include "job_dock.h"
#include "ynet_rpc.h"
#include "ylib.h"
#include "redis.h"
#include "etcd.h"
#include "md_proto.h"
#include "md_lib.h"
#include "dbg.h"

typedef struct {
        sy_rwlock_t lock;
        uint64_t begin;
        uint64_t cur;
        uint64_t end;
} mdid_t;

static mdid_t *__mdid__ = NULL;

#define MDID_STEP 100000
#define SYSTEMID "systemid"

static int __md_newid(mdid_t *mdid, fidtype_t type)
{
        int ret, idx;
        char key[MAX_PATH_LEN], value[MAX_BUF_LEN];
        uint64_t begin, end;
        int step = (ng.daemon ? MDID_STEP : 1);

        (void) mdid;

retry:
        snprintf(key, MAX_NAME_LEN, "%d", type);
        ret = etcd_get_text(SYSTEMID, key, value, &idx);
        if(ret) {
                if (ret == ENOKEY) {
                        begin = 1;
                        end = begin + step;
                        snprintf(value, MAX_NAME_LEN, "%ju", end);
                        
                        DINFO("try to create %s %s\n", key, value);
                        ret = etcd_create_text(SYSTEMID, key, value, -1);
                        if (ret) {
                                if (ret == EEXIST) {
                                        DWARN("%s created by other\n", key);
                                        goto retry;
                                } else {
                                        GOTO(err_ret, ret);
                                }
                                        
                        }

                        goto out;
                } else {
                        GOTO(err_ret, ret);
                }
        }

        begin = atoll(value);
        end = begin + step;
        snprintf(value, MAX_NAME_LEN, "%ju", end);

        DINFO("try to set %s %s\n", key, value);
        ret = etcd_update_text(SYSTEMID, key, value, &idx, 0);
        if(ret) {
                if (ret == EEXIST) {
                        DWARN("%s update by other\n", key);
                        goto retry;
                } else {
                        GOTO(err_ret, ret);
                }
        }
        
out:
        mdid->begin = begin;
        mdid->end = end;
        mdid->cur = begin;
        
        return 0;
err_ret:
        return ret;
}

int md_newid(fidtype_t type, uint64_t *id)
{
        int ret;
        mdid_t *mdid;

        YASSERT(type < idtype_max);
        YASSERT(__mdid__);

        mdid = &__mdid__[type];

        ret = sy_rwlock_wrlock(&mdid->lock);
        if(ret)
                GOTO(err_ret, ret);

        if (mdid->cur >= mdid->end) {
                ret = __md_newid(mdid, type);
                if(ret)
                        GOTO(err_lock, ret);
        }

        YASSERT(mdid->cur < mdid->end);
        *id = mdid->cur;
        mdid->cur++;
        
        sy_rwlock_unlock(&mdid->lock);

        return 0;
err_lock:
        sy_rwlock_unlock(&mdid->lock);
err_ret:
        return ret;
}

int md_init()
{
        int ret, i;
        mdid_t *array, *mdid;

        YASSERT(__mdid__ == NULL);
        
        ret = ymalloc((void **)&array, sizeof(*array) * idtype_max);
        if(ret)
                GOTO(err_ret, ret);

        for (i = 0; i < idtype_max; i++) {
                mdid = &array[i];
                
                ret = sy_rwlock_init(&mdid->lock, "mdid");
                if(ret)
                        GOTO(err_ret, ret);

                mdid->begin = 0;
                mdid->end = 0;
                mdid->cur = 0;
        }

        __mdid__ = array;

        return 0;
err_ret:
        return ret;
}

#define DBG_SUBSYS S_YFSLIB

#include "chk_proto.h"
#include "etcd.h"
#include "redis_util.h"
#include "redis_conn.h"
#include "redis.h"
#include "configure.h"
#include "dbg.h"
#include "adt.h"
#include "net_global.h"
#include "schedule.h"
#include "cJSON.h"
#include "sdfs_conf.h"
#include "math.h"

typedef struct {
        sy_rwlock_t lock;
        hashtable_t tab;
} vol_tab_t;

static vol_tab_t *__vol_tab__;

typedef struct {
        uint64_t volid;
        void *vol;
} entry_t;

#if 1
static uint32_t __key (const void *_key)
{
        return *(uint64_t *)_key;
}

static int __cmp(const void *_v1, const void *_v2)
{
        const entry_t *ent = (entry_t *)_v1;
        uint64_t v1, v2;

        v1 = ent->volid;
        v2 = *(uint64_t *)_v2;

        //DINFO("%d --> %d\n", v1, v2);

        return v1 - v2;
}
#endif

int redis_vol_init()
{
        int ret;
        vol_tab_t *vol_tab;

        ret = ymalloc((void **)&vol_tab, sizeof(*vol_tab));
        if(ret)
                GOTO(err_ret, ret);

        ret = sy_rwlock_init(&vol_tab->lock, "vol_tab");
        if(ret)
                GOTO(err_ret, ret);

        vol_tab->tab = hash_create_table(__cmp, __key, "vol_tab");
        if(ret)
                GOTO(err_ret, ret);

        __vol_tab__ = vol_tab;
        
        return 0;
err_ret:
        return ret;
}

int redis_vol_get(uint64_t volid, void **conn)
{
        int ret;
        entry_t *ent;

        ret = sy_rwlock_rdlock(&__vol_tab__->lock);
        if(ret)
                GOTO(err_ret, ret);
        
        ent = hash_table_find(__vol_tab__->tab, &volid);
        if (ent == NULL) {
                ret = ENOENT;
                GOTO(err_lock, ret);
        }

        *conn = ent->vol;

        sy_rwlock_unlock(&__vol_tab__->lock);
        
        return 0;
err_lock:
        sy_rwlock_unlock(&__vol_tab__->lock);
err_ret:
        return ret;
}

int redis_vol_release(uint64_t volid)
{
        (void) volid;
        return 0;
}

int redis_vol_insert(uint64_t volid, void *conn)
{
        int ret;
        entry_t *ent;

        ret = sy_rwlock_wrlock(&__vol_tab__->lock);
        if(ret)
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&ent, sizeof(*ent));
        if(ret)
                GOTO(err_lock, ret);

        ent->vol = conn;
        ent->volid = volid;
        
        ret = hash_table_insert(__vol_tab__->tab, ent, &volid, 0);
        if (ret) {
                GOTO(err_free, ret);
        }

        sy_rwlock_unlock(&__vol_tab__->lock);
        
        return 0;
err_free:
        yfree((void **)&ent);
err_lock:
        sy_rwlock_unlock(&__vol_tab__->lock);
err_ret:
        return ret;
}

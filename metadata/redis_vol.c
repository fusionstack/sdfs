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
#include "variable.h"
#include "cJSON.h"
#include "sdfs_conf.h"
#include "math.h"

typedef struct {
        pthread_rwlock_t lock;
        hashtable_t tab;
} vol_tab_t;

static vol_tab_t *__vol_tab__;
static __thread vol_tab_t *__vol_tab_private__;

typedef struct {
        volid_t volid;
        void *vol;
} entry_t;

static vol_tab_t *__redis_vol_self()
{
        return __vol_tab_private__;
}

#if 1
static uint32_t __key (const void *_key)
{
        return ((volid_t *)_key)->volid;
}

static int __cmp(const void *_v1, const void *_v2)
{
        const entry_t *ent = (entry_t *)_v1;
        const volid_t *v1, *v2;

        v1 = &ent->volid;
        v2 = (volid_t *)_v2;

        //DINFO("%d --> %d\n", v1, v2);

        uint64_t r = v1->volid - v2->volid;
        if (r)
                return r;
        else
                return v1->snapvers - v2->snapvers;
}
#endif

int __redis_vol_init(vol_tab_t **_vol_tab)
{
        int ret;
        vol_tab_t *vol_tab;

        ret = ymalloc((void **)&vol_tab, sizeof(*vol_tab));
        if(ret)
                GOTO(err_ret, ret);

        ret = pthread_rwlock_init(&vol_tab->lock, NULL);
        if(ret)
                GOTO(err_ret, ret);

        vol_tab->tab = hash_create_table(__cmp, __key, "vol_tab");
        if(ret)
                GOTO(err_ret, ret);

        *_vol_tab = vol_tab;
        
        return 0;
err_ret:
        return ret;
}

int redis_vol_init()
{
        int ret;

        ret = __redis_vol_init(&__vol_tab__);
        if(ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int redis_vol_private_init()
{
        int ret;
        vol_tab_t *vol_tab;

        ret = __redis_vol_init(&vol_tab);
        if(ret)
                GOTO(err_ret, ret);

        __vol_tab_private__ = vol_tab;
        
        return 0;
err_ret:
        return ret;
}

void __redis_vol_private_destroy(void *arg1, void *arg2)
{
        entry_t *ent = arg1;
        func_t func = arg2;

        func(ent->vol);
        yfree((void **)&ent);
}

void redis_vol_private_destroy(func_t func)
{
        int ret;
        vol_tab_t *vol_tab_private = __redis_vol_self();
        vol_tab_t  *vol_tab = vol_tab_private;

        ret = pthread_rwlock_wrlock(&vol_tab->lock);
        if(ret)
                UNIMPLEMENTED(__WARN__);

        hash_destroy_table(vol_tab->tab, __redis_vol_private_destroy, func);

        pthread_rwlock_unlock(&vol_tab->lock);

        __vol_tab_private__ = NULL;
}

int redis_vol_get(const volid_t *volid, void **conn)
{
        int ret;
        entry_t *ent;
        vol_tab_t *vol_tab_private = __redis_vol_self();
        vol_tab_t  *vol_tab = vol_tab_private ? vol_tab_private : __vol_tab__;

        DBUG("private table %p\n", vol_tab_private);
        
        //YASSERT(!schedule_running());
        
        ret = pthread_rwlock_rdlock(&vol_tab->lock);
        if(ret)
                GOTO(err_ret, ret);
        
        ent = hash_table_find(vol_tab->tab, (void *)volid);
        if (ent == NULL) {
                ret = ENOENT;
                GOTO(err_lock, ret);
        }

        *conn = ent->vol;

        pthread_rwlock_unlock(&vol_tab->lock);
        
        return 0;
err_lock:
        pthread_rwlock_unlock(&vol_tab->lock);
err_ret:
        return ret;
}

int redis_vol_release(const volid_t *volid)
{
        (void) volid;
        return 0;
}

int redis_vol_insert(const volid_t *volid, void *conn)
{
        int ret;
        entry_t *ent;
        vol_tab_t *vol_tab_private = __redis_vol_self();
        vol_tab_t  *vol_tab = vol_tab_private ? vol_tab_private : __vol_tab__;

        ret = pthread_rwlock_wrlock(&vol_tab->lock);
        if(ret)
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&ent, sizeof(*ent));
        if(ret)
                GOTO(err_lock, ret);

        ent->vol = conn;
        ent->volid = *volid;
        
        ret = hash_table_insert(vol_tab->tab, ent, (void *)volid, 0);
        if (ret) {
                GOTO(err_free, ret);
        }

        pthread_rwlock_unlock(&vol_tab->lock);
        
        return 0;
err_free:
        yfree((void **)&ent);
err_lock:
        pthread_rwlock_unlock(&vol_tab->lock);
err_ret:
        return ret;
}

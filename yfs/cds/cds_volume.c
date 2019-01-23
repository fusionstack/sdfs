

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/poll.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSCDS

#include "yfs_conf.h"
#include "net_global.h"
#include "yfscds_conf.h"
#include "chk_meta.h"
#include "cds_volume.h"
#include "disk.h"
#include "md_proto.h"
#include "jnl_proto.h"
#include "cds_hb.h"
#include "dbg.h"

static hashtable_t volume_table = NULL;
static sy_rwlock_t volume_table_rwlock;
static uint32_t volume_count;

typedef struct {
        volrept_t rept;
        sy_spinlock_t lock;
} entry_t;

static int __cds_volume_update(const void *arg, int len, int64_t offset, void *arg1)
{
        int ret;
        const chkjnl_t *j = arg;
        (void) arg1;
        (void) len;
        (void) offset;

        DBUG("vol %llu increase %d\n", (LLU)j->chkid.volid, j->increase);
        
        ret = cds_volume_update(j->chkid.volid, j->increase);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __cds_volume_load()
{
        int ret;
        jnl_handle_t jnl;

        snprintf(jnl.home, MAX_PATH_LEN, "%s/chkinfo/%s",
                 ng.home, YFS_CDS_DIR_JNL_PRE);

        //do nothing
        goto out;

        ret = jnl_open(jnl.home, &jnl, 0);
        if (ret) {
                if (ret == ENOENT)
                        goto out;
                else
                        GOTO(err_ret, ret);
        }

        ret = jnl_iterator(&jnl, 0, __cds_volume_update, NULL);
        if (ret)
                GOTO(err_jnl, ret);

        (void) jnl_close(&jnl);

out:
        return 0;
err_jnl:
        (void) jnl_close(&jnl);
err_ret:
        return ret;
}

static uint32_t __key(const void *args)
{
        return *(uint32_t *)args;
}

static int __cmp(const void *v1, const void *v2)
{
        const entry_t *ent1 = (entry_t *)v1;

        if (ent1->rept.volid == *(uint32_t *)v2)
                return 0;
        else
                return 1;
}

int cds_volume_init()
{
        int ret;

        volume_table = hash_create_table(__cmp, __key, "cds vol");
        if (volume_table == NULL) {
                ret = ENOMEM;
                DERROR("ret (%d) %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        ret = sy_rwlock_init(&volume_table_rwlock, NULL);
        if (ret)
                GOTO(err_ret, ret);

        volume_count = 0;

        ret = __cds_volume_load();
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int cds_volume_update(uint32_t volid, int size)
{
        int ret;
        entry_t *ent;

retry:
        ret = sy_rwlock_rdlock(&volume_table_rwlock);
        if (ret)
                GOTO(err_ret, ret);

        ent = hash_table_find(volume_table, &volid);
        if (ent == NULL) {
                sy_rwlock_unlock(&volume_table_rwlock);

                ret = ymalloc((void **)&ent, sizeof(entry_t));
                if (ret)
                        GOTO(err_ret, ret);

                sy_spin_init(&ent->lock);
                ent->rept.volid = volid;
                ent->rept.size = size;

                ret = sy_rwlock_wrlock(&volume_table_rwlock);
                if (ret)
                        GOTO(err_free, ret);

                ret = hash_table_insert(volume_table, (void *)ent,
                                        (void *)&ent->rept.volid, 0);
                if (ret) {
                        if (ret == EEXIST) {
                                sy_rwlock_unlock(&volume_table_rwlock);
                                yfree((void **)&ent);
                                goto retry;
                        } else
                                GOTO(err_free, ret);
                }

                volume_count ++;
        } else {
                sy_spin_lock(&ent->lock);

                ent->rept.size += size;

                sy_spin_unlock(&ent->lock);
        }

        sy_rwlock_unlock(&volume_table_rwlock);

        return 0;
err_free:
        yfree((void **)&ent);
err_ret:
        return ret;
}

static void __getstat(void *arg, void *_ent)
{
        volinfo_t *info = arg;
        entry_t *ent = _ent;

        info->volrept[info->volreptnum] = ent->rept;
        info->volreptnum ++;

        DBUG("volume %u size %llu\n", (uint32_t)ent->rept.volid, (LLU)ent->rept.size);
}

int cds_volume_get(volinfo_t **_info)
{
        int ret;
        volinfo_t *info;

        ret = sy_rwlock_wrlock(&volume_table_rwlock);
        if (ret)
                GOTO(err_ret, ret);

        DBUG("volume %u\n", volume_count);

        ret = ymalloc((void **)&info, sizeof(volinfo_t) + sizeof(volrept_t) * volume_count);
        if (ret)
                GOTO(err_lock, ret);

        memset(info, 0x0, sizeof(volinfo_t) + sizeof(volrept_t) * volume_count);

        hash_iterate_table_entries(volume_table, __getstat, info);

        YASSERT(info->volreptnum == volume_count);

        sy_rwlock_unlock(&volume_table_rwlock);

        *_info = info;

        return 0;
err_lock:
        sy_rwlock_unlock(&volume_table_rwlock);
err_ret:
        return ret;
}

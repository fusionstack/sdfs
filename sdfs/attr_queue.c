#include <unistd.h>
#include <string.h>
#include <sys/eventfd.h>
#include <errno.h>
#include <dirent.h>

#define DBG_SUBSYS S_YFSLIB

#include "ylib.h"
#include "net_global.h"
#include "network.h"
#include "core.h"
#include "plock.h"
#include "main_loop.h"
#include "schedule.h"
#include "md_lib.h"
#include "io_analysis.h"
#include "attr_queue.h"
#include "xattr.h"
#include "dbg.h"

#define ATTR_OP_EXTERN    0x00001
#define ATTR_OP_SETTIME   0x00002
#define ATTR_OP_TRUNCATE  0x00004

#define ATTR_QUEUE_TMO 5

typedef struct {
        struct list_head hook;
        fileid_t fileid;
        volid_t volid;
        int running;
        __set_size size;
        __set_time atime;
        __set_time btime;
        __set_time ctime;
        __set_time mtime;
        struct list_head wait_list;
} entry_t;

typedef struct {
        plock_t plock;
        time_t update;
        hashtable_t tab;
        struct list_head list;
        int count;
} attr_queue_t;

typedef struct {
        struct list_head hook;
        task_t task;
} wait_t;
        

static void __attr_queue_update(entry_t *ent, int op, const void *arg)
{
        if (op == ATTR_OP_EXTERN) {
                const uint64_t *size = arg;
                if (ent->size.set_it == __SET_EXTERN) {
                        ent->size.size = ent->size.size > *size
                                ? ent->size.size : *size;
                        ent->size.set_it = __SET_EXTERN;
                } else if (ent->size.set_it == __NOT_SET_SIZE) {
                        ent->size.size = *size;
                        ent->size.set_it = __SET_EXTERN;
                } else {
                        YASSERT(ent->size.set_it = __SET_TRUNCATE);
                        ent->size.size = ent->size.size > *size ? ent->size.size : *size;
                }

        } else if (op == ATTR_OP_TRUNCATE) {
                const uint64_t *size = arg;
                ent->size.size = *size;
                ent->size.set_it = __SET_TRUNCATE;
                DBUG("----truncate "CHKID_FORMAT" %u--\n",
                      CHKID_ARG(&ent->fileid), ent->size.size);
        } else if (op == ATTR_OP_SETTIME) {
                const setattr_t *setattr = arg;
                ent->atime = setattr->atime;
                ent->btime = setattr->btime;
                ent->ctime = setattr->ctime;
                ent->mtime = setattr->mtime;
        } else {
                UNIMPLEMENTED(__DUMP__);
        }

        return;
}

static int __attr_queue_create(attr_queue_t *attr_queue, const volid_t *volid,
                               const fileid_t *fileid, int op, const void *arg)
{
        int ret;
        entry_t *ent;

        ret = ymalloc((void **)&ent, sizeof(*ent));
        if (ret)
                GOTO(err_ret, ret);

        memset(ent, 0x0, sizeof(*ent));
        INIT_LIST_HEAD(&ent->wait_list);

        ent->volid = *volid;
        ent->fileid = *fileid;
        YASSERT(fileid->type);
        
        __attr_queue_update(ent, op, arg);

        DBUG("add "CHKID_FORMAT"\n", CHKID_ARG(&ent->fileid));

        ret = hash_table_insert(attr_queue->tab, (void *)ent, (void *)&ent->fileid, 0);
        if (ret)
                UNIMPLEMENTED(__DUMP__);
        
        list_add_tail(&ent->hook, &attr_queue->list);
        attr_queue->count++;

        return 0;
err_ret:
        return ret;
}

static void __attr_queue_remove(attr_queue_t *attr_queue, entry_t *ent)
{
        int ret;
        struct list_head *pos, *n;
        wait_t *wait;
        entry_t *tmp;

        DBUG("remove "CHKID_FORMAT" %p\n", CHKID_ARG(&ent->fileid), ent);
        
        ret = hash_table_remove(attr_queue->tab, (void *)&ent->fileid, (void **)&tmp);
        YASSERT(ret == 0);

        list_del(&ent->hook);
        attr_queue->count--;

        list_for_each_safe(pos, n, &ent->wait_list) {
                list_del(pos);
                wait = (void *)pos;
                schedule_resume(&wait->task, 0, NULL);
        }

        yfree((void **)&ent);
}

static int __attr_wait(entry_t *ent)
{
        int ret;
        wait_t wait;
        
        wait.task = schedule_task_get();
        list_add_tail(&wait.hook, &ent->wait_list);

        ret = schedule_yield1("attr_queue_wait", NULL, NULL, NULL, -1);
        if (ret) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __attr_queue(const volid_t *volid, const fileid_t *fileid, int op, const void *arg)
{
        int ret, retry = 0;
        attr_queue_t *attr_queue = variable_get(VARIABLE_ATTR_QUEUE);
        entry_t *ent;

        if (attr_queue == NULL) {
                return 0;
        }
        
        DBUG("queue "CHKID_FORMAT"\n", CHKID_ARG(fileid));

        if (unlikely(volid == NULL)) {
                //UNIMPLEMENTED(__WARN__);
                volid_t _volid = {fileid->volid, 0};
                volid = &_volid;
        }

retry:
        ent = hash_table_find(attr_queue->tab, (void *)fileid);
        if (ent) {
                if (ent->running) {
                        ret = __attr_wait(ent);
                        if (ret)
                                GOTO(err_ret, ret);

                        DWARN("queue "CHKID_FORMAT", retry %u\n", CHKID_ARG(fileid), retry);
                        retry++;

                        goto retry;
                } else {
                        __attr_queue_update(ent, op, arg);
                }
        } else {
                ret = __attr_queue_create(attr_queue, volid, fileid, op, arg);
                if (ret)
                        GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}

int attr_queue_settime(const volid_t *volid, const fileid_t *fileid, const void *setattr)
{
        DBUG("set "CHKID_FORMAT"\n", CHKID_ARG(fileid));
        return __attr_queue(volid, fileid, ATTR_OP_SETTIME, setattr);
}

int attr_queue_extern(const volid_t *volid, const fileid_t *fileid, uint64_t size)
{
        DBUG("set "CHKID_FORMAT"\n", CHKID_ARG(fileid));
        return __attr_queue(volid, fileid, ATTR_OP_EXTERN, &size);
}

int attr_queue_truncate(const volid_t *volid, const fileid_t *fileid, uint64_t size)
{
        DBUG("set "CHKID_FORMAT"\n", CHKID_ARG(fileid));
        return __attr_queue(volid, fileid, ATTR_OP_TRUNCATE, &size);
}

int attr_queue_update(const volid_t *volid, const fileid_t *fileid, void *_md)
{
        attr_queue_t *attr_queue = variable_get(VARIABLE_ATTR_QUEUE);
        entry_t *ent;
        md_proto_t *md = _md;

        if (attr_queue == NULL) {
                return 0;
        }
        
        (void) volid;
        
        ent = hash_table_find(attr_queue->tab, (void *)fileid);
        if (ent == NULL) {
                return 0;
        }

        DBUG("update "CHKID_FORMAT"\n", CHKID_ARG(fileid));

        if (ent->size.set_it == __SET_EXTERN) {
                md->at_size = (md->at_size > ent->size.size)
                        ? md->at_size : ent->size.size;
                DBUG("update "CHKID_FORMAT" size\n", CHKID_ARG(&ent->fileid));
        } else if (ent->size.set_it == __SET_TRUNCATE) {
                md->at_size = ent->size.size;
        }

        setattr_t setattr;
        setattr_init(&setattr, -1, -1, NULL, -1, -1, -1);
        setattr.atime = ent->atime;
        setattr.btime = ent->btime;
        setattr.ctime = ent->ctime;
        setattr.mtime = ent->mtime;
        md_attr_update(md, &setattr);

        return 0;
}

static void __attr_queue_run__(entry_t *ent)
{
        int ret, retry = 0;
        setattr_t setattr;

        setattr_init(&setattr, -1, -1, NULL, -1, -1, -1);
        
        if (ent->size.set_it) {
                setattr.size = ent->size;
        }

        if (ent->atime.set_it != __DONT_CHANGE
            || ent->btime.set_it != __DONT_CHANGE
            || ent->ctime.set_it != __DONT_CHANGE
            || ent->mtime.set_it != __DONT_CHANGE) {
                setattr.atime = ent->atime;
                setattr.btime = ent->btime;
                setattr.ctime = ent->ctime;
                setattr.mtime = ent->mtime;
        }

retry:
        ret = md_setattr(&ent->volid, &ent->fileid, &setattr, 1);
        
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }
        
        return;
err_ret:
        DWARN("set "CHKID_FORMAT" fail\n", CHKID_ARG(&ent->fileid));
        return;
}

static void __attr_queue_run_task(void *var)
{
        int ret;
        entry_t *ent;
        attr_queue_t *attr_queue = variable_get_byctx(var, VARIABLE_ATTR_QUEUE);

        ret = plock_trywrlock(&attr_queue->plock);
        if (ret) {
                if (ret == EBUSY) {
                        return;
                } else {
                        UNIMPLEMENTED(__DUMP__);
                }
        }
        
        while (!list_empty(&attr_queue->list)) {
                ent = (void *)attr_queue->list.next;
                YASSERT(ent->running == 0);
                ent->running = 1;
                __attr_queue_run__(ent);
                
                __attr_queue_remove(attr_queue, ent);
        }

        plock_unlock(&attr_queue->plock);

}

void attr_queue_run(void *var)
{
        attr_queue_t *attr_queue = variable_get_byctx(var, VARIABLE_ATTR_QUEUE);
        time_t time = gettime();

        if (attr_queue == NULL) {
                return;
        }
        
        if (time - attr_queue->update < ATTR_QUEUE_TMO) {
                return;
        }
        
        if (list_empty(&attr_queue->list)) {
                return;
        }

        DINFO("attr queue count %u\n", attr_queue->count);
        attr_queue->update = time;
        schedule_task_new("attr_queue_run", __attr_queue_run_task, var, -1);
        schedule_run(variable_get_byctx(var, VARIABLE_SCHEDULE));
        
        return;
}

static uint32_t __key(const void *args)
{
        return ((fileid_t *)args)->id;
}

static int __cmp(const void *v1, const void *v2)
{
        const entry_t *ent = (entry_t *)v1;
        const fileid_t *fileid = v2;

        DBUG("cmp "CHKID_FORMAT" "CHKID_FORMAT"\n",
              CHKID_ARG(&ent->fileid), CHKID_ARG(fileid));
        
        return chkid_cmp(&ent->fileid, fileid);
}

static int __attr_queue_init(attr_queue_t *attr_queue)
{
        int ret;

        attr_queue->tab = hash_create_table(__cmp, __key, "cds vol");
        if (attr_queue->tab == NULL) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        ret = plock_init(&attr_queue->plock, "attr queue");
        if (ret)
                GOTO(err_ret, ret);
        
        INIT_LIST_HEAD(&attr_queue->list);
        attr_queue->update = 0;
        attr_queue->count = 0;
        
        return 0;
err_ret:
        return ret;
}

int attr_queue_init()
{
        int ret;
        attr_queue_t *attr_queue;

        ret = ymalloc((void **)&attr_queue, sizeof(*attr_queue));
        if (ret)
                GOTO(err_ret, ret);
        
        ret = __attr_queue_init(attr_queue);
        if (ret)
                GOTO(err_ret, ret);

        variable_set(VARIABLE_ATTR_QUEUE, attr_queue);
        
        return 0;
err_ret:
        return ret;
}

int attr_queue_destroy()
{
        int ret;
        attr_queue_t *attr_queue = variable_get_byctx(NULL, VARIABLE_ATTR_QUEUE);

        schedule_task_new("attr_queue_run", __attr_queue_run_task, NULL, -1);
        schedule_run(NULL);

        if (!list_empty(&attr_queue->list)) {
                ret = EBUSY;
                GOTO(err_ret, ret);
        }

        hash_destroy_table(attr_queue->tab, NULL, NULL);
        attr_queue->tab = NULL;
        yfree((void **)&attr_queue);
        
        variable_unset(VARIABLE_ATTR_QUEUE);
        
        return 0;
err_ret:
        return ret;
}

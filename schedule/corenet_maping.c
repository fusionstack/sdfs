

#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBSCHEDULE

#include "configure.h"
#include "net_table.h"
#include "net_global.h"
#include "timer.h"
#include "corenet.h"
#include "corenet_maping.h"
#include "corenet_connect.h"
#include "variable.h"
#include "dbg.h"

extern int nofile_max;

static  corenet_maping_t *__corenet_maping_get()
{
        return variable_get(VARIABLE_MAPING);
}

typedef struct {
        nid_t nid;
        sockid_t sockid;
        int res;
} arg_t;

typedef struct {
        struct list_head hook;
        nid_t nid;
        task_t task;
} wait_t;

STATIC void __corenet_maping_close_finally__(const sockid_t *sockid);

int corenet_maping_loading(const nid_t *nid)
{
        int ret;
        corenet_maping_t *maping, *entry;

        maping = __corenet_maping_get();

        entry = &maping[nid->id];
        ret = sy_spin_lock(&entry->lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        entry->loading = 1;

        sy_spin_unlock(&entry->lock);

        return 0;
}

static void __corenet_maping_resume__(struct list_head *list, nid_t *nid, int res)
{
        struct list_head *pos, *n;
        wait_t *wait;

        list_for_each_safe(pos, n, list) {
                wait = (wait_t *)pos;
                if (!nid_cmp(&wait->nid, nid)) {
                        schedule_resume(&wait->task, res, NULL);

                        list_del(&wait->hook);
                        yfree((void **)&wait);
                }
        }
}

static void __corenet_maping_resume(void *_arg)
{
        int ret;
        arg_t *arg = _arg;
        corenet_maping_t *maping, *entry;
        nid_t *nid = &arg->nid;
        int res = arg->res;

        maping = __corenet_maping_get();

        entry = &maping[nid->id];
        ret = sy_spin_lock(&entry->lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        entry->loading = 0;
        __corenet_maping_resume__(&entry->list, nid, res);

        sy_spin_unlock(&entry->lock);

        yfree((void **)&arg);
}

void corenet_maping_resume(core_t *core, const nid_t *nid, int res)
{
        int ret;
        arg_t *arg;

        ret = ymalloc((void **)&arg, sizeof(*arg));
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        arg->nid = *nid;
        arg->res = res;

        ret = schedule_request(core->schedule, -1, __corenet_maping_resume, arg, "corenet_resume");
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);
}

static void __corenet_maping_accept(void *_arg)
{
        int ret, close_sockid = 0;
        arg_t *arg = _arg;
        corenet_maping_t *maping, *entry;
        nid_t *nid = &arg->nid;
        sockid_t *sockid = &arg->sockid, tmp_sockid;

        maping = __corenet_maping_get();

        YASSERT(sockid->sd < nofile_max);
        entry = &maping[nid->id];
        ret = sy_spin_lock(&entry->lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        if (entry->sockid.sd != -1 && entry->sockid.sd != sockid->sd) {
                DWARN("maping %p %s nid[%u], sockid %u exist, new %u\n",
                      maping, network_rname(nid), nid->id, entry->sockid.sd, sockid->sd);

                tmp_sockid = entry->sockid;
                close_sockid = 1;
        }

        YASSERT(sockid->sd < nofile_max);
        DINFO("set maping %s nid[%u], sockid %u\n", network_rname(nid), nid->id, sockid->sd);

        entry->sockid = *sockid;

        entry->loading = 0;
        __corenet_maping_resume__(&entry->list, nid, 0);

        sy_spin_unlock(&entry->lock);

        if (close_sockid)
                __corenet_maping_close_finally__(&tmp_sockid);

        yfree((void **)&arg);
}

int corenet_maping_accept(core_t *core, const nid_t *nid, const sockid_t *sockid)
{
        int ret;
        arg_t *arg;

        YASSERT(sockid->sd < nofile_max);
        ret = ymalloc((void **)&arg, sizeof(*arg));
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        arg->nid = *nid;
        arg->sockid = *sockid;

        ret = schedule_request(core->schedule, -1, __corenet_maping_accept, arg, "corenet_accept");
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        return 0;
}

STATIC void __corenet_maping_close_finally__(const sockid_t *sockid)
{
        if (gloconf.rdma && sockid->rdma_handler > 0)
                corenet_rdma_close((rdma_conn_t *)sockid->rdma_handler);
        else
                corenet_tcp_close(sockid);

}

STATIC int __corenet_maping_connect__(const nid_t *nid, sockid_t *sockid)
{
        if (gloconf.rdma)
                return corenet_rdma_connect(nid, sockid);
        else
                return corenet_tcp_connect(nid, sockid);
}

STATIC int __corenet_maping_connected__(const sockid_t *sockid)
{
        if (gloconf.rdma)
                return corenet_rdma_connected(sockid);
        else
                return corenet_tcp_connected(sockid);
}

STATIC int __corenet_maping_connect(const nid_t *nid)
{
        int ret;
        sockid_t sockid;
        core_t *core = core_self();

        ret = __corenet_maping_connect__(nid, &sockid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        YASSERT(sockid.sd != -1);
        ret = corenet_maping_accept(core, nid, &sockid);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        return 0;
err_ret:
        corenet_maping_resume(core, nid, ret);
        return ret;
}

int __corenet_maping_connect_wait(corenet_maping_t *entry, const nid_t *nid)
{
        int ret;
        wait_t *wait;

        ret = ymalloc((void **)&wait, sizeof(*wait));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        wait->nid = *nid;
        wait->task = schedule_task_get();

        list_add(&wait->hook, &entry->list);

        ret = schedule_yield("maping_connect", NULL, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int corenet_maping(const nid_t *nid, sockid_t *sockid)
{
        int ret;
        corenet_maping_t *entry;

        //ANALYSIS_BEGIN(0);
        YASSERT(__corenet_maping_get());

retry:
        entry = &__corenet_maping_get()[nid->id];
        if (unlikely(entry->sockid.sd == -1 || !__corenet_maping_connected__(&entry->sockid))) {
                /**
                 * 保证过程唯一性，只有一个task发起连接，其它并发task等待连接完成
                 * 发起连接的task，完成后唤醒所有等待task
                 */
                ret = sy_spin_lock(&entry->lock);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

                if (entry->loading == 0) {
                        entry->loading = 1;

                        sy_spin_unlock(&entry->lock);

                        DINFO("connect to %s\n", network_rname(nid));
                        ret = __corenet_maping_connect(nid);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);
                } else {
                        sy_spin_unlock(&entry->lock);
                }

                DINFO("connect to %s wait\n", network_rname(nid));
                ret = __corenet_maping_connect_wait(entry, nid);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                goto retry;
        }

        *sockid = entry->sockid;

        //ANALYSIS_QUEUE(0, IO_WARN, "corenet_maping");

        return 0;
err_ret:
        return ret;
}

static void __corenet_maping_close__(void *arg)
{
        corenet_maping_t *entry;
        sockid_t *sockid;
        const nid_t *nid = arg;

        entry = &__corenet_maping_get()[nid->id];
        sockid = &entry->sockid;

        if (sockid->sd != -1) {
                DINFO("close maping %s nid[%u], sockid %u\n",
                                network_rname(nid), nid->id, sockid->sd);

                __corenet_maping_close_finally__(sockid);
                sockid->sd = -1;
        }

        yfree((void **)&nid);
}

int corenet_maping_init(corenet_maping_t **_maping)
{
        int ret, i;
        corenet_maping_t *maping, *entry;

        ret = ymalloc((void **)&maping, sizeof(*maping) *  NODEID_MAX);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (i = 0; i < NODEID_MAX; i++) {
                entry = &maping[i];
                ret = sy_spin_init(&entry->lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                INIT_LIST_HEAD(&entry->list);
                entry->loading = 0;
                entry->sockid.sd = -1;
        }

        variable_set(VARIABLE_MAPING, maping);
        if (_maping)
                *_maping = maping;

        return 0;
err_ret:
        return ret;
}

void __corenet_maping_close(void *_core, void *_opaque)
{
        int ret;
        core_t *core = _core;
        nid_t *nid = _opaque;

        ret = ymalloc((void **)&nid, sizeof(*nid));
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        memcpy(nid, _opaque, sizeof(*nid));

        ret = schedule_request(core->schedule, -1, __corenet_maping_close__, nid, "corenet_close");
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);
}

void corenet_maping_close(const nid_t *nid)
{
        if (ng.daemon) {
                core_iterator(__corenet_maping_close, nid);
        }
}

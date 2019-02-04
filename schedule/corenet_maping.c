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
#include "conn.h"
#include "network.h"
#include "variable.h"
#include "dbg.h"

extern int nofile_max;

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
STATIC int __corenet_maping_connected__(const sockid_t *sockid);

static  corenet_maping_t *__corenet_maping_get__()
{
        return variable_get(VARIABLE_MAPING);
}

static int __connected__(corenet_maping_t *entry, uint32_t addr)
{
        sockid_t *sockid;
        
        for (int i = 0; i < entry->count; i++) {
                sockid = &entry->sockid[i];

                if (sockid->sd == -1) {
                        continue;
                }                        

                if (addr == sockid->addr) {
                        return 1;
                }
        }

        return 0;
}

int corenet_maping_loading(const nid_t *nid)
{
        int ret;
        corenet_maping_t *maping, *entry;

        maping = __corenet_maping_get__();

        entry = &maping[nid->id];
        ret = sy_spin_lock(&entry->lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        entry->loading = 1;

        sy_spin_unlock(&entry->lock);

        return 0;
}

static void __corenet_maping_resume__(struct list_head *list, const nid_t *nid, int res)
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

        maping = __corenet_maping_get__();

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

static int __corenet_maping_accept__(const nid_t *nid, const sockid_t *_sockid)
{
        int ret, i;
        corenet_maping_t *maping, *entry;
        sockid_t *sockid;

        maping = __corenet_maping_get__();

        YASSERT(_sockid->sd < nofile_max);
        entry = &maping[nid->id];
        
        ret = sy_spin_lock(&entry->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (__connected__(entry, _sockid->addr)) {
                DWARN("%s @ %s already connected\n", _inet_ntoa(_sockid->addr), network_rname(nid));
                __corenet_maping_close_finally__(_sockid);
                goto out;
        } else {
                DINFO("%s @ %s connected\n", _inet_ntoa(_sockid->addr), network_rname(nid));
        }
        
        for (i = 0; i < entry->count; i++) {
                sockid = &entry->sockid[i];

                YASSERT(sockid->sd != _sockid->sd);
                if (sockid->sd == -1) {
                        *sockid = *_sockid;
                        break;
                } else {
                        if (__corenet_maping_connected__(sockid)) {
                                continue;
                        } else {
                                *sockid = *_sockid;
                                break;
                        }
                }
        }

        YASSERT(i < netconf.count);
        YASSERT(i < CORENET_DEV_MAX);
        
        if (i == entry->count) {
                entry->sockid[i] = *_sockid;
                entry->count++;
        }

out:
        entry->loading = 0;
        __corenet_maping_resume__(&entry->list, nid, 0);

        sy_spin_unlock(&entry->lock);

        return 0;
err_ret:
        return ret;
}

static void __corenet_maping_accept(void *_arg)
{
        int ret;
        arg_t *arg = _arg;
        nid_t *nid = &arg->nid;
        sockid_t *_sockid = &arg->sockid;

        ret = __corenet_maping_accept__(nid, _sockid);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        yfree((void **)&arg);
}

int corenet_maping_accept(core_t *core, const nid_t *nid, const sockid_t *sockid, int count)
{
        int ret, i;
        arg_t *arg;

        for (i = 0; i < count; i++) {
                YASSERT(sockid->sd < nofile_max);
                ret = ymalloc((void **)&arg, sizeof(*arg));
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);
                
                arg->nid = *nid;
                arg->sockid = sockid[i];
                
                ret = schedule_request(core->schedule, -1, __corenet_maping_accept,
                                       arg, "corenet_accept");
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);
        }

        return 0;
}

STATIC void __corenet_maping_close_finally__(const sockid_t *sockid)
{
#if ENABLE_RDMA
        if (gloconf.rdma && sockid->rdma_handler > 0)
                corenet_rdma_close((rdma_conn_t *)sockid->rdma_handler);
        else
                corenet_tcp_close(sockid);
#else
        corenet_tcp_close(sockid);
#endif

}

STATIC int __corenet_maping_connect____(const nid_t *nid, const ynet_sock_info_t *_sock, int sock_count,
                                        sockid_t *_sockid, int *_count)
{
        int ret;
        sockid_t sockid;
        const ynet_sock_info_t *sock;

        YASSERT(sock_count);
        
        int count = 0;
        for (int i = 0; i < sock_count; i++) {
                sock = &_sock[i];
                
                DINFO("connect to %s @ %s %u\n", _inet_ntoa(sock->addr), network_rname(nid), i);

#if ENABLE_RDMA
                if (gloconf.rdma)
                        ret = corenet_rdma_connect(nid, sock->addr, &sockid);
                else
                        ret = corenet_tcp_connect(nid, sock->addr, &sockid);
#else
                ret = corenet_tcp_connect(nid, sock->addr, &sockid);
#endif
                if (unlikely(ret))
                        continue;
                
                _sockid[count] = sockid;
                count++;
        }

        if (count == 0) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }

        *_count = count;
        
        return 0;
err_ret:
        return ret;
}

STATIC int __corenet_maping_connect__(const nid_t *nid, sockid_t *_sockid, int *_count)
{
        int ret;
        ynet_net_info_t *info;
        char buf[MAX_BUF_LEN];
        
        ret = network_connect(nid, NULL, 0, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        info = (void *)buf;
        ret = conn_getinfo(nid, info);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __corenet_maping_connect____(nid, info->corenet, info->info_count, _sockid, _count);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}


STATIC int __corenet_maping_connected__(const sockid_t *sockid)
{
#if ENABLE_RDMA
        if (gloconf.rdma)
                return corenet_rdma_connected(sockid);
        else
                return corenet_tcp_connected(sockid);
#else
        return corenet_tcp_connected(sockid);
#endif   
}

STATIC int __corenet_maping_connect(const nid_t *nid)
{
        int ret, max;
        sockid_t sockid[CORENET_DEV_MAX];
        core_t *core = core_self();

        max = CORENET_DEV_MAX;
        ret = __corenet_maping_connect__(nid, sockid, &max);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        //YASSERT(sockid.sd != -1);
        ret = corenet_maping_accept(core, nid, sockid, max);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        return 0;
err_ret:
        corenet_maping_resume(core, nid, ret);
        return ret;
}

static int __corenet_maping_connect_wait(corenet_maping_t *entry, const nid_t *nid)
{
        int ret;
        wait_t *wait;

        ret = ymalloc((void **)&wait, sizeof(*wait));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        wait->nid = *nid;
        wait->task = schedule_task_get();

        list_add(&wait->hook, &entry->list);

        return 0;
err_ret:
        return ret;
}

static int __corenet_maping_get(const nid_t *nid, corenet_maping_t *entry, sockid_t *_sockid)
{
        int ret, i;
        sockid_t *sockid;
        
        for (i = 0; i < entry->count; i++) {
                sockid = &entry->sockid[(i + entry->cur) % entry->count];
                if (likely(__corenet_maping_connected__(sockid))) {
                        *_sockid = *sockid;
                        entry->cur = (entry->cur + 1) % entry->count;
                        break;
                } else {
                        DWARN("%s @ %s already closed, sd %d\n",
                              _inet_ntoa(_sockid->addr), network_rname(nid), sockid->sd);
                        sockid->sd = -1;
                        continue;
                }
        }

        if (i == entry->count) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}

static void __corenet_maping_connect_task(void *arg)
{
        int ret;
        corenet_maping_t *entry = arg;
        const nid_t *nid = &entry->nid;

        DINFO("connect to %s\n", network_rname(nid));
        ret = __corenet_maping_connect(nid);
        if (ret) {
                DWARN("connect to %s fail\n", network_rname(nid));
        }
}

int corenet_maping(const nid_t *nid, sockid_t *sockid)
{
        int ret;
        corenet_maping_t *entry;

        //ANALYSIS_BEGIN(0);
        YASSERT(__corenet_maping_get__());

retry:
        entry = &__corenet_maping_get__()[nid->id];

        ret = __corenet_maping_get(nid, entry, sockid);
        if (unlikely(ret)) {
                /**
                 * 保证过程唯一性，只有一个task发起连接，其它并发task等待连接完成
                 * 发起连接的task，完成后唤醒所有等待task
                 */

                ret = sy_spin_lock(&entry->lock);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

                ret = __corenet_maping_connect_wait(entry, nid);
                if (unlikely(ret))
                        GOTO(err_lock, ret);

                if (entry->loading == 0) {
                        entry->loading = 1;

                        sy_spin_unlock(&entry->lock);

                        DINFO("connect to %s\n", network_rname(nid));

                        schedule_task_new("corenet_maping", __corenet_maping_connect_task, entry, -1);
                } else {
                        sy_spin_unlock(&entry->lock);
                }

                DINFO("connect to %s wait\n", network_rname(nid));
                ret = schedule_yield("maping_connect", NULL, NULL);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
                
                goto retry;
        }

        return 0;
err_lock:
        sy_spin_unlock(&entry->lock);
err_ret:
        return ret;
}

static void __corenet_maping_close__(void *_arg)
{
        int i;
        corenet_maping_t *entry;
        sockid_t *sockid;
        const arg_t *arg = _arg;

        entry = &__corenet_maping_get__()[arg->nid.id];

        for (i = 0; i < entry->count; i++) {
                sockid = &entry->sockid[i];
                if (sockid->sd != -1) {
                        if (arg->sockid.sd != -1
                            && arg->sockid.sd == sockid->sd
                            && arg->sockid.seq == sockid->seq) {
                                
                                DINFO("close maping one sock %s nid[%u], sockid %u\n",
                                      network_rname(&arg->nid), arg->nid.id, sockid->sd);
                                
                                __corenet_maping_close_finally__(sockid);
                                sockid->sd = -1;
                                break;
                        } else {
                                DINFO("close maping all sock %s nid[%u], sockid %u\n",
                                      network_rname(&arg->nid), arg->nid.id, sockid->sd);
                                
                                __corenet_maping_close_finally__(sockid);
                                sockid->sd = -1;
                        }
                }
        }

        yfree((void **)&arg);
}

int corenet_maping_init(corenet_maping_t **_maping)
{
        int ret, i;
        corenet_maping_t *maping, *entry;
        nid_t nid;

        ret = ymalloc((void **)&maping, sizeof(*maping) *  INT16_MAX);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (i = 0; i < INT16_MAX; i++) {
                nid.id = i;
                entry = &maping[i];
                ret = sy_spin_init(&entry->lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                INIT_LIST_HEAD(&entry->list);
                entry->loading = 0;
                entry->count = 0;
                entry->nid = nid;
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
        arg_t *_arg = _opaque, *arg;

        ret = ymalloc((void **)&arg, sizeof(*arg));
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        *arg = *_arg;
        
        ret = schedule_request(core->schedule, -1, __corenet_maping_close__, arg, "corenet_close");
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);
}

void corenet_maping_close(const nid_t *nid, const sockid_t *sockid)
{
        arg_t arg;
        
        if (ng.daemon) {
                if (sockid) {
                        arg.sockid = *sockid;
                } else {
                        arg.sockid.sd = -1;
                }

                arg.nid = *nid;
                core_iterator(__corenet_maping_close, &arg);
        }
}

static int __corenet_maping_getlost(corenet_maping_t *entry, const ynet_net_info_t *info,
                                     ynet_sock_info_t *array, int *_lost)
{
        int ret, i, count;
        const ynet_sock_info_t *sock;
        sockid_t *sockid;
        int lost = 0;
        
        ret = sy_spin_lock(&entry->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (entry->loading) {
                goto out;
        }
        
        for (i = 0; i < entry->count; i++) {
                sockid = &entry->sockid[i];
                if (__corenet_maping_connected__(sockid)) {
                        count++;
                } else {
                        sockid->sd = -1;
                }
        }

        if (count == info->info_count) {
                goto out;
        }

        for (i = 0; i < info->info_count; i++) {
                //sockid = &entry->sockid[i];
                sock = &info->corenet[i];
                
                if (__connected__(entry, sock->addr)) {
                        continue;
                }

                array[lost] = *sock;
                lost++;
        }

out:
        *_lost = lost;
        sy_spin_unlock(&entry->lock);

        return 0;
err_ret:
        return ret;
}

static void __corenet_maping_check__(void *_info)
{
        int ret, i, connected, lost = 0;
        const ynet_net_info_t *info = _info;
        const nid_t *nid = &info->id;
        corenet_maping_t *entry;
        sockid_t socks[CORENET_DEV_MAX];
        ynet_sock_info_t info_array[CORENET_DEV_MAX];

        
        entry = &__corenet_maping_get__()[nid->id];
        ret = __corenet_maping_getlost(entry, info, info_array, &lost);
        if (unlikely(ret))
                goto out;

        if (lost == 0) {
                goto out;
        }

        DWARN("%s need connect, lost %d\n", info->name, lost);
        
        ret = __corenet_maping_connect____(nid, info_array, lost, socks, &connected);
        if (unlikely(ret))
                goto out;

        for (i = 0; i < connected; i++) {
                ret = __corenet_maping_accept__(nid, &socks[i]);
                if (unlikely(ret))
                        continue;
        }

out:
        return;
}

static void __corenet_maping_check(void *_core, void *_opaque)
{
        int ret;
        core_t *core = _core;
        ynet_net_info_t *info = _opaque;
        void *arg;

        ret = ymalloc((void **)&arg, info->len);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        memcpy(arg, info, info->len);
        
        ret = schedule_request(core->schedule, -1, __corenet_maping_check__, arg, "corenet_check");
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);
}

void corenet_maping_check(const ynet_net_info_t *info)
{
        if (ng.daemon) {
                core_iterator(__corenet_maping_check, info);
        }
}

#include <dirent.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdarg.h>

#define DBG_SUBSYS S_LIBYNET

#include "ylib.h"
#include "cache.h"
#include "configure.h"
#include "job_dock.h"
#include "ynet_rpc.h"
#include "rpc_table.h"
#include "mem_cache.h"
#include "network.h"
#include "corenet.h"
#include "net_global.h"
#include "ylog.h"
#include "dbg.h"

rpc_table_t *__rpc_table__;

static int __rpc_table_used(solt_t *solt)
{
        int ret;

        ret = sy_spin_trylock(&solt->used);
        if (unlikely(ret)) {
                YASSERT(ret == EBUSY);
                //YASSERT(solt->used == 0);
                return 1;
        } else {
                sy_spin_unlock(&solt->used);
                return 0;
        }
}

static int __rpc_table_use(solt_t *solt)
{
        return sy_spin_trylock(&solt->used);
}

static void __rpc_table_free(solt_t *solt)
{

        solt->func = NULL;
        solt->arg = NULL;
        solt->timeout = 0;
        solt->figerprint_prev = solt->msgid.figerprint;
        solt->msgid.figerprint = 0;

        sy_spin_unlock(&solt->used);

        //__rpc_table()->cur = solt->msgid.idx;
}

static int __rpc_table_lock(solt_t *solt)
{
        return sy_spin_lock(&solt->lock);
}

static int __rpc_table_trylock(solt_t *solt)
{
        return sy_spin_trylock(&solt->lock);
}

static int __rpc_table_unlock(solt_t *solt)
{
        return sy_spin_unlock(&solt->lock);
}

static int __rpc_table_check(solt_t *solt, uint32_t now)
{
        int ret, retval = ETIMEDOUT;
        sockid_t *closed = NULL, sockid;
        const char *conn;

        ret = __rpc_table_trylock(solt);
        if (unlikely(ret)) {
                if (ret == EBUSY) {
                        DINFO("%s @ %s/%u check, id (%u, %x) busy\n", solt->name,
                              _inet_ntoa(solt->sockid.addr), solt->sockid.sd, solt->msgid.idx,
                              solt->msgid.figerprint);
                        return 0;
                } else
                        GOTO(err_ret, ret);
        }

        if (solt->nid.id) {
                if (netable_connected(&solt->nid)) {
                        conn = "connected";
                } else {
                        conn = "disconnected";
                }
        } else {
                conn = "unknow";
        }

        if (solt->timeout && now > solt->timeout && (now - solt->timeout > (uint32_t)gloconf.rpc_timeout / 2)) {
                DINFO("%s @ %s/%u(%s) check, id (%u, %x), used %u timeout %d\n", solt->name,
                      _inet_ntoa(solt->sockid.addr), solt->sockid.sd, conn, solt->msgid.idx,
                      solt->msgid.figerprint, (int)(now - solt->begin), solt->timeout);
        } else {
                DBUG("%s @ %s/%u(%s) check, id (%u, %x), used %u\n", solt->name,
                     _inet_ntoa(solt->sockid.addr), solt->sockid.sd, conn, solt->msgid.idx,
                     solt->msgid.figerprint, (int)(now - solt->begin));
        }

        if (solt->timeout && now > solt->timeout) {
                DWARN("%s @ %s/%u(%s) timeout, id (%u, %x), rpc %u used %u timeout %d\n", solt->name,
                      _inet_ntoa(solt->sockid.addr), solt->sockid.sd, conn, solt->msgid.idx,
                      solt->msgid.figerprint,
                      gloconf.rpc_timeout,
                      (int)(now - solt->begin), solt->timeout);

                YASSERT(solt->func);
                YASSERT(solt->arg);

                ANALYSIS_BEGIN(0);
                uint64_t latency = -1;
                solt->func(solt->arg, &retval, NULL, &latency);
                ANALYSIS_END(0, 1000 * 100, solt->name);

                ANALYSIS_RESET(0);
                sockid = solt->sockid;
                closed = &sockid;
                ANALYSIS_END(0, 1000 * 100, solt->name);

                __rpc_table_free(solt);
        }

        __rpc_table_unlock(solt);

        if (closed) {
                if (closed->type == SOCKID_CORENET && closed->sd != -1) {
#if 1
                        UNIMPLEMENTED(__WARN__);
#else
                        corenet_tcp_close(closed);
#endif
                } else {
#if 0
                        net_handle_t nh;
                        sock2nh(&nh, sockid);
                        sdevent_close_force(&nh);
#endif
                }
        }
        
        return 0;
err_ret:
        return ret;
}

static void __rpc_table_scan(rpc_table_t *rpc_table)
{
        solt_t *solt;
        uint32_t i, used = 0, checked = 0;
        int cycle = 0;
        time_t now = gettime();
        
        ANALYSIS_BEGIN(0);
                
        for (i = 0; i < rpc_table->count; i++) {
                solt = &rpc_table->solt[i];

                if (!__rpc_table_used(solt)) {
                        continue;
                }

                used++;

                __rpc_table_check(solt, now);

                checked++;
        }

        ANALYSIS_END(0, IO_WARN, NULL);

        rpc_table->last_scan = now;

        if (used && (cycle % 100 == 0)) {
                cycle++;
                DINFO("rpc table %s used %u checked %u\n", rpc_table->name, used, checked);
        }
}

static void __rpc_table_scan_task(void *args)
{
        rpc_table_t *rpc_table = args;

        __rpc_table_scan(rpc_table);
}

void rpc_table_scan(rpc_table_t *rpc_table, int interval, int newtask)
{
        int tmo;
        time_t now;
        
        now = gettime();
        if (now < rpc_table->last_scan) {
                DERROR("update time %u --> %u\n", (int)now, (int)rpc_table->last_scan);
                rpc_table->last_scan = now;
                return;
        }

        if (now - rpc_table->last_scan > interval) {
                DBUG("scan %s now:%ld\n", rpc_table->name, now);
                tmo = (now - rpc_table->last_scan) - interval;
                if (tmo > 2 && rpc_table->last_scan) {
                        DINFO("scan %s delay %ds\n", rpc_table->name, tmo);
                }

                if (newtask) {
                        schedule_task_new("rpc_table_scan", __rpc_table_scan_task, rpc_table, -1);
                } else {
                        __rpc_table_scan(rpc_table);
                }
        }
}

static void  *__rpc_table_scan_worker(void *arg)
{
        int interval;
        rpc_table_t *rpc_table = arg;

        while (1) {
                if (rpc_table == NULL) {
                        continue;
                }

                if (ng.daemon) {
                        sleep(2);
                } else {
                        sleep(1);
                }

                interval = _ceil(gloconf.rpc_timeout, 10);
                rpc_table_scan(rpc_table, interval, 0);
        }

        return NULL;
}


static solt_t *__rpc_table_getsolt_sequence(rpc_table_t *rpc_table)
{
        int ret, retry = 0;
        solt_t *solt;
        uint32_t i, cur;

        cur = rpc_table->cur;
        for (i = 0; i < rpc_table->count; i++) {
                solt = &rpc_table->solt[(i + cur) %  rpc_table->count];
                ret = __rpc_table_use(solt);
                if (unlikely(ret == EBUSY)) {
                        retry++;
                        if (retry > 1000) {
                                DWARN("retry %u\n", retry);
                        }

                        continue;
                }

                rpc_table->cur = (i + cur + 1) % rpc_table->count;

                //DINFO("cur %d\n", rpc_table->cur);

                return solt;
        }

        return NULL;
}

static void __rpc_table_new(rpc_table_t *rpc_table, solt_t *solt)
{
        int rand, retry = 0;

        while (1) {
                rand = ++rpc_table->sequence;
                if (solt->figerprint_prev != (uint32_t)rand) {
                        solt->msgid.figerprint = rand;
                        break;
                } else {
                        YASSERT(retry < 100);
                        retry++;
                }
        }
}

int rpc_table_getsolt(rpc_table_t *rpc_table, msgid_t *msgid, const char *name)
{
        int ret;
        solt_t *solt;

        YASSERT(!schedule_suspend());

        solt = __rpc_table_getsolt_sequence(rpc_table);
        if (unlikely(solt == NULL)) {
                ret = ENOSPC;
                GOTO(err_ret, ret);
        }

        __rpc_table_new(rpc_table, solt);

        *msgid = solt->msgid;
        strcpy(solt->name, name);

        return 0;
err_ret:
        return ret;
}

static solt_t *__rpc_table_lock_solt(rpc_table_t *rpc_table, const msgid_t *msgid)
{
        int ret;
        solt_t *solt;

        solt = &rpc_table->solt[msgid->idx];
        if (unlikely(msgid->figerprint != solt->msgid.figerprint)) {
                DWARN("solt[%u] already closed\n", msgid->idx);
                return NULL;
        }

        ret = __rpc_table_lock(solt);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        if (likely(__rpc_table_used(solt)))
                return solt;
        else {
                DWARN("solt[%u] unused\n", msgid->idx);
                __rpc_table_unlock(solt);
                return NULL;
        }

        return solt;
}

int rpc_table_setsolt(rpc_table_t *rpc_table, const msgid_t *msgid, func3_t func,
                      void *arg, const sockid_t *sockid, const nid_t *nid, int timeout)
{
        int ret;
        solt_t *solt;

        YASSERT(timeout <= 180);

        solt = __rpc_table_lock_solt(rpc_table, msgid);
        if (unlikely(solt == NULL)) {
                ret = ESTALE;
                GOTO(err_ret, ret);
        }

        solt->func = func;
        solt->arg = arg;
        solt->begin = gettime();
        solt->timeout = solt->begin + timeout;

        if (sockid) 
                solt->sockid = *sockid;
        else
                memset(&solt->sockid, 0x0, sizeof(*sockid));

        if (nid) {
                //YASSERT(netable_connected(nid));
                solt->nid = *nid;
        } else {
                memset(&solt->nid, 0x0, sizeof(*nid));
        }
        
        __rpc_table_unlock(solt);

        return 0;
err_ret:
        return ret;
}

int rpc_table_post(rpc_table_t *rpc_table, const msgid_t *msgid, int retval, buffer_t *buf, uint64_t latency)
{
        int ret;
        solt_t *solt;

        solt = __rpc_table_lock_solt(rpc_table, msgid);
        if (solt == NULL) {
                ret = ESTALE;
                GOTO(err_ret, ret);
        }

        solt->func(solt->arg, &retval, buf, &latency);

        __rpc_table_free(solt);

        __rpc_table_unlock(solt);

        return 0;
err_ret:
        return ret;
}

int rpc_table_free(rpc_table_t *rpc_table, const msgid_t *msgid)
{
        int ret;
        solt_t *solt;

        solt = __rpc_table_lock_solt(rpc_table, msgid);
        if (solt == NULL) {
                ret = ESTALE;
                GOTO(err_ret, ret);
        }

        __rpc_table_free(solt);

        __rpc_table_unlock(solt);

        return 0;
err_ret:
        return ret;
}

static int __rpc_table_reset(const char *name, solt_t *solt, const sockid_t *sockid, const nid_t *nid)
{
        int ret, retval = ECONNRESET;

        if (!__rpc_table_used(solt)) {
                return 0;
        }

        ret = __rpc_table_trylock(solt);
        if (unlikely(ret)) {
                if (ret == EBUSY) {
                        return 0;
                } else
                        GOTO(err_ret, ret);
        }

        YASSERT(sockid || nid);
        if (solt->timeout && ((sockid && sockid_cmp(&solt->sockid, sockid) == 0)
                              || (nid && (nid_cmp(&solt->nid, nid) == 0)))) {
                YASSERT(solt->func);
                YASSERT(solt->arg);

                DINFO("table %s %s @ %s(%s) reset, id (%u, %x), used %u\n", name, solt->name,
                      _inet_ntoa(solt->sockid.addr), nid ? network_rname(nid) : "NULL", solt->msgid.idx,
                      solt->msgid.figerprint, (int)(gettime() - solt->begin));
                uint64_t latency = -1;
                solt->func(solt->arg, &retval, NULL, &latency);

                __rpc_table_free(solt);
        }

        __rpc_table_unlock(solt);

        return 0;
err_ret:
        return ret;
}

void  rpc_table_reset(rpc_table_t *rpc_table, const sockid_t *sockid, const nid_t *nid)
{
        uint32_t i;
        solt_t *solt;

        if (rpc_table == NULL) {
                DWARN("rpc table not inited\n");
                return;
        }
#if 0
        if (nid) {
                DINFO("reset %s node %s\n", rpc_table->name, network_rname(nid));
        } else {
                //DINFO("reset %s node %s\n", rpc_table->name, _inet_ntoa(sockid->addr));
                DINFO("reset %s\n", rpc_table->name);
        }
#endif
        
        for (i = 0; i < rpc_table->count; i++) {
                solt = &rpc_table->solt[i];
                __rpc_table_reset(rpc_table->name, solt, sockid, nid);
        }
}

static int __rpc_table_create(const char *name, int count, int tabid, rpc_table_t **_rpc_table)
{
        int ret, i;
        solt_t *solt;
        rpc_table_t *rpc_table;

        ret = ymalloc((void **)&rpc_table, sizeof(rpc_table_t) + sizeof(solt_t) * count);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (i = 0; i < count; i++) {
                solt = &rpc_table->solt[i];
                ret = sy_spin_init(&solt->lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ret = sy_spin_init(&solt->used);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                solt->msgid.idx = i;
                solt->msgid.tabid = tabid;
                solt->msgid.figerprint = 0;
                solt->figerprint_prev = 0;
                solt->timeout = 0;
                solt->name[0] = '\0';
                solt->arg = NULL;
                solt->func = NULL;
        }

        strcpy(rpc_table->name, name);
        rpc_table->sequence = _random();
        rpc_table->count = count;
        rpc_table->tabid = tabid;
        rpc_table->last_scan = 0;
        *_rpc_table = rpc_table;

        return 0;
err_ret:
        return ret;
}

int rpc_table_init(const char *name, rpc_table_t **rpc_table, int scan)
{
        int ret, count;
        rpc_table_t *tmp;

        count = RPC_TABLE_MAX;
        ret = __rpc_table_create(name, count, 0, &tmp);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (scan) {
                pthread_t th;
                pthread_attr_t ta;

                (void) pthread_attr_init(&ta);
                (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

                ret = pthread_create(&th, &ta, __rpc_table_scan_worker, tmp);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        *rpc_table = tmp;

        return 0;
err_ret:
        return ret;
}


void rpc_table_destroy(rpc_table_t **_rpc_table)
{
        solt_t *solt;
        rpc_table_t *rpc_table = *_rpc_table;
        uint64_t latency = -1;
        int retval = ECONNRESET;

        for (int i = 0; i < (int)rpc_table->count; i++) {
                solt = &rpc_table->solt[i];

                if (!__rpc_table_used(solt)) {
                        continue;
                }

                solt->func(solt->arg, &retval, NULL, &latency);
                __rpc_table_free(solt);
                
        }

        yfree((void **)&rpc_table);
}

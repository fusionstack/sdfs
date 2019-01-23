#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "configure.h"
#include "net_table.h"
#include "net_global.h"
#include "sdevent.h"
#include "ylib.h"
#include "timer.h"
#include "xnect.h"
#include "ylock.h"
#include "ynet_net.h"
#include "ynet_rpc.h"
#include "job_dock.h"
#include "main_loop.h"
#include "rpc_table.h"
#include "bh.h"
#include "adt.h"
#include "heartbeat.h"
#include "squeue.h"
#include "corenet_maping.h"
#include "net_table.h"
#include "net_rpc.h"
#include "dbg.h"

typedef ynet_net_conn_t entry_t;

typedef struct {
        nid_t parent;
        sockid_t sockid;
        uint64_t timeout;
        time_t ltime;
        sy_spinlock_t lock;
        int reference;
        time_t last_update;
} hb_entry_t;

extern worker_handler_t hb_jobtracker;

static void __heartbeat(void *_ent);

typedef struct {
        sockid_t sockid;
        uint64_t reply;
} hb_ctx_t;

static void __netable_heartbeat(void *_ctx)
{
        int ret;
        hb_ctx_t *ctx;

        ctx = _ctx;

        ANALYSIS_BEGIN(0);

        ret = net_rpc_heartbeat(&ctx->sockid, ctx->reply);
        if (unlikely(ret)) {
                DWARN("heartbeat %s/%u fail ret:%d\n", _inet_ntoa(ctx->sockid.addr),
                      ctx->sockid.sd, ret);
        } else {
                sdevent_heartbeat_set(&ctx->sockid, NULL, &ctx->reply);
        }

        ANALYSIS_END(0, 1000 * 1000, NULL);

        yfree((void **)&ctx);
}

static int __heartbeat__(const sockid_t *sockid, uint64_t reply)
{
        int ret;
        hb_ctx_t *ctx;

        ret = ymalloc((void **)&ctx, sizeof(*ctx));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ctx->sockid = *sockid;
        ctx->reply = reply;

        ret = main_loop_request(__netable_heartbeat, ctx, "heartbeat");
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static void __heartbeat(void *_ent)
{
        int ret, lost;
        uint64_t sent, reply;
        hb_entry_t *hb_ent;
        time_t ltime;

#if 0
        YASSERT(core_self() == NULL);
#endif
        
        hb_ent = _ent;

        ltime = netable_conn_time(&hb_ent->parent);
        if (ltime != hb_ent->ltime) {
                ret = EEXIST;
                DINFO("%s maigc %u -> %u, heartbeat exit\n", netable_rname_nid(&hb_ent->parent),
                      hb_ent->ltime, ltime);
                GOTO(err_ret, ret);
        }

        ret = sdevent_heartbeat_get(&hb_ent->sockid, &sent, &reply);
        if (unlikely(ret))
                GOTO(err_ret, ret);

#if 0 //_inet_ntoa maybe timeout
        DBUG("heartbeat %s/%u seq %llu %llu\n",
             _inet_ntoa(hb_ent->sockid.addr), hb_ent->sockid.sd, (LLU)sent, (LLU)reply);
#endif

        lost = sent - reply;
        if (lost > 1 && lost <= gloconf.hb_retry) {
                DINFO("heartbeat %s lost ack %u\n", netable_rname_nid(&hb_ent->parent), lost);
        } else if (lost > gloconf.hb_retry) {
                DWARN("heartbeat %s fail, lost ack %u\n", netable_rname_nid(&hb_ent->parent), lost);
                SWARN(0, "%s, heartbeat %s fail\n", M_FUSIONSTOR_HB_TIMEOUT_WARN,
                      netable_rname_nid(&hb_ent->parent));

                netable_dump_hb_timeout((char *)netable_rname_nid(&hb_ent->parent));
                //netable_close_withrpc(&hb_ent->sockid, &hb_ent->parent, "timeout at hb");
                netable_close(&hb_ent->parent, "timeout at hb", &hb_ent->ltime);
                ret = ETIME;
                GOTO(err_ret, ret);
        }

        sent++;

        ret = sdevent_heartbeat_set(&hb_ent->sockid, &sent, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __heartbeat__(&hb_ent->sockid, sent);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        /**
         * @brief 一个心跳周期会发送多条心跳包，如果心跳包丢失大于1，则重置
         *
         * @todo 高负载情况下，为了减少误判的可能性，此方案有待改进
         */
        ret = timer_insert("heartbeat", hb_ent, __heartbeat, hb_ent->timeout / gloconf.hb_retry);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        return;
err_ret:
        yfree((void **)&hb_ent);
        return;
}

int heartbeat_add(const sockid_t *sockid, const nid_t *parent, suseconds_t timeout, time_t ltime)
{
        int ret;
        hb_entry_t *hb_ent;

#if 0
        YASSERT(core_self() == NULL);
#endif
        YASSERT(sockid->addr);

        ret = ymalloc((void **)&hb_ent, sizeof(*hb_ent));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        hb_ent->sockid = *sockid;
        hb_ent->parent = *parent;
        hb_ent->ltime = ltime;
        hb_ent->timeout = timeout;
        hb_ent->reference = 0;
        hb_ent->last_update = gettime();

        ret = sy_spin_init(&hb_ent->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

#if 0 //_inet_ntoa maybe timeout
        DBUG("add heartbeat %s/%u\n",
             _inet_ntoa(hb_ent->sockid.addr), hb_ent->sockid.sd);
#endif

        ret = timer_insert("heartbeat", hb_ent, __heartbeat, timeout);
        if (unlikely(ret))
                GOTO(err_free, ret);

        return 0;
err_free:
        yfree((void **)&hb_ent);
err_ret:
        return ret;
}

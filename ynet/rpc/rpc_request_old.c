#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_YRPC

#include "ynet_net.h"
#include "adt.h"
#include "job_tracker.h"
#include "job.h"
#include "job_dock.h"
#include "ylib.h"
#include "ynet_rpc.h"
#include "configure.h"
#include "net_global.h"
#include "network.h"
#include "dbg.h"
#include "rpc_table.h"
#include "schedule.h"

/*static uint32_t ynetrpc_rq_reqid = 0;*/
extern jobtracker_t *jobtracker;


extern int rpc_inited;
extern int net_send(const net_handle_t *nh, job_t *job, uint64_t hash, int is_request);

typedef struct {
        sem_t sem;
        task_t task;
        int retval;
        buffer_t buf;
        uint64_t latency;

        int job; //是否是job
        jobid_t jobid;
} rpc_ctx_t;

void __rpc1_request_cb(void *_rpc_ctx, void *_retval, void *_buf)
{
        int ret;
        int *retval;
        buffer_t *buf = NULL;
        rpc_ctx_t *ctx;
        net_handle_t *nh;

        ctx = _rpc_ctx;
        retval = _retval;
        nh = NULL;

        if (_buf)
                buf = _buf;

        ret = jobdock_resume(&ctx->jobid, buf, nh, *retval);
        if (ret) {
                if (ret == ENOENT) {
                        DWARN("orphan job "JOBID_FORMAT"\n", JOBID_ARG(&ctx->jobid));
                        if (buf)
                                mbuffer_free(buf);
                } else
                        YASSERT(0);

        }

        yfree((void **)&ctx);

        return ;
}

static void __rpc1_request_nonblock_error(void *_job)
{
        int ret;
        job_t *job = _job;

        ret = job_lock(job);
        if (ret)
                YASSERT(0);

        net_handle_reset(&job->net);
        job->timeout = 0;

        job_unlock(job);

        ret = jobtracker_insert(job);
        if (ret) {
                DERROR("insert error\n");
        }
}

static void __rpc1_request_nonblock_reply(void *_job)
{
        int ret;
        job_t *job = _job;

        ret = job_lock(job);
        if (ret)
                YASSERT(0);

        net_handle_reset(&job->net);
        job->timeout = 0;

        job_unlock(job);

        ret = jobtracker_insert(job);
        if (ret) {
                DERROR("insert error\n");
        }
}

static void __rpc1_request_block_error(void *_job)
{
        int ret;
        job_t *job = _job;

        ret = job_lock(job);
        if (ret)
                YASSERT(0);

        net_handle_reset(&job->net);
        job->timeout = 0;

        job_unlock(job);

        (void) sem_post(&job->sem);
}

static void __rpc1_request_post_sem(void *arg1, void *arg2, void *arg3, void *arg4)
{
        rpc_ctx_t *ctx = arg1;
        int retval = *(int *)arg2;
        buffer_t *buf = arg3;
        uint64_t latency = *(uint64_t *)arg4;

        if (ctx->job) {
                return __rpc1_request_cb(arg1, arg2, arg3);
        }

        ctx->latency = latency;
        ctx->retval = retval;
        if (buf) {
                mbuffer_merge(&ctx->buf, buf);
        } else {
                ctx->buf.len = 0;
        }

        sem_post(&ctx->sem);
}

static void __rpc1_request_block_reply(void *_job) /*handler after send*/
{
        int ret;
        job_t *job = _job;

        ret = job_lock(job);
        if (ret)
                YASSERT(0);

        net_handle_reset(&job->net);
        job->timeout = 0;

        job_unlock(job);

        (void) sem_post(&job->sem);
}

int __rpc1_request_getsolt_job(job_t *job, const net_handle_t *nh)
{
        int ret;
        rpc_ctx_t *ctx;
        msgid_t msgid;
        sockid_t sockid;
        ynet_net_head_t *net_req;

        //在__rpc_requst_cb中free
        ret = ymalloc((void**)&ctx, sizeof(rpc_ctx_t));
        if (ret)
                GOTO(err_ret, ret);

        ctx->job = 1;
        ctx->jobid = job->jobid;

        ret = rpc_table_getsolt(__rpc_table__, &msgid, job->name);
        if (ret)
                GOTO(err_ret, ret);

        if (nh->type == NET_HANDLE_PERSISTENT) {
                ret = network_connect2(nh, 0);
                if (ret)
                        GOTO(err_ret, ret);

                ret = netable_getsock(&nh->u.nid, &sockid);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                sockid = nh->u.sd;
        }

        net_req = mbuffer_head(&job->request);
        net_req->msgid = msgid;
        job->msgid = msgid;

        DBUG("msgid: "RPCID_FORMAT", job:" JOBID_FORMAT"\n",
             RPCID_ARG(&msgid), JOBID_ARG(&ctx->jobid));

        ret = sem_init(&ctx->sem, 0, 0);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        ret = rpc_table_setsolt(__rpc_table__, &msgid, __rpc1_request_post_sem,
                                ctx, &sockid, &nh->u.nid, job->timeout);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        return 0;
err_ret:
        return ret;
}

int rpc1_request_prep_core(buffer_t *buf, const msgid_t *msgid,
                const void *request, int reqlen,
                const buffer_t *data, int prog)
{
        int ret;
        ynet_net_head_t *net_req;

        // 不超过buffer_t的一个seg长度(PAGE_SIZE)
        if (reqlen + sizeof(ynet_net_head_t) > PAGE_SIZE) {
                DERROR("why we get such a request? %u\n", reqlen);
                YASSERT(0);
        }

        ret = mbuffer_init(buf, sizeof(ynet_net_head_t) + reqlen);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        net_req = mbuffer_head(buf);
        net_req->magic = YNET_PROTO_TCP_MAGIC;
        net_req->len = sizeof(ynet_net_head_t) + reqlen;
        net_req->type = YNET_MSG_REQ;
        net_req->prog = prog;
        net_req->msgid = *msgid;
        net_req->crcode = 0;
        net_req->blocks = 0;
        net_req->load = jobdock_load();
        memcpy(net_req->buf, request, reqlen);

        if (data) {
                net_req->len += data->len;
                net_req->blocks = data->len;
                mbuffer_reference(buf, data);
        }

        if (gloconf.net_crc) {
                (void) ynet_pack_crcsum(buf);
                DBUG("crc %u\n", net_req->crcode);
        }

        return 0;
err_ret:
        return ret;
}

int rpc1_request_prep(job_t *job, void **req, uint32_t len, int prog)
{
        int ret;
        ynet_net_head_t *net_req;

        if (len + sizeof(ynet_net_head_t) > Y_PAGE_SIZE) {
                DERROR("why we get such a request? %u\n", len);

                YASSERT(0);
        }

        DBUG("reqlen %u, headlen %lu\n", len, (unsigned long)sizeof(ynet_net_head_t));

        ret = mbuffer_init(&job->request, sizeof(ynet_net_head_t) + len);
        if (ret)
                GOTO(err_ret, ret);

        net_req = mbuffer_head(&job->request);
        net_req->magic = YNET_PROTO_TCP_MAGIC;
        net_req->len = sizeof(ynet_net_head_t) + len;
        net_req->type = YNET_MSG_REQ;
        net_req->prog = prog;
        net_req->crcode = 0;
        net_req->blocks = 0;
        net_req->load = jobdock_load();

        if (req)
                *req = net_req->buf;

        mbuffer_init(&job->reply, 0);

        return 0;
err_ret:
        return ret;
}

int __rpc1_request_timedwait(job_t *job, int sec)
{
        int ret;
        struct timespec ts;
        time_t t;

        if (sec == 0)
                sec = gloconf.rpc_timeout * 2;

        _memset(&ts, 0x0, sizeof(struct timespec));

        t = time(NULL);
        if (t == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ts.tv_sec = t + sec;
        ts.tv_nsec = 0;

        ret = _sem_timedwait(&job->sem, &ts);
        if (ret) {
                if (sec >= gloconf.rpc_timeout) {
                        DERROR("job %s no %u seq %u timeout peer %s\n",
                               job->name, job->msgid.idx, job->msgid.tabid,
                               netable_rname((void *)&job->net));
                }

                if (ret == ETIMEDOUT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        ret = job_get_ret(job, 0);
        if (ret) {
                goto err_ret;
        }

        return 0;
err_ret:
        return ret;
}

void rpc1_request_append(job_t *job, buffer_t *buf)
{
        ynet_net_head_t *net_req;

        net_req = mbuffer_head(&job->request);
        net_req->len += buf->len;

        mbuffer_merge(&job->request, buf);
}

static int __rpc1_queue_prep(job_t *job, const net_handle_t *nid,
                            mbuffer_op_t op, nio_type_t type, nio_priority_t priority,
                            void (*reply)(void *), void (*error)(void *), buffer_t *buf)
{
        int ret;
        niocb_t *iocb;
        //uint32_t len;
        ynet_net_head_t *net_req;

        if (rpc_inited == 0) {
                ret = ENOSYS;
                goto err_ret;
        }

        YASSERT(op == KEEP_JOB);

        DBUG("%s\n", job->dump(job, __MSG__(Y_BUG)));

        iocb = &job->iocb;

        iocb->buf = &job->request;
        iocb->error = error;
        iocb->reply = reply;
        iocb->sent = NULL;
        iocb->op = op;
        iocb->priority = priority;
        iocb->type = type;

        /** @sa jobdock_queue */

#if 1
        if (job->timeout == 0)
                job->timeout = RW_TIMEOUT;
#endif

        _memcpy(&job->net, nid, sizeof(net_handle_t));

        DBUG("job %s tmo %llu\n", job->name, (LLU)job->timeout);

        net_req = mbuffer_head(&job->request);

        if (buf) {
                net_req->len += buf->len;
                net_req->blocks = buf->len;

                DBUG("job %s len %u io %u\n", job->name, net_req->len, net_req->blocks);

                //YASSERT(buf->len % SDFS_BLOCK_SIZE == 0);

                mbuffer_merge(&job->request, buf);
        }

        if (gloconf.net_crc) {
                (void) ynet_pack_crcsum(&job->request);
                DBUG("crc %u\n", net_req->crcode);
        }

        job->msgid = net_req->msgid;

        return 0;
err_ret:
        return ret;
}

int rpc1_request_queue_wait(job_t *job, const net_handle_t *nid, mbuffer_op_t op,
                            nio_type_t type)
{
        int ret;

        sem_init(&job->sem, 0, 0);

        ret = __rpc1_request_getsolt_job(job, nid);
        if (ret)
                GOTO(err_ret, ret);

        ret = __rpc1_queue_prep(job, nid, op, type, NIO_NORMAL, __rpc1_request_block_reply,
                               __rpc1_request_block_error, NULL);
        if (ret)
                goto err_ret;

        job_set_ret(job, 0, 0);

        ANALYSIS_BEGIN(0);

        ret = net_send(nid, job, 1, 1);
        if (ret) {
                if (ret == ENONET)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        DBUG("begin to wait job %p no %u seq %u sec\n", job, job->msgid.idx,
             job->msgid.tabid);

        ret = __rpc1_request_timedwait(job, job->timeout);
        if (ret) {
#if 0
                if (ret == ENONET || ret == ETIMEDOUT || ret == ECONNRESET
                    || ret == EHOSTUNREACH || ret == ETIMEDOUT) {
                        ret = EAGAIN;
                }
#endif

                goto err_ret;
        }

        ANALYSIS_END(0, 1000 * 2000, job->name);

        DBUG("job no %u, id %u replied\n", job->msgid.idx, job->msgid.tabid);

        return 0;
err_ret:
        job->timeout = 0;
        return ret;
}

int rpc1_request_queue_wait1(job_t *job, const net_handle_t *nid, int tmo, buffer_t *buf)
{
        int ret;

        sem_init(&job->sem, 0, 0);

        ret = __rpc1_request_getsolt_job(job, nid);
        if (ret)
                GOTO(err_ret, ret);

        ret = __rpc1_queue_prep(job, nid, KEEP_JOB, NIO_BLOCK, NIO_NORMAL,
                               __rpc1_request_block_reply, 
                               __rpc1_request_block_error, buf);
        if (ret)
                goto err_ret;

        job_set_ret(job, 0, 0);

        ret = net_send(nid, job, 1, 1);
        if (ret) { 
                if (ret == ENONET)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        DBUG("begin to wait job %p no %u seq %u sec\n", job, job->msgid.idx,
             job->msgid.tabid);

        ret = __rpc1_request_timedwait(job, tmo);
        if (ret)
                goto err_ret;

        DBUG("job no %u, id %u replied\n", job->msgid.idx, job->msgid.tabid);

        return 0;
err_ret:
        job->timeout = 0;
        return ret;
}


/**
 * @param nid dst
 * @param op
 */
int rpc1_request_queue(job_t *job, const net_handle_t *nid, mbuffer_op_t op,
                      nio_type_t type, buffer_t *buf)
{
        int ret;

        //ANALYSIS_BEGIN(0);
        ret = __rpc1_request_getsolt_job(job, nid);
        if (ret)
                GOTO(err_ret, ret);

        ret = __rpc1_queue_prep(job, nid, op, type, NIO_NORMAL,
                               __rpc1_request_nonblock_reply,
                               __rpc1_request_nonblock_error, buf);
        if (ret)
                goto err_ret;

        ret = net_send(nid, job, 0, 1);
        if (ret) {
                GOTO(err_ret, ret);
        }

        //ANALYSIS_END(0, 1000 * 1, job->name);

        return 0;
err_ret:
        job->timeout = 0;
        return ret;
}

int rpc1_request_queue1(job_t *job, const net_handle_t *nid, mbuffer_op_t op,
                       nio_type_t type, buffer_t *buf, nio_priority_t priority, uint64_t hash)
{
        int ret;

        ret = __rpc1_request_getsolt_job(job, nid);
        if (ret)
                GOTO(err_ret, ret);

        ret = __rpc1_queue_prep(job, nid, op, type, priority,
                               __rpc1_request_nonblock_reply,
                               __rpc1_request_nonblock_error, buf);
        if (ret)
                goto err_ret;

        ret = net_send(nid, job, hash, 1);
        if (ret) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        job->timeout = 0;
        return ret;
}

void rpc1_request_finished(job_t *job)
{
        mbuffer_free(&job->request);
        mbuffer_free(&job->reply);
        mbuffer_init(&job->request, 0);
        mbuffer_init(&job->reply, 0);
}



#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_YRPC

#include "ynet_net.h"
#include "net_global.h"
#include "job_tracker.h"
#include "job_dock.h"
#include "configure.h"
#include "ylib.h"
#include "ynet_rpc.h"
#include "dbg.h"

extern jobtracker_t *jobtracker;

int rpc1_reply_prep(job_t *job, void **req, uint32_t len)
{
        int ret;
        ynet_net_head_t *net_rep;

        if (len + sizeof(ynet_net_head_t) > Y_PAGE_SIZE) {
                DERROR("why we get such a request? \n");

                YASSERT(0);
        }

        DBUG("reqlen %u, headlen %lu\n", len, (unsigned long)sizeof(ynet_net_head_t));

        ret = mbuffer_init(&job->reply, sizeof(ynet_net_head_t) + len);
        if (ret)
                GOTO(err_ret, ret);

        net_rep = mbuffer_head(&job->reply);
        net_rep->magic = YNET_PROTO_TCP_MAGIC;
        net_rep->len = sizeof(ynet_net_head_t) + len;
        net_rep->type = YNET_MSG_REP;
        net_rep->prog = MSG_NULL;
        net_rep->msgid = job->msgid;
        net_rep->crcode = 0;
        net_rep->blocks = 0;
        net_rep->load = jobdock_load();

        if (req)
                *req = net_rep->buf;

        return 0;
err_ret:
        return ret;
}

int rpc1_reply_error(job_t *job, int _error)
{
        int ret;
        ynet_net_head_t *net_rep;
        ynet_net_err_t *net_err;
        uint32_t len;

        rpc1_reply_finished(job);

        len = sizeof(ynet_net_err_t);

        ret = mbuffer_init(&job->reply, sizeof(ynet_net_head_t) + len);
        if (ret)
                GOTO(err_ret, ret);

        net_rep = mbuffer_head(&job->reply);
        net_rep->magic = YNET_PROTO_TCP_MAGIC;
        net_rep->len = sizeof(ynet_net_head_t) + len;
        net_rep->type = YNET_MSG_REP;
        net_rep->prog = MSG_NULL;
        net_rep->msgid = job->msgid;
        net_rep->crcode = 0;
        net_rep->load = jobdock_load();
        net_rep->blocks = 0;

        net_err = (void *)net_rep->buf;

        net_err->magic = YNET_NET_ERR_MAGIC;
        net_err->err = _error;

        ret = rpc1_reply_send(job, &job->reply, FREE_JOB);
        if (ret) {
                if (ret == ENONET || ret == ECONNRESET)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int rpc1_reply_appendmem(job_t *job, void *src, uint32_t size)
{
        int ret;
        ynet_net_head_t *net_rep;

        ret = mbuffer_appendmem(&job->reply, src, size);
        if (ret)
                GOTO(err_ret, ret);

        net_rep = mbuffer_head(&job->reply);
        net_rep->len += size;

        return 0;
err_ret:
        return ret;
}

void rpc1_reply_append(job_t *job, const char *rep, uint32_t len)
{
        ynet_net_head_t *net_rep;

        net_rep = mbuffer_head(&job->reply);
        net_rep->len += len;

        mbuffer_copy(&job->reply, rep, len);
}

int rpc1_reply_send(job_t *job, buffer_t *_buf, mbuffer_op_t op)
{
        int ret;
        net_handle_t nh;
        buffer_t buf;

        YASSERT(_buf == &job->reply);
        YASSERT(op == FREE_JOB);

        if (gloconf.net_crc) {
                (void) ynet_pack_crcsum(&job->reply);
        }

        mbuffer_init(&buf, 0);
        mbuffer_merge(&buf, _buf);
        sock2nh(&nh, &job->sock);

        job_destroy(job);
        
        ret = sdevent_queue(&nh, &buf, 0);
        if (ret) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int rpc1_reply_send1(job_t *job, buffer_t *buf, mbuffer_op_t op,
                    nio_priority_t priority, uint64_t hash)
{
        ynet_net_head_t *net_rep;

        (void) hash;
        (void) priority;

        net_rep = mbuffer_head(&job->reply);

        //ret = mbuffer_tee(&context->tee, &context->buf);

        if (buf) {
                YASSERT(buf != &job->reply);
                net_rep->len += buf->len;
                net_rep->blocks = buf->len;
                mbuffer_merge(&job->reply, buf);
        }
        
        return rpc1_reply_send(job, &job->reply, op);
}

void rpc1_reply_finished(job_t *job)
{
        mbuffer_free(&job->request);
        mbuffer_free(&job->reply);
}

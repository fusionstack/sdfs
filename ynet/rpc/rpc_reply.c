#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "ynet_net.h"
#include "net_global.h"
#include "job_dock.h"
#include "core.h"
#include "configure.h"
#include "ylib.h"
#include "ynet_rpc.h"
#include "dbg.h"

/*int corenet_rdma_write_seg_free(void *arg)
{
        ynet_net_head_t *net_rep = arg;

        mbuffer_free(&net_rep->reply_buf);
        mem_cache_free(MEM_CACHE_512, arg);

        return 0;
} */

void rpc_reply_prep1(const msgid_t *msgid, buffer_t *buf, buffer_t *data)
{
        int ret;
        ynet_net_head_t *net_rep;
        
       /* net_rep = mem_cache_calloc(MEM_CACHE_512, 1);
 
        ret = mbuffer_initwith(buf, (void *)net_rep, sizeof(ynet_net_head_t), (void *)net_rep, corenet_rdma_write_seg_free);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);*/
        ret = mbuffer_init(buf, sizeof(ynet_net_head_t));
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        net_rep = mbuffer_head(buf);
        net_rep->magic = YNET_PROTO_TCP_MAGIC;
        net_rep->len = sizeof(ynet_net_head_t);
        net_rep->type = YNET_DATA_REP;
        net_rep->prog = MSG_NULL;
        net_rep->msgid = *msgid;
        net_rep->crcode = 0;
        net_rep->blocks = 0;
        net_rep->load = core_latency_get();;
        net_rep->priority = 0;
        net_rep->time = gettime();

        mbuffer_init(&net_rep->reply_buf, 0);
        mbuffer_merge(&net_rep->reply_buf, data);

        DBUG("msgid %d.%d\n", net_rep->msgid.idx, net_rep->msgid.figerprint);
}

void rpc_reply_prep(const msgid_t *msgid, buffer_t *buf, buffer_t *data, int flag)
{
        int ret;
        ynet_net_head_t *net_rep;

        ret = mbuffer_init(buf, sizeof(ynet_net_head_t));
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        net_rep = mbuffer_head(buf);
        net_rep->magic = YNET_PROTO_TCP_MAGIC;
        net_rep->len = sizeof(ynet_net_head_t);
        net_rep->type = YNET_MSG_REP;
        net_rep->prog = MSG_NULL;
        net_rep->msgid = *msgid;
        net_rep->crcode = 0;
        net_rep->blocks = 0;
        net_rep->priority = 0;
        net_rep->load = core_latency_get();
        net_rep->time = gettime();

        if (data) {
                if (flag) {
                        net_rep->len += data->len;
                        net_rep->blocks = data->len;
                        mbuffer_merge(buf, data);
                } else {
                        mbuffer_init(&net_rep->reply_buf, 0);
                        mbuffer_reference(&net_rep->reply_buf, data);
                }
        }

        DBUG("msgid %d.%d\n", net_rep->msgid.idx, net_rep->msgid.figerprint);
}

 void rpc_reply1(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf)
{
        int ret;
        buffer_t buf;
        net_handle_t nh;

        DBUG("reply msgid (%d, %x) %s\n", msgid->idx, msgid->figerprint,
              _inet_ntoa(sockid->addr));

        rpc_reply_prep(msgid, &buf, _buf, 1);

#if 1
        ret = core_pipeline_send(sockid, &buf, 0);
        if (unlikely(ret)) {
                ret = _errno_net(ret);
                if (ret == ENOSYS) {
                        sock2nh(&nh, sockid);
                        ret = sdevent_queue(&nh, &buf, 0);
                        if (unlikely(ret)) {
                                ret = _errno_net(ret);
                                GOTO(err_free, ret);
                        }
                } else
                        GOTO(err_free, ret);
        }
#else
        sock2nh(&nh, sockid);
        ret = sdevent_queue(&nh, &buf, 0);
        if (unlikely(ret)) {
                ret = _errno_net(ret);
                GOTO(err_free, ret);
        }
#endif

        return;
err_free:
        mbuffer_free(&buf);
        return;
}

void rpc_reply(const sockid_t *sockid, const msgid_t *msgid, const void *_buf, int len)
{
        buffer_t buf;

        mbuffer_init(&buf, 0);
        if (len)
                mbuffer_copy(&buf, _buf, len);

        rpc_reply1(sockid, msgid, &buf);
}

void rpc_reply_error_prep(const msgid_t *msgid, buffer_t *buf, int _error)
{
        int ret;
        ynet_net_head_t *net_rep;
        ynet_net_err_t *net_err;
        uint32_t len;

        len = sizeof(ynet_net_err_t);

        ret = mbuffer_init(buf, sizeof(ynet_net_head_t) + len);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        net_rep = mbuffer_head(buf);
        net_rep->magic = YNET_PROTO_TCP_MAGIC;
        net_rep->len = sizeof(ynet_net_head_t) + len;
        net_rep->type = YNET_MSG_REP;
        net_rep->prog = MSG_NULL;
        net_rep->msgid = *msgid;
        net_rep->crcode = 0;
        net_rep->blocks = 0;
        net_rep->priority = 0;
        net_rep->load = core_latency_get();
        net_rep->time = gettime();

        net_err = (void *)net_rep->buf;

        net_err->magic = YNET_NET_ERR_MAGIC;
        net_err->err = _error;
}

void rpc_reply_error(const sockid_t *sockid, const msgid_t *msgid, int _error)
{
        int ret;
        ynet_net_head_t *net_rep;
        ynet_net_err_t *net_err;
        uint32_t len;
        buffer_t buf;
        net_handle_t nh;

        len = sizeof(ynet_net_err_t);

        ret = mbuffer_init(&buf, sizeof(ynet_net_head_t) + len);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        net_rep = mbuffer_head(&buf);
        net_rep->magic = YNET_PROTO_TCP_MAGIC;
        net_rep->len = sizeof(ynet_net_head_t) + len;
        net_rep->type = YNET_MSG_REP;
        net_rep->prog = MSG_NULL;
        net_rep->msgid = *msgid;
        net_rep->crcode = 0;
        net_rep->blocks = 0;
        net_rep->priority = 0;
        net_rep->load = core_latency_get();
        net_rep->time = gettime();

        net_err = (void *)net_rep->buf;

        net_err->magic = YNET_NET_ERR_MAGIC;
        net_err->err = _error;

        sock2nh(&nh, sockid);
        ret = sdevent_queue(&nh, &buf, 0);
        if (unlikely(ret))
                mbuffer_free(&buf);
}

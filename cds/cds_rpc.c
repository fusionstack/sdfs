#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/resource.h>
#include <unistd.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <semaphore.h>
#include <poll.h> 
#include <pthread.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSLIB

#include "ynet_rpc.h"
#include "job_dock.h"
#include "net_global.h"
#include "ynet_rpc.h"
#include "rpc_proto.h"
#include "cds_rpc.h"
#include "md_lib.h"
#include "network.h"
#include "mem_cache.h"
#include "../cds/replica.h"
#include "schedule.h"
#include "corenet_connect.h"
#include "corerpc.h"
#include "dbg.h"

extern net_global_t ng;

typedef enum {
        CDS_NULL = 400,
        CDS_WRITE,
        CDS_READ,
 
 
        CDS_MAX,
} cds_op_t;

typedef struct {
        uint32_t op;
        uint32_t buflen;
        chkid_t  chkid;
        char buf[0];
} msg_t;

static __request_handler_func__  __request_handler__[CDS_MAX - CDS_NULL];
static char  __request_name__[CDS_MAX - CDS_NULL][__RPC_HANDLER_NAME__ ];

static void __request_set_handler(int op, __request_handler_func__ func, const char *name)
{
        YASSERT(strlen(name) + 1 < __RPC_HANDLER_NAME__ );
        strcpy(__request_name__[op - CDS_NULL], name);
        __request_handler__[op - CDS_NULL] = func;
}

static void __request_get_handler(int op, __request_handler_func__ *func, const char **name)
{
        *func = __request_handler__[op - CDS_NULL];
        *name = __request_name__[op - CDS_NULL];
}

static void __getmsg(buffer_t *buf, msg_t **_req, int *buflen, char *_buf)
{
        msg_t *req;

        YASSERT(buf->len <= MEM_CACHE_SIZE4K);

        req = (void *)_buf;
        *buflen = buf->len - sizeof(*req);
        mbuffer_get(buf, req, buf->len);

        *_req = req;
}

static void __request_handler(void *arg)
{
        int ret;
        msg_t req;
        sockid_t sockid;
        msgid_t msgid;
        buffer_t buf;
        __request_handler_func__ handler;
        const char *name;

        request_trans(arg, NULL, &sockid, &msgid, &buf, NULL);

        if (buf.len < sizeof(req)) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        mbuffer_get(&buf, &req, sizeof(req));

        DBUG("new op %u from %s, id (%u, %x)\n", req.op,
             _inet_ntoa(sockid.addr), msgid.idx, msgid.figerprint);

#if 0
        if (!netable_connected(net_getadmin())) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }
#endif

        __request_get_handler(req.op, &handler, &name);
        if (handler == NULL) {
                ret = ENOSYS;
                DWARN("error op %u\n", req.op);
                GOTO(err_ret, ret);
        }

        schedule_task_setname(name);

        ret = handler(&sockid, &msgid, &buf);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        mbuffer_free(&buf);

        DBUG("reply op %u from %s, id (%u, %x)\n", req.op,
              _inet_ntoa(sockid.addr), msgid.idx, msgid.figerprint);

        return ;
err_ret:
        mbuffer_free(&buf);
        if (sockid.type == SOCKID_CORENET) {
                DBUG("corenet\n");
                corerpc_reply_error(&sockid, &msgid, ret);
        } else {
                rpc_reply_error(&sockid, &msgid, ret);
        }
        DBUG("error op %u from %s, id (%u, %x)\n", req.op,
             _inet_ntoa(sockid.addr), msgid.idx, msgid.figerprint);
        return;
}

static int __cds_srv_read(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf)
{
        int ret, buflen;
        msg_t *req;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        const io_t *io;
        buffer_t reply;
        const nid_t *reader;

        __getmsg(_buf, &req, &buflen, buf);

        _opaque_decode(req->buf, buflen, &reader, NULL, &io, NULL, NULL);

        mbuffer_init(&reply, 0);

        DBUG("read "CHKID_FORMAT" offset %ju size %u\n",
              CHKID_ARG(&io->id), io->offset, io->size);
        
        ret = replica_read(io, &reply);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (sockid->type == SOCKID_CORENET) {
                DBUG("corenet read\n");
                corerpc_reply1(sockid, msgid, &reply);
        } else {
                rpc_reply1(sockid, msgid, &reply);
        }

        mbuffer_free(&reply);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

int cds_rpc_read(const nid_t *nid, const io_t *io, buffer_t *_buf)
{
        int ret;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t count;
        msg_t *req;

        ret = network_connect(nid, NULL, 1, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ANALYSIS_BEGIN(0);

        //YASSERT(io->offset <= YFS_CHK_LEN_MAX);

        DBUG("read "CHKID_FORMAT" offset %ju size %u\n",
              CHKID_ARG(&io->id), io->offset, io->size);
        
        req = (void *)buf;
        req->op = CDS_READ;
        req->chkid = io->id;
        _opaque_encode(&req->buf, &count, net_getnid(), sizeof(nid_t), io,
                       sizeof(*io), NULL);

#if ENABLE_CORERPC
        if (likely(ng.daemon)) {
                DBUG("corenet read\n");
                ret = corerpc_postwait("cds_rpc_read", nid,
                                       req, sizeof(*req) + count, NULL,
                                       _buf, MSG_CORENET, io->size, _get_timeout());
                if (unlikely(ret)) {
                        YASSERT(ret != EINVAL);
                        GOTO(err_ret, ret);
                }
        } else {
                ret = rpc_request_wait2("cds_rpc_read", nid,
                                        req, sizeof(*req) + count, _buf,
                                        MSG_REPLICA, 0, _get_timeout());
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }
#else 
        ret = rpc_request_wait2("cds_rpc_read", nid,
                                req, sizeof(*req) + count, _buf,
                                MSG_REPLICA, 0, _get_timeout());
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

static int __cds_srv_write(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf)
{
        int ret;
        msg_t *req;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t buflen;
        const nid_t *writer;
        const io_t *io;

        req = (void *)buf;
        mbuffer_get(_buf, req, sizeof(*req));
        buflen = req->buflen;
        ret = mbuffer_popmsg(_buf, req, buflen + sizeof(*req));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        _opaque_decode(req->buf, buflen, &writer, NULL, &io, NULL, NULL);

        DBUG("write chunk "CHKID_FORMAT", off %llu, len %u:%u\n",
              CHKID_ARG(&req->chkid), (LLU)io->offset, io->size, _buf->len);

        YASSERT(_buf->len == io->size);

        ret = replica_write(io, _buf);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        if (sockid->type == SOCKID_CORENET) {
                DBUG("corenet write\n");
                corerpc_reply(sockid, msgid, NULL, 0);
        } else {
                rpc_reply(sockid, msgid, NULL, 0);
        }

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

int cds_rpc_write(const nid_t *nid, const io_t *io, const buffer_t *_buf)
{
        int ret;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t count;
        msg_t *req;

        ret = network_connect(nid, NULL, 1, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ANALYSIS_BEGIN(0);

        YASSERT(_buf->len == io->size);
        //YASSERT(io->offset <= YFS_CHK_LEN_MAX);

        req = (void *)buf;
        req->op = CDS_WRITE;
        req->chkid = io->id;
        _opaque_encode(&req->buf, &count, net_getnid(), sizeof(nid_t), io,
                       sizeof(*io), NULL);

        req->buflen = count;

#if ENABLE_CORERPC
        if (likely(ng.daemon)) {
                DBUG("corenet write\n");
                ret = corerpc_postwait("cds_rpc_write", nid,
                                       req, sizeof(*req) + count, _buf,
                                       NULL, MSG_CORENET, io->size, _get_timeout());
                if (unlikely(ret)) {
                        YASSERT(ret != EINVAL);
                        GOTO(err_ret, ret);
                }
        }  else {
                ret = rpc_request_wait1("cds_rpc_write", nid,
                                        req, sizeof(*req) + count, _buf,
                                        MSG_REPLICA, 0, _get_timeout());
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }
#else
        ret = rpc_request_wait1("cds_rpc_write", nid,
                                req, sizeof(*req) + count, _buf,
                                MSG_REPLICA, 0, _get_timeout());
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
#endif

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

int cds_rpc_init()
{
        DINFO("replica rpc init\n");

        __request_set_handler(CDS_READ, __cds_srv_read, "cds_srv_read");
        __request_set_handler(CDS_WRITE, __cds_srv_write, "cds_srv_write");
        
        if (ng.daemon) {
                rpc_request_register(MSG_REPLICA, __request_handler, NULL);

#if ENABLE_CORERPC
                corerpc_register(MSG_CORENET, __request_handler, NULL);
#endif
        }

        return 0;
}

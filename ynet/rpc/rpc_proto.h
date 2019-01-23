#ifndef __RPC_PROTO_H__
#define __RPC_PROTO_H__

#include "sdevent.h"
#include "mem_cache.h"
#include "job.h"

typedef struct {
        nid_t nid;
        sockid_t sockid;
        msgid_t msgid;
        buffer_t buf;
        void *ctx;
} rpc_request_t;

int rpc_pack_handler(const nid_t *nid, const sockid_t *sockid, buffer_t *buf);
int rpc_pack_len(void *buf, uint32_t len, int *msg_len, int *io_len);

typedef int (*__request_handler_func__)(const sockid_t *sockid, const msgid_t *msgid, buffer_t *buf);
#define __RPC_HANDLER_NAME__ 128

inline static void request_trans(void *arg, nid_t *nid, sockid_t *sockid, msgid_t *msgid, buffer_t *buf, void **ctx)
{
        rpc_request_t *rpc_request;

        rpc_request = arg;
        YASSERT(rpc_request->sockid.addr);
        //YASSERT(rpc_request->sockid.sd);
        //YASSERT(rpc_request->sockid.seq);

        mbuffer_init(buf, 0);
        mbuffer_merge(buf, &rpc_request->buf);
        *msgid = rpc_request->msgid;
        *sockid = rpc_request->sockid;

        if (nid)
                *nid = rpc_request->nid;
        if (ctx)
                *ctx = rpc_request->ctx;

        mem_cache_free(MEM_CACHE_128, rpc_request);
}

#endif

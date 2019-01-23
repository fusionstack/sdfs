

#include <errno.h>
#include <unistd.h>
#include <rpc/rpc.h>
#include <rpc/svc.h>
#include <rpc/pmap_clnt.h>
#include <sys/poll.h>

#define DBG_SUBSYS S_YNFS

#include "ylib.h"
#include "job_dock.h"
#include "mem_cache.h"
#include "job_tracker.h"
#include "schedule.h"
#include "nfs_events.h"
#include "nfs_state_machine.h"
#include "nfs_job_context.h"
#include "xdr_nfs.h"
#include "sunrpc_reply.h"
#include "sunrpc_passive.h"
#include "sunrpc_proto.h"
#include "ynfs_conf.h"
#include "nfs_conf.h"
#include "ynet_rpc.h"
#include "dbg.h"

typedef struct {
        sockid_t sockid;
        sunrpc_request_t req;
        uid_t uid;
        gid_t gid;
        buffer_t buf;
} sunrpc_req_t;

int acl_null_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                 uid_t uid, gid_t gid, nfsarg_t *arg, buffer_t *buf)
{
        int ret;

        (void) req;
        (void) uid;
        (void) gid;
        (void) arg;
        (void) buf;

        
        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_ERROR, NULL, NULL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int nfs_acl(const sockid_t *sockid, const sunrpc_request_t *req,
             uid_t uid, gid_t gid, buffer_t *buf)
{
        int ret;
        nfs_handler handler = NULL;
        xdr_arg_t xdr_arg = NULL;
        nfsarg_t nfsarg;
        char *name;
        xdr_t xdr;

        switch (req->procedure) {
        case ACL_NULL:
                handler = acl_null_svc;
                xdr_arg = (xdr_arg_t)__xdr_void;
                name = "ACL_NULL";
                break;
        default:
                DERROR("unknow procedure\n");
        }

        if (handler == NULL) {
                ret = EINVAL;
                DERROR("error proc %s\n", name);
                GOTO(err_ret, ret);
        }

        DBUG("<-------------------new procedure %s------------------>\n", name);

        xdr.op = __XDR_DECODE;
        xdr.buf = buf;

        if (xdr_arg) {
                if (xdr_arg(&xdr, &nfsarg)) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }

        ret = handler(sockid, req, uid, gid, &nfsarg, buf);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static void __nfs_exec(void *ctx)
{
        sunrpc_req_t *rpc_request = ctx;
        sunrpc_request_t *req = &rpc_request->req;

        DBUG("program %u version %u\n", req->program, req->progversion);

        if (req->program == MOUNTPROG && (req->progversion == MOUNTVERS3 ||
                                          req->progversion == MOUNTVERS1)) {
                nfs_mount(&rpc_request->sockid, &rpc_request->req,
                          rpc_request->uid, rpc_request->gid, &rpc_request->buf);
        } else if (req->program == NFS3_PROGRAM && req->progversion == NFS_V3) {
                nfs_ver3(&rpc_request->sockid, &rpc_request->req,
                          rpc_request->uid, rpc_request->gid, &rpc_request->buf);
        } else if (req->program == ACL_PROGRAM) {
                //UNIMPLEMENTED(__DUMP__);
                nfs_acl(&rpc_request->sockid, &rpc_request->req,
                        rpc_request->uid, rpc_request->gid, &rpc_request->buf);
        } else if (req->program == NLM_PROGRAM) {
                DINFO("NLM REQUEST\n");
                nfs_nlm4(&rpc_request->sockid, &rpc_request->req,
                         rpc_request->uid, rpc_request->gid, &rpc_request->buf);
        }else{
                DERROR("we got wrong prog %u v%u, halt\n", req->program,
                       req->progversion);  //XXX: handle this --gray
        }

        mbuffer_free(&rpc_request->buf);
        mem_cache_free(MEM_CACHE_128, ctx);
}

void nfs_newtask(const sockid_t *sockid, const sunrpc_request_t *req,
                 uid_t uid, gid_t gid, buffer_t *buf)
{
        sunrpc_req_t *rpc_request;
        

#ifdef HAVE_STATIC_ASSERT
        static_assert(sizeof(*rpc_request)  < sizeof(mem_cache128_t), "rpc_request_t");
#endif
        
        rpc_request = mem_cache_calloc(MEM_CACHE_128, 0);
        if (!rpc_request) {
                UNIMPLEMENTED(__DUMP__);
        }

        rpc_request->sockid = *sockid;
        rpc_request->req = *req;
        rpc_request->uid = uid;
        rpc_request->gid = gid;
        mbuffer_init(&rpc_request->buf, 0);
        mbuffer_merge(&rpc_request->buf, buf);

        schedule_task_new("sunrpc", __nfs_exec, rpc_request, 0);
}


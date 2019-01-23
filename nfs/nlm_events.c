/*
 * =====================================================================================
 *
 *       Filename:  nlm_events.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  04/07/2011 10:22:54 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *        Company:
 *
 * =====================================================================================
 */


#include <errno.h>
#include <unistd.h>
#include <rpc/rpc.h>
#include <rpc/svc.h>
#include <rpc/pmap_clnt.h>
#include <sys/poll.h>

#include "ylib.h"
#include "job_dock.h"
#include "job_tracker.h"
#include "nfs_events.h"
#include "nlm_state_machine.h"
#include "nlm_job_context.h"
#include "sunrpc_passive.h"
#include "sunrpc_proto.h"
#include "ynfs_conf.h"
#include "ynet_rpc.h"
#include "nlm_lkcache.h"
#include "xdr_nlm.h"

#include "dbg.h"

extern jobtracker_t *jobtracker;

typedef int (*hash_args_t)(void*);

int nlm_event_handler(job_t *job)
{
        int ret;
        sunrpc_request_t *req;
        state_machine_t handler = NULL;
        xdr_arg_t xdr_arg = NULL;
        nlm_job_context_t *context;
        xdr_t xdr;
        hash_args_t hash_args = NULL;

        req = (void *)job->buf;
#if 0
int xdr_res(xdr_t *, nlm_res *);
int xdr_testres(xdr_t *, nlm_testres *);

int xdr_lockargs(xdr_t *, nlm_lockargs *);
int xdr_unlockargs(xdr_t *, nlm_unlockargs *);

int xdr_cancargs(xdr_t *, nlm_cancargs *);
int xdr_testargs(xdr_t *, nlm_testargs *);
#endif

        switch (req->procedure) {
        case PNLM4_NULL:
                DINFO("NLM4_NULL\n");
                handler = nlm_null_svc;
                xdr_arg = NULL;
                job_setname(job, "nfs_null");
                break;
        case PNLM4_TEST:
                DINFO("NLM4_TEST\n");
                handler = nlm4_test_svc;
                xdr_arg = (xdr_arg_t)xdr_nlm_testargs;
		        hash_args = (hash_args_t)hash_test;
                job_setname(job, "nfs_test");
                break;
        case PNLM4_LOCK:
                handler = nlm4_lock_svc;
                xdr_arg = (xdr_arg_t)xdr_nlm_lockargs;
		        hash_args = (hash_args_t)hash_lock;
                DINFO("NLM4_LOCK\n");
                break;
        case PNLM4_CANCEL:
                handler = nlm4_cancel_svc;
                xdr_arg =(xdr_arg_t)xdr_nlm_cancargs;
                DINFO("NLM4_CANCEL\n");
                break;
        case PNLM4_UNLOCK:
                handler = nlm4_unlock_svc;
                xdr_arg = (xdr_arg_t)xdr_nlm_unlockargs;
		        hash_args = (hash_args_t)hash_unlock;
                DINFO("NLM4_UNLOCK\n");
                break;
        case PNLM4_GRANTED:
                DINFO("NLM4_GRANTED UNIMPLEMENT\n");
                break;
        case PNLM4_TEST_MSG:
                DINFO("NLM4_TEST_MSG UNIMPLEMENT\n");
                break;
        case PNLM4_LOCK_MSG:
                DINFO("NLM4_LOCK_MSG UNIMPLEMENT\n");
                break;
        case PNLM4_CANCEL_MSG:
                DINFO("NLM4_CANCEL_MSG UNIMPLEMENT\n");
                break;
        case PNLM4_UNLOCK_MSG:
                DINFO("NLM4_UNLOCK_MSG UNIMPLEMENT\n");
                break;
        case PNLM4_GRANTED_MSG:
                DINFO("NLM4_GRANTED_MSG UNIMPLEMENT\n");
                break;
        case PNLM4_TEST_RES:
                DINFO("NLM4_TEST_MSG UNIMPLEMENT\n");
                break;
        case PNLM4_LOCK_RES:
                DINFO("NLM4_LOCK_RES UNIMPLEMENT\n");
                break;
        case PNLM4_CANCEL_RES:
                DINFO("NLM4_CANCEL_RES UNIMPLEMENT\n");
                break;
        case PNLM4_UNLOCK_RES:
                DINFO("NLM4_UNLOCK_RES UNIMPLEMNET\n");
                break;
        case PNLM4_GRANTED_RES:
                DINFO("NLM4_GRANTED_RES UNIMPLEMENT\n");
                break;
        case PNLM4_NSM_NOTIFY:
                /*
                 * call
                 */
                handler = nlm4_notify_svc;
                xdr_arg = (xdr_arg_t)xdr_nlm_notifyargs;
		        hash_args = (hash_args_t)hash_notify;
                DINFO("NLM4_NSM_NOTIFY\n");
                break;
        case PNLM4_SHARE:
                DINFO("NLM4_SHARE UNIMPLEMENT\n");
                break;
        case PNLM4_UNSHARE:
                DINFO("NLM4_UNSHARE UNIMPLEMENT\n");
                break;
        case PNLM4_NM_LOCK:
                DINFO("NLM4_NM_LOCK UNIMPLEMENT\n");
                break;
        case PNLM4_FREE_ALL:
                DINFO("NLM4_FREE_ALL UNIMPLEMENT\n");
                break;
        default:
                DERROR("error procedure\n");
        }
        DBUG("<-------------------new procedure %s------------------>\n", job->name);

        if (handler == NULL) {
                ret = EINVAL;
                DERROR("error proc %s\n", job->name);

                GOTO(err_ret, ret);
        }

        DBUG("got proc %s\n", job->name);

        ret = job_context_create(job, sizeof(nlm_job_context_t));
        if (ret)
                GOTO(err_ret, ret);

        xdr.op = __XDR_DECODE;
        xdr.buf = &job->request;
        context = job->context;

        if (xdr_arg) {
                if (xdr_arg(&xdr, &context->arg)) {
                        ret = EINVAL;

                        DERROR("error %s\n", job->name);

                        GOTO(err_ret, ret);
                }
        }

        if (hash_args)
                job->key = hash_args(&context->arg);
        else
                job->key = random();
        job->status = 0;
        job->state_machine = handler;

        ret = jobtracker_insert(job);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

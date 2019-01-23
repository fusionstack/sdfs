#ifndef __SUNRPC_REPLY_H__
#define __SUNRPC_REPLY_H__

#include "job.h"
#include "xdr.h"

extern int sunrpc1_reply_prep(job_t *job, xdr_ret_t xdr_ret, void *buf
                             , int state);
extern int sunrpc1_reply_send(job_t *job, buffer_t *buf,  mbuffer_op_t op);
int sunrpc_reply(const sockid_t *sockid, const sunrpc_request_t *req,
                 int state, void *res, xdr_ret_t xdr_ret);

#endif

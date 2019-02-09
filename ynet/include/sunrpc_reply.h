#ifndef __SUNRPC_REPLY_H__
#define __SUNRPC_REPLY_H__

#include "job.h"
#include "xdr.h"

int sunrpc_reply(const sockid_t *sockid, const sunrpc_request_t *req,
                 int state, void *res, xdr_ret_t xdr_ret);

#endif

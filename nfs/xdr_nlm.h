/*
 * =====================================================================================
 *
 *       Filename:  xdr_nlm.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  04/07/2011 10:09:44 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */
#ifndef __YNLM_XDR_H__
#define __YNLM_XDR_H__

#include "nlm_state_machine.h"
#include "xdr.h"
typedef int (*xdr_arg_t)(xdr_t *, void *);


/* NLM protocol */
#if 1
int xdr_nlm_res(xdr_t *, nlm_res *);
int xdr_nlm_testres(xdr_t *, nlm_testres *);

int xdr_nlm_lockargs(xdr_t *, nlm_lockargs *);
int xdr_nlm_unlockargs(xdr_t *, nlm_unlockargs *);

int xdr_nlm_cancargs(xdr_t *, nlm_cancargs *);
int xdr_nlm_testargs(xdr_t *, nlm_testargs *);
int xdr_nlm_testres(xdr_t *xdrs, nlm_testres *testres); 
int xdr_nlm_res(xdr_t *xdrs, nlm_res *res);
bool_t xdr_nlm_notifyargs(xdr_t *xdrs, nlm_notifyargs *nlm_notify);
uint64_t hash_test(nlm_testargs *args);
uint64_t hash_lock(nlm_lockargs *args);
uint64_t hash_unlock(nlm_unlockargs *args);
uint64_t hash_notify(nlm_notifyargs *args);
#endif
#endif


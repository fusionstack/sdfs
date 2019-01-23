/*
 * =====================================================================================
 *
 *       Filename:  xdr_nlm.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  04/07/2011 10:09:20 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *        Company:
 *
 * =====================================================================================
 */


#include <errno.h>
#include "file_proto.h"
#include "sdfs_lib.h"
#include "ylib.h"
#include "dbg.h"
#include "xdr_nlm.h"
#include "sdfs_id.h"


#define __FALSE -1
#define __TRUE 0
inline uint64_t hash_test(nlm_testargs *args)
{
	fileid_t *fileid;
	fileid = (fileid_t*)args->alock.fh.val;
        return fileid->id * (fileid->volid >> 16) << fileid->id;
}
inline uint64_t hash_lock(nlm_lockargs *args)
{
	fileid_t *fileid;
	fileid = (fileid_t*)args->alock.fh.val;
        return fileid->id * (fileid->volid >> 16) << fileid->id;

}
inline uint64_t hash_unlock(nlm_unlockargs *args)
{
	fileid_t *fileid;
	fileid = (fileid_t*)args->alock.fh.val;
	 return fileid->id * (fileid->volid >> 16) << fileid->id;
}
inline uint64_t hash_notify(nlm_notifyargs *args)
{
	return hash_mem(args->cookies.data, args->cookies.len);
}

int xdr_nlm_netobj(xdr_t *xdrs, struct ynetobj *netobj)
{
        if (__xdr_bytes(xdrs, (char**)&netobj->data,(u_int*)&netobj->len, 1024))
                return __FALSE;
        else
                return __TRUE;
}
#if 0
ret = __xdr_bool(xdrs, &attr->attr_follow);
if (ret)
        GOTO(err_ret, ret);
#endif
int xdr_nlm_lock(xdr_t *xdrs, struct nlm_lock *lock)
{
        if (__xdr_bytes(xdrs, (char**)&lock->caller,(u_int*)&lock->len,1024))
                goto out;
        if (__xdr_bytes(xdrs, (char**)&lock->fh.val,(u_int*)&lock->fh.len,64))
                goto out;
        if (__xdr_bytes(xdrs, (char**)&lock->oh.len,(u_int*)&lock->oh.len,1024))
                goto out;
        if (__xdr_uint32(xdrs, &lock->svid))
                goto out;
        if (__xdr_uint64(xdrs, &lock->l_offset))
                goto out;
        if (__xdr_uint64(xdrs, &lock->l_len))
                goto out;
        return __TRUE;
out:
        return __FALSE;
}


int xdr_nlm_holder(xdr_t *xdrs, nlm4_holder *holder)
{
        if (__xdr_bool(xdrs, &holder->exclusive))
                goto out;
        /*there is maybe a bug!!!!!!!!!!!!!!!!!!!!!
         */
        if (__xdr_uint32(xdrs, (uint32_t*)&holder->svid))
                goto out;

        if (xdr_nlm_netobj(xdrs, &holder->oh))
                goto out;
        if (__xdr_uint64(xdrs, &holder->l_offset))
                goto out;
        if (__xdr_uint64(xdrs, &holder->l_len))
                goto out;
        return __TRUE;
out:
        return __FALSE;
}

int xdr_nlm_testrply(xdr_t *xdrs, nlm_testrply *testrply)
{
        if (__xdr_enum(xdrs, (enum_t*)&testrply->status))
                return __FALSE;
        switch (testrply->status) {
        case  NLM4_DENIED:
                if (xdr_nlm_holder(xdrs, &testrply->nlm_testrply_u.holder))
                        goto out;
                break;
        default:
                break;
        }

        return __TRUE;
out:
        return __FALSE;
}

int xdr_nlm_lockargs(xdr_t *xdrs, nlm_lockargs *lockargs)
{
        if (xdr_nlm_netobj(xdrs, &lockargs->cookies))
                goto out;
        if (__xdr_bool(xdrs, &lockargs->block))
                goto out;
        if (__xdr_bool(xdrs, &lockargs->exclusive))
                goto out;
        if (xdr_nlm_lock(xdrs, &lockargs->alock))
                goto out;
        if (__xdr_bool(xdrs, &lockargs->reclaim))
                goto out;
        if (__xdr_uint32(xdrs, (uint32_t*)&lockargs->state))
                goto out;
        return __TRUE;
out:
        return __FALSE;
}

int xdr_nlm_unlockargs(xdr_t *xdrs, nlm_unlockargs *unlockargs)
{
        if (xdr_nlm_netobj(xdrs, &unlockargs->cookies))
                goto out;
        if (xdr_nlm_lock(xdrs, &unlockargs->alock))
                goto out;
        return __TRUE;
out:
        return __FALSE;
}

int xdr_nlm_notifyargs(xdr_t *xdrs, nlm_notifyargs *notifyargs)
{
        if (xdr_nlm_netobj(xdrs, &notifyargs->cookies))
                goto out;
        if (__xdr_uint32(xdrs, (uint32_t*)&notifyargs->state))
                goto out;
        if (__xdr_opaque(xdrs, notifyargs->priv, 16))
                goto out;
        return __TRUE;
out:
        return __FALSE;
}

int xdr_nlm_cancargs(xdr_t *xdrs, nlm_cancargs *cancargs)
{
        if (xdr_nlm_netobj(xdrs, &cancargs->cookies))
                goto out;
        if (__xdr_bool(xdrs, &cancargs->block))
                goto out;
        if (__xdr_bool(xdrs, &cancargs->exclusive))
                goto out;
        if (xdr_nlm_lock(xdrs, &cancargs->alock))
                goto out;
        return __TRUE;
out:
        return __FALSE;
}

int xdr_nlm_testargs(xdr_t *xdrs, nlm_testargs *testargs)
{
        if (xdr_nlm_netobj(xdrs, &testargs->cookies))
                goto out;
        if (__xdr_bool(xdrs, &testargs->exclusive))
                goto out;
        if (xdr_nlm_lock(xdrs, &testargs->alock))
                goto out;
        return __TRUE;
out:
        return __FALSE;
}

int xdr_nlm_testres(xdr_t *xdrs, nlm_testres *testres)
{
        if (xdr_nlm_netobj(xdrs, &testres->cookies))
                goto out;
        if (xdr_nlm_testrply(xdrs, &testres->test_stat))
                goto out;
        return __TRUE;
out:
        return __FALSE;
}
int xdr_nlm_res(xdr_t *xdrs, nlm_res *res)
{
        if (xdr_nlm_netobj(xdrs, &res->cookies))
                goto out;
        if (__xdr_enum(xdrs, (enum_t *)&res->stat))
                goto out;
        return __TRUE;
out:
        return __FALSE;
}


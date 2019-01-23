/*
 * =====================================================================================
 *
 *       Filename:  nlm_nsm.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  04/11/2011 02:42:34 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */
#include "nlm_nsm.h"
#include "xdr.h"
#include "dbg.h"
bool_t xdr_nsm_myid(XDR *xdrs,  struct my_id  *my_id) {
        bool_t ret;
	char buf[64];
	memset(buf, 0x0, 64);
	memcpy(buf, my_id->name, my_id->len);
	DINFO("name %s  len %u\n",buf ,my_id->len);
        ret = xdr_bytes(xdrs, &my_id->name, (unsigned int*)&my_id->len,1024);

        ret = xdr_int(xdrs, &my_id->my_prog);

        ret = xdr_int(xdrs, &my_id->my_vers);

        ret = xdr_int(xdrs, &my_id->my_proc);
        return ret;
}
bool_t xdr_nsm_monid(XDR *xdrs, struct mon_id *mon_id)
{
        bool_t ret;
        ret = xdr_bytes(xdrs, &mon_id->name, (unsigned int*)&mon_id->len, 1024);

        ret = xdr_nsm_myid(xdrs, &mon_id->my_id);
        return ret;
}



bool_t xdr_nsm_monargs(XDR *xdrs, struct mon *mon)
{
	char data[16];

        bool_t ret;
        ret = xdr_nsm_monid(xdrs, &mon->mon_id);
#if 0
	for (i = 0; i < 16; i++)
	{
		ret = xdr_bytes(xdrs,&buf,(unsigned int*)&len, 1);
	}
	if (ret!= 1)
		DINFO("xdr error\n");
#endif
	ret = xdr_opaque(xdrs,data,16);
        return ret;
}
bool_t xdr_nsm_unmonargs(XDR *xdrs, struct mon_id *mon_id)
{
        bool_t ret;
        ret = xdr_nsm_monid(xdrs, mon_id);

        return ret;
}

bool_t xdr_nsm_monres(XDR *xdrs,  struct sm_stat_res *res)
{
        bool_t ret;
        xdr_enum(xdrs, (enum_t*)&res->res_stat);

        ret = xdr_int(xdrs, &res->state);
        return ret;
}
bool_t xdr_nsm_unmonres(XDR *xdrs,  struct sm_stat *res)
{
        bool_t ret;
        ret = xdr_int(xdrs, &res->state);
        return ret;
}

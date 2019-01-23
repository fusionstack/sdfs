/*
 * =====================================================================================
 *
 *       Filename:  nlm_nsm.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  04/11/2011 02:43:15 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *        Company:
 *
 * =====================================================================================
 */
#ifndef NLM_NSM_H_
#define NLM_NSM_H_
#include <rpc/rpc.h>
#include <rpc/xdr.h>

#define SM_PROGRAM  100024
#define SM_VERSION  1
#define SM_MON 2
#define SM_UNMON 3
#define SM_UNMON_ALL 4
#define SM_NOTIFY 6

typedef enum {
        STAT_SUCC = 0,
        STAT_FAIL = 1,
} statres;

struct sm_stat_res {
        statres res_stat;
        int     state;
};

struct sm_stat {
        int     state;
} ;

struct my_id {
        int len;
        char *name;
        int my_prog;
        int my_vers;
        int my_proc;
};

struct mon_id {
        int  len;
        char *name;
        struct my_id my_id;
};

struct mon {
        struct mon_id mon_id;
};
struct state_chg {
        int len;
        char *name;
        int state;
};
bool_t xdr_nsm_monargs(XDR *xdrs, struct mon *mon);
bool_t xdr_nsm_unmonargs(XDR *xdrs, struct mon_id *mon_id);
bool_t xdr_nsm_monres(XDR *xdrs,  struct sm_stat_res *res);
bool_t xdr_nsm_unmonres(XDR *xdrs,  struct sm_stat *res);
bool_t xdr_nsm_myid(XDR *xdrs,  struct my_id  *my_id);
#endif

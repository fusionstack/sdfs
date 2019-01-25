/*
 * =====================================================================================
 *
 *       Filename:  mini_nlm.c
 *
 *    Description:  network lock manage implement
 *
 *        Version:  1.0
 *        Created:  04/07/2011 09:25:25 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *        Company:
 *
 * =====================================================================================
 */


#include <sys/types.h>
#include <sys/wait.h>
#include <rpc/pmap_clnt.h>
#include <errno.h>
#include <getopt.h>
#if 0
#define DBG_SUBSYS S_YNFS
#endif
#include "get_version.h"
#include "fd_cache.h"
#include "fh_cache.h"
#include "stat_cache.h"
#include "job_dock.h"
#include "nlm_events.h"
#include "nlm_state_machine.h"
#include "nlm_lkcache.h"
#include "hostcache.h"
#include "ylib.h"
#include "sunrpc_passive.h"
#include "sunrpc_proto.h"
#include "sdfs_lib.h"
#include "xdr_nfs.h"
#include "ynet_rpc.h"
#include "ynfs_conf.h"
#include "md_lib.h"
#include "configure.h"
#include "nfs_proc.h"
#include "net_table.h"
#include "proc.h"
#include "dbg.h"

int opt_portmapper = 1;
int grace_period   = 1;
void * grace_period_change(void *arg)
{
        (void)arg;
        sleep(8);
        grace_period = 0;
        DINFO("grace_period finished\n");
        return NULL;
}

int register_nlm_service()
{
        int ret;

        if (opt_portmapper)
                pmap_unset(NLM_PROGRAM, NLM_VERSION);

        if (! pmap_set (NLM_PROGRAM, NLM_VERSION, opt_portmapper ? IPPROTO_TCP : 0,
                        3001)) {
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }
        return 0;
err_ret:
        return ret;
}


int nlm_srv(int daemon)
{
        int ret;
        pthread_t tid;
        const char *service = NLM_SERVICE_DEF;
#if 0
        signal process
#endif
#if 0
        ret = mountlist_init();
        if (ret)
                GOTO(err_ret, ret);

        /* init write verifier */
        regenerate_write_verifier();

	memset(&net_op, 0x0, sizeof(net_proto_t));

	//net_op.reset_handler = ynfs_reset_handler;
#endif
        ret = ly_prep(daemon, "nlm", -1);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init(daemon, "nlm", -1);
        if (ret)
                GOTO(err_ret, ret);

        ret = network_connect_mond(0);
        if (ret)
                GOTO(err_ret, ret);

#if 0
        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);
        ret = pthread_create(&th_cdsstate, &ta, handler_ynfsstate, NULL);
#endif
        DINFO("nlm started...\n");

        ret = rpc_start(); /*begin serivce*/
        if (ret)
                GOTO(err_ret, ret);

        sunrpc_procedure_register(NLM_PROGRAM, nlm_event_handler);
        ret = nlmlk_cache_init(&nlmlk_cache);
        if (ret)
                GOTO(err_ret, ret);
        ret = hostcache_init(&hostcache);
        if (ret)
                GOTO(err_ret, ret);

        ret = sunrpc_tcp_passive(service);
        if (ret)
                GOTO(err_ret, ret);

        ret = register_nlm_service();
        if (ret)
                DWARN("register nlm error %s\n", strerror(ret));
        pthread_create(&tid, NULL, grace_period_change, NULL);
#if 1
        ret = nlm_unmon_process();
        if (ret) {
                DINFO("UNMON ER\n");
        } else
                DINFO("UNMON OK\n");
#endif

        while (srv_running) { //we got nothing to do here
                ret = netable_wait(&ng.mds_nh, 1);
                if (ret) {
                        if (ret == ETIMEDOUT)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }
        }
        DINFO("exiting...\n");
        return 0;
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret, daemon = 1, maxcore = 0;
        int c_opt;
        (void)c_opt;
        (void)daemon;
        (void)maxcore;
        (void)argc;
        (void)argv;

        while (srv_running) {
                break;
        }

        ret = nlm_srv(daemon);
        if (ret)
                GOTO(err_ret, ret);
        return 0;
err_ret:
        return ret;
}


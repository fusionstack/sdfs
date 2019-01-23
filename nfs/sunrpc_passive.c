

#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>


#define DBG_SUBSYS S_YRPC

#include "ynet_net.h"
#include "../sock/sock_tcp.h"
#include "job_tracker.h"
#include "net_global.h"
#include "sunrpc_passive.h"
#include "sunrpc_proto.h"
#include "dbg.h"
#include "configure.h"

event_job_t sunrpc_nfs3_handler;
event_job_t sunrpc_mount_handler;
event_job_t sunrpc_acl_handler;
event_job_t sunrpc_nlm_handler;
extern jobtracker_t *sunrpc_jobtracker;

#if 0
void sunrpc_procedure_register(uint32_t program, event_job_t handler)
{
        if (program == MOUNTPROG)
                sunrpc_mount_handler = handler;
        else if (program == NFS3_PROGRAM)
                sunrpc_nfs3_handler = handler;
        else if (program == ACL_PROGRAM)
                sunrpc_acl_handler = handler;
        else if (program == NLM_PROGRAM)
                sunrpc_nlm_handler = handler;
        else {
                DERROR("we got wrong program %u-----%u\n", program, (unsigned int)NLM_PROGRAM);
                YASSERT(0);
        }
}
#endif

static void *__sunrpc_tcp_passive__(void *_arg)
{
        int ret, *_sd, sd;

        _sd = _arg;
        sd = *_sd;

        yfree((void**)&_sd);
        
        DINFO("--------------start listen %d--------\n", sd);

        while (1) { 
                ret = sock_poll_sd(sd, 1000 * 1000, POLLIN);
                if (unlikely(ret)) {
                        if (ret == ETIMEDOUT || ret == ETIME)
                                continue;
                        else
                                GOTO(err_ret, ret);
                 }

                DINFO("------------accept from %d------------\n", sd);

                ret = sunrpc_accept(sd);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return NULL;
err_ret:
        return NULL;
}

int sunrpc_tcp_passive(const char *port)
{
        int ret, sd, *_sd;
        pthread_t th;
        pthread_attr_t ta; 

        ret = tcp_sock_hostlisten(&sd, NULL, port,
                                  YNET_QLEN, YNET_RPC_NONBLOCK, 0);
        if (ret) {
                GOTO(err_ret, ret);
        }

        DINFO("-----------------sunrpc port %s @ %d--------\n", port, sd);
        
        ret = ymalloc((void**)&_sd, sizeof(*_sd));
        if (ret)
                GOTO(err_ret, ret);
        
        *_sd = sd;
        
        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        ret = pthread_create(&th, &ta, __sunrpc_tcp_passive__, _sd);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

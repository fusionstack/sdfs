

#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>


#define DBG_SUBSYS S_YRPC

#include "ynet_net.h"
#include "../sock/sock_tcp.h"
#include "net_global.h"
#include "sunrpc_passive.h"
#include "sunrpc_proto.h"
#include "configure.h"
#include "main_loop.h"
#include "dbg.h"

static void *__sunrpc_tcp_passive__(void *_arg)
{
        int ret, *_sd, sd;

        _sd = _arg;
        sd = *_sd;

        yfree((void**)&_sd);

        main_loop_hold();        
        
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

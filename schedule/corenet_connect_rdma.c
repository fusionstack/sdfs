

#include <limits.h>
#include <time.h>
#include <string.h>
#include <sys/epoll.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBSCHEDULE

#include "sysutil.h"
#include "net_proto.h"
#include "ylib.h"
#include "../net/xnect.h"
#include "net_table.h"
#include "rpc_table.h"
#include "configure.h"
#include "net_global.h"
#include "job_dock.h"
#include "msgarray.h"
#include "schedule.h"
#include "bh.h"
#include "timer.h"
#include "adt.h"
#include "cluster.h"
#include "../../ynet/sock/sock_tcp.h"
#include "core.h"
#include "corerpc.h"
#include "corenet_maping.h"
#include "corenet.h"
#include "dbg.h"

typedef struct {
        int hash;
        nid_t from;
        nid_t to;
} corenet_msg_t;

static int __rdma_connect_request(va_list ap)
{
        const char *host = va_arg(ap, const char *);
        const char *port = va_arg(ap, const char *);
        core_t *core = va_arg(ap, core_t *);
        sockid_t *sockid = va_arg(ap, sockid_t *);

        va_end(ap);

        return corenet_rdma_connect_by_channel(host, port, core, sockid);
}

int corenet_rdma_connect(const nid_t *nid, sockid_t *sockid)
{
        int ret;
        char host[MAX_NAME_LEN], port[MAX_NAME_LEN];
        core_t *core = core_self();

        ANALYSIS_BEGIN(0);

        ret = network_connect(nid, NULL, 0, 0);
        if (ret)
                GOTO(err_ret, ret);
        
        ret = network_rname1(nid, host);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        snprintf(port, MAX_NAME_LEN, "%u", gloconf.rdma_base_port + core->hash);

        ret = schedule_newthread(SCHE_THREAD_MISC, _random(), FALSE, "rdma_connect", -1, __rdma_connect_request,
                        host, port, core, sockid);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        ANALYSIS_END(0, 1000 * 1000 * 5, NULL);
        return 0;
err_ret:
        ANALYSIS_END(0, 1000 * 1000 * 5, NULL);
        return ret;
}

static void * __corenet_passive_rdma(void *arg)
{
        int ret;
        int cpu_idx = *(int *)arg;

        yfree(&arg);

        ret = corenet_rdma_listen_by_channel(cpu_idx);
        if (ret)
                GOTO(err_ret, ret);

#if CORENET_RDMA_ON_ACTIVE_WAIT
        ret = corenet_rdma_on_passive_event(cpu_idx);
        if (ret)
                GOTO(err_ret, ret);
#endif

        return NULL;
err_ret:
        UNIMPLEMENTED(__DUMP__);
        return NULL;
}

int corenet_rdma_passive()
{
        int i, ret;
        pthread_t th;
        pthread_attr_t ta;
        int *p = NULL;

        ret = corenet_rdma_evt_channel_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (i = 0; i < cpuset_useable(); i++) {
                (void) pthread_attr_init(&ta);
                (void) pthread_attr_setdetachstate(&ta,PTHREAD_CREATE_DETACHED);

                ymalloc((void **)&p, sizeof(int));
                *p = i;
                ret = pthread_create(&th, &ta, __corenet_passive_rdma, (void*)p);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

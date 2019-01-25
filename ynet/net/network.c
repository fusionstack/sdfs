#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <openssl/md5.h>
#include <openssl/aes.h>
#include <dirent.h>

#define DBG_SUBSYS S_LIBYNET

#include "rpc_proto.h"
#include "ynet_rpc.h"
#include "net_global.h"
#include "fnotify.h"
#include "sdevent.h"
#include "timer.h"
#include "bh.h"
#include "conn.h"
#include "net_global.h"
#include "network.h"
#include "mond_rpc.h"
#include "schedule.h"
#include "configure.h"
#include "dbg.h"

typedef struct {
        nid_t nid;
        sem_t sem;
        int force;
} net_conn_context_t;

#if 0
static worker_handler_t network_connect_worker;
#endif

int network_connect_byname(const char *name, nid_t *nid)
{
        int ret;
        char buf[MAX_BUF_LEN];
        ynet_net_info_t *info;
        net_handle_t nh;

retry:
        ret = maping_host2nid(name, nid);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        info = (void *)buf;
        ret = maping_nid2netinfo(nid, info);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = netable_connect_info(&nh, info, 0);
        if (unlikely(ret)) {
                if (ret == ENONET)
                        GOTO(err_ret, ret);
                else if (ret == ENOKEY) {
                        char tmp[MAX_BUF_LEN];
                        DINFO("no such key, drop %u\n", nid->id);
                        snprintf(tmp, MAX_PATH_LEN, NID_FORMAT, NID_ARG(&info->id));
                        maping_drop(NID2NETINFO, tmp);
                        maping_drop(HOST2NID, name);
                        goto retry;
                } else
                        GOTO(err_ret, ret);
        }

        *nid = nh.u.nid;

        return 0;
err_ret:
        return ret;
}

static int __network_connect(const nid_t *nid)
{
        int ret;
        char buf[MAX_BUF_LEN];
        ynet_net_info_t *info;
        net_handle_t nh;

        info = (void *)buf;
        ret = conn_getinfo(nid, info);
        if (unlikely(ret)) {
                if (ret == ENOKEY)
                        ret = ENONET;
                GOTO(err_ret, ret);
        }

        if (nid_cmp(&info->id, net_getnid()) == 0) {
                goto out;
        }

        ret = netable_connect_info(&nh, info, 1);
        if (unlikely(ret))
                GOTO(err_ret, ret);

out:

        return 0;
err_ret:
        return ret;
}

int network_connect_mond(int force)
{
        int ret;
        nid_t nid;

        ANALYSIS_BEGIN(0);

        (void) force;
        
        if (net_islocal(net_getadmin())) {
                DBUG("skiped\n");
                return 0;
        }

        if (netable_connected(net_getadmin()) && force == 0) {
                return 0;
        }

        ret = maping_getmaster(&nid, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (net_islocal(&nid)) {
                return 0;
        }
        
        DBUG("get nid %d\n", nid.id);
        
        ret = __network_connect(&nid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = mond_rpc_null(&nid);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        DBUG("set admin %u\n", nid.id);
        net_setadmin(&nid);
        
#ifdef RPC_ASSERT
        ANALYSIS_ASSERT(0, 1000 * 1000 * (_get_rpc_timeout() * 3), NULL);
#else
        ANALYSIS_END(0, IO_WARN, NULL);
#endif
        
        return 0;
err_ret:
#ifdef RPC_ASSERT
        ANALYSIS_ASSERT(0, 1000 * 1000 * (_get_rpc_timeout() * 3), NULL);
#else
        ANALYSIS_END(0, IO_WARN, NULL);
#endif
        return ret;
}

static int __network_connect_exec(const nid_t *nid, int force)
{
        int ret, retry = 0;
        net_handle_t nh;
        char buf[MAX_BUF_LEN];
        ynet_net_info_t *info;

        ANALYSIS_BEGIN(0);

        DBUG("connect to %s\n", netable_rname_nid(nid));

        if (netable_connected(nid)) {
                DINFO("%s already connected\n", netable_rname_nid(nid));
                return 0;
        }

        ANALYSIS_END(0, IO_WARN, NULL);
retry:
        if (!net_islocal(net_getadmin())  && !netable_connected(net_getadmin())) {
                DINFO("connect to master %s, local %s\n",
                      network_rname(net_getadmin()),
                      network_rname(net_getnid()));
                ret = network_connect_mond(0);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        ANALYSIS_END(0, IO_WARN, NULL);

        info = (void *)buf;
        ret = maping_nid2netinfo(nid, info);
        if (unlikely(ret)) {
                if (ret == ENOENT || ret == ENOKEY) {
                        DBUG("net "NID_FORMAT" ret %u\n", NID_ARG(nid), ret);
                        ret = ENONET;
                        GOTO(err_ret, ret);
                } else if (ret == EAGAIN || ret == ENOSYS) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 3, (100 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        ANALYSIS_END(0, IO_WARN, NULL);

        YASSERT(info->len < MAX_BUF_LEN);

        ret = netable_connect_info(&nh, info, force);
        if (unlikely(ret)) {
                if (ret == ENONET) {
                        DBUG("connect %s ret %u\n", info->name, ret);
                        GOTO(err_ret, ret);
                } else if (ret == ENOKEY) {
                        DINFO("no such key, drop %u\n", nid->id);
                        char tmp[MAX_BUF_LEN];
                        snprintf(tmp, MAX_PATH_LEN, NID_FORMAT, NID_ARG(nid));
                        maping_drop(NID2NETINFO, tmp);
                        goto retry;
                } else
                        GOTO(err_ret, ret);
        }

        DINFO("%s connected\n", info->name);

        ANALYSIS_END(0, IO_WARN, NULL);

        return 0;
err_ret:
        return ret;
}

typedef struct {
        int pipe[2];
        nid_t nid;
        int force;
} conn_arg_t;

static void *__network_connect__(void *_arg)
{
        conn_arg_t *arg = _arg;
        int retval;

        retval = __network_connect_exec(&arg->nid, arg->force);
        write(arg->pipe[1], &retval, sizeof(retval));
        close(arg->pipe[1]);
        yfree((void **)&arg);

        pthread_exit(NULL);
}


static int __network_connect_wait_thread(const nid_t *nid, int force, int timeout)
{
        int ret, out, retval;
        pthread_t th;
        pthread_attr_t ta;
        conn_arg_t *arg;

        ANALYSIS_BEGIN(0);
        
        ret = ymalloc((void **)&arg, sizeof(*arg));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        arg->nid = *nid;
        arg->force = force;
        ret = pipe(arg->pipe);
        if (unlikely(ret))
                GOTO(err_free, ret);

        out = arg->pipe[0];
        
        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        ret = pthread_create(&th, &ta, __network_connect__, (void *)arg);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        if (timeout) {
                ret = sock_poll_sd(out, (1000 * 1000) * timeout, POLLIN);
                if (unlikely(ret)) {
                        close(out);
                        DWARN("connect %s timeout\n", network_rname(nid));
                        GOTO(err_ret, ret);
                }

                ret = read(out, &retval, sizeof(retval));
                if (unlikely(ret < 0)) {
                        UNIMPLEMENTED(__DUMP__);
                }

                close(out);
                
                if (retval) {
                        ret = retval;
                        GOTO(err_ret, ret);
                }

                
                if (!netable_connected(nid)) {
                        ret = ENONET;
                        GOTO(err_ret, ret);
                }

                DINFO("connect finish\n");
        } else {
                close(out);
                ret = ENONET;
                GOTO(err_ret, ret);
        }

        ANALYSIS_END(0, IO_WARN * (timeout + 1), NULL);
        
        return 0;
err_free:
        yfree((void **)&arg);
err_ret:
        ANALYSIS_END(0, IO_WARN * (timeout + 1), NULL);
        return ret;
}

static int __network_connect_exec_schedule(va_list ap)
{
        const nid_t *nid = va_arg(ap, const  nid_t *);
        int force = va_arg(ap, int);
        int timeout = va_arg(ap, int);

        va_end(ap);

        return __network_connect_wait_thread(nid, force, timeout);
}


static int __network_connect_wait(const nid_t *nid, int timeout, int force)
{
        int ret;

        if (!netable_connectable(nid, force)) {
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }

        DBUG("try to connect %s \n", netable_rname_nid(nid));

        netable_update_retry(nid);

        if (schedule_running()) {
                ret = schedule_newthread(SCHE_THREAD_MISC, _random(), FALSE, "network_connect",
                                         -1, __network_connect_exec_schedule, nid, force, timeout);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        } else {
                ret = __network_connect_wait_thread(nid, force, timeout);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int network_connect1(const nid_t *nid)
{
        return network_connect(nid, NULL, 1, 0);
}

int network_ltime(const nid_t *nid, time_t *ltime)
{
        return network_connect(nid, ltime, 0, 0);
}

void network_ltime_reset(const nid_t *nid, time_t ltime, const char *why)
{
        //DINFO("reset %s %s\n", network_rname(nid), why);

#if 0
        netable_ltime_reset(nid, ltime, why);
#else
        netable_close(nid, why, &ltime);
#endif
}


time_t network_ltime1(const nid_t *nid)
{
        int ret;
        time_t ltime;

        ret = network_connect(nid, &ltime, 0, 0);
        if (unlikely(ret)) {
                ltime = 0;
        }

        return ltime;
}

int IO_FUNC network_connect(const nid_t *nid, time_t *_ltime, int _timeout, int _force)
{
        int ret, force, timeout;
        time_t ltime;
        instat_t instat;

        YASSERT(nid->id > 0);

retry:
        ltime = netable_conn_time(nid);
        if (likely(ltime != 0)) {
                if (_ltime)
                        *_ltime = ltime;
        
                goto out;
        }


        if (ng.daemon == 0) {
                ret = mond_rpc_getstat(nid, &instat);
                if (ret)
                        GOTO(err_ret, ret);

                if (!instat.online) {
                        ret = ENONET;
                        GOTO(err_ret, ret);
                }

                timeout = 10;
                force = 1;
        } else if (net_islocal(nid)) {
                timeout = 10;
                force = 1;
        } else {
                timeout = _timeout;
                force = _force;
        }

        if (timeout) {
                ret = __network_connect_wait(nid, timeout, force);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                goto retry;
        } else {
                ret = ENONET;
                goto err_ret;
        }

out:
        return 0;
err_ret:
        return _errno_net(ret);
}

const char *network_rname(const nid_t *nid)
{
        return netable_rname_nid(nid);
}

int network_rname1(const nid_t *nid, char *name)
{
        int ret;

        network_connect(nid, NULL, 1, 0);
        
        ret = netable_rname1(nid, name);
        if (ret) {
                ret = maping_nid2host(nid, name);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        DBUG("%u %s\n", nid->id, name);
        
        return 0;
err_ret:
        return ret;
}

void network_close(const nid_t *nid, const char *why, const time_t *ltime)
{
        netable_close(nid, why, ltime);
}

static int __network_connect_master__()
{
        int ret;
        nid_t nid;

        ANALYSIS_BEGIN(0);

        ret = maping_getmaster(&nid, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __network_connect(&nid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DINFO("set admin %u\n", nid.id);
        net_setadmin(&nid);
        
#ifdef RPC_ASSERT
        ANALYSIS_ASSERT(0, 1000 * 1000 * (_get_rpc_timeout() * 3), NULL);
#else
        ANALYSIS_END(0, IO_WARN, NULL);
#endif
        
        return 0;
err_ret:
#ifdef RPC_ASSERT
        ANALYSIS_ASSERT(0, 1000 * 1000 * (_get_rpc_timeout() * 3), NULL);
#else
        ANALYSIS_END(0, IO_WARN, NULL);
#endif
        return ret;
}


static void *__network_connect_master(void *args)
{
        (void) args;

        while (1) {
                sleep(1);

                __network_connect_master__();
        }

        return NULL;
}

int network_init()
{
        int ret;

        UNIMPLEMENTED(__NULL__);
        
        return 0;

#if 0
        DINFO("network init\n");
        ret = jobdock_worker_create(&network_connect_worker, "network_connect");
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif

        if (ng.daemon == 0) {
                ret = sy_thread_create2(__network_connect_master, NULL, "master connect");
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}


//just for compatible, will be removed
int network_connect2(const net_handle_t *nh, int force)
{
        return network_connect(&nh->u.nid, NULL, 1, force);
}

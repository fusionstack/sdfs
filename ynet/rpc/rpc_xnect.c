#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <netdb.h>
#include <sys/ioctl.h>
#ifdef __CYGWIN__
#include <net/if.h>
#else
#include <linux/if.h>
#endif

#define DBG_SUBSYS S_YRPC

#include "ynet_net.h"
#include "net_global.h"
#include "main_loop.h"
#include "ylib.h"
#include "ynet_rpc.h"
#include "sdfs_conf.h"
#include "configure.h"
#include "md.h"
#include "../sock/sock_tcp.h"
#include "nodeid.h"
#include "network.h"
#include "dbg.h"
#include "atomic_id.h"
#include "yatomic.h"
#ifndef __CYGWIN__
//#include "leveldb_util.h"
//#include "leveldb_queue.h"
#endif

#define POLL_TMO 2

int rpc_info2nid(net_handle_t *nh, const ynet_net_info_t *info)
{
        (void) nh;
        (void) info;
        UNIMPLEMENTED(__DUMP__);

        return 0;
                
        //return net_info2nid(nh, info);
}

/**
 * @param info
 * @param _nh[out]
 * @param infonum[out]
 */
#if 0
int rpc_info2nid_multi(net_handle_t **_nh, ynet_net_info_t *info,
                       uint32_t *infonum)
{
        int ret, nids;
        uint32_t len, i;
        void *ptr;
        net_handle_t *nh;
        ynet_net_info_t *netinfo;

        if (*infonum == 0) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }

        len = sizeof(net_handle_t) * *infonum;

        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_ret, ret);

        nh = (net_handle_t *)ptr;

        netinfo = info;
        nids = 0;

        for (i = 0; i < *infonum; i++) {
                if (!(netinfo->id.id == ng.local_nid.id)) {

                        if (netinfo->info_count > 0) {
                                ret = net_info2nid(&nh[nids], netinfo);
                                if (ret)
                                        GOTO(err_nid, ret);

                                nids++;
                        } else
                                break;
                } else {
                        //DWARN("nid %llu_v%u\n", (LLU)netinfo->id.id, netinfo->id.version);
                }

                netinfo = (void *)netinfo + sizeof(ynet_net_info_t)
                        + sizeof(ynet_sock_info_t) * info->info_count;
        }

        if (nids == 0) {
                ret = ENONET;
                GOTO(err_nid, ret);
        }

        *_nh = nh;
        *infonum = nids;

        DBUG("infonum %u\n", *infonum);

        return 0;
err_nid:
        yfree((void **)&nh);
err_ret:
        return ret;
}
#endif

int  rpc_master_listen(int *_sd, const char *addr, const char *port)
{
        int ret, sd;

        DINFO("listen %s:%s\n", addr, port);

        ret = rpc_hostlisten(&sd, addr, port, 256, YNET_RPC_BLOCK);
        if (ret)
                GOTO(err_ret, ret);

        *_sd = sd;

        return 0;
err_ret:
        return ret;
}

#if 0
int rpc_master_get(ynet_net_info_t *info, uint32_t buflen, const char *addr,
                   const char *port, int live, int timeout)
{
        int ret, sd, tmo, step, retry = 0;
        masterget_req_t req;
        masterget_rep_t *rep;
        char buf[MAX_PATH_LEN], hostname[MAX_NAME_LEN];

        DINFO("connect to master %s:%s, a moment please..\n", addr, port);

        gethostname(hostname, MAX_NAME_LEN);
        
        rep = (void *)buf;
        req.live = live;
        req.nid = ng.local_nid;
        strcpy(req.clustername, gloconf.cluster_name);
        snprintf(req.nodename, MAX_LINE_LEN, "%s:%s", hostname, ng.name);

        if (timeout == -1) {
                tmo = 1;
                step = 0;
        } else {
                YASSERT(timeout > 0);
                tmo = timeout;
                step = POLL_TMO;
        }

        while (1) {
                ret = sock_hostconnect(&sd, addr, port, YNET_RPC_BLOCK, timeout);
                if (ret) {
                        if (ret == ENETUNREACH) {
                                GOTO(err_ret, ret);
                        } else if (ret == ETIME || ret == ECONNREFUSED)
                                goto err_ret;
                        else
                                GOTO(err_ret, ret);
                }

                ret = _send(sd, &req, sizeof(req), MSG_NOSIGNAL);
                if (ret < 0) {
                        ret = -ret;
                        close(sd);
                        if (ret == ETIME || ret == EINTR) {
                                DINFO("get master retry...\n");
                                sleep(5);
                                continue;
                        } else if (ret == ECONNREFUSED || ret == EHOSTUNREACH
                                   || ret == ETIMEDOUT)
                                goto err_ret;
                        else
                                GOTO(err_ret, ret);
                }

                ret = sock_poll_sd(sd, 1000 * POLL_TMO, POLLIN);
                if (ret) {
                        close(sd);
                        if (ret == ETIME || ret == EINTR) {
                                DINFO("get master retry...\n");
                                tmo -= step;
                                if (tmo < 0) {
                                        ret = ETIMEDOUT;
                                        goto err_ret;
                                }

                                if (srv_running)
                                        continue;
                                else {
                                        ret = ETIMEDOUT;
                                        goto err_ret;
                                }
                        } else
                                GOTO(err_ret, ret);
                }

retry:
                ret = _recv(sd, rep, MAX_BUF_LEN, MSG_DONTWAIT);
                if (ret < 0) {
                        ret = -ret;
                        if (ret == EAGAIN) {
                                USLEEP_RETRY(err_sd, ret, retry, retry, 50, 100);
                        } else
                                GOTO(err_sd, ret);
                }

                close(sd);
                break;
        }

        YASSERT(buflen > rep->infolen);

        memcpy(info, rep->info, rep->infolen);

        DBUG("local nid "DISKID_FORMAT"\n", DISKID_ARG(&ng.local_nid));

        if (live) {
                if (nid_cmp(&rep->nid, &ng.local_nid) == 0) {
                        DINFO("got master info %u reuse nid "DISKID_FORMAT"\n",
                              rep->infolen, DISKID_ARG(&ng.local_nid));
                } else {
                        if (ng.local_nid.id != 0) {
                                DWARN("old data detected in %s, "
                                      "local "DISKID_FORMAT" rep "DISKID_FORMAT", exiting\n",
                                      ng.home, DISKID_ARG(&ng.local_nid), DISKID_ARG(&rep->nid));
                                EXIT(2);
                        }

                        ng.local_nid = rep->nid;
                        DINFO("got master info %u new nid "DISKID_FORMAT"\n",
                              rep->infolen, DISKID_ARG(&ng.local_nid));

                        char key[MAX_NAME_LEN], value[MAX_BUF_LEN];
                        sprintf(key, "%s/%s/nid", ng.home, YFS_STATUS_PRE);
                        nid2str(value, &ng.local_nid);

                        ret = path_validate(key, YLIB_NOTDIR, YLIB_DIRCREATE);
                        if (ret)
                                GOTO(err_ret, ret);
                        
                        ret = _set_text(key, value, strlen(value) + 1, O_EXCL | O_CREAT);
                        if (ret)
                                GOTO(err_ret, ret);
                }
        }

        return 0;
err_sd:
        close(sd);
err_ret:
        return ret;
}

#endif

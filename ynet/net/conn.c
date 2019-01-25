#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "configure.h"
#include "net_table.h"
#include "net_global.h"
#include "sdevent.h"
#include "ylib.h"
#include "timer.h"
#include "xnect.h"
#include "ylock.h"
#include "ynet_net.h"
#include "ynet_rpc.h"
#include "job_dock.h"
#include "main_loop.h"
#include "rpc_table.h"
#include "bh.h"
#include "conn.h"
#include "adt.h"
#include "net_table.h"
#include "heartbeat.h"
#include "base64_urlsafe.h"
#include "etcd.h"
#include "corenet_maping.h"
#include "net_rpc.h"
#include "network.h"
#include "mond_rpc.h"
#include "dbg.h"

#define ENABLE_ETCD_CONN 0

#if ENABLE_ETCD_CONN

typedef struct {
        nid_t nid;
        int online;
} arg_t;

static etcd_lock_t *__lock__ = NULL;
static time_t __faultdomain_last_update__ = 0;

static int __conn_watch(const nid_t *nid)
{
        int ret, i;
        sockid_t sockid;
        uint64_t reply = 0, _reply = 0;
        time_t ltime;

        ret = network_connect(nid, &ltime, 0, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        netable_getsock(nid, &sockid);

        ret = sdevent_heartbeat_get(&sockid, NULL, &reply);
        if (unlikely(ret))
                GOTO(err_close, ret);

        for(i = 0; i < gloconf.hb_timeout * 2; i++) {
                ret = sdevent_heartbeat_get(&sockid, NULL, &_reply);
                if (unlikely(ret))
                        GOTO(err_close, ret);

                if (reply != _reply) {
                        DBUG("check %s success\n", netable_rname_nid(nid));
                        break;
                } else {
                        sleep(1);
                }
        }

        if (i == gloconf.hb_timeout * 2) {
                DERROR("check %s fail\n", netable_rname_nid(nid));
                netable_close(nid, "conn watch", &ltime);
        }
        
        return 0;
err_close:
        netable_close(nid, "conn watch", &ltime);
err_ret:
        return ret;
}

static int __conn_close(const nid_t *nid)
{
#if 0
        if (ng.daemon == 0) {
                DWARN("close %s\n", netable_rname_nid(nid));
        }
#endif

        if (net_islocal(nid)) {
                DWARN("skip close localhost\n");
                goto out;
        }

        __faultdomain_last_update__ = 0;
        
        DINFO("close %s\n", netable_rname_nid(nid));

out:

        //DERROR("__conn_close disabled\n");
        //netable_close_withrpc(NULL, nid, "offline");

        __conn_watch(nid);
        
        return 0;
}

#endif

static int __conn_add(const nid_t *nid)
{
        int ret;
        char key[MAX_NAME_LEN], buf[MAX_BUF_LEN], tmp[MAX_BUF_LEN];
        ynet_net_info_t *info;
        net_handle_t nh;
        size_t len;
        instat_t instat;

        if (netable_connected(nid)) {
                netable_update(nid);
                goto out;
        }
        
        snprintf(key, MAX_NAME_LEN, "%u.info", nid->id);

        ret = etcd_get_text(ETCD_CONN, key, tmp, NULL);
        if (ret)
                GOTO(err_ret, ret);

        len = MAX_BUF_LEN;
        info = (void *)buf;
        ret = urlsafe_b64_decode(tmp, strlen(tmp), (void *)info, &len);
        YASSERT(ret == 0);        


#if 0
        __faultdomain_last_update__ = 0;
#endif

        ret = mond_rpc_getstat(nid, &instat);
        if (ret) {
                DINFO("%u %s not online\n", nid->id, info->name);
                goto out;
        }

        DINFO("connect to %u %s\n", nid->id, info->name);
        
        ret = netable_connect_info(&nh, info, 1);
        if (ret) {
                DINFO("connect to %u %s fail\n", nid->id, info->name);
                GOTO(err_ret, ret);
        }

out:

        return 0;
err_ret:
        return ret;
}

#if ENABLE_ETCD_CONN

static void *__conn_apply__(void *_arg)
{
        int ret, retry = 0;
        arg_t *arg = _arg;

        if (arg->online) {
                while (1) {
                        ret = __conn_add(&arg->nid);
                        if (ret) {
                                if (retry < 10) {
                                        sleep(1);
                                        retry++;
                                        continue;
                                } else {
                                        DWARN("connect to %u fail\n", arg->nid.id);
                                        GOTO(err_ret, ret);
                                }
                        }
                        
                        break;
                }
        } else {
                ret = __conn_close(&arg->nid);
                if (ret)
                        GOTO(err_ret, ret);
        }

        yfree((void **)&arg);
        pthread_exit(NULL);
err_ret:
        yfree((void **)&arg);
        pthread_exit(NULL);
}

static int __conn_apply(const etcd_node_t *node)
{
        int ret;
        char tmp[MAX_PATH_LEN];
        arg_t *arg;

        if (node->value && !ng.daemon) {
                DINFO("skip cmd conn\n");
                goto out;
        }
        
        if (strstr(node->key, ".lock") == NULL) {
                DINFO("skip %s\n", node->key);
                goto out;
        }

        ret = ymalloc((void**)&arg, sizeof(*arg));
        if (ret)
                GOTO(err_ret, ret);

        snprintf(tmp, MAX_NAME_LEN, "%s/%s", ETCD_ROOT, ETCD_CONN);
        str2nid(&arg->nid, node->key + strlen(tmp) + 1);
        if (node->value) {
                arg->online = 1 ;
        } else
                arg->online = 0;

        DBUG("watch return node %u idx %u key %s\n",
             node->num_node, node->modifiedIndex, node->key);

        pthread_t th;
        pthread_attr_t ta;

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        ret = pthread_create(&th, &ta, __conn_apply__, arg);
        if (ret)
                GOTO(err_ret, ret);

out:
        return 0;
err_ret:
        return ret;
}

static void *__conn_worker(void *arg)
{
        int ret, idx = 0;
        etcd_node_t  *node = NULL;
        etcd_session  sess;
        char key[MAX_PATH_LEN], *host;

        (void) arg;

        host = strdup("localhost:2379");
        sess = etcd_open_str(host);
        if (!sess) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }

        snprintf(key, MAX_NAME_LEN, "%s/%s", ETCD_ROOT, ETCD_CONN);
        //snprintf(key, MAX_NAME_LEN, "%s/%s", ETCD_ROOT, "test");
        while (1) {
                ret = etcd_watch(sess, key, &idx, &node);
                if(ret != ETCD_OK){
                        ret = EPERM;
                        GOTO(err_close, ret);
                }

                DBUG("conn watch node:%s nums:%d\n", node->key, node->num_node);
                idx = node->modifiedIndex + 1;
                ret = __conn_apply(node);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);

                free_etcd_node(node);
        }

        etcd_close_str(sess);
        free(host);
        pthread_exit(NULL);
err_close:
        etcd_close_str(sess);
err_ret:
        free(host);
        pthread_exit(NULL);
}

#endif

static int __conn_scan__()
{
        int ret, i;
        etcd_node_t *list = NULL, *node;
        char tmp[MAX_NAME_LEN];
        nid_t nid;

retry:
        ret = network_connect_mond(1);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        sleep(5);
                        goto retry;
                } else
                        GOTO(err_ret, ret);
        }
        
        ret = etcd_list(ETCD_CONN, &list);
        if (unlikely(ret)) {
                if (ret == ENOKEY) {
                        DINFO("conn table empty\n");
                        goto out;
                } else
                        GOTO(err_ret, ret);
        }

        for(i = 0; i < list->num_node; i++) {
                node = list->nodes[i];
 
                if (strstr(node->key, ".info") == NULL) {
                        DBUG("skip %s\n", node->key);
                        continue;
                }

                snprintf(tmp, MAX_NAME_LEN, "%s/%s", ETCD_ROOT, ETCD_CONN);
                str2nid(&nid, node->key + strlen(tmp) + 1);

                DBUG("scan %s\n", network_rname(&nid));
                ret = __conn_add(&nid);
        }

        free_etcd_node(list);

out:
        return 0;
err_ret:
        return ret;
}

static void *__conn_scan(void *arg)
{
        (void) arg;
        
        while (1) {
                sleep(gloconf.rpc_timeout / 2);
                __conn_scan__();
        }

        pthread_exit(NULL);
}

int conn_init()
{
        int ret;

#if ENABLE_ETCD_CONN
        ret = sy_thread_create2(__conn_worker, NULL, "__conn_worker");
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif

        ret = sy_thread_create2(__conn_scan, NULL, "__conn_scan");
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

static void *__conn_retry(void *arg)
{
        int retry = 0;
        const nid_t *nid = arg;

        while (1) {
                if (retry > gloconf.rpc_timeout * 2 && !conn_online(nid, -1)) {
                        DINFO("retry conn to %s fail, exit\n", network_rname(nid));
                        break;
                }
                
                __conn_add(nid);
                if (netable_connected(nid)) {
                        DINFO("retry conn to %s success\n", network_rname(nid));
                        break;
                }

                DINFO("retry conn to %s, sleep %u\n", network_rname(nid), retry);
                retry++;

                sleep(1);
        }

        yfree((void **)&arg);
        pthread_exit(NULL);
}

int conn_retry(const nid_t *_nid)
{
        int ret;
        nid_t *nid;

        ret = ymalloc((void**)&nid, sizeof(*nid));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        *nid = *_nid;

        ret = sy_thread_create2(__conn_retry, nid, "__conn_scan");
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __conn_init_info(nid_t *_nid)
{
        int ret, retry = 0;
        char key[MAX_NAME_LEN], buf[MAX_BUF_LEN], tmp[MAX_BUF_LEN];
        ynet_net_info_t *info;
        uint32_t buflen;
        size_t size;
        nid_t nid;

        info = (void *)buf;
        buflen = MAX_BUF_LEN;
        ret = rpc_getinfo(buf, &buflen);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        YASSERT(info->len);
        YASSERT(info->info_count);
        size = MAX_BUF_LEN;
        ret = urlsafe_b64_encode((void *)info, info->len, tmp, &size);
        YASSERT(ret == 0);

        nid = *net_getnid();
        YASSERT(nid.id == info->id.id);
        snprintf(key, MAX_NAME_LEN, "%u.info", nid.id);

retry:
        DINFO("register %s value %s\n", key, tmp);
        ret = etcd_create_text(ETCD_CONN, key, tmp, 0);
        if (unlikely(ret)) {
                ret = etcd_update_text(ETCD_CONN, key, tmp, NULL, 0);
                if (unlikely(ret)) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 30, (1000 * 1000));
                }
        }

        *_nid = nid;

        return 0;
err_ret:
        return ret;
}

#if ENABLE_ETCD_CONN
static int __conn_register__(etcd_lock_t *lock)
{
        int ret;
        char key[MAX_NAME_LEN];
        nid_t nid;

retry:
        nid = *net_getnid();
        snprintf(key, MAX_NAME_LEN, "%u.lock", nid.id);
        DINFO("lock %s\n", key);
        ret = etcd_lock(lock);
        if (unlikely(ret)) {
                if (ret == EPERM) {
                        DWARN("remove %s/%s\n", ETCD_CONN, key);
                        etcd_del(ETCD_CONN, key);
                        goto retry;
                } else {
                        GOTO(err_ret, ret);
                }
        }

        DINFO("lock %s success\n", key);

        while (1) {
                usleep(100 * 1000);
                if (!etcd_lock_health(lock)) {
                        DWARN("lock fail\n");
                        ret = EAGAIN;
                        GOTO(err_ret, ret);
                }
        }
        
        return 0;
err_ret:
        return ret;
}

static void *__conn_register(void *arg)
{
        int ret;
        etcd_lock_t *lock = arg;

        main_loop_hold();
        
        while (1) {
                ret = __conn_register__(lock);
                if (unlikely(ret)) {
                        DWARN("register fail\n");
                        sleep(1);
                }
        }

        UNIMPLEMENTED(__DUMP__);
        pthread_exit(NULL);
}

static int __conn_init_lock(const nid_t *nid)
{
        int ret;
        char key[MAX_NAME_LEN];
        etcd_lock_t *lock;
        pthread_t th;
        pthread_attr_t ta;

        if (__lock__) {
                return 0;
        }
        
        ret = ymalloc((void**)&lock, sizeof(*lock));
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        snprintf(key, MAX_NAME_LEN, "%u.lock", nid->id);
        ret = etcd_lock_init(lock, ETCD_CONN, key, gloconf.rpc_timeout,
                             -1, gloconf.rpc_timeout / 2);
        if (unlikely(ret))
                GOTO(err_free, ret);

        DINFO("remove key %s\n", key);
        etcd_del(ETCD_CONN, key);

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        ret = pthread_create(&th, &ta, __conn_register, lock);
        if (unlikely(ret))
                GOTO(err_free, ret);
        
        __lock__ = lock;
        
        return 0;
err_free:
        yfree((void **)&lock);
err_ret:
        return ret;
}
#endif

int conn_register()
{
        int ret;
        nid_t nid;

        ret = __conn_init_info(&nid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

#if ENABLE_ETCD_CONN
        ret = __conn_init_lock(&nid);
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif

        return 0;
err_ret:
        return ret;
}

int conn_getinfo(const nid_t *nid, ynet_net_info_t *info)
{
        int ret;
        char key[MAX_NAME_LEN], tmp[MAX_BUF_LEN];
        size_t  len;

        snprintf(key, MAX_NAME_LEN, "%u.info", nid->id);
        ret = etcd_get_text(ETCD_CONN, key, tmp, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DBUG("get %s value %s\n", key, tmp);
        len = MAX_BUF_LEN;
        ret = urlsafe_b64_decode(tmp, strlen(tmp), (void *)info, &len);
        YASSERT(ret == 0);
        YASSERT(info->info_count * sizeof(ynet_sock_info_t) + sizeof(ynet_net_info_t) == info->len);
        YASSERT(info->info_count);

        return 0;
err_ret:
        return ret;
}


int conn_setinfo()
{
        int ret;
        char key[MAX_NAME_LEN], buf[MAX_BUF_LEN], tmp[MAX_BUF_LEN];
        ynet_net_info_t *info;
        uint32_t buflen;
        size_t size;

        info = (void *)buf;
        buflen = MAX_BUF_LEN;
        ret = rpc_getinfo(buf, &buflen);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        YASSERT(info->len);
        YASSERT(info->info_count);
        size = MAX_BUF_LEN;
        ret = urlsafe_b64_encode((void *)info, info->len, tmp, &size);
        YASSERT(ret == 0);
        
        snprintf(key, MAX_NAME_LEN, "%u.info", info->id.id);

        DINFO("register %s value %s\n", key, tmp);
        ret = etcd_create_text(ETCD_CONN, key, tmp, 0);
        if (unlikely(ret)) {
                ret = etcd_update_text(ETCD_CONN, key, tmp, NULL, 0);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}

#if ENABLE_ETCD_CONN
int conn_online(const nid_t *nid, int timeout)
{
        int ret;
        char key[MAX_NAME_LEN], value[MAX_BUF_LEN];

        (void) timeout;
        
        snprintf(key, MAX_NAME_LEN, "%u.lock", nid->id);
        ret = etcd_get_text(ETCD_CONN, key, value, NULL);
        if (unlikely(ret)) {
                return 0;
        }

        return 1;
}

#else

int conn_online(const nid_t *nid, int _tmo)
{
        int tmo;

        DBUG("conn_online not implimented\n");

        if (netable_connected(nid))
                return 1;

        tmo = _tmo == -1 ? gloconf.rpc_timeout : _tmo;
        time_t last_update = netable_last_update(nid);

        if (gettime() - last_update < tmo) {
                return 1;
        }

        return 0;
}

#endif

#if 0
int conn_faultdomain(int *_total, int *_online)
{
        int ret;
        static time_t now;
        static lich_stat_t stat;

        now = gettime();
        if (now - __faultdomain_last_update__ > 2) {
                __faultdomain_last_update__ = now;
                ret = dispatch_sysstat(&stat, 1);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                DINFO("update fault domain rack %u/%u, node %u/%u\n",
                      stat.rack_online, stat.rack_total,
                      stat.node_online, stat.node_total)
        }

        if (stat.rack_total == 1) {
                *_total = stat.node_total;
                *_online = stat.node_online;
        } else {
                *_total = stat.rack_total;
                *_online = stat.rack_online;
        }

        return 0;
err_ret:
        return ret;
}
#endif

int conn_listnode(nid_t *array, int *_count)
{
        int ret, i, count = 0;
        etcd_node_t *list = NULL, *node;
        char tmp[MAX_NAME_LEN];
        nid_t *nid;

        ret = etcd_list(ETCD_CONN, &list);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        for(i = 0; i < list->num_node; i++) {
                node = list->nodes[i];
 
                if (strstr(node->key, ".info") == NULL) {
                        DBUG("skip %s\n", node->key);
                        continue;
                }

                nid = &array[count];
                
                snprintf(tmp, MAX_NAME_LEN, "%s/%s", ETCD_ROOT, ETCD_CONN);
                str2nid(nid, node->key + strlen(tmp) + 1);

                DBUG("scan node[$u] %s\n", count, network_rname(nid));

                count++;
        }

        free_etcd_node(list);

        *_count = count;
        
        return 0;
err_ret:
        return ret;
}

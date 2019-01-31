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
#include "rpc_table.h"
#include "timer.h"
#include "worker.h"
#include "network.h"
#include "corenet_maping.h"
#include "core.h"
#include "bh.h"
#include "adt.h"
#include "nodeid.h"
#include "net_table.h"
#include "heartbeat.h"
#include "dbg.h"
#include "main_loop.h"

#define REPLICA_MAX YFS_CHK_REP_MAX
#define INFO_QUEUE_LEN (2 * 1024 * 1024)
#define YNET_NAME_UNKNOWN   "unknown"

static uint32_t __ltime_seq__ = 1;

#define LTIME_UPDATE(__lname__, __ltime__)                               \
        do {                                                            \
                time_t now;                                             \
                int __retry__ = 0;                                            \
                                                                        \
                while (1) {                                             \
                        now = ++__ltime_seq__;                          \
                        if ((__ltime__)->prev == now || now == 0) {     \
                                DWARN("conn %s too fast, %u %u retry %u\n", __lname__, (int)(__ltime__)->prev, (int)now, __retry__); \
                                if (__retry__ > 10) {                   \
                                        EXIT(EAGAIN);                   \
                                }                                       \
                                __retry__++;                            \
                                continue;                               \
                        }                                               \
                        break;                                          \
                }                                                       \
                                                                        \
                (__ltime__)->prev = now;                                      \
                (__ltime__)->now = now;                                       \
        } while (0)

#define LTIME_INIT(__ltime__)              \
        do {                                    \
                (__ltime__)->now = 0;                \
                (__ltime__)->prev = 0;               \
        } while (0)

#define LTIME_DROP(__ltime__)              \
        do {                                    \
                (__ltime__)->now = 0;          \
        } while (0)


typedef struct {
        uint64_t load;
        nid_t nid;
} section_t;

typedef struct {
        sy_spinlock_t lock;
        int size;
        char buf[0];
} netable_infoqueue_t;

extern int node_get_deleting();

typedef ynet_net_conn_t entry_t;

typedef struct {
        entry_t *ent;
        sy_rwlock_t rwlock;
} net_table_t;

static net_table_t *__net_table__ = NULL;
static netable_infoqueue_t *netable_infoqueue;
static worker_handler_t __netable_timer__;
static int __running__ = 0;

#define RETRY_INTERVAL (10)
#define NETABLE_PREFIX  SHM_ROOT"/net_table"

worker_handler_t hb_jobtracker;

static int __netable_worker(void *arg);

typedef struct {
        struct list_head hook;
        func1_t handler;
        void *ctx;
} reset_handler_t;

typedef struct {
        struct list_head hook;
        nid_t nid;
        char lname[MAX_NAME_LEN];
        struct list_head reset_handler;
        net_handle_t sock;
} reset_entry_t;

typedef struct {
        worker_handler_t sem;
        struct list_head list;
        sy_spinlock_t lock;
} reset_queue_t;

static reset_queue_t reset_queue;

int netable_rdlock(const nid_t *nid)
{
        YASSERT(nid->id);
        return sy_rwlock_rdlock(&__net_table__[nid->id].rwlock);
}

int netable_wrlock(const nid_t *nid)
{
        YASSERT(nid->id);
        return sy_rwlock_wrlock(&__net_table__[nid->id].rwlock);
}

void netable_unlock(const nid_t *nid)
{
        YASSERT(nid->id);
        sy_rwlock_unlock(&__net_table__[nid->id].rwlock);
}

int netable_start()
{
        __running__ = 1;

        return 0;
}

inline static int __netable_running()
{
        return ng.daemon == 0 ? 1 : __running__;
}

static int __netable_set(const char *key, int status)
{
        int ret;
        char path[MAX_PATH_LEN], value[MAX_PATH_LEN];

        snprintf(path, MAX_PATH_LEN, "%s/%s", NETABLE_PREFIX, key);
        snprintf(value, MAX_PATH_LEN, "%u", status);

        DINFO("%s %s\n", path, value);

        ret = _set_text(path, value, strlen(value) + 1, O_CREAT | O_TRUNC);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret; 
}

static void __netable_reset_handler_add(entry_t *ent, func1_t handler, void *ctx)
{
        int ret;
        reset_handler_t *reset;
        struct list_head *pos;

        list_for_each(pos, &ent->reset_handler) {
                reset = (void *)pos;

                if (reset->handler == handler) {
                        DWARN("handler exist\n");
                        return;
                }
        }

        ret = ymalloc((void **)&reset, sizeof(*reset));
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        reset->handler = handler;
        reset->ctx = ctx;
        list_add(&reset->hook, &ent->reset_handler);
}

static void __netable_reset_handler_exec(struct list_head *list, const nid_t *nid)
{
        reset_handler_t *reset;
        struct list_head *pos;

        list_for_each(pos, list) {
                reset = (void *)pos;
                reset->handler((void *)nid, reset->ctx);
        }
}

static void __netable_reset_handler_free(struct list_head *list)
{
        struct list_head *pos, *n;

        list_for_each_safe(pos, n, list) {
                list_del(pos);
                yfree((void **)&pos);
        }
}

static void __netable_update_ltime(entry_t *ent)
{
        LTIME_UPDATE(ent->lname, &ent->ltime);
}

static int __entry_load(entry_t *ent, const ynet_net_info_t *info, const net_handle_t *sock)
{
        int ret;

        YASSERT(ent->ltime.now == 0);
        YASSERT(list_empty(&ent->reset_handler));
        
        if (ent->info == NULL) {
                ret = ymalloc((void **)&ent->info, info->len);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                _memcpy(ent->info, info, info->len);
        }

        ent->sock = *sock;
        YASSERT(sock->u.sd.type == SOCKID_NORMAL);
        ent->status = NETABLE_CONN;
        //ent->last_retry = 0;

        sprintf(ent->lname, "%s", info->name);
        ent->timeout = 1000 * 1000 * gloconf.hb_timeout;
        INIT_LIST_HEAD(&ent->reset_handler);

        __netable_update_ltime(ent);

        DBUG("info.name %s \n", ent->lname);

        return 0;
err_ret:
        return ret;
}

static int __entry_create(entry_t **_ent, const nid_t *nid)
{
        int ret;
        entry_t *ent;

        ret = ymalloc((void **)&ent, sizeof(*ent));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        _memset(ent, 0x0, sizeof(*ent));

        ent->nh.type = NET_HANDLE_PERSISTENT;
        ent->nh.u.nid = *nid;
        ent->status = NETABLE_NULL;
        ent->info = NULL;
        ent->update = gettime();
        ent->last_retry = 0;
        LTIME_INIT(&ent->ltime);

        INIT_LIST_HEAD(&ent->reset_handler);

        ret = sy_spin_init(&ent->load_lock);
        if (unlikely(ret))
                GOTO(err_free, ret);

        *_ent = ent;

        return 0;
err_free:
        yfree((void **)&ent);
err_ret:
        return ret;
}

inline static ynet_net_conn_t *__netable_nidfind(const nid_t *nid)
{
#if 0
        if (unlikely(ng.offline))
                return NULL;
#endif

        YASSERT(nid->id < NODEID_MAX);
        return __net_table__[nid->id].ent;
}

static int __iterate_handler(void *arg_null, void *net)
{
        int ret;
        (void) arg_null;
        entry_t *ent = (entry_t *) net;
        nid_t nid = ent->nh.u.nid;

        ret = netable_rdlock(&nid);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        if (ent->info == NULL) {
                DINFO("%s, load: %llu, status %u, info %p\n", ent->lname,
                      (LLU)ent->load, ent->status, ent->info);
        } else {
                DINFO("%s, load: %llu, status %u, deleting %u\n", ent->lname,
                      (LLU)ent->load, ent->status, ent->info->deleting);
        }

        netable_unlock(&nid);

        return 0;
}

typedef struct {
	nid_t nid;
	time_t ltime;
} arg_t;

static void __netable_close(void *_arg)
{
        arg_t *arg = _arg;

        netable_close(&arg->nid, "close by sdevent", &arg->ltime);

        yfree((void **)&_arg);
}

static int __netable_connect__(entry_t *ent, const net_handle_t *sock, const ynet_net_info_t *info, int flag)
{
        int ret;
        arg_t *arg;

        YASSERT(sock->type == NET_HANDLE_TRANSIENT);
        YASSERT(sock->u.sd.type == SOCKID_NORMAL);
        
        if (ent->status == NETABLE_CONN) {
                if (flag) {
                        ret = EEXIST;
                        GOTO(err_ret, ret);
                } else {
                        DINFO("%s already connected\n", ent->lname);
                        goto out;
                }
        }

        ret = __entry_load(ent, info, sock);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ret = ymalloc((void **)&arg, sizeof(*arg));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        arg->nid = ent->nh.u.nid;
	arg->ltime = ent->ltime.now;
        ret = sdevent_add(sock, &ent->nh.u.nid, Y_EPOLL_EVENTS, arg, __netable_close);
        if (unlikely(ret))
                GOTO(err_free, ret); /*XXX:clean*/
        
#if ENABLE_HEARTBEAT
        DINFO("add heartbeat to %s\n", ent->lname);
        ret = heartbeat_add(&ent->sock.u.sd, &ent->nh.u.nid,
                            ent->timeout, ent->ltime.now);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);
#else
        DINFO("disable heartbeat\n");
#endif

        ent->last_retry = 0;

out:
        return 0;
err_free:
        yfree((void **)&arg);
err_ret:
        return ret;
}

static int __network_connect2(entry_t *ent, const ynet_net_info_t *info)
{
        int ret;
        net_handle_t sock;

        nid_t nid = info->id;
        ret = netable_wrlock(&nid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (ent->status == NETABLE_CONN) {
                DINFO("connect to %s sockid %s/%d time %u, exist\n",
                      ent->lname, _inet_ntoa(ent->sock.u.sd.addr), ent->sock.u.sd.sd, (int)ent->ltime.now);
                goto out;
        }
        
        ANALYSIS_BEGIN(0);
        ret = net_connect(&sock, info, 2);
        if (unlikely(ret)) {
                DWARN("connect to %s fail ret %u %s\n", info->name, ret, strerror(ret));
                if (ret == EBADF)
                        ret = ETIMEDOUT;
                GOTO(err_lock, ret);
        }

        ANALYSIS_END(0, IO_WARN, NULL);
        
        ret = __netable_connect__(ent, &sock, info, 1);
        if (unlikely(ret)) {
                GOTO(err_lock, ret);
        }

        __netable_set(info->name, 1);

        DINFO("connect to %s sockid %s/%d time %u\n",
              ent->lname, _inet_ntoa(ent->sock.u.sd.addr), ent->sock.u.sd.sd, (int)ent->ltime.now);

out:
        netable_unlock(&nid);

        return 0;
err_lock:
        LTIME_DROP(&ent->ltime);
        ent->status = NETABLE_DEAD;
        ent->last_retry = gettime();
        netable_unlock(&nid);
err_ret:
        return ret;
}

static void __netable_close2(struct list_head *handler, const nid_t *nid,
                             const char *name, const sockid_t *sockid)
{
        (void) name;
        
        DBUG("reset %s, %s/%d\n", netable_rname_nid(nid),
              _inet_ntoa(sockid->addr), sockid->sd);
        rpc_table_reset(__rpc_table__, sockid, nid);
        __netable_reset_handler_exec(handler, nid);
        __netable_reset_handler_free(handler);
}

static int __netable_reset_worker()
{
        int ret;
        reset_entry_t *ent;

        while (1) {
                ret = sy_spin_lock(&reset_queue.lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                if (list_empty(&reset_queue.list)) {
                        sy_spin_unlock(&reset_queue.lock);
                        break;
                }

                ent = (void *)(reset_queue.list.next);
                list_del(&ent->hook);

                sy_spin_unlock(&reset_queue.lock);

                __netable_close2(&ent->reset_handler, &ent->nid,
                                 ent->lname, &ent->sock.u.sd);
                
                yfree((void **)&ent);
        }

        return 0;
err_ret:
        return ret;
}

int netable_init(int daemon)
{
        int ret, i;
        net_table_t *array, *netable;

        (void) daemon;

        ret = ymalloc((void**)&array, sizeof(*array) * NODEID_MAX);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (i = 0; i < NODEID_MAX; i++) {
                netable = &array[i];
                
                netable->ent = NULL;
                ret = sy_rwlock_init(&netable->rwlock, "net_table.rwlock");
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        __net_table__ = array;

#if 0
        ret = jobdock_worker_create(&hb_jobtracker, "heartbeat");
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif

        ret = ymalloc((void**)&netable_infoqueue, 
                      sizeof(*netable_infoqueue) + INFO_QUEUE_LEN);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        netable_infoqueue->size = 0;
        ret = sy_spin_init(&netable_infoqueue->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        INIT_LIST_HEAD(&reset_queue.list);
        ret = sy_spin_init(&reset_queue.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = timer1_create(&__netable_timer__, "netable_updateinfo", __netable_worker, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = timer1_settime(&__netable_timer__, USEC_PER_SEC * 30);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        ret = worker_create(&reset_queue.sem, "netable_reset",
                            __netable_reset_worker, NULL, NULL,
                            WORKER_TYPE_SEM, 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int netable_destroy(void)
{
        //sem_post(&netable_sem);

        //sem_wait(&netable_exit_sem);

        //hash_destroy_table(net_table, NULL);

        return 0;
}

#if 0
static int __netable_insert(entry_t *ent, const nid_t *nid)
{
        int ret;
        net_table_t *net_table;

        YASSERT(nid->id < NODEID_MAX);
        net_table = &__net_table__[nid->id];

        ret = sy_rwlock_wrlock(&net_table->rwlock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (net_table->ent) {
                ret = EEXIST;
                GOTO(err_lock, ret);
        }

        net_table->ent = ent;

        sy_rwlock_unlock(&net_table->rwlock);

        return 0;
err_lock:
        sy_rwlock_unlock(&net_table->rwlock);
err_ret:
        return ret;
}
#endif

static int __netable_new(const nid_t *nid, entry_t **_ent)
{
        int ret;
        entry_t *ent = NULL;

        ret = netable_wrlock(nid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (__netable_nidfind(nid)) {
                ret = EEXIST;
                GOTO(err_lock, ret);
        }

        ret = __entry_create(&ent, nid);
        if (unlikely(ret)) {
                GOTO(err_lock, ret);
        }

        __net_table__[nid->id].ent = ent;

        netable_unlock(nid);
        
        *_ent = ent;

        return 0;
err_lock:
        netable_unlock(nid);
err_ret:
        return ret;
}

int netable_connect_info(net_handle_t *nh, const ynet_net_info_t *info, int force)
{
        int ret;
        entry_t *ent;

        (void) force;

#if ENABLE_START_PARALLEL
        if (__netable_running() == 0) {
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }
#endif

        YASSERT(!net_isnull(&info->id));

        if (info->deleting) {
                maping_drop(HOST2NID, info->name);
        }

        main_loop_hold();

retry:
        ent = __netable_nidfind(&info->id);
        if (ent == NULL) {
                ret = __netable_new(&info->id, &ent);
                if (unlikely(ret)) {
                        if (ret == EEXIST) {
                                DWARN("connect exist\n");
                                goto retry;
                        } else
                                GOTO(err_ret, ret);
                }
        }

        if (ent->status != NETABLE_CONN) {
                ret = __network_connect2(ent, info);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        }

        id2nh(nh, &info->id);

        return 0;
err_ret:
        return ret;
}


ynet_net_conn_t *netable_nhfind(const net_handle_t *nh)
{
        if (nh->type != NET_HANDLE_PERSISTENT)
                return NULL;
        else
                return __netable_nidfind(&nh->u.nid);
}

static void __netable_close1(entry_t *ent)
{
        YASSERT(ent->status == NETABLE_CONN);

        __netable_reset_handler_free(&ent->reset_handler);

        LTIME_DROP(&ent->ltime);
        ent->status = NETABLE_DEAD;
        //ent->unstable = 0;

        if (ent->info) {
                yfree((void **)&ent->info);
                ent->info = NULL;
        }

        DBUG("net %s closed\n", ent->lname);
}

static int __netable_close_bh(entry_t *ent)
{
        int ret;
        reset_entry_t *reset_entry;

        YASSERT(!net_isnull(&ent->nh.u.nid));

        ret = sy_spin_lock(&reset_queue.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = ymalloc((void**)&reset_entry, sizeof(reset_entry_t));
        if (unlikely(ret))
                GOTO(err_lock, ret);

        INIT_LIST_HEAD(&reset_entry->reset_handler);
        list_splice_init(&ent->reset_handler, &reset_entry->reset_handler);
        reset_entry->nid = ent->nh.u.nid;
        strcpy(reset_entry->lname, ent->lname);

        reset_entry->sock = ent->sock;

        list_add_tail(&reset_entry->hook, &reset_queue.list);

        sy_spin_unlock(&reset_queue.lock);

        worker_post(&reset_queue.sem);

        return 0;
err_lock:
        sy_spin_unlock(&reset_queue.lock);
err_ret:
        return ret;
}

void netable_close(const nid_t *nid, const char *why, const time_t *ltime)
{
        int ret;
        entry_t *ent = NULL;
        net_handle_t sock;

        ent = __netable_nidfind(nid);
        if (ent == NULL) {
                return;
        }

        ret = netable_wrlock(nid);
        if (unlikely(ret)) {
                EXIT(EAGAIN);
        }

        // 如果第二task进入该过程, 会如何?
        // TODO 如果此时连接已经由第一task关闭,同时由别的过程建立? 会不会误判?

        DBUG("close %s by %s %d -> %d \n", ent->lname, why,
             ent->ltime.now, ltime ? (int)*ltime : -1);
        if (ent->status != NETABLE_CONN || (ltime && ent->ltime.now != *ltime)) {
                ret = EBUSY;
		goto err_lock;
        }

        DINFO("close %s by '%s', ltime %p\n", ent->lname, why, ltime);
        __netable_set(ent->lname, 0);

        ret = __netable_close_bh(ent);
        if (unlikely(ret))
                GOTO(err_lock, ret);

        sock = ent->sock;
        __netable_close1(ent);

        netable_unlock(nid);

#if 1
        UNIMPLEMENTED(__WARN__);
#else
        corenet_maping_close(nid);
#endif
        sdevent_close_force(&sock);

#if 0
        conn_retry(nid);
#endif
        
        return;
err_lock:
        netable_unlock(nid);
///err_ret:
        return;
}

void netable_close_withrpc(const sockid_t *sockid, const nid_t *nid, const char *why)
{
        /**
         * Bug #10937 maybe rpc_pack_len assert caused by netable close order?
         * so current netable close order: rpc_table_reset --> corenet_maping_close --> netable_close
         * please don't change!
         */
        rpc_table_reset(__rpc_table__, sockid, nid);
        //corenet_maping_close(nid);
        netable_close(nid, why, NULL);
}

const char *__netable_rname(const nid_t *nid)
{
        int ret;
        char name[MAX_NAME_LEN];
        static __thread char buf[MAX_MSG_SIZE];

        if (__net_table__ == NULL || net_isnull(nid)) {
                snprintf(buf, MAX_NAME_LEN, ""YNET_NAME_UNKNOWN"("NID_FORMAT")",
                         NID_ARG(nid));
        } else {
                ret = maping_nid2host(nid, name);
                if (unlikely(ret)) {
                        snprintf(buf, MAX_NAME_LEN, ""YNET_NAME_UNKNOWN"("NID_FORMAT")",
                                 NID_ARG(nid));
                } else {
                        sprintf(buf, "%s", name);
                }
        }

        return buf;
}

const char *netable_rname_nid(const nid_t *nid)
{
        entry_t *ent;

        if (__net_table__ == NULL || net_isnull(nid)) {
                return __netable_rname(nid);
        } else {
                if (net_islocal(nid)) {
                        return ng.name;
                }

                ent = __netable_nidfind(nid);

                if (ent && strlen(ent->lname)) {
                        return ent->lname;
                } else {
                        return __netable_rname(nid);
                }
        }
}


const char *netable_rname(const void *_nh)
{
        const net_handle_t *nh = _nh;

        if (nh->type == NET_HANDLE_PERSISTENT) {
                return netable_rname_nid(&nh->u.nid);
        } else {
                return "";
        }
}

void netable_load_update(const nid_t *nid, uint64_t load)
{
        //int ret;
        entry_t *ent;

        ANALYSIS_BEGIN(0);

        YASSERT(!net_isnull(nid));
        ent = __netable_nidfind(nid);
        if (ent == NULL) {
                return;
        }

#if 1
        DBUG("update %s latency %llu\n", ent->lname, (LLU)load);
        //ent->load = (ent->load +  load) / 2;
        ent->load = load;
#else
        ret = sy_spin_trylock(&ent->load_lock);
        if (unlikely(ret)) {
                DINFO("update busy\n");
                return;
        }

        ent->load = load;

        sy_spin_unlock(&ent->load_lock);
#endif

        ANALYSIS_END(0, IO_WARN, NULL);

        return;
}

void netable_iterate(void)
{
        int i;
        net_table_t *net_table;

        for (i = 0; i < NODEID_MAX; i++) {
                net_table = &__net_table__[i];
                if (net_table->ent) {
                        __iterate_handler(NULL, net_table->ent);
                }
        }
}

int netable_getinfo(const nid_t *nid, ynet_net_info_t *info, uint32_t *buflen)
{
        int ret;
        entry_t *ent;

        ent = __netable_nidfind(nid);
        if (ent == NULL) {
                ret = ENONET;
                DBUG("nid "NID_FORMAT" no found\n", NID_ARG(nid));
                GOTO(err_ret, ret);
        }

        ret = netable_rdlock(nid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if ((int)ent->ltime.now == 0) {
                ret = ENONET;
                DBUG("nid "NID_FORMAT" not connected\n", NID_ARG(nid));
                GOTO(err_lock, ret);
        }

        if (ent->status == NETABLE_CONN) {
                if (*buflen < ent->info->len) {
                        ret = EINVAL;
                        GOTO(err_lock, ret);
                } else
                        *buflen = ent->info->len;

                _memcpy(info, ent->info, ent->info->len);
        } else {
                ret = ENONET;
                GOTO(err_lock, ret);
        }

        netable_unlock(nid);

        return 0;
err_lock:
        netable_unlock(nid);
err_ret:
        return ret;
}

#define LOCAL_LTIME 1234567890

time_t IO_FUNC netable_conn_time(const nid_t *nid)
{
        entry_t *ent;

#if ENABLE_START_PARALLEL
        if (__netable_running() == 0) {
                return 0;
        }
#endif
        
        
        ent = __netable_nidfind(nid);
        if (unlikely(ent == NULL))
                return 0;

#if 0
        if (ent->unstable)
                return 0;
#endif

        //YASSERT(ent->ltime.now != (time_t)-1);
        return ent->ltime.now;
}

int netable_connected(const nid_t *nid)
{
        int ret;
        entry_t *ent;

#if ENABLE_START_PARALLEL
        if (__netable_running() == 0) {
                return 0;
        }
#endif
        
        ent = __netable_nidfind(nid);
        if (ent == NULL)
                return 0;

#if 1
        if (ent->status == NETABLE_CONN && ent->ltime.now != 0 ) {
                //YASSERT((int)ent->ltime.now != 0);
                ret = 1;
        } else {
                ret = 0;
        }
#else
        ret = netable_rdlock(nid);
        if (unlikely(ret)) {
                if (ret == ETIMEDOUT || ret == ESHUTDOWN) {
                        UNIMPLEMENTED(__DUMP__);
                        return 0;
                } else if (ret == ESTALE) {
                        EXIT(EAGAIN);
                } else 
                        UNIMPLEMENTED(__DUMP__);
        }

        if (ent->status == NETABLE_CONN && ent->ltime.now != 0 ) {
                //YASSERT((int)ent->ltime.now != 0);
                ret = 1;
        } else
                ret = 0;

        netable_unlock(nid);
#endif

        return ret;
}

int netable_add_reset_handler(const nid_t *nid, func1_t handler, void *ctx)
{
        int ret;
        entry_t *ent;

        ent = __netable_nidfind(nid);
        if (ent == NULL) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        ret = netable_wrlock(nid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (ent->status != NETABLE_CONN) {
                ret = ENOENT;
                GOTO(err_lock, ret);
        }

        __netable_reset_handler_add(ent, handler, ctx);

        netable_unlock(nid);
        DINFO("add handler: %s\n", netable_rname_nid(nid));

        return 0;
err_lock:
        netable_unlock(nid);
err_ret:
        return ret;
}

static int __netable_updateinfo(const ynet_net_info_t *info)
{
        int ret;
        entry_t *ent;
        nid_t nid = info->id;

        ent = __netable_nidfind(&nid);
        if (ent == NULL) {
                ret = ENOENT;
                goto err_ret;
        }

        DBUG(""NID_FORMAT" update info\n", NID_ARG(&info->id));

        ret = netable_wrlock(&nid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (ent->status == NETABLE_CONN) {
                YASSERT(ent->info);
                if (ent->info->len != info->len) {
                        ret = yrealloc((void **)&ent->info, ent->info->len, info->len);
                        if (unlikely(ret))
                                GOTO(err_lock, ret);
                }

                memcpy(ent->info, info, info->len);
        }

        //DINFO("node %s deleting %u\n", info->name, info->deleting);

        netable_unlock(&nid);

        return 0;
err_lock:
        netable_unlock(&nid);
err_ret:
        return ret;
}

static int __netable_updateinfo_worker(char *buf)
{
        int ret, size, left;
        ynet_net_info_t *info;

        ret = sy_spin_lock(&netable_infoqueue->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        size = netable_infoqueue->size;
        memcpy(buf, netable_infoqueue->buf, size);
        netable_infoqueue->size = 0;

        sy_spin_unlock(&netable_infoqueue->lock);

        left = size;
        info = (void *)buf;

        DBUG("update info %u\n", left);

        while (left) {
                __netable_updateinfo(info);
                left -= info->len;
                info = (void *)info + info->len;
        }

        return 0;
err_ret:
        return ret;
}


static int __netable_worker(void *arg)
{
        int ret;
        static char *buf = NULL;

        (void) arg;

        if (buf == NULL) {
                ret = ymalloc((void **)&buf, INFO_QUEUE_LEN);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);
        }

        DBUG("netable worker\n");

        __netable_updateinfo_worker(buf);

        ret = timer1_settime(&__netable_timer__, USEC_PER_SEC * 30);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        return 0;
}




int netable_updateinfo(const ynet_net_info_t *info)
{
        int ret;

        //DINFO("host %s deleting %u\n", info->name, info->deleting);

        if (ng.daemon == 0)
                return 0;

        ANALYSIS_BEGIN(0);

        ret = sy_spin_lock(&netable_infoqueue->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (info->len + netable_infoqueue->size > INFO_QUEUE_LEN) {
                DWARN("got info queue len %u, newinfo len %u, drop newinfo\n",
                      netable_infoqueue->size, info->len);
                goto out;
        }

        memcpy(netable_infoqueue->buf + netable_infoqueue->size, info, info->len);
        netable_infoqueue->size += info->len;

out:
        sy_spin_unlock(&netable_infoqueue->lock);

        ANALYSIS_END(0, 1000 * 100, NULL);

        if (info->deleting) {
                maping_drop(HOST2NID, info->name);
        }

        return 0;
err_ret:
        return ret;
}

int netable_getname(const nid_t *nid, char *name)
{
        int ret;
        char buf[MAX_BUF_LEN];
        ynet_net_info_t *info;
        uint32_t len;

        info = (void *)buf;
        len = MAX_BUF_LEN;
        ret = netable_getinfo(nid, (void *)info, &len);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        strcpy(name, info->name);

        return 0;
err_ret:
        return ret;
}

int netable_gethost(const nid_t *nid, char *name)
{
        int ret;
        entry_t *ent;

        ent = __netable_nidfind(nid);
        if (ent == NULL) {
                ret = ENONET;
                DBUG("nid "NID_FORMAT" no found\n", NID_ARG(nid));
                GOTO(err_ret, ret);
        }

        strcpy(name, ent->lname);

        return 0;
err_ret:
        return ret;
}

static int __netable_load_cmp(const void *arg1, const void *arg2)
{
        const section_t *sec1 = arg1, *sec2 = arg2;
        return sec1->load - sec2->load;
}

void netable_sort(nid_t *nids, int count)
{
        int i;
        ynet_net_conn_t *net;
        section_t section[REPLICA_MAX], *sec;
        char buf[MAX_NAME_LEN];

        YASSERT(count <= REPLICA_MAX);
        YASSERT(count * sizeof(nid_t) < MAX_BUF_LEN);

        memcpy(buf, nids, count * sizeof(nid_t));

        for (i = 0; i < count; i++) {
                sec = &section[i];
                sec->nid = nids[i];

                if (net_islocal(&sec->nid)) {
                        sec->load = core_latency_get();
                } else {
                        net = __netable_nidfind(&sec->nid);
                        if (net == NULL || net->status != NETABLE_CONN) {
                                DINFO("%s not online, no balance\n", netable_rname_nid(&sec->nid));
                                return;
                        }

                        DBUG("%s latency %llu\n", netable_rname_nid(&sec->nid), (LLU)net->load);

                        if (net->load < 0.5)
                                sec->load = 0;
                        else
                                sec->load = net->load;
                }
        }

        qsort(section, count, sizeof(section_t), __netable_load_cmp);
        for (i = 0; i < count; i++) {
                sec = &section[i];
                nids[i] = sec->nid;

                DBUG("node[%u] %s latency %llu\n", i, netable_rname_nid(&sec->nid), (LLU)section[i].load);
        }
}

static int __netable_select(const section_t *sec, int count)
{
        int i;
        uint64_t rand = 0;
        double loadinfo[REPLICA_MAX + 1], max;
        char msg[MAX_BUF_LEN];

        YASSERT(count <= REPLICA_MAX);

        max = 0;
        loadinfo[0] = 0;
        msg[0] = '\0';
        sprintf(msg + strlen(msg), "latency:[");
        for (i = 1; i <= count; i++) {
                loadinfo[i] = sec[i - 1].load;
                if (loadinfo[i] < 100)
                        loadinfo[i] = 100;

                max = max > loadinfo[i] ? max : loadinfo[i];
                sprintf(msg + strlen(msg), "%u ", (int)loadinfo[i]);
        }

        sprintf(msg + strlen(msg), "] reciprocal:[");

        for (i = 1; i <= count; i++) {
                loadinfo[i] =  ((max  * 1000 * 1000) / loadinfo[i]);
                sprintf(msg + strlen(msg), "%u ", (int)loadinfo[i]);
        }

        sprintf(msg + strlen(msg), "] range:");

        for (i = 1; i <= count; i++) {
                loadinfo[i] = loadinfo[i] + loadinfo[i - 1];
                sprintf(msg + strlen(msg), "[%u, %u) ", (int)loadinfo[i - 1], (int)loadinfo[i]);
        }

        rand = fastrandom() % (int)(loadinfo[count]);

        for (i = 0; i < count; i++) {
                if (rand >= loadinfo[i] && rand < loadinfo[i + 1]) {
                        DBUG("%s chosen %u, rand %ju\n", msg, (int)i, rand);
                        return i;
                }
        }

        DWARN("%s chosen %u\n", msg, (int)rand);

        return 0;
}

/**
 * 读的负载均衡： 倒数法，根据各节点的负载，选择读哪一个
 */
void IO_FUNC netable_select(const nid_t *nids, int count, nid_t *nid)
{
        int i;
        ynet_net_conn_t *net;
        section_t section[REPLICA_MAX], *sec;
        char buf[MAX_NAME_LEN];

        YASSERT(count <= REPLICA_MAX);
        YASSERT(count * sizeof(nid_t) < MAX_BUF_LEN);

        memcpy(buf, nids, count * sizeof(nid_t));

        for (i = 0; i < count; i++) {
                sec = &section[i];
                sec->nid = nids[i];

                if (net_islocal(&sec->nid)) {
                        sec->load = core_latency_get();
                } else {
                        net = __netable_nidfind(&sec->nid);
                        if (net == NULL || net->status != NETABLE_CONN) {
                                sec->load = UINT64_MAX;
                                continue;
                        }

                        DBUG("%s latency %llu\n", netable_rname_nid(&sec->nid), (LLU)net->load);

                        sec->load = net->load;
                }
        }

        i = __netable_select(section, count);

        *nid = section[i].nid;
}

int netable_update_retry(const nid_t *nid)
{
        int ret;
        entry_t *ent = NULL;

retry:
        ent = __netable_nidfind(nid);
        if (ent == NULL) {
                DBUG("add null ent for %u\n", nid->id);
                ret = __netable_new(nid, &ent);
                if (unlikely(ret)) {
                        if (ret == EEXIST) {
                                goto retry;
                        } else
                                GOTO(err_ret, ret);
                }
        }

        ent->last_retry = gettime();

        return 0;
err_ret:
        return ret;
}

int netable_connectable(const nid_t *nid, int force)
{
        int tmo, wait;
        entry_t *ent;

        if (force)
                return 1;

#if 1
        wait = gloconf.lease_timeout / 2;
#else
        uint32_t now = gettime();
        if (now - ng.uptime < 60) {
                wait = 3;
        } else {
                wait = 30;
        }
#endif
        
        ent = __netable_nidfind(nid);
        if (ent == NULL)
                return 1;
        else {
                tmo = gettime() - ent->last_retry;
                if (tmo < wait) {
                        DBUG("conn %s will retry after %d sec\n",
                             netable_rname_nid(nid), wait - tmo);
                        return 0;
                } else {
                        return 1;
                }
        }
}

int netable_getsock(const nid_t *nid, sockid_t *sockid)
{
        int ret;
        entry_t *ent;

        ent = __netable_nidfind(nid);
        if (ent == NULL) {
                ret = ENONET;
                DWARN("nid "NID_FORMAT" not online\n", NID_ARG(nid));
                GOTO(err_ret, ret);
        }

        ret = netable_rdlock(nid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (ent->status == NETABLE_CONN) {
                *sockid = ent->sock.u.sd;
                YASSERT(sockid->type == SOCKID_NORMAL);
        } else {
                ret = ENONET;
                DBUG("nid "NID_FORMAT"\n", NID_ARG(&ent->nh.u.nid));
                GOTO(err_lock, ret);
        }

        netable_unlock(nid);

        ret = sdevent_check(sockid);
        if (unlikely(ret)) {
                DWARN("%s lost\n", network_rname(nid));
                netable_close(nid, "lost socket", NULL);
                GOTO(err_ret, ret);
        }
        
        return 0;
err_lock:
        netable_unlock(nid);
err_ret:
        return ret;
}

int netable_dump_hb_timeout(char *lname)
{
        int ret, idx;
        time_t now;
        struct tm t;
        FILE * fp = NULL;
        struct stat sb;
        char path[MAX_PATH_LEN], line[MAX_LINE_LEN], time_buf[MAX_LINE_LEN];

        /* old name node2/0, new name node2 */
        //sscanf(ng.name, "%*[^/]/%d", &idx);
        idx = 0;
        snprintf(path, MAX_PATH_LEN, "/dev/shm/lich4/hb_timeout/%d", idx);
        ret = path_validate(path, 0, 1);
        if (ret)
                GOTO(err_ret, ret);

        DINFO("dump hb_timeout %s to %s\n", lname, path);

        ret = stat(path, &sb);
        if (ret < 0) {
                ret = errno;
                if (ret != ENOENT)
                        GOTO(err_ret, ret);
        } else {
                if (sb.st_size > 4096) {
                        DINFO("truncate %s to 0\n", path);
                        truncate(path, 0);
                }
        }

        fp = fopen(path, "a+");
        if (fp == NULL) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        now = gettime();
        strftime(time_buf, MAX_LINE_LEN, "%F %T", localtime_safe(&now, &t));
        sprintf(line, "%s; "NID_FORMAT"; %s; %s\n", time_buf, NID_ARG(&ng.local_nid), ng.name, lname);
        ret = fwrite(line, 1, strlen(line), fp);
        if (ret != (int)strlen(line)) {
                ret = errno;
                GOTO(err_close, ret);
        }

        fclose(fp);

        return 0;
err_close:
        fclose(fp);
err_ret:
        DERROR("dump hb_timeout %s to %s, ret: %d\n", lname, path, ret);
        return ret;
}

STATIC int __netable_accept(entry_t *ent, const net_handle_t *sock,
                            const ynet_net_info_t *info)
{
        int ret;

#if ENABLE_START_PARALLEL
        if (__netable_running() == 0) {
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }
#endif

        nid_t nid = info->id;
        ret = netable_wrlock(&nid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (ent->status == NETABLE_CONN) {
                if (net_getnid()->id > info->id.id) {
                        ret = ECONNRESET;
                        GOTO(err_lock, ret);
                } else if (net_getnid()->id == info->id.id) {
                        ret = sdevent_add(sock, &info->id, Y_EPOLL_EVENTS, NULL, NULL);
                        if (unlikely(ret))
                                GOTO(err_lock, ret);

                        DINFO("local conn %s\n", info->name);
                        goto out;
                } else {
                        DINFO("dup conn, close exist conn of %s\n",
                              info->name);
                        ret = __netable_close_bh(ent);
                        if (unlikely(ret))
                                GOTO(err_lock, ret);

                        __netable_close1(ent);
                        sdevent_close_force(&ent->sock);
                        rpc_table_reset(__rpc_table__, &ent->sock.u.sd, &info->id);
                }
        }

        ret = __netable_connect__(ent, sock, info, 0);
        if (unlikely(ret))
                GOTO(err_lock, ret);

        __netable_set(info->name, 1);

out:
        netable_unlock(&nid);

        return 0;
err_lock:
        netable_unlock(&nid);
err_ret:
        return ret;
}

int netable_accept(const ynet_net_info_t *info, const net_handle_t *sock)
{
        int ret;
        entry_t *ent;

        YASSERT(sock->u.sd.type == SOCKID_NORMAL);
        
        DINFO("accept %s sd %d\n", info->name, sock->u.sd.sd);
        
        YASSERT(!net_isnull(&info->id));

        if (info->deleting) {
                maping_drop(HOST2NID, info->name);
        }

        YASSERT(!net_islocal(&info->id));

retry:
        ent = __netable_nidfind(&info->id);
        if (ent == NULL) {
                ret = __netable_new(&info->id, &ent);
                if (unlikely(ret)) {
                        if (ret == EEXIST) {
                                DINFO("accept %s sd %d, retry\n", info->name, sock->u.sd.sd);
                                goto retry;
                        } else
                                GOTO(err_ret, ret);
                }
        }

        ret = __netable_accept(ent, sock, info);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int netable_rname1(const nid_t *nid, char *name)
{
        int ret;
        entry_t *ent;

        if (__net_table__ == NULL || net_isnull(nid)) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }
        
        if (net_islocal(nid)) {
                strcpy(name, ng.name);
        } else {
                ent = __netable_nidfind(nid);
                
                if (ent && strlen(ent->lname)) {
                        strcpy(name, ent->lname);
                } else {
                        ret = ENONET;
                        GOTO(err_ret, ret);
                }
        }

        DBUG("%u %s\n", nid->id, name);
        
        return 0;
err_ret:
        return ret;
}

void netable_ltime_reset(const nid_t *nid, time_t ltime, const char *why)
{
        int ret;
        entry_t *ent;

        ent = __netable_nidfind(nid);
        if (ent == NULL)
                return;

        ret = netable_wrlock(nid);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        if (ent->status == NETABLE_CONN && ent->ltime.now == ltime) {
                DWARN("%s, reset %u by %s\n", ent->lname, ltime, why);
                __netable_update_ltime(ent);

#if ENABLE_HEARTBEAT
                ret = heartbeat_add(&ent->sock.u.sd, &ent->nh.u.nid,
                                    ent->timeout, ent->ltime.now);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);
#else
                DINFO("disable heartbeat\n");
#endif
        }

        netable_unlock(nid);
}

void netable_update(const nid_t *nid)
{
        entry_t *ent;

        ent = __netable_nidfind(nid);
        if (ent == NULL)
                return;

        if (ent->status == NETABLE_CONN) {
                ent->update = gettime();
                DBUG("update %s\n", ent->lname);
        }
}

time_t netable_last_update(const nid_t *nid)
{
        entry_t *ent;

        ent = __netable_nidfind(nid);
        if (ent == NULL)
                return 0;

        return ent->update;
}

void netable_put(net_handle_t *nh, const char *why)
{
        netable_close(&nh->u.nid, why, NULL);
}

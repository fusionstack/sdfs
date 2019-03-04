#define DBG_SUBSYS S_YFSLIB

#include "chk_proto.h"
#include "etcd.h"
#include "redis_util.h"
#include "redis_conn.h"
#include "redis.h"
#include "configure.h"
#include "adt.h"
#include "net_global.h"
#include "schedule.h"
#include "cJSON.h"
#include "sdfs_conf.h"
#include "math.h"
#include "maping.h"
#include "dbg.h"

//static redis_vol_t *__conn__;
static int __conn_magic__ = 0;
extern int __redis_conn_pool__;
extern int __use_pipeline__;
extern __thread int __use_co__;

static int __redis_vol_get(const volid_t *volid, redis_vol_t **_vol, int flag);


static int __redis_connect(const char *volume, int sharding, int magic, __conn_t *conn)
{
        int ret, count;
        char addr[MAX_BUF_LEN], key[MAX_BUF_LEN], id[MAX_NAME_LEN];
        char *list[2];
        time_t ctime;

        snprintf(key, MAX_NAME_LEN, "%s/slot/%d/master", volume, sharding);
        snprintf(id, MAX_NAME_LEN, "%s/%d", volume, sharding);
        ret = maping_get(NAME2ADDR, id, addr, &ctime);
        if(ret) {
                if (ret == ENOENT) {
                retry:
                        ret = etcd_get_text(ETCD_VOLUME, key, addr, NULL);
                        if(ret) {
                                DBUG("%s not found\n", key);
                                GOTO(err_ret, ret);
                        }

                        ret = maping_set(NAME2ADDR, id, addr);
                        if(ret)
                                GOTO(err_ret, ret);
                } else
                        GOTO(err_ret, ret);
        } else {
                DBUG("found %s, %s\n", id, addr);
                if (time(NULL) - ctime > 5) {
                        DBUG("%s, %s, time expired\n", id, addr);
                        goto retry;
                }
        }

        count = 2;
        _str_split(addr, ' ', list, &count);
        if (count != 2) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        DBUG("get volume %s sharding[%d] master @ %s:%s\n", volume, sharding, list[0], list[1]);

        int port = atoi(list[1]);
        ret = redis_connect(&conn->conn, list[0], &port, key);
        if(ret) {
                DWARN("connect volume %s sharding[%d] master @ %s:%s fail\n",
                      volume, sharding, list[0], list[1]);
                GOTO(err_ret, ret);
        }

        conn->magic = magic;
        conn->used = 0;
        conn->erased = 0;

        DBUG("redis connected\n");

        return 0;
err_ret:
        return ret;
}

static int __redis_reconnect(const char *volume, int sharding, int magic, __conn_t *conn)
{
        int ret;

        redis_disconnect(conn->conn);
        conn->conn = NULL;

        YASSERT(conn->used == 0);
        ret = __redis_connect(volume, sharding, magic, conn);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

inline static int __redis_connect_sharding(const char *volume, __conn_sharding_t *sharding, int idx)
{
        int ret, count, i;
        __conn_t *conn;

#if 0
        count = __use_co__ ? 1 : __redis_conn_pool__;
#else
        count = __redis_conn_pool__;
#endif
        YASSERT(count);

        ret = ymalloc((void **)&conn, sizeof(*conn) * count);
        if(ret)
                GOTO(err_ret, ret);

        for (i = 0; i < count; i++) {
                ret = __redis_connect(volume, idx, __conn_magic__++, &conn[i]);
                if(ret)
                        GOTO(err_free, ret);
        }

        sharding->count = count;
        sharding->conn = conn;
        YASSERT(conn);

        DINFO("redis sharding[%u] conn %u connected\n", idx, count);

        return 0;
err_free:
        yfree((void **)&conn);
err_ret:
        return ret;
}

static int __redis_vol_connect(const volid_t *volid, const char *volume, int sharding,
                               redis_vol_t **_vol)
{
        int ret, i, retry = 0;
        redis_vol_t *vol;

        YASSERT(strlen(volume) < MAX_NAME_LEN);
        
        ret = ymalloc((void **)&vol, sizeof(*vol));
        if(ret)
                GOTO(err_ret, ret);

        DINFO("connect to vol %d, sharding %u\n", volid->volid, sharding);
        
        vol->sharding = sharding;
        vol->volid = *volid;
        strcpy(vol->volume, volume);

        YASSERT(vol->sharding);
        
        ret = ymalloc((void **)&vol->shardings, sizeof(*vol->shardings) * vol->sharding);
        if(ret)
                UNIMPLEMENTED(__DUMP__);

        ret = pthread_rwlock_init(&vol->lock, NULL);
        if(ret)
                UNIMPLEMENTED(__DUMP__);
        
        for (i = 0; i < vol->sharding; i++) {
                ret = pthread_rwlock_init(&vol->shardings[i].lock, NULL);
                if(ret)
                        UNIMPLEMENTED(__DUMP__);

        retry:
                ret = __redis_connect_sharding(volume, &vol->shardings[i], i);
                if(ret) {
                        if (ret == ENOKEY) {
                                USLEEP_RETRY(err_free, ret, retry, retry, 10, (1000 * 1000));
                        } else 
                                GOTO(err_free, ret);
                }
        }

        vol->sequence = 0;
        *_vol = vol;
        
        return 0;
err_free:
        yfree((void **)&vol->shardings);
        yfree((void **)&vol);
err_ret:
        return ret;
}

static int __redis_conn_get__(__conn_t *conn, redis_handler_t *handler)
{
        int ret;

        YASSERT(conn);
        
        if (conn->conn == NULL) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        if (conn->erased) {
                ret = ESTALE;
                GOTO(err_ret, ret);
        }

        if (__use_co__) {
                YASSERT(conn->used == 0);
        }
                
        if (conn->used) {
                ret = EBUSY;
                GOTO(err_ret, ret);
        }

        conn->used = 1;
        handler->conn = conn->conn;
        handler->magic = conn->magic;

        return 0;
err_ret:
        return ret;
}

static int __redis_conn_get_sharding(__conn_sharding_t *sharding, int worker,
                                     redis_handler_t *handler)
{
        int ret, idx;

        ret = pthread_rwlock_wrlock(&sharding->lock);
        if(ret)
                GOTO(err_ret, ret);

        //YASSERT(worker <= sharding->count && worker >= 0);
        idx = worker % __redis_conn_pool__;
        YASSERT(sharding->conn);
        ret = __redis_conn_get__(&sharding->conn[idx], handler);
        if(ret)
                GOTO(err_lock, ret);

        handler->idx = idx;

        pthread_rwlock_unlock(&sharding->lock);
        
        return 0;
err_lock:
        pthread_rwlock_unlock(&sharding->lock);
err_ret:
        return ret;
}

int redis_conn_get(const volid_t *volid, int sharding, uint32_t worker,
                   redis_handler_t *handler)
{
        int ret, idx;
        redis_vol_t *vol;

        //YASSERT(!schedule_running());
        
        ret = __redis_vol_get(volid, &vol, O_CREAT);
        if(ret)
                GOTO(err_ret, ret);
        
        ret = pthread_rwlock_rdlock(&vol->lock);
        if(ret)
                GOTO(err_release, ret);

        idx = sharding % vol->sharding;
        handler->sharding = idx;
        ret = __redis_conn_get_sharding(&vol->shardings[handler->sharding], worker, handler);
        if(ret)
                GOTO(err_lock, ret);

        handler->volid = *volid;
        
        pthread_rwlock_unlock(&vol->lock);
        redis_vol_release(volid);
        
        DBUG("use vol (%d,%d)\n", handler->sharding, handler->idx);

        return 0;
err_lock:
        pthread_rwlock_unlock(&vol->lock);
err_release:
        redis_vol_release(volid);
err_ret:
        return ret;
}

static int __redis_conn_release__(const char *volume, __conn_sharding_t *sharding,
                                  const redis_handler_t *handler)
{
        int ret;
        __conn_t *conn;

        ret = pthread_rwlock_wrlock(&sharding->lock);
        if(ret)
                GOTO(err_ret, ret);

        conn = &sharding->conn[handler->idx];
        
        if (handler->magic == conn->magic) {
                YASSERT(conn->used);
                conn->used = 0;
                if (conn->erased) {
                        ret = __redis_reconnect(volume, handler->sharding,
                                                __conn_magic__++, conn);
                        if(ret)
                                GOTO(err_lock, ret);

                        DINFO("redis (%d, %d) reconnected\n",
                              handler->sharding, handler->idx);
                }
        }

        pthread_rwlock_unlock(&sharding->lock);

        return 0;
err_lock:
        pthread_rwlock_unlock(&sharding->lock);
err_ret:
        return ret;
}

int redis_conn_release(const redis_handler_t *handler)
{
        int ret;
        redis_vol_t *vol;

        ret = __redis_vol_get(&handler->volid, &vol, 0);
        if(ret)
                GOTO(err_ret, ret);
        
        ret = pthread_rwlock_rdlock(&vol->lock);
        if(ret)
                GOTO(err_release, ret);

        ret = __redis_conn_release__(vol->volume, &vol->shardings[handler->sharding], handler);
        if(ret)
                GOTO(err_lock, ret);

        pthread_rwlock_unlock(&vol->lock);
        redis_vol_release(&handler->volid);

        DBUG("release vol (%d,%d)\n", handler->sharding, handler->idx);
        
        return 0;
err_lock:
        pthread_rwlock_unlock(&vol->lock);
err_release:
        redis_vol_release(&handler->volid);
err_ret:
        return ret;
}

static int __redis_conn_new__(const volid_t *volid, uint8_t *idx)
{
        int ret, seq;
        redis_vol_t *vol;

        ANALYSIS_BEGIN(0);
        
        ret = __redis_vol_get(volid, &vol, O_CREAT);
        if(ret)
                GOTO(err_ret, ret);
        
        if (ng.daemon) {
                seq = ++ vol->sequence;
        } else {
                seq = _random();
        }

        *idx = seq % vol->sharding;

#if 0
        DINFO("sharding %u %u %u\n", *idx, vol->sequence, vol->sharding);
#endif
        
        redis_vol_release(volid);

        ANALYSIS_END(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

static int __redis_conn_new(va_list ap)
{
        const volid_t *volid = va_arg(ap, const volid_t *);
        uint8_t *idx = va_arg(ap, uint8_t *);

        va_end(ap);

        return __redis_conn_new__(volid, idx);
}


int redis_conn_new(const volid_t *volid, uint8_t *idx)
{
        int ret;

        ANALYSIS_BEGIN(0);
        
        if (schedule_running()) {
                ret = schedule_newthread(SCHE_THREAD_ETCD, ++__conn_magic__, FALSE,
                                          "redis newid", -1, __redis_conn_new, volid, idx);
        } else {
                ret = __redis_conn_new__(volid, idx);
        }
        if (ret)
                GOTO(err_ret, ret);

        
        ANALYSIS_END(0, IO_WARN, NULL);

        return 0;
err_ret:
        return ret;
}

static int __redis_conn_close__(__conn_sharding_t *sharding, const redis_handler_t *handler)
{
        int ret;
        __conn_t *conn;

        ret = pthread_rwlock_wrlock(&sharding->lock);
        if(ret)
                GOTO(err_ret, ret);

        conn = &sharding->conn[handler->idx];

        YASSERT(conn->used);
        if (handler->magic == conn->magic) {
                DINFO("redis (%d, %d) close\n", handler->sharding, handler->idx);
                conn->erased = 1;
        }

        pthread_rwlock_unlock(&sharding->lock);
        
        return 0;
err_ret:
        return ret;
}

int redis_conn_close(const redis_handler_t *handler)
{
        int ret;
        redis_vol_t *vol;
 
        ret = __redis_vol_get(&handler->volid, &vol, 0);
        if(ret)
                GOTO(err_ret, ret);
               
        ret = pthread_rwlock_rdlock(&vol->lock);
        if(ret)
                GOTO(err_release, ret);

        ret = __redis_conn_close__(&vol->shardings[handler->sharding], handler);
        if(ret)
                GOTO(err_lock, ret);
        
        pthread_rwlock_unlock(&vol->lock);
        redis_vol_release(&handler->volid);
        
        return 0;
err_lock:
        pthread_rwlock_unlock(&vol->lock);
err_release:
        redis_vol_release(&handler->volid);
err_ret:
        return ret;
}

int redis_conn_init()
{
        int ret;

        ret = redis_vol_init();
        if(ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

static int __redis_vol_getnamebyid(const volid_t *_volid,
                                   char *volume, int *sharding)
{
        int ret, i;
        char *name;
        char key[MAX_NAME_LEN], value[MAX_BUF_LEN], id[MAX_NAME_LEN], buf[MAX_BUF_LEN];
        etcd_node_t *array, *node;

        snprintf(id, MAX_NAME_LEN, "%ju_%ju", _volid->volid, _volid->snapvers);
        ret = maping_get(ID2NAME, id, buf, NULL);
        if(ret) {
                if (ret == ENOENT) {
                        //pass
                } else
                        GOTO(err_ret, ret);
        } else {
                ret = sscanf(buf, "%[^,],%d", volume, sharding);
                DBUG("found %s\n", volume);
                YASSERT(ret == 2);
                goto out;
        }
        
        ret = etcd_list(ETCD_VOLUME, &array);
        if(ret)
                GOTO(err_ret, ret);

        for (i = 0; i < array->num_node; i++) {
                node = array->nodes[i];
                name = node->key;

                snprintf(key, MAX_NAME_LEN, "%s/volid", name);
                DBUG("key %s\n", key);
                ret = etcd_get_text(ETCD_VOLUME, key, value, NULL);
                if(ret)
                        continue;

                uint64_t volid = atol(value);

                snprintf(key, MAX_NAME_LEN, "%s/snapvers", name);
                DBUG("key %s\n", key);
                ret = etcd_get_text(ETCD_VOLUME, key, value, NULL);
                if(ret)
                        continue;

                uint64_t snapvers = atol(value);
                
                if (volid == _volid->volid && snapvers == _volid->snapvers) {
                        strcpy(volume, name);

                        snprintf(key, MAX_NAME_LEN, "%s/sharding", name);
                        ret = etcd_get_text(ETCD_VOLUME, key, value, NULL);
                        if(ret)
                                GOTO(err_free, ret);

                        *sharding = atoi(value);
                        
                        break;
                } else {
                        DBUG("volid %ju:%ju, snapvers %ju:%ju\n", volid, _volid->volid, snapvers, _volid->snapvers);
                }
        }        

        if (i == array->num_node) {
                ret = ENOENT;
                GOTO(err_free, ret);
        }

        free_etcd_node(array);

        snprintf(buf, MAX_NAME_LEN, "%s,%u", volume, *sharding);
        ret = maping_set(ID2NAME, id, buf);
        if(ret)
                GOTO(err_ret, ret);
        
out:
        return 0;
err_free:
        free_etcd_node(array);
err_ret:
        return ret;
}

static void __redis_close_sharding(__conn_sharding_t *sharding)
{
        int i;
        __conn_t *conn;

        for (i = 0; i < sharding->count; i++) {
                conn = &sharding->conn[i];
                redis_disconnect(conn->conn);
                conn->conn = NULL;
        }

        yfree((void **)&sharding->conn);
}

void redis_conn_vol_close(void *_vol)
{
        int i;
        redis_vol_t *vol = _vol;

        for (i = 0; i < vol->sharding; i++) {
                __redis_close_sharding(&vol->shardings[i]);
        }

        YASSERT(vol->sharding);
        yfree((void **)&vol->shardings);
        yfree((void **)&vol);
}

int redis_conn_vol(const volid_t *volid)
{
        int ret, sharding;
        char volume[MAX_NAME_LEN];
        redis_vol_t *vol;

        ret = __redis_vol_getnamebyid(volid, volume, &sharding);
        if(ret)
                GOTO(err_ret, ret);
        
        ret = __redis_vol_connect(volid, volume, sharding, &vol);
        if(ret)
                GOTO(err_ret, ret);

        YASSERT(vol->sharding);
        YASSERT(vol->shardings);
        ret = redis_vol_insert(volid, vol);
        if(ret) {
                GOTO(err_close, ret);
        }

        return 0;
err_close:
        redis_conn_vol_close(vol);
err_ret:
        return ret;
}

static int __redis_vol_get(const volid_t *volid, redis_vol_t **_vol, int flag)
{
        int ret;

retry:
        ret = redis_vol_get(volid, (void **)_vol);
        if(ret) {
                if (ret == ENOENT && (flag & O_CREAT)) {
                        ret = redis_conn_vol(volid);
                        if(ret)
                                GOTO(err_ret, ret);

                        goto retry;
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}


#include <sys/socket.h>
#include <getopt.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <dirent.h>

#include "yfs_conf.h"
#include "configure.h"
#include "sdfs_lib.h"
#include "net_global.h"
#include "md_lib.h"
#include "configure.h"
#include "yatomic.h"
#include "network.h"
#include "sdfs_list.h"
#include "redis_util.h"
#include "redis_conn.h"
#include "../../sdfs/sdfs_chunk.h"
#include "nodectl.h"

#define RECOVER_MAX 100
#define SCAN_MAX 24

#define RECOVER_MAX     100
#define SCAN_MAX        24
#define MSEC_PERMIN     1 * 60 * 1000   /*  1min */

typedef struct {
        int fd;
        uint64_t lost;
        uint64_t offset;
        sy_rwlock_t rwlock;
        char home[MAX_PATH_LEN];

        /* Job queue */
        struct list_head task;
        /* How many task I have */
        int task_count;
        /* The read write lock of task queue */

        int pthread_total;
        pthread_t pthreads[SCAN_MAX];
        int pthread_running;

        pthread_mutex_t mutex;
        pthread_cond_t notify;

        int stop;
} rept_t;

typedef enum {
        __RUNNING__,
        __WAITING__,
        __SUSPEND__,
        __SCANNING__,
} recovery_status_t;

typedef struct {
        int laststatus;
        uint64_t need;
        uint64_t lost;
        uint64_t total;
        uint64_t success;
        uint64_t fail;
        time_t lasttime;
} result_t;

void usage(const char *prog)
{
        printf("%s [-h] [p]\n", prog);
}

typedef struct {
        struct list_head hook;
        fileid_t fid;
        char path[MAX_PATH_LEN];
} dir_objck_t;

#if 0
typedef struct {
        int fd;
        uint64_t lost;
        uint64_t offset;
        sy_rwlock_t rwlock;
        char home[MAX_PATH_LEN];
} rept_t;
#endif

static bool __check_flag__ = false;
static int __full__ = 0;
static char __workdir__[MAX_PATH_LEN];
static time_t __last_report__ = 0;

#if 0
static int __retval__ = 0;
#endif
yatomic_t __total__;
yatomic_t __need__;


static uint64_t __succ__ = 0;
static uint64_t __fail__ = 0;

static int __redis_scan(const char *volume, int sharding, rept_t *rept);
static void __prefix(char *key, const char *volume, int sharding)
{
        snprintf(key, MAX_PATH_LEN, "%s/solt/%d", volume, sharding);
}

static int __get_qos_sleep()
{
        int qos_sleep = 0;

        qos_sleep = nodectl_get_int("recovery/qos_sleep", "0");
        qos_sleep = qos_sleep <= 0 ? 0 : qos_sleep;
        qos_sleep = qos_sleep > 10 * MSEC_PERMIN ? 10 * MSEC_PERMIN : qos_sleep;

        return qos_sleep;
}

static int __chunk_check(void *_rept, const void *k, const void *_chkinfo, const fileinfo_t *_md)
{
        int ret, online, i, needcheck;
        objinfo_t *objinfo;
        char _buf[MAX_BUF_LEN], _buf2[MAX_BUF_LEN];
        diskid_t *diskid;
        net_handle_t nh;
        rept_t *rept;
        chkinfo_t *chkinfo;

        objinfo = (void *)_buf;
        chkinfo = (void *)_buf2;
        (void)k;

        chkinfo = memcpy(chkinfo, _chkinfo, CHK_SIZE(((chkinfo_t *)_chkinfo)->repnum));
        rept = _rept;

        if (chkinfo->chkid.id == CHKID_NULL && chkinfo->chkid.id == CHKVER_NULL)
                return 0;

        yatomic_get_and_inc(&__total__, NULL);

        chk2obj(objinfo, chkinfo);

        if (__full__) {
                printf("  chunk "CHKID_FORMAT" force sync\n", CHKID_ARG(&chkinfo->chkid));

                ret = sdfs_chunk_recovery(&chkinfo->chkid);
                if (ret)
                        GOTO(err_ret, ret);

                return 0;
        }

        online = 0;
        needcheck = 0;
        if (chkinfo->repnum > _md->repnum) {
                needcheck++;
        } else {
                for (i = 0; i < (int)objinfo->repnum; i++) {
                                diskid = &objinfo->diskid[i];

                                if (is_null(diskid)) {
                                        DWARN("chunk "OBJID_FORMAT" rep %d is null\n",
                                                        OBJID_ARG(&chkinfo->chkid), i);
                                        continue;
                                }

                                id2nh(&nh, diskid);
                                ret = network_connect2(&nh, 0);
                                if (ret) {
                                        //YASSERT(0);
                                        continue;
                                }

                                online++;

                                if (diskid->status & __S_DIRTY) {
                                        needcheck++;
                                }
                        }

        }

        if (needcheck || online != (int)objinfo->repnum ) {
                printf("  chunk "OBJID_FORMAT" rep %u online %u dirty %u\n",
                                OBJID_ARG(&chkinfo->chkid), chkinfo->repnum, online, needcheck);

                yatomic_get_and_inc(&__need__, NULL);

                ret = sy_rwlock_wrlock(&rept->rwlock);
                if (ret)
                        GOTO(err_ret, ret);

                ret = _write(rept->fd, &objinfo->id, sizeof(objinfo->id));
                if (ret < 0) {
                        YASSERT(0);
                }
                rept->lost++;

                sy_rwlock_unlock(&rept->rwlock);
        }

        return 0;
err_ret:
        return ret;
}

static int __chunk_recover_send(objid_t *id, int count)
{
        int ret, i;
        objid_t *objid;

        for (i = 0; i < count; i++) {
                objid = &id[i];
                ret = sdfs_chunk_recovery(objid);
                if (ret) {
                        DWARN("chunk "OBJID_FORMAT" ret: %d\n", OBJID_ARG(objid), ret);
                        objid->id = 0;
                        objid->volid = 0;
                        objid->idx = 0;
                        __fail__++;
                        continue;
                }
        }

        return 0;
}

static int __chunk_recover_check(const objid_t *id, int count)
{
        int ret, i, retry, slp = 0;
        objid_t *objid;

        printf("\n");

        for (i = 0; i < count; i++) {
                objid = (void *)&id[i];

                if (objid->id == 0 && objid->volid == 0 && objid->idx == 0)
                        continue;

                slp = __get_qos_sleep();
                if (slp > 0) {
                        DINFO("recover qos obj["OBJID_FORMAT"] usleep[%d]\n", OBJID_ARG(objid), slp * 1000);
                        usleep(slp * 1000);
                }

                retry = 0;
retry:
                ret = sdfs_chunk_check(objid);
                if (ret) {
                        if (retry < 2) {
                                printf("  sync chunk "OBJID_FORMAT" timeout\n", OBJID_ARG(objid));
                                retry++;
                                sleep(1);
                                goto retry;
                        } else {
                                __fail__++;
                                continue;
                        }
                }

                __succ__++;

                printf("+");
                fflush(NULL);
        }

        return 0;
}

static int __etcd_report(const char *volume, int sharding, uint64_t lost, int force)
{
        int ret;
        char key[MAX_PATH_LEN], value[MAX_PATH_LEN], prefix[MAX_PATH_LEN];
        time_t now = time(NULL);

        if (force == 0) {
                if (now - __last_report__ < 5)
                        return 0;
        }

        __prefix(prefix, volume, sharding);
        snprintf(key, MAX_PATH_LEN, "%s/health", prefix);
        snprintf(value, MAX_PATH_LEN, "lost:%llu", (LLU)lost);
        DINFO("key %s value %s, last report %u\n", key, value, __last_report__);
        ret = etcd_update_text(ETCD_VOLUME, key, value, NULL, 0);
        if (ret)
                GOTO(err_ret, ret);

        __last_report__ = now;
        
        return 0;
err_ret:
        return ret;
}

static int __chunk_recover(const char *volume, int sharding, rept_t *rept, int max)
{
        int ret, count;
        objid_t buf[RECOVER_MAX];
        char path[MAX_PATH_LEN], value[MAX_BUF_LEN];;
        struct stat stbuf;

        ret = fstat(rept->fd, &stbuf);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        printf("lost %llu, begin recover from offset %llu size %llu\n", (LLU)rept->lost,
                        (LLU)rept->offset, (LLU)stbuf.st_size);

        ret = __etcd_report(volume, sharding, rept->lost, 1);
        if (ret)
                GOTO(err_ret, ret);

        ret = lseek(rept->fd, rept->offset, SEEK_SET);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        snprintf(path, MAX_PATH_LEN, "%s/losted.offset", __workdir__);

        while (1) {
                ret = _read(rept->fd, buf, sizeof(objid_t) * max);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                if (ret == 0)
                        break;

                YASSERT(ret % sizeof(objid_t) == 0);

                rept->offset += ret;
                count = ret / sizeof(objid_t);

                ret = __chunk_recover_send(buf, count);
                if (ret)
                        YASSERT(0);

                ret = __chunk_recover_check(buf, count);
                if (ret)
                        YASSERT(0);

                snprintf(value, MAX_BUF_LEN, "%llu", (LLU)rept->offset);
                printf("\nset %s %s finished %u\n", path, value, count);
                ret = _set_value(path, value, strlen(value) + 1, O_CREAT);
                if (ret)
                        GOTO(err_ret, ret);

                ret = __etcd_report(volume, sharding, rept->lost - __succ__, 0);
                if (ret)
                        GOTO(err_ret, ret);
        }

        snprintf(path, MAX_PATH_LEN, "%s/losted", __workdir__);
        unlink(path);

        ret = __etcd_report(volume, sharding, rept->lost - __succ__, 1);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

static int __stage_load(rept_t *rept, const char *volume, int sharding)
{
        int ret, fd;
        char path[MAX_PATH_LEN], tmp[MAX_PATH_LEN], buf[MAX_BUF_LEN];

        snprintf(path, MAX_PATH_LEN, "%s/losted", __workdir__);

        ret = path_validate(path, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (ret)
                GOTO(err_ret, ret);
        
        rept->fd = -1;
        rept->lost = 0;
        rept->offset = 0;

        fd = open(path, O_RDWR, 0644);
        if (fd < 0) {
                ret = errno;
                if (ret == ENOENT) {
                        snprintf(tmp, MAX_PATH_LEN, "%s/losted.tmp", __workdir__);

                        fd = open(tmp, O_CREAT | O_RDWR, 0644);
                        if (fd < 0) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }

                        snprintf(buf, MAX_BUF_LEN, "0");
                        snprintf(path, MAX_PATH_LEN, "%s/losted.offset", __workdir__);
                        ret = _set_value(path, buf, strlen(buf) + 1, O_CREAT);
                        if (ret)
                                GOTO(err_fd, ret);

                        rept->fd = fd;
                        ret = __redis_scan(volume, sharding, rept);
                        if (ret)
                                GOTO(err_fd, ret);

                        snprintf(buf, MAX_BUF_LEN, "%llu", (LLU)rept->lost);
                        snprintf(path, MAX_PATH_LEN, "%s/losted.count", __workdir__);
                        ret = _set_value(path, buf, strlen(buf) + 1, O_CREAT);
                        if (ret)
                                GOTO(err_fd, ret);

                        snprintf (path, MAX_PATH_LEN, "%s/losted", __workdir__);

                        rename(tmp, path);
                } else
                        GOTO(err_fd, ret);
        } else {
                snprintf(path, MAX_PATH_LEN, "%s/losted.count", __workdir__);
                ret = _get_value(path, buf, MAX_BUF_LEN);
                if (ret < 0) {
                        ret = -ret;
                        if (ret == ENOENT)
                                rept->lost = 0;
                        else
                                GOTO(err_fd, ret);
                } else {
#ifdef __x86_64__
                        ret = sscanf(buf, "%lu", &rept->lost);
#else
                        ret = sscanf(buf, "%llu", &rept->lost);
#endif
                }

                snprintf(path, MAX_PATH_LEN, "%s/losted.offset", __workdir__);
                ret = _get_value(path, buf, MAX_BUF_LEN);
                if (ret < 0) {
                        ret = -ret;
                        if (ret == ENOENT)
                                rept->offset = 0;
                        else
                                GOTO(err_fd, ret);
                } else {
#ifdef __x86_64__
                        ret = sscanf(buf, "%lu", &rept->offset);
#else
                        ret = sscanf(buf, "%llu", &rept->offset);
#endif
                }
        }

        rept->fd = fd;

        return 0;
err_fd:
        close(fd);
err_ret:
        return ret;
}

typedef struct {
        const char *slot;
        int sharding;
        rept_t *rept;
} itor_ctx_t;

static int __file_scan(const fileid_t *fileid, itor_ctx_t *ctx)
{
        int ret;
        fileinfo_t md;
        chkinfo_t *chkinfo;
        char buf[MAX_BUF_LEN];
        uint32_t i;
        chkid_t chkid;

        (void) ctx;
        
        ret = md_getattr((void *)&md, fileid);
        if (ret)
                GOTO(err_ret, ret);

        DINFO(CHKID_FORMAT" chkunm %u\n", CHKID_ARG(fileid), md.chknum);

        chkinfo = (chkinfo_t *)buf;
        for (i = 0; i < md.chknum; i++) {
                fid2cid(&chkid, &md.fileid, i);
                ret = md_chunk_load(&chkid, chkinfo);
                if (ret) {
                        if (ret == ENOENT)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }

                ret = __chunk_check(ctx->rept, NULL, chkinfo, &md);
                if (ret)
                        GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}

static void __redis_scan__(void *_key, void *_ctx)
{
        int ret;
        itor_ctx_t *ctx = _ctx;
        char *key = _key;
        fileid_t fileid;

        (void) ctx;

        memset(&fileid, 0x0, sizeof(fileid));
        ret = sscanf(key, "f:%ju/%ju", &fileid.volid, &fileid.id);
        YASSERT(ret == 2);
        fileid.sharding = ctx->sharding;
        fileid.type = ftype_file;
        
        DINFO("scan %s -> "CHKID_FORMAT"\n", key, CHKID_ARG(&fileid));

        __file_scan(&fileid, ctx);
        
        return;
}

static int __redis_scan(const char *volume, int sharding, rept_t *rept)
{
        int ret, count;
        itor_ctx_t ctx;
        char *list[2], key[MAX_NAME_LEN],
                value[MAX_NAME_LEN], prefix[MAX_NAME_LEN];
        redis_conn_t *conn;

        __prefix(prefix, volume, sharding);
        ctx.sharding = sharding;
        ctx.rept = rept;

        snprintf(key, MAX_PATH_LEN, "%s/health", prefix);
        snprintf(value, MAX_PATH_LEN, "lost:0");
        ret = etcd_create_text(ETCD_VOLUME, key, value, 0);
        if (ret) {
                if (ret == EEXIST) {
                        //pass
                } else
                        GOTO(err_ret, ret);
        }
        
        snprintf(key, MAX_PATH_LEN, "%s/master", prefix);
        ret = etcd_get_text(ETCD_VOLUME, key, value, NULL);
        if (ret)
                GOTO(err_ret, ret);

        printf("scan redis slot %s addr (%s)\n", prefix, value);

        count = 2;
        _str_split(value, ' ', list, &count);
        if (count != 2) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        int port = atoi(list[1]);
        ret = redis_connect(&conn, list[0], &port);
        if(ret)
                GOTO(err_ret, ret);

        sy_rwlock_init(&rept->rwlock, NULL);
        
        ret = redis_iterator(conn, "f:*", __redis_scan__, &ctx);
        if(ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

int __health_redis_online(const char *_addr)
{
        int ret, count;
        char *list[2], addr[MAX_NAME_LEN];
        redis_conn_t *conn;

        strcpy(addr, _addr);

        count = 2;
        _str_split(addr, ' ', list, &count);
        if (count != 2) {
                ret = EINVAL;
                UNIMPLEMENTED(__DUMP__);
        }

        int port = atoi(list[1]);
        ret = redis_connect(&conn, list[0], &port);
        if (ret)
                return 0;
        else {
                redis_disconnect(conn);
                return 1;
        }
}

static int __health_redis(const char *volume, int slot, int replica, int *master, int *_slave)
{
        int ret, i, slave, online;
        char master_addr[MAX_NAME_LEN], addr[MAX_NAME_LEN], key[MAX_NAME_LEN];

        snprintf(key, MAX_NAME_LEN, "%s/solt/%d/master", volume, slot);
        ret = etcd_get_text(ETCD_VOLUME, key, master_addr, NULL);
        if(ret) {
                if (ret == ENOKEY) {
                        *master = 0;
                        *_slave = 0;
                        goto out;
                } else
                        GOTO(err_ret, ret);
        }

        slave = 0;
        *master = 0;
        for (i = 0; i < replica; i++) {
                snprintf(key, MAX_PATH_LEN, "%s/solt/%d/redis/%d", volume, slot, i);

                ret = etcd_get_text(ETCD_VOLUME, key, addr, NULL);
                if(ret) {
                        GOTO(err_ret, ret);
                }

                online = __health_redis_online(addr);
                if (strncmp(addr, master_addr, strlen(master_addr)) == 0) {
                        *master = online;
                } else {
                        slave += online;
                }
        }

        *_slave = slave;
out:
        
        return 0;
err_ret:
        return ret;
}

static int __health_dump_sharding(const char *volume, int idx, int replica)
{
        int ret, master, slave;
        char key[MAX_PATH_LEN], value[MAX_PATH_LEN];

        snprintf(key, MAX_PATH_LEN, "%s/solt/%d/health", volume, idx);
        ret = etcd_get_text(ETCD_VOLUME, key, value, NULL);
        if(ret) {
                if (ret == ENOKEY) {
                        snprintf(value, MAX_PATH_LEN, "lost:0");
                } else 
                        GOTO(err_ret, ret);
        }

        ret = __health_redis(volume, idx, replica, &master, &slave);
        if(ret)
                GOTO(err_ret, ret);
        
        printf("    metadata[%d]:\n"
               "        master:%s\n"
               "        slave:%d/%d\n"
               "        %s\n",
               idx,
               master ? "online" : "offline",
               slave, replica - 1,
               value);
        
        return 0;
err_ret:
        return ret;
}

static int __health_dump_volume(const char *volume)
{
        int ret, i, sharding, replica;
        char key[MAX_PATH_LEN], value[MAX_PATH_LEN];

        snprintf(key, MAX_PATH_LEN, "%s/sharding", volume);
        ret = etcd_get_text(ETCD_VOLUME, key, value, NULL);
        if (ret)
                GOTO(err_ret, ret);

        sharding = atoi(value);

        snprintf(key, MAX_PATH_LEN, "%s/replica", volume);
        ret = etcd_get_text(ETCD_VOLUME, key, value, NULL);
        if (ret)
                GOTO(err_ret, ret);

        replica = atoi(value);

        
        for (i = 0; i < sharding; i++) {
                ret = __health_dump_sharding(volume, i, replica);
                if(ret)
                        GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}

static int __health_dump()
{
        int ret, i;
        etcd_node_t *array, *node;

        ret = etcd_list(ETCD_VOLUME, &array);
        if (ret)
                GOTO(err_ret, ret);

        for (i = 0; i < array->num_node; i++) {
                node = array->nodes[i];

                printf("%s:\n", node->key);
                ret = __health_dump_volume(node->key);
                if (ret)
                        GOTO(err_free, ret);
        }        

        free_etcd_node(node);

        return 0;
err_free:
        free_etcd_node(node);
err_ret:
        return ret;
}

void __path_trans(const char *slot, char *volume, int *sharding)
{
        int count;
        char path[MAX_PATH_LEN];
        char *list[10];

        strcpy(path, slot);
        count = 10;
        _str_split(path, '/', list, &count);

        strcpy(volume, list[3]);
        *sharding = atoi(list[5]);
}

static int __scan_slot(rept_t *rept, int thread, const char *volume, int sharding)
{
        int ret, retry = 0;
        
        yatomic_init(&__total__, 0);
        yatomic_init(&__need__, 0);
        __succ__ = 0;
        
        snprintf(__workdir__, MAX_PATH_LEN, "%s/%s/%d", ng.home, volume, sharding);
        printf("workdir %s\n", __workdir__);

        ret = __stage_load(rept, volume, sharding);
        if (ret) {
                GOTO(err_ret, ret);
        } else {
                if(true == __check_flag__){
                        printf("check finish.\n");
                        exit(ret);
                }
        }

retry:
        ret = __chunk_recover(volume, sharding, rept, thread);
        if (ret)
                GOTO(err_ret, ret);


        if (__need__.value == __succ__) {
                printf("recover success\n");
        } else {
                printf("recover (%llu/%llu), retry %d\n",
                       (LLU)__succ__, (LLU)__need__.value, retry);

                retry++;
                if (retry < 10) {
                        usleep(1000 * 1000 * 1);
                        goto retry;
                } else {
                        ret = EAGAIN;
                        GOTO(err_ret, ret);
                }
        }
        
        return 0;
err_ret:
        return ret;
}

static int __scan_all__(rept_t *rept, int thread, const char *volume)
{
        int ret, i, sharding;
        char key[MAX_PATH_LEN], value[MAX_PATH_LEN];

        snprintf(key, MAX_PATH_LEN, "%s/sharding", volume);
        ret = etcd_get_text(ETCD_VOLUME, key, value, NULL);
        if (ret)
                GOTO(err_ret, ret);

        sharding = atoi(value);

        for (i = 0; i < sharding; i++) {
                ret = __scan_slot(rept, thread, volume, i);
                if(ret)
                        GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}

static int __scan_all(rept_t *rept, int thread)
{
        int ret, i;
        etcd_node_t *array, *node;

        ret = etcd_list(ETCD_VOLUME, &array);
        if (ret)
                GOTO(err_ret, ret);

        for (i = 0; i < array->num_node; i++) {
                node = array->nodes[i];

                ret = __scan_all__(rept, thread, node->key);
                if (ret) {
                        if (ret == ENOKEY) {
                                continue;
                        } else 
                                GOTO(err_free, ret);
                }
        }        

        free_etcd_node(array);

        return 0;
err_free:
        free_etcd_node(array);
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret, t;
        char c_opt;
        const char *slot = NULL;
        rept_t rept;

        t = 100;
        while (srv_running) {
                int option_index = 0;

                static struct option long_options[] = {
                        {"check", 0, NULL, 'c'},
                        {"full", 0, NULL, 'f'},
                        {"thread", required_argument, NULL, 't'},
                        {"scan", required_argument, NULL, 's'},
                        {NULL, 0, NULL, 0}
                };

                c_opt = getopt_long(argc, argv, "cft:s:",
                                    long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 'c':
                        __check_flag__ = true;
                        break;
                case 'f':
                        __full__ = 1;
                        break;
                case 't':
                        t = atoi(optarg);
                        break;
                case 's':
                        slot = optarg;
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        usage(argv[0]);
                        exit(1);
                }
        }

        t = t < RECOVER_MAX ? t : RECOVER_MAX;
        //printf("scan home: %s\n", home);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if(ret)
                exit(1);

        dbg_info(0);

        ret = ly_init_simple("sdfs.health");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        }

        if (slot == NULL) {
                __health_dump();
        } else {
                int double_check = 0;
                int retry = 0;
        retry:
                __fail__ = 0;
                __succ__ = 0;
                if (strcmp(slot, "all") == 0) {
                        ret = __scan_all(&rept, t);
                        if (ret)
                                GOTO(err_ret, ret);
                } else {
                        int sharding;
                        char volume[MAX_NAME_LEN];

                        __path_trans(slot, volume, &sharding);

                        ret = __scan_slot(&rept, t, volume, sharding);
                        if (ret)
                                GOTO(err_ret, ret);
                }

                if (__fail__) {
                        retry++;
                        DWARN("success %u fail %u, retry %u\n", __succ__, __fail__, retry);
                        sleep(1);
                        if (retry > 10) {
                                ret = EAGAIN;
                                GOTO(err_ret, ret);
                        } else {
                                goto retry;
                        }
                } else {
                        double_check++;
                        if (double_check < 2) {
                                DINFO("double check\n");
                                goto retry;
                        }
                }
        }


        return 0;
err_ret:
        exit(ret);
}

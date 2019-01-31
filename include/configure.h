#ifndef __CONFIGURE_H__
#define __CONFIGURE_H__

#include <stdint.h>
#include "sdfs_conf.h"
#define MAXSIZE 256

#define MAX_WAIT_LEN 1024

#define IDLE_THREADS 2

#define SPLIT_MIN 23
#define SPLIT_MAX 27

#define LOCK_TIMEOUT 20
#define LICH_IO_TIMEOUT 25

#define LICH_SPLIT_MAX 31
#define LICH_VM_MAX 1024
#define SCHEDULE_MAX  (LICH_VM_MAX * 2)
#define SDEVENTS_THREADS_MAX 256
#define DISK_WORKER_MAX 16

#define BUF_SIZE_4K     (1024 * 4)
#define BUF_SIZE_8K     (1024 * 8)
#define BUF_SIZE_16K    (1024 * 16)
#define BUF_SIZE_32K    (1024 * 32)
#define BUF_SIZE_64K    (1024 * 64)
#define BUF_SIZE_128K   (1024 * 128)
#define BUF_SIZE_512K   (1024 * 512)
#define BUF_SIZE_10M  (1024 * 1024 * 10)
#define BUF_SIZE_100M  (1024 * 1024 * 100)

#define PAGE_SIZE_4K (1024*4)
#define PAGE_SIZE_32K (1024*32)
#define DEFAULT_STACK_SIZE (BUF_SIZE_512K)
#define KEEP_STACK_SIZE (1024)
#define MAX_MSG_SIZE (512)
#define UUID_LEN        (64)

enum {
    V_STATE,
    V_NUMBER,
    V_STRING,
} value_type;

struct yftp_export_t
{
        char user[MAX_USERNAME_LEN];//32
        char password[MAX_PASSWD_LEN];//128
        char path[MAX_PATH_LEN];//4k
        char permision[MAX_INFO_LEN];//512
};

struct yftp_conf_t
{
        int export_size;
        struct yftp_export_t yftp_export[1024];
};

struct nfsconf_export_t
{
        char path[MAXSIZE];
        char ip[16];
        char permision[MAXSIZE];
};

#define  MAX_NET_COUNT 12

typedef struct {
        int count;
        struct {
                uint32_t network;
                uint32_t mask;
        } network[MAX_NET_COUNT];
} netconf_t;

/* nfs configure */
struct nfsconf_t
{
        int use_export;
        int export_size;
        int nfs_port;
        int nlm_port;
        int rsize;
        int wsize;
        struct nfsconf_export_t nfs_export[1024];
};

/* iscsi configure */
struct sanconf_t
{
        char iqn[MAXSIZE];
        int lun_blk_shift;
        int tcp_discovery;
};

/* web configure */
struct webconf_t
{
    int webport;
    int use_ratelimit;
};

/* mds configure */
struct mdsconf_t
{
        int chknew_hardend;
        char db[MAX_NAME_LEN];
        uint64_t disk_keep;

#if 0
        int redis_wait;
        int redis_thread;

        int redis_total;
        char leveldb[MAX_PATH_LEN];
        int leveldb_physical_package_id;
        int schedule_physical_package_id;

        
        int leveldb_queue; //线程池的个数
        int leveldb_queue_pb; //线程池中处理Pb任务的个数
        int leveldb_queue_worker; //每个线程池中，线程个数
#endif

        int main_loop_threads;
        int redis_baseport;
        int redis_sharding;
        int redis_replica;
};

/* cds configure */
struct cdsconf_t
{
        int disk_timeout;
        int unlink_async;
        int queue_depth;
        int prealloc_max;
        int ec_lock;
        int io_sync;

        int lvm_qos_refresh;
};

struct logconf_t
{
        int log_ylib;
        int log_yliblock;
        int log_ylibmem;
        int log_ylibskiplist;
        int log_ylibnls;
        int log_ysock;
        int log_ynet;
        int log_yrpc;
        int log_yfscdc;
        int log_yfsmdc;
        int log_fsmachine;
        int log_yfslib;
        int log_yiscsi;
        int log_ynfs;
        int log_yfsmds;
        int log_cdsmachine;
        int log_yfscds;
        int log_yfscds_robot;
        int log_proxy;
        int log_yftp;
        int log_yfuse;
};


/* global configure */
struct gloconf_t
{
        char cluster_name[MAXSIZE];
        char iscsi_iqn[MAXSIZE];
        int coredump;
        int backtrace;
        int testing;
        int rpc_timeout;
        int lease_timeout;
        int hb_timeout;
        int hb_retry;
        char nfs_srv[MAXSIZE];

        int solomode;

        int memcache_count;
        int memcache_seg;
        
        //fake config
        int rdma;

        int chunk_rep;
        char workdir[MAXSIZE];
        int check_mountpoint;
        int check_license;
        int maxcore;
        uint32_t network;
        uint32_t mask;
        int performance_analysis;
        int write_back;
        uint64_t cache_size;
        int net_crc;
        int dir_refresh;
        int file_refresh;
        int wmem_max;
        int rmem_max;
        int check_version;
        int io_dump;
        int restart;
        char master_vip[MAXSIZE]; //ip1,ip2,vip
        int valgrind;
        int polling_core;
        int polling_timeout;
        int aio_core;
        int direct_port;

        int sdevents_threads;
        int jobdock_size;

        uint64_t chunk_entry_max;

        int disk_mt;
        int disk_mt_hdd;
        int disk_mt_ssd;

        int disk_worker;

        int hb;

        int main_loop_threads; //yfslib 的 schedule个数
        int schedule_physical_package_id;
        int max_lvm; /*最大lvm个数*/
};

int conf_init(const char *conf_path);
int conf_destroy(void);

extern struct sanconf_t sanconf;
extern struct nfsconf_t nfsconf;
extern struct webconf_t webconf;
extern struct mdsconf_t mdsconf;
extern struct cdsconf_t cdsconf;
extern struct gloconf_t gloconf;
extern struct logconf_t logconf;
extern struct c60conf_t c60conf;
extern netconf_t netconf;
extern struct yftp_conf_t yftp_conf;

#define MAX_SMALL_VOLUME 1024

#define SYSTEM_ROOT "/system"
#define SMALL_ROOT "/small"

#define SYSTEM_SMALL_VOLUME_LIST "/system/small_volume_list"

#define SOCKID_NORMAL 10
#define SOCKID_CORENET 20

#define REDIS_BASEPORT 6490

#define NLM_SERVICE_DEF 3001
#define NFS_SERVICE_DEF 3049

#endif

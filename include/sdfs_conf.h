#ifndef __SYSY_CONST_H__
#define __SYSY_CONST_H__

#include <cmakeconfig.h>
#include <unistd.h>
#include <stdint.h>

#define META_VERSION 0
#define META_VERSION_KEY "meta_version"

#if USE_EPOLL
#include <sys/epoll.h>
#else
#include <sys/select.h>
#endif

#if USE_EPOLL

    #if 1
    #define EPOLL_EDGE
    #endif

    #ifdef EPOLLRDHUP
    #define HAVE_EPOLLRDHUP
    #endif

    #ifdef EPOLL_EDGE

    #ifdef HAVE_EPOLLRDHUP
    #define Y_EPOLL_EVENTS_LISTEN (EPOLLIN | EPOLLERR | EPOLLRDHUP)
    #define Y_EPOLL_EVENTS (Y_EPOLL_EVENTS_LISTEN | EPOLLET)
    #else
    #define Y_EPOLL_EVENTS_LISTEN (EPOLLIN | EPOLLERR)
    #define Y_EPOLL_EVENTS (Y_EPOLL_EVENTS_LISTEN | EPOLLET)
    #endif

    #else

    #ifdef HAVE_EPOLLRDHUP
    #define Y_EPOLL_EVENTS_LISTEN (EPOLLIN | EPOLLERR | EPOLLRDHUP)
    #define Y_EPOLL_EVENTS Y_EPOLL_EVENTS_LISTEN
    #else
    #define Y_EPOLL_EVENTS_LISTEN  (EPOLLIN | EPOLLERR)
    #define Y_EPOLL_EVENTS Y_EPOLL_EVENTS_LISTEN
    #endif

    #endif
#else
    #define _EPOLLIN 0x001
    #define _EPOLLOUT 0x004
    #define _EPOLLERR 0x008
    #define _EPOLLRDHUP 0x2000

    #define Y_EPOLL_EVENTS_LISTEN (_EPOLLIN | _EPOLLERR | _EPOLLRDHUP)
    #define Y_EPOLL_EVENTS Y_EPOLL_EVENTS_LISTEN
#endif


#define MAX_USERNAME_LEN 32

#define MIN_PASSWD_LEN 6
#define MAX_PASSWD_LEN 128

#define MAX_PATH_LEN    (1024 * 4)
#define MAX_NAME_LEN    (256)
#define MAX_INFO_LEN    (512)
#define MAX_U64_LEN     (64)

#define MAX_BUF_LEN     (1024 * 4)
#define MAX_MSG_LEN     (1024 * 16)
#define BIG_BUF_LEN     (1024 * 512)

#define PAGE_SIZE (1024 * 4)
#define SDFS_BLOCK_SIZE 512

#define RW_TIMEOUT gloconf.rpc_timeout

#define MAX_OPEN_FILE 100000
/**
 * for block-level checksum
 */
#define YFS_SEG_CHECKSUM 0

#define YFS_SEG_LEN      MAX_BUF_LEN

#define MAX_LINE_LEN    (1024 * 2)

#define BITS_PER_CHAR   8

#define SKIPLIST_MAX_LEVEL   24
#define SKIPLIST_CHKSIZE_DEF 1

#define DIR_SUB_MAX 1024LLU

#define MAX_RETRY 5
#define NFS_MAX_RETRY 180

#define YFS_LINEAR_DATA_FLOW 0
#define CDS_DATA_CACHE 0

#define YFS_CONFIGURE_FILE SDFS_HOME"/etc/sdfs.conf"
#define YFTP_PASSWORD_FILE SDFS_HOME"/etc/ftp.conf"
#define YNFS_CONFIGURE_FILE SDFS_HOME"/etc/exports.conf"
#define YFS_REDIS_CLUSTER_CONF SDFS_HOME"/etc/redis/redis_cluster.conf"

#define YFS_REDIS_ID_DIR "/var/lib/redis/atomic_id"
#define YFS_LEVELDB_ID_DIR "/var/lib/leveldb/atomic_id"
#define USS_SYSTEM_ATTR_SYMLINK "uss_symtem_symlink"
#define USS_SYSTEM_ATTR_ENGINE "uss_system_engine"
#define USS_SYSTEM_ATTR_TIER "uss_system_tier"

typedef enum {
        TIER_SSD = 0,
        TIER_HDD,
        TIER_ALL,
} tier_type_t;
#define TIER_NULL (-1)

typedef uint64_t yfs_off_t;
typedef uint64_t yfs_size_t;

typedef uint32_t chk_off_t;
typedef uint32_t chk_size_t;

typedef unsigned long long LLU;
typedef long long LLD;

#define EXPIRED_TIME  60

#ifndef O_NOATIME
#define O_NOATIME 0
#endif

#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif

#define SLIST_GROUP 10
#define SLIST_CHUNK_SIZE 1
#define SO_XMITBUF (1024 * 1024 * 100)     /* 100MB */

//#define WRITE_ASYNC

#define RETRY_ERROR(__ret__)                                            \
        (__ret__ == ETIME || __ret__ == ETIMEDOUT || __ret__ == ENODATA \
         || __ret__ == ENONET || __ret__ == ENOTCONN)

#define SHM_MAX  (10 * 1024 * 1024) /*10M?*/

#define EXT3_SUPER_MAGIC     0xEF53
#define EXT4_SUPER_MAGIC     0xEF53
#define REISERFS_SUPER_MAGIC 0x52654973
#define XFS_SUPER_MAGIC      0x58465342
#define JFS_SUPER_MAGIC 0x3153464a

#define YFS_CHK_REP_MAX (32)
#define YFS_CHK_SIZE   (sizeof(char) * 1024 * 1024 * 64)     /* 64MB */


/*magic in disk*/
#define YFS_MAGIC (0x6d12ecaf + 1)
#define META_VERSION0  (YFS_MAGIC + 11)
#define META_VERSION1  (META_VERSION0 + 1)
#define META_VERSION2  (META_VERSION0 + 2) /*haven't used yet*/

#define YFS_STATUS_PRE "status"

#define SYS_PAGE_SIZE 4096

#define STEP_MAX 100
#define QUEUE_MAX 1000

#define LEVELDB_QUEUE_WORKER_MAX 2
#define LEVELDB_QUEUE_PB_MAX 8
#define LEVELDB_QUEUE_MAX 48

//#define NETWORK_CONF SDFS_HOME"/etc/cluster.conf"

#if 0
#define __CENTOS5__
#endif

#define PIPE_SIZE (4096 * 16)

#define DEBUG_NULL_NID

#pragma pack(8)

typedef struct {
        uint32_t crc;
        uint32_t len;
        char buf[0];
} container_blk_t;

#pragma pack()

#define MAX_NODEID_LEN 128
#define BUFFER_SEG_SIZE (1024 * 1024)

#define SEC_PER_MIN                 (60)
#define SEC_PER_HOU                 (60 * 60)
#define SEC_PER_DAY                 (60 * 60 * 24)

#define MSEC_PER_SEC                (1000)
#define USEC_PER_SEC                (1000 * 1000)
#define USEC_PER_MIN                ((LLU)USEC_PER_SEC * SEC_PER_MIN)
#define USEC_PER_HOU                ((LLU)USEC_PER_SEC * SEC_PER_HOU)
#define USEC_PER_DAY                ((LLU)USEC_PER_SEC * SEC_PER_DAY)

#define ROLE_MOND "mond"
#define ROLE_CDS "cds"
#define ROLE_NFS3 "nfs3"
#define ROLE_FTP "ftp"

#define ETCD_REDIS "redis"
#define ETCD_VOLUME "volume"
#define ETCD_CDS "cds"
#define ETCD_DISKMAP "diskmap"

#define ENABLE_HEARTBEAT 1

#define ENABLE_CORENET 1
#define ENABLE_CORERPC 1
#define ENABLE_COREAIO 1
#define ENABLE_COREAIO_THREAD 0

#define ENABLE_QUOTA 0
#define ENABLE_MD_POSIX 1
#define ENABLE_KLOCK 1

#define SDFS_SYSTEM_VOL "system"

#define REDIS_CONN_POOL 64

#define ENABLE_CO_WORKER 1

#if ENABLE_CO_WORKER
#define ENABLE_REDIS_CO 1
#else
#define ENABLE_CORE_PIPELINE 1
#endif

#define ENABLE_REDIS_PIPELINE 0

#define ENABLE_LOCK_FREE_LIST 0

#define ENABLE_ATTR_QUEUE 1

#define MAX_READDIR_ENTRIES 400

#define YFS_META_VERSION "meta (2017Q3)\n"

#define FAKE_BLOCK 4096

#define ATTR_QUEUE_TMO 5

#endif

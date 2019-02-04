#ifndef __YNET_NET_H__
#define __YNET_NET_H__

#include <stdint.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "sdfs_buffer.h"
#include "net_proto.h"
#include "sdevent.h"
#include "sdfs_id.h"
#include "ylock.h"
#include "ylib.h"
#include "ynet_conf.h"
#include "msgqueue.h"
#include "configure.h"
#include "job_dock.h"
#include "adt.h"
#include "../sock/ynet_sock.h"
#include "dbg.h"

#define YNET_PROTO_TCP_MAGIC 0xeebc0def

#define YNET_NET_ERR_MAGIC   0x1bcdef69

typedef enum {
        YNET_MSG_REQ = 0x01,
        YNET_MSG_RECV = 0x02,
        YNET_MSG_REP = 0x04,
        YNET_DATA_REP = 0x08,
} net_msgtype_t;

#define YNET_NET_REQ_OFF (sizeof(uint32_t) * 3)

#pragma pack(8)

typedef struct  {
        uint32_t magic;
        uint32_t len;
        uint32_t crcode;     /* crc code of following data */
        uint16_t priority;
        uint16_t type;
        uint32_t prog;
        uint32_t blocks;
        msgid_t msgid;
        uint32_t time;
        uint32_t master_magic;
        uint64_t load;
        buffer_t reply_buf;
        char buf[0];
} ynet_net_head_t;

#pragma pack()

#define YNET_REQ_CRC(req) \
do { \
        uint32_t crcode; \
 \
        crcode = crc32_sum(\
                          (const char *)((void *)(req) + YNET_NET_REQ_OFF), \
                          (req)->rq_reqlen - YNET_NET_REQ_OFF); \
 \
        (req)->rq_crcode = crcode; \
} while (0)

#define __YNET_PACK_CRC(req) \
do { \
        uint32_t crcode; \
 \
        crcode = crc32_sum((const char *)((void *)(req) + YNET_NET_REQ_OFF), \
                          (req)->len - YNET_NET_REQ_OFF); \
 \
        (req)->crcode = crcode; \
} while (0)

int ynet_pack_crcsum(buffer_t *pack);
int ynet_pack_crcverify(buffer_t *pack);

typedef struct {
        uint32_t magic;
        uint32_t err;
} ynet_net_err_t;

static inline int ynet_pack_err(buffer_t *pack)
{
        ynet_net_err_t net_err;

        if (pack->len == sizeof(ynet_net_err_t)) {
                mbuffer_get(pack, &net_err, sizeof(ynet_net_err_t));
                if (net_err.magic == YNET_NET_ERR_MAGIC)
                        return net_err.err;

                return 0;
        }

        return 0;
}

#define YNET_NET_REP_OFF (sizeof(uint32_t) * 2)

#define YNET_REP_CRC(rep) \
do { \
        uint32_t crcode; \
 \
        crcode = crc32_sum((const char *)((void *)(rep) + YNET_NET_REP_OFF), \
                          (rep)->rq_replen - YNET_NET_REP_OFF); \
 \
        (rep)->rq_crcode = crcode; \
} while (0)

#pragma pack(8)

/**
 * @note persist in etcd
 */
typedef struct {
        uint32_t len; /*length of the info*/
        uint32_t uptime;
        nid_t id;
        char name[MAX_NODEID_LEN];
        char nodeid[MAX_NODEID_LEN];
        uint32_t magic;
        uint16_t deleting;
        uint16_t info_count;       /**< network interface number */
        uint16_t __padding;
        ynet_sock_info_t main;
        ynet_sock_info_t corenet[0];  /**< host byte order */
        //ynet_sock_info_t info[0];
} ynet_net_info_t;

#pragma pack()

static inline int str2netinfo(ynet_net_info_t *info, const char *buf)
{
        int ret;
        const char *addrs;
        char addr[MAX_NAME_LEN], name[MAX_NAME_LEN];
        uint32_t port, i;

        memset(info, 0x0, sizeof(*info));

        ret = sscanf(buf,
                     "len:%d\n"
                     "uptime:%u\n"
                     "nid:%u\n"
                     "hostname:%[^\n]\n"
                     "nodeid:%[^\n]\n"
                     "magic:%d\n"
                     "main:%[^/]/%d\n"
                     "corenet_count:%"SCNd16"\n"
                     "corenet:",
                     &info->len,
                     &info->uptime,
                     &info->id.id,
                     info->name,
                     info->nodeid,
                     &info->magic,
                     name, &port,
                     &info->info_count);
        if (ret != 9) {
                UNIMPLEMENTED(__DUMP__);
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }

        info->main.addr = inet_addr(name);
        info->main.port = htons(port);
        addrs = strstr(buf, "corenet:") + 5;
        for (i = 0; i < info->info_count; ++i) {
                ret = sscanf(addrs, "%[^/]/%d", addr, &port);
                YASSERT(ret == 2);
                addrs = strchr(addrs, ',') + 1;

                info->corenet[i].addr = inet_addr(addr);
                info->corenet[i].port = htons(port);
        }

        return 0;
err_ret:
        return ret;
}

static inline void netinfo2str(char *buf, const ynet_net_info_t *info)
{
        uint32_t i;
        const ynet_sock_info_t *sock;

        snprintf(buf, MAX_NAME_LEN,
                 "len:%d\n"
                 "uptime:%u\n"
                 "nid:"NID_FORMAT"\n"
                 "hostname:%s\n"
                 "nodeid:%s\n"
                 "magic:%d\n"
                 "main:%s/%d\n"
                 "corenet_count:%u\n"
                 "corenet:",
                 info->len,
                 info->uptime,
                 NID_ARG(&info->id),
                 info->name,
                 info->nodeid,
                 info->magic,
                 _inet_ntoa(info->main.addr), ntohs(info->main.port),
                 info->info_count);

        YASSERT(strlen(info->name));
        YASSERT(info->info_count * sizeof(ynet_sock_info_t) + sizeof(ynet_net_info_t) == info->len);
        
        for (i = 0; i < info->info_count; i++) {
                sock = &info->corenet[i];
                snprintf(buf + strlen(buf), MAX_NAME_LEN, "%s/%u,",
                         _inet_ntoa(sock->addr), ntohs(sock->port));
        }

        //DINFO("\n%s\n", buf);
}

static inline int net_gethostname(char *hostname, int len)
{
        int ret;
        char name[MAX_NAME_LEN];

        (void) len;

        ret = gethostname(name, MAX_NAME_LEN);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        strncpy(hostname, name, len);

        return 0;
err_ret:
        return ret;
}

#define INFO_DUMP(__netinfo__)                                          \
do {                                                                    \
        uint32_t __i__;                                                 \
        uint32_t __info_count__;                                        \
        struct in_addr __sin__;                                         \
                                                                        \
        DINFO("info %s [len %u, nid "NID_FORMAT", magic %u, rep %u, count %u]\n", \
              (__netinfo__)->name, (__netinfo__)->len,                  \
              NID_ARG(&__netinfo__->id),                             \
              (__netinfo__)->magic, (__netinfo__)->conn_rep, (__netinfo__)->info_count); \
                                                                        \
        __info_count__ = (__netinfo__)->info_count < 2 ? (__netinfo__)->info_count:2; \
        for (__i__ = 0; __i__ < __info_count__; __i__++) {              \
                __sin__.s_addr = (__netinfo__)->info[__i__].addr; \
                DBUG("sock %p [%u, %u]\n", &(__netinfo__)->info[__i__], \
                     (__netinfo__)->info[__i__].addr, (__netinfo__)->info[__i__].port); \
                DINFO("net[%u]: %s:%u\n", __i__,                    \
                      inet_ntoa(__sin__), (__netinfo__)->info[__i__].port); \
        }                                                               \
} while (0);

#if 0
typedef enum {
        NETABLE_NULL,
        NETABLE_CONN,
        NETABLE_DEAD,
} netstatus_t;

typedef struct {
        net_handle_t sock;
} socklist_t;

typedef struct {
        uint32_t prev;
        uint32_t now;
} ltime_t;

typedef struct __connection {
        net_handle_t nh;
        netstatus_t status;
        sy_spinlock_t load_lock;

        char lname[MAX_NAME_LEN];
        ynet_net_info_t *info;

        uint64_t load;        ///< latency，用于副本读的负载均衡
        ltime_t ltime;
        time_t update;
        time_t last_retry;
        uint32_t timeout;

        struct list_head reset_handler;

        net_handle_t sock;
} ynet_net_conn_t;
#endif

/* net_lib.c */
int net_init(net_proto_t *);
int net_destroy(void);

/* net_passive.c */
int net_hostbind(int *srv_sd, const char *host, const char *service, int nonblock);
int net_hostlisten(int *srv_sd, const char *host, const char *service,
                          int qlen, int nonblock);
int net_addrlisten(int *srv_sd, uint32_t addr, uint32_t port, int qlen, int nonblock);
int net_portlisten(int *srv_sd, uint32_t addr, uint32_t *port, int qlen, int nonblock);
int net_getinfo(char *infobuf, uint32_t *infobuflen, uint32_t port);

/* net_xmit.c */

void hosts_split(const char *name, char *site, char *rack, char *node, char *disk);
void disk2node(const char *disk, char *node);
void disk2rack(const char *disk, char *rack);
void disk2site(const char *disk, char *site);

int hosts_init(void);
void hosts_dump(void);
int ip2hostname(const char *ip, char *name);

//just for compatible, will be removed
static inline void ynet_net_info_dump(void *infobuf)
{
        uint32_t i;
        uint32_t info_count;
        struct in_addr sin;
        ynet_net_info_t *netinfo = (ynet_net_info_t *)infobuf;

        // DINFO("uuid %s\n", netinfo->nodeid);
        DBUG("info %p [len %u, "DISKID_FORMAT" %u %u]\n",
                netinfo, netinfo->len, DISKID_ARG(&netinfo->id),
                netinfo->magic, netinfo->info_count);

        info_count = netinfo->info_count < 2 ? netinfo->info_count:2;
        for (i = 0; i < info_count; ++i) {
                sin.s_addr = netinfo->corenet[i].addr;
                DBUG("sock %p [%u, %u]\n", &netinfo->corenet[i], netinfo->corenet[i].addr, netinfo->corenet[i].port);
                DBUG("net[%u]: (%u)%s:%u\n", i, netinfo->corenet[i].addr, inet_ntoa(sin), netinfo->corenet[i].port);
        }
}

#endif

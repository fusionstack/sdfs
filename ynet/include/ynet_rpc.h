#ifndef __YNET_RPC_H__
#define __YNET_RPC_H__

#include <stdint.h>

#include "ynet_net.h"
#include "net_table.h"
#include "job.h"
#include "sdfs_id.h"
#include "adt.h"
#include "sdfs_conf.h"

/**
 * @file RPC Network Library
 *
 * 有三类rpc：
 * - minirpc.h
 * - ynet_rpc.h
 * - corerpc.h (sockid + core hash)
 *
 * 节点之间两两有心跳(提高感知故障的灵敏度）：
 * - heartbeat.h
 *
 * 离线消息处理：
 * - msgqueue.h
 *
 * 应用层采用nid作为通信端点标识，映射到sockid（单连接）
 *
 * @see minirpc.h
 * @see corerpc.h
 *
 */

typedef struct {
        nid_t nid;
} rpc_getinfo_t;

/* rpc_lib.c */
int rpc_init(net_proto_t *op, const char *name, int seq, const char *path);
int rpc_destroy(void);

/* rpc_passive.c */
int rpc_passive(uint32_t port);
int rpc_start();
int rpc_hostlisten(int *sd, const char *host, const char *service,
                          int qlen, int nonblock);
int rpc_portlisten(int *sd, uint32_t addr, uint32_t *port, int qlen,
                          int nonblock);

void rpc_request_register(int type, net_request_handler handler, void *context);

/* rpc_reply.c */
void rpc_reply_prep1(const msgid_t *msgid, buffer_t *buf, buffer_t *data);
void rpc_reply_prep(const msgid_t *msgid, buffer_t *buf, buffer_t *data, int flag);
void rpc_reply_error(const sockid_t *sockid, const msgid_t *msgid, int _error);
void rpc_reply_error_prep(const msgid_t *msgid, buffer_t *buf, int _error);
void rpc_reply(const sockid_t *sockid, const msgid_t *msgid,
               const void *_buf, int len);
void rpc_reply1(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf);

/* rpc_xnect.c */
int rpc_getinfo(char *infobuf, uint32_t *infobuflen);

/* rpc_request.c */

// for write
int rpc_request_wait1(const char *name, const nid_t *nid, const void *request,
                      int reqlen, const buffer_t *wbuf, int msg_type,
                      int priority, int timeout);

// for read
int rpc_request_wait2(const char *name, const nid_t *nid, const void *request,
                      int reqlen, buffer_t *rbuf, int msg_type,
                     int priority, int timeout);
int rpc_request_wait(const char *name, const nid_t *nid, const void *request,
                     int reqlen, void *reply, int *replen, int msg_type,
                     int priority, int timeout);

int rpc_request_wait_sock(const char *name, const net_handle_t *nh, const void *request,
                          int reqlen, void *reply, int *replen, int msg_type,
                          int priority, int timeout);
int rpc_request_prep(buffer_t *buf, const msgid_t *msgid, const void *request,
                     int reqlen, const buffer_t *data, int prog, int merge, int priority);

#if 0
#define RET_AGAIN(__ret__, __err_req__)                                 \
        if (__ret__ == ENONET || __ret__ == ETIMEDOUT || __ret__ == EAGAIN) { \
                __ret__ = EAGAIN;                                       \
                goto __err_req__;                                       \
        } else

#endif

int rpc_accept(int *cli_sd, int srv_sd, int tuning, int nonblock);
extern int rpc_peek_sd_sync(int sd, char *buf, uint32_t buflen, int timeout);
extern int rpc_discard_sd_sync(int sd, uint32_t len, int timeout);

#endif

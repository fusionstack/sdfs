#ifndef __CORERPC_H__
#define __CORERPC_H__
#include "core.h"

/**
 * @file CORERPC
 *
 * 上层采用nid作为节点标识，
 * 发送端：(hash, nid)通过cornet_mapping机制转化为sockid。
 * 接收端
 *
 * core hash用于定位core，集群中所有节点上具有相同hash值的core构成corenet。
 * 这种结构关系在整个运行期间要保持稳定。
 *
 * 每个core有自己的corenet： sd -> corenet_node_t
 *
 * 网络层分为三层：
 * - rpc
 * - net
 * - sock
 *
 * 有三类RPC：
 * - minirpc
 * - rpc
 * - corerpc (用于core之间的通信）
 *
 * @note 网络层经过了协程改造
 * @note 两个实体之间采用单连接
 * @note 节点上的core数量是否需要一样？
 */

typedef struct {
        int running;
        sockid_t sockid;
        nid_t nid;
} corerpc_ctx_t;

enum corerp_opcode {
        CORERPC_WRITE = 0,
        CORERPC_READ,
};

// rpc table
void corerpc_register(int type, net_request_handler handler, void *context);

int corerpc_postwait(const char *name, const nid_t *nid, const void *request,
                     int reqlen, const buffer_t *wbuf, buffer_t *rbuf, int msg_type, int msg_size, int timeout);

// sockid-based
int corerpc_send_and_wait(const char *name, const sockid_t *sockid, const nid_t *nid, const void *request,
                          int reqlen, const buffer_t *wbuf, buffer_t *rbuf, int msg_type, int msg_size, int timeout);

void corerpc_reply(const sockid_t *sockid, const msgid_t *msgid, const void *_buf, int len);
void corerpc_reply1(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf);
void corerpc_reply_error(const sockid_t *sockid, const msgid_t *msgid, int _error);

int corerpc_recv(void *ctx, void *buf, int *count);

void corerpc_scan();

// callback
void corerpc_close(void *ctx);
void corerpc_reset(const sockid_t *sockid);


//rpc table
int corerpc_init(const char *name, core_t *core);
#if ENABLE_RDMA
void corerpc_rdma_reset(const sockid_t *sockid);
int corerpc_rdma_recv_msg(void *_ctx, void *iov, int *_count);
int corerpc_rdma_recv_data(void *_ctx, void *_data_buf, void *_msg_buf);

#endif

#endif

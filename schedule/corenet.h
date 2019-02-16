#ifndef __CORENET_H__
#define __CORENET_H__

#include <sys/epoll.h>
#include <semaphore.h>
#include <linux/aio_abi.h> 
#include <pthread.h>

#include "net_proto.h"
#include "../sock/ynet_sock.h"
#include "cache.h"
#include "core.h"
#include "plock.h"
#include "ylock.h"
#include "ynet_net.h"

#define ENABLE_RDMA 0
#define ENABLE_TCP_THREAD 0

#if ENABLE_RDMA

typedef struct {
            int node_loc;
            int ref;
            int is_closing;
            int is_connected;
            core_t *core;
            struct rdma_cm_id *cm_id;
            struct ibv_mr *mr;
            struct ibv_mr *iov_mr;
            struct ibv_qp *qp;
            struct ibv_pd *pd;
            int qp_ref;
            void *private_mem;
            void *iov_addr;
} rdma_conn_t;

typedef struct {
            uint64_t mode:4;
            uint64_t err:2;
            uint64_t n:58;
            struct ibv_send_wr sr;
            struct ibv_recv_wr rr;
            uint32_t index;
            struct ibv_sge sge;
            buffer_t msg_buf;
            buffer_t data_buf;
            rdma_conn_t    *rdma_handler;
} hyw_iovec_t;

#define CORENET_RDMA_ON_ACTIVE_WAIT FALSE

enum corenet_rdma_op_code {
        RDMA_RECV_MSG = 0,
        RDMA_SEND_MSG, 
        RDMA_WRITE,
        RDMA_READ,
        RDMA_OP_END,
};

#endif

#if ENABLE_TCP_THREAD
#define CORE_IOV_MAX ((uint64_t)1024 * 10)
#else
#define CORE_IOV_MAX (1024 * 1)
#endif
#define DEFAULT_MH_NUM 1024
#define MAX_REQ_NUM ((DEFAULT_MH_NUM) / 2)
#define EXTRA_SIZE (4)

typedef struct {
        struct list_head hook;
        int ev;
        sockid_t sockid;
        sy_spinlock_t lock;
        void *ctx;

        core_exec exec;
        func_t reset;
        func_t recv;
        func_t check;

        buffer_t send_buf;
        buffer_t recv_buf;
        buffer_t queue_buf;
        struct list_head send_list;
        int ref;
        int closed;

#if ENABLE_TCP_THREAD
        plock_t rwlock;
#endif
        
        char name[MAX_NAME_LEN / 2];
} corenet_tcp_node_t;

typedef struct {
        struct list_head hook;
        int ev;
        sockid_t sockid;
        sy_spinlock_t lock;
        void *ctx;

        core_exec exec;
        core_exec1 exec1;
        func_t reset;
        func_t recv;
        func_t check;

        buffer_t send_buf;
        buffer_t recv_buf;
        buffer_t queue_buf;
#if ENABLE_RDMA
        rdma_conn_t handler;
        void *head_sr;
#endif
        int total_seg_count;
        struct list_head send_list;
        int ref;
        int closed;
#if 0
        char name[MAX_NAME_LEN / 2]; /*!!!!can't be larger more*/
#endif
} corenet_rdma_node_t;


typedef struct {
        int epoll_fd;
	int size;
        sy_spinlock_t lock;
        time_t last_check;
        struct list_head check_list;
        struct list_head add_list;
        struct list_head forward_list;
        uint32_t figerprint;
} corenet_t;

typedef struct {
        corenet_t corenet;
        corenet_rdma_node_t array[0];
} corenet_rdma_t;

typedef struct {
        corenet_t corenet;
#if !ENABLE_TCP_THREAD
        struct iovec iov[CORE_IOV_MAX]; //iov for send/recv
#endif
        corenet_tcp_node_t array[0];
} corenet_tcp_t;


int corenet_tcp_init(int max, corenet_tcp_t **corenet);
void corenet_tcp_destroy();

int corenet_tcp_add(corenet_tcp_t *corenet, const sockid_t *sockid, void *ctx,
                    core_exec exec, func_t reset, func_t check, func_t recv, const char *name);
void corenet_tcp_close(const sockid_t *sockid);

void corenet_tcp_check();
int corenet_tcp_connected(const sockid_t *sockid);

int corenet_tcp_poll(void *ctx, int tmo);
int corenet_tcp_send(void *ctx, const sockid_t *sockid, buffer_t *buf, int flag);
void corenet_tcp_commit(void *ctx);

#if ENABLE_RDMA
// below is RDMA transfer

int corenet_rdma_init(int max, corenet_rdma_t **corenet, void *private_mem);
int corenet_rdma_dev_create(rdma_info_t * res);

int rdma_alloc_pd(rdma_info_t * res);
int rdma_create_cq(rdma_info_t *res, int ib_port);

//struct ibv_mr *rdma_get_mr();
void *rdma_get_mr_addr();
void *rdma_register_mgr(void* pd, void* buf, size_t size);

int corenet_rdma_add(core_t *core, const nid_t *nid, sockid_t *sockid, void *ctx,
                     core_exec exec, core_exec1 exec1, func_t reset, func_t check, func_t recv, rdma_conn_t **_handler);
void corenet_rdma_close(rdma_conn_t *rdma_handler);

void corenet_rdma_check();

int corenet_rdma_connected(const sockid_t *sockid);
void corenet_rdma_established(struct rdma_cm_event *ev, void *core);
void corenet_rdma_disconnected(struct rdma_cm_event *ev, void *core);

int corenet_rdma_send(const sockid_t *sockid, buffer_t *buf, int flag);
void corenet_rdma_commit();
int corenet_rdma_poll(core_t *core);

int corenet_rdma_post_recv(void *ptr);

void corenet_rdma_put(rdma_conn_t *rdma_handler);
void corenet_rdma_get(rdma_conn_t *rdma_handler, int n);

int corenet_rdma_evt_channel_init();
int corenet_rdma_listen_by_channel(int cpu_idx);

int corenet_rdma_connect_by_channel(const nid_t *nid, uint32_t addr, const char *port, core_t *core,
                                    sockid_t *sockid);

// int corenet_rdma_on_passive_event(int cpu_idx);

void corenet_rdma_timewait_exit(struct rdma_cm_event *ev, void *core);
void corenet_rdma_connect_request(struct rdma_cm_event *ev, void *core);
#endif

#endif

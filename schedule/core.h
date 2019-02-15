#ifndef __CORE_H__
#define __CORE_H__

#include <sys/epoll.h>
#include <semaphore.h>
#include <linux/aio_abi.h>         /* Defines needed types */
#include <pthread.h>

#if ENABLE_RDMA
#include <rdma/rdma_cma.h>
#endif

#include "net_proto.h"
#include "../sock/ynet_sock.h"
#include "cache.h"
#include "ylock.h"
#include "schedule.h"
#include "variable.h"
#include "cpuset.h"

typedef int (*core_exec)(void *ctx, void *buf, int *count);
typedef int (*core_exec1)(void *ctx, void *data_buf, void *msg_buf);
typedef int (*core_reconnect)(int *fd, void *ctx);
typedef int (*core_func)();
typedef void (*core_exit)();

#define CORE_FILE_MAX 64

#if ENABLE_RDMA
typedef struct {
        struct list_head list;

        struct ibv_cq *cq;
        struct ibv_pd *pd;
        struct ibv_mr *mr;

        struct ibv_context *ibv_verbs;
        struct ibv_device_attr device_attr;
        // int ref;
} rdma_info_t;

typedef struct __sub_poller {
        struct list_head list_entry;
        char name[64];
        void (*poll)();
        void *user_data;
}sub_poller_t;
#endif

typedef struct __core {
        int interrupt_eventfd;   // === schedule->eventfd, 通知机制
        //int aio_eventfd;      // === __aio__->aio_eventfd, aio事件完成通知

        int idx;
        int hash;
        int flag;
        coreinfo_t *main_core;
        int aio_core;

        void *tcp_net;
        void *maping;
        void *rpc_table;

        char name[MAX_NAME_LEN];

        schedule_t *schedule;

#if ENABLE_RDMA
        void *rdma_net;
        struct list_head rdma_dev_list;

        rdma_info_t *active_dev;
        struct rdma_event_channel *iser_evt_channel;
#ifdef NVMF
        struct lich_nvmf_subsystem  *subsystem;
#endif
#endif

        void *tls[VARIABLE_MAX];

        //void *iser_dev;
        struct list_head check_list;
        time_t last_check;
        sy_spinlock_t keepalive_lock; // for keepalive
        time_t keepalive;

        sem_t sem;
        int   counter;
        struct list_head poller_list;
} core_t;

#define CORE_FLAG_ACTIVE  0x0001
#define CORE_FLAG_PASSIVE 0x0002
#define CORE_FLAG_AIO     0x0004
#define CORE_FLAG_REDIS   0x0008
#define CORE_FLAG_PRIVATE 0x0010

int core_create(core_t **_core, int hash, int flag);
int core_init(int polling_core, int polling_timeout, int flag);

#if 0
int core_spdk_init(int flag);
#endif
int core_init_register(func_t init, void *_ctx, const char *name);
void core_check_register(core_t *core, const char *name, void *opaque, func1_t func);

int core_hash(const fileid_t *fileid);
int core_attach(int hash, const sockid_t *sockid, const char *name, void *ctx,
                core_exec func, func_t reset, func_t check);
core_t *core_get(int hash);
core_t *core_self();

void core_worker_exit(core_t *core);
int core_request_async(int hash, int priority, const char *name, func_t exec, void *arg);
int core_request(int hash, int priority, const char *name, func_va_t exec, ...);
int core_request_new(core_t *core, int priority, const char *name, func_va_t exec, ...);
void core_check_dereg(const char *name, void *opaque);
void core_register_tls(int type, void *ptr);

void core_iterator(func1_t func, const void *opaque);

void core_latency_update(uint64_t used);
int core_dump_memory(uint64_t *memory);

int core_poller_register(core_t *core, const char *name, void (*poll)(void *,void*), void *user_data);
int core_poller_unregister(core_t *core, void (*poll)());

#if ENABLE_CORE_PIPELINE
int core_pipeline_send(const sockid_t *sockid, buffer_t *buf, int flag);
#endif

#define CORE_ANALYSIS_BEGIN(mark)               \
        struct timeval t1##mark, t2##mark;      \
        int used##mark;                         \
                                                \
        _gettimeofday(&t1##mark, NULL);         \


#define CORE_ANALYSIS_UPDATE(mark, __usec, __str)                       \
        _gettimeofday(&t2##mark, NULL);                                 \
        used##mark = _time_used(&t1##mark, &t2##mark);                  \
        core_latency_update(used##mark);                                \
        if (used##mark > (__usec)) {                                    \
                if (used##mark > 1000 * 1000 * gloconf.rpc_timeout) {   \
                        DWARN_PERF("analysis used %f s %s, timeout\n", (double)(used##mark) / 1000 / 1000, (__str) ? (__str) : ""); \
                } else {                                                \
                        DINFO_PERF("analysis used %f s %s\n", (double)(used##mark) / 1000 / 1000, (__str) ? (__str) : ""); \
                }                                                       \
        }                                                               \

uint64_t core_latency_get();

#endif

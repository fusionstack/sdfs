#ifndef __VM_H__
#define __VM_H__

#include <sys/epoll.h>
#include <semaphore.h>
#include <linux/aio_abi.h> 
#include <pthread.h>

#include "net_proto.h"
#include "schedule.h"
#include "../sock/ynet_sock.h"
#include "cache.h"
#include "ylock.h"

typedef int (*vm_reconnect)(int *fd, void *ctx);
typedef int (*vm_exec)(int *count);
typedef int (*vm_func)();
typedef void (*vm_exit)();

#define VM_FILE_MAX 64
#define VM_IOV_MAX (1024)

typedef struct __vm {
        int sd; //io socket
        int interrupt_eventfd;
        int idx;
        int stop;
        int exiting;
        char name[MAX_NAME_LEN];

        sem_t sem;
        
        schedule_t *schedule;

        buffer_t send_buf;
        buffer_t recv_buf;
        struct iovec iov[VM_IOV_MAX]; //iov for send/recv

        /*callback  and ctx */
        vm_exec exec; 
        vm_exit exit;
        vm_func init;
        vm_func check;
        vm_reconnect reconnect;
        void *ctx;

        /*forward*/
        struct list_head forward_list;

        /*aio cb*/
        //aio_context_t  ioctx;
        int iocb_count;
        struct iocb *iocb[TASK_MAX];
} vm_t;

typedef struct {
        char *name;
        int sd;
        vm_exec exec;
        vm_exit exit;
        vm_func check;
        vm_func init;
        vm_reconnect reconnect;
        void *ctx;
} vm_op_t;

vm_t *vm_self();
int vm_create(const vm_op_t *vm_op, vm_t **_vm);
void vm_stop(vm_t *vm);
void vm_send(buffer_t *buf, int flag);
int vm_forward(const sockid_t *sockid, buffer_t *buf, int flag);

int vm_request(vm_t *vm, void (*exec)(void *buf), void *buf, const char *name);


#endif


#ifndef __SOCK_BUFFER_H__
#define __SOCK_BUFFER_H__

#include <pthread.h>
#include <stdint.h>
#include <sys/uio.h>

#include "sdfs_list.h"
#include "job.h"
#include "sdfs_buffer.h"
#include "../../ynet/include/net_proto.h"

#define SOCK_IOV_MAX (1024)

struct sockstate
{
	unsigned long long int bytes_send;
	unsigned long long int bytes_recv;
};

typedef struct{
        int fd; /*fd ref shm_t*/
        void *addr; /*begin of buffer*/
        uint32_t recv_off;
        uint32_t split_off;
} rbuf_seg_t;

typedef struct{
        sy_spinlock_t lock;
        struct iovec iov[SOCK_IOV_MAX]; //iov for send
        buffer_t buf;
} sock_rbuffer_t;

typedef struct{
        sy_spinlock_t lock;
        buffer_t buf;
        struct iovec iov[SOCK_IOV_MAX]; //iov for send
        int closed;
} sock_wbuffer_t;

#define BUFFER_KEEP 0x00000001

//extern void get_sockstate(struct sockstate *);
extern int sock_rbuffer_create(sock_rbuffer_t *buf);
extern void sock_rbuffer_free(sock_rbuffer_t *buf);
int sock_rbuffer_recv(sock_rbuffer_t *sbuf, int sd);
//extern int sock_rbuffer_recv(sock_rbuffer_t *buf, int sd, int *retry);
extern int sock_rbuffer_destroy(sock_rbuffer_t *buf);
extern int sock_wbuffer_create(sock_wbuffer_t *buf);
extern int sock_wbuffer_send(sock_wbuffer_t *buf, int);
extern int sock_wbuffer_queue(int sd, sock_wbuffer_t *wbuf,
                                     const buffer_t *buf, int flag);
int sock_wbuffer_destroy(sock_wbuffer_t *buf);
int sock_wbuffer_isempty(sock_wbuffer_t *sbuf);

extern int g_var;

#endif

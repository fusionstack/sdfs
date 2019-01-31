#ifndef __SDEVENT_H__
#define __SDEVENT_H__

//#include "config.h"

/*
sdevent is a special fast array. it is lock free.
only for save value which have a key of fd or other small unequal int.
be careful to use it
*/
#include <sys/epoll.h>
#include <semaphore.h>
#include <pthread.h>

#include "net_proto.h"
#include "../sock/ynet_sock.h"
#include "ylock.h"

typedef struct epoll_event event_t;
typedef int (*event_handler_t)(event_t *);
typedef int (*event_handler_func)(event_t *);


int sdevent_init(int max);
void sdevent_destroy(void);

int sdevent_open(net_handle_t *nh, const net_proto_t *proto);
void sdevent_close_force(const net_handle_t *nh);

int sdevent_accept(int sd, net_handle_t *nh,
                    const net_proto_t *proto, int nonblock);
int sdevent_connect(const ynet_sock_info_t *info,
                    net_handle_t *nh, net_proto_t *proto, int nonblock, int timeout);

int sdevent_add(const net_handle_t *socknh, const nid_t *nid, int event,
                void *ctx, func_t reset);
int sdevent_queue(const net_handle_t *nh, const buffer_t *buf, int flag);

int sdevent_session(const sockid_t *id,  func1_t func, void *arg);
int sdevent_check(const sockid_t *id);

int sdevent_recv(int fd);
void sdevent_exit(int fd);

int sdevent_heartbeat_get(const sockid_t *id, uint64_t *send, uint64_t *reply);
int sdevent_heartbeat_set(const sockid_t *id, const uint64_t *send, const uint64_t *reply);

#endif

#ifndef __SOCK_UDP_H__
#define __SOCK_UDP_H__

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include "ynet_sock.h"

int udp_sock_listen(int *srv_sd, const char *host, const char *service, int nonblock);
int udp_sock_broadcast(int sd, uint32_t net, uint32_t mask, uint32_t port, const void *buf, int buflen);
int udp_sock_connect(int *srv_sd, const char *host, const char *service, int nonblock);


#endif

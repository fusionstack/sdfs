#ifndef __SOCK_TCP_H__
#define __SOCK_TCP_H__

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include "ynet_sock.h"

extern int tcp_sock_hostbind(int *srv_sd, const char *host,
                             const char *service, int nonblock);
extern int tcp_sock_hostlisten(int *srv_sd, const char *host,
                               const char *service, int qlen, int nonblock, int tuning);
/* for ipv6 */
extern int tcp_sock_hostlisten6(int *srv_sd, const char *host,
                               const char *service, int qlen, int nonblock, int tuning);
extern int tcp_sock_addrlisten(int *srv_sd, uint32_t addr, uint32_t port,
                               int qlen, int nonblock);
extern int tcp_sock_portlisten(int *srv_sd, uint32_t addr, uint32_t *port,
                               int qlen, int nonblock);
extern int tcp_sock_accept(net_handle_t *, int srv_sd, int tuning, int nonblock);
extern int tcp_sock_getinfo(uint32_t *info_count, ynet_sock_info_t *info,
                            uint32_t info_count_max, uint32_t port);
extern int tcp_sock_hostconnect(net_handle_t *nh, const char *host,
                                const char *service, int nonblock, int timeout, int tuning);
extern int tcp_sock_connect(net_handle_t *nh, struct sockaddr_in *, int nonblock,
                            int timeout, int tuning);
extern int tcp_sock_close(int sd);
int tcp_sock_accept_sd(int *_sd, int srv_sd, int tuning, int nonblock);
int tcp_sock_getdevice(char *name, const char *addr);
int tcp_sock_getaddr(uint32_t *info_count, ynet_sock_info_t *info,
                     uint32_t info_count_max, uint32_t port);
int tcp_sock_tuning(int sd, int tuning, int nonblock);

#endif /* __SOCK_TCP_H__ */

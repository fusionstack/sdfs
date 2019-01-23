#ifndef __YNET_SOCK_H__
#define __YNET_SOCK_H__

#include <stdint.h>
#include <poll.h>

#include "sdfs_list.h"
#include "sock_buffer.h"
#include "sdfs_conf.h"
#include "net_proto.h"
#include "ytime.h"

#pragma pack(8)
typedef struct {
        uint32_t addr;
        uint32_t port;
} ynet_sock_info_t;
#pragma pack()

typedef struct {
        net_handle_t nh;
        nid_t *nid;
        nid_t __nid__;
        net_proto_t proto;
        uint32_t align;

        uint64_t send_total;
        uint64_t recv_total;

        double limit_rate;
        ytime_t start;
        int delay;
        int sendclose;/*close after sent*/
        int used;

        sock_wbuffer_t wbuf;
        sock_rbuffer_t rbuf;
} ynet_sock_conn_t;

/* sock_passive.c */
extern int sock_hostbind(int *srv_sd, const char *host, const char *service, int nonblock);
extern int sock_hostlisten(int *srv_sd, const char *host, const char *service,
                           int qlen, int nonblock);
extern int sock_addrlisten(int *srv_sd, uint32_t addr, uint32_t port, int qlen, int nonblock);
extern int sock_portlisten(int *srv_sd, uint32_t addr, uint32_t *port,
                           int qlen, int nonblock);
extern int sock_accept(net_handle_t *, int srv_sd, int tuning, int nonblock);
extern int sock_getinfo(uint32_t *info_count, ynet_sock_info_t *,
                        uint32_t info_count_max, uint32_t port);
extern int sock_setblock(int sd);
extern int sock_setnonblock(int sd);

/* sock_xnect.c */
extern int sock_hostconnect(int *sd, const char *host, const char *service,
                            int nonblock, int timeout);
extern int sock_init(ynet_sock_conn_t *sock, ynet_sock_info_t *info);
extern int sock_info2sock(net_handle_t *nh, const ynet_sock_info_t *, int nonblock, int timeout);
extern int sock_close(ynet_sock_conn_t *);
extern int sock_sdclose(int sd);

/* sock_xmit.c */
/*
 * Return 1 if socket is good for read/write, 0 otherwise.
 */
extern int sock_sd_isgood(int sd);
/*
 * Do block-IO's, where # of bytes to be sent is the length of the buffer.
 * @retval Returns # of bytes sent or -errno if there was an error.
 */
extern int sock_send_sync(ynet_sock_conn_t *, const char *buf, uint32_t buflen);
extern int sock_send_sd_sync(int sd, const char *buf, uint32_t buflen);
extern int sock_send_sd_async(int sd, const char *buf, uint32_t buflen);
extern int sock_poll_sd(int sd, uint64_t timeout, short event);
/*
 * for recv/peek, specify a timeout within which data should be received.
 * @retval Returns # of bytes recv'ed or -errno if there was an error.
 */

extern int sock_peek_sd_sync(int sd, char *buf, uint32_t buflen, int timeout);
#endif

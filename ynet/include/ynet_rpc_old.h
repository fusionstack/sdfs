#ifndef __YNET_RPC_OLD_H__
#define __YNET_RPC_OLD_H__

#include <stdint.h>

#include "ynet_net.h"
#include "net_table.h"
#include "job.h"
#include "sdfs_conf.h"

#if 0
typedef struct {
        uint32_t live;
        ynet_net_nid_t nid;
        char clustername[MAXSIZE];
        char nodename[MAXSIZE];
} masterget_req_t;

typedef struct {
        ynet_net_nid_t nid;
        uint32_t infolen;
        char     info[0];
} masterget_rep_t;

typedef struct {
        ynet_net_nid_t nid;
} rpc_getinfo_t;

/* rpc_lib.c */
extern int rpc_init(net_proto_t *op, const char *name, int seq, 
                    const char *path);
extern int rpc_destroy(void);

/* rpc_passive.c */
int rpc_passive(int hash);
extern int rpc_hostlisten(int *sd, const char *host, const char *service,
                          int qlen, int nonblock);
extern int rpc_portlisten(int *sd, uint32_t addr, uint32_t *port, int qlen,
                          int nonblock);
int rpc_start();

void rpc_reply_prep_core(const msgid_t *rpcid, buffer_t *buf, buffer_t *data);
void rpc_reply1_core(const sockid_t *sockid, const msgid_t *rpcid, buffer_t *_buf);
void rpc_reply_core(const sockid_t *sockid, const msgid_t *rpcid, const void *_buf, int len);
void rpc_reply_error_prep_core(const msgid_t *rpcid, buffer_t *buf, int _error);
void rpc_reply_error_core(const sockid_t *sockid, const msgid_t *rpcid, int _error);

/* rpc_xnect.c */
extern int rpc_getinfo(char *infobuf, uint32_t *infobuflen);
extern int rpc_host2nid(net_handle_t *, const char *host,
                        const char *service, int timeout,
                        connecter_t after_connect, int persistent);
extern int rpc_info2nid(net_handle_t *, const ynet_net_info_t *);
extern int rpc_info2nid_multi(net_handle_t **, ynet_net_info_t *,
                              uint32_t *infonum);
extern int network_connect1(const ynet_net_nid_t *nid);
int rpc_request_getinfo(const ynet_net_nid_t *tracker, const ynet_net_nid_t *nid,
                        ynet_net_info_t *info);
int rpc_request_ping(const nid_t *peer, int timeout);


/* rpc_xmit.c */
extern int rpc_send_sd_sync(int sd, const char *buf, uint32_t buflen);

#endif

extern int rpc_peek_sd_sync(int sd, char *buf, uint32_t buflen, int timeout);
extern int rpc_discard_sd_sync(int sd, uint32_t len, int timeout);
extern int rpc_accept(int *cli_sd, int srv_sd, int tuning, int nonblock);

//just for compatible, will be removed
typedef struct {
        uint32_t live;
        ynet_net_nid_t nid;
        char clustername[MAXSIZE];
        char nodename[MAXSIZE];
} masterget_req_t;

typedef struct {
        ynet_net_nid_t nid;
        uint32_t infolen;
        char     info[0];
} masterget_rep_t;

/* rpc_request.c */
int rpc1_request_prep(job_t *, void **req, uint32_t len, int prog);
int rpc1_request_queue_wait(job_t *, const net_handle_t *, mbuffer_op_t, nio_type_t);
int rpc1_request_queue_wait1(job_t *job, const net_handle_t *nid, int tmo, buffer_t *buf);
void rpc1_request_append(job_t *job, buffer_t *buf);
int rpc1_request_queue(job_t *, const net_handle_t *, mbuffer_op_t, nio_type_t type,
                      buffer_t *buf);
int rpc1_request_queue1(job_t *job, const net_handle_t *nid, mbuffer_op_t op,
                       nio_type_t type, buffer_t *buf, nio_priority_t priority, uint64_t hash);
void rpc1_request_finished(job_t *);


/* rpc_reply.c */
extern int rpc1_reply_prep(job_t *job, void **req, uint32_t len);
extern int rpc1_reply_appendmem(job_t *job, void *ptr, uint32_t size);
extern void rpc1_reply_append(job_t *job, const char *, uint32_t len);
int rpc1_reply_send(job_t *job, buffer_t *buf, mbuffer_op_t op);
int rpc1_reply_send1(job_t *job, buffer_t *buf, mbuffer_op_t op,
                    nio_priority_t priority, uint64_t hash);
extern void rpc1_reply_finished(job_t *job);
extern void rpc1_reply_init(job_t *job);
extern int rpc1_reply_error(job_t *job, int _error);

int  rpc_master_listen(int *_sd, const char *addr, const char *port);
int  rpc_master_poll(int sd, ynet_net_info_t *info, int *running);
int rpc_master_get(ynet_net_info_t *info, uint32_t buflen, const char *addr,
                   const char *port, int live, int timeout);


void rpc1_request_register(int type, net1_request_handler handler, void *context);
#if 0
//void rpc1_reset_register(net_reset_handler handler);
#endif

#endif

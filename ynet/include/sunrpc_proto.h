#ifndef __SUNRPC_PROTO_H__
#define __SUNRPC_PROTO_H__

#include "job.h"
#include "sdevent.h"

#define MOUNTPROG 100005
#define NFS3_PROGRAM 100003
#define ACL_PROGRAM 100227
#define NLM_PROGRAM 100021
#define NLM_VERSION 4


#define MOUNTVERS1 1
#define MOUNTVERS3 3

#define NFS_V3 3

enum {
        SUNRPC_REQ_MSG       = 0x00,
        SUNRPC_REP_MSG       = 0x01,
};

#pragma pack(1)

typedef struct {
        uint32_t length;
        uint32_t xid;
        uint32_t msgtype;
        uint32_t rpcversion;
        uint32_t program;
        uint32_t progversion;
        uint32_t procedure;
} sunrpc_request_t;

typedef struct {
        uint32_t flavor;
        uint32_t length;
} auth_head_t;


//https://www.ietf.org/rfc/rfc1057.txt
//9.2 UNIX Authentication
typedef struct {
    unsigned int stamp;
    char *machinename; //string<255>
    unsigned int uid;
    unsigned int gid;
    //unsigned int gids; //unsigned int gids<16>;
} auth_unix_t;

typedef struct {
        uint32_t length;
        uint32_t xid;
        uint32_t msgtype;
} sunrpc_proto_t;

#pragma pack()

int sunrpc_accept(int srv_sd);
int sunrpc_pack_len(void *buf, uint32_t len, int *msg_len, int *io_len);

#endif

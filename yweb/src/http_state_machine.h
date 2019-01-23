#ifndef __HTTP_STATE_MACHINE__
#define __HTTP_STATE_MACHINE__

#include "request.h"
#include "ynet_rpc.h"


typedef struct {
        fileid_t fileid;
        struct aiocb iocb;
        buffer_t buf;
        //char buf[MAX_BUF_LEN];/*request buf*/
        struct http_request http_req;
        off_t offset;
        off_t size;
} http_job_context_t;


enum {
        HTTP_GET_BEGIN,
        HTTP_GET_READ,
        HTTP_GET_WAIT_READ,
        HTTP_GET_WAIT_SEND,
        HTTP_GET_DONE,
        HTTP_GET_ERROR,
};

extern int http_state_machine_get(job_t *job, char *);
extern int http_state_machine_error(job_t *, char *);
#endif

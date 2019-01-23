#ifndef __NETBIOS_REPLY_H__
#define __NETBIOS_REPLY_H__

#include "job.h"
#include "netbios_proto.h"
#include "xdr.h"

int netbios_reply_prep1(job_t *job, void **buf, int wc, int bc, int status);
int netbios_reply_send(job_t *job, buffer_t *buf,  mbuffer_op_t op);
int netbios_reply_error(job_t *job, int retval);
int netbios_andx_reply_prep(job_t *job);
int netbios_andx_reply_append_word(job_t *job, void *buf, int add);
int netbios_andx_reply_append_byte(job_t *job, int count, ...);
void netbios_andx_reply_setree(job_t *job, int treeid);
int netbios_reply_prep(job_t *job, smb_head_t **_head);
int netbios_reply_append(job_t *job, void **ptr, int add);
int netbios_reply_append_word(job_t *job, void **ptr, int add);
int netbios_reply_append_byte(job_t *job, void **ptr, int add);
int netbios_reply_append1(job_t *job, void **word, char wc,
                          void **byte, uint16_t bc);
#endif



#include <stdarg.h>
#include <arpa/inet.h>
#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_YRPC

#include "netbios_proto.h"
#include "net_global.h"
#include "job_dock.h"
#include "netbios_reply.h"
#include "job_tracker.h"
#include "ylib.h"
#include "ynet_net.h"
#include "dbg.h"

extern jobtracker_t *jobtracker;

#define SMB_FLAG 0x80
//#define SMB_FLAG2 0xc003
#define SMB_FLAG2 0x4003
//#define SMB_FLAG2 0x0


static void __netbios_reply_finish_handler(void *job)
{
        int ret;

        ret = jobtracker_insert((job_t *)job);
        if (ret) {
                DERROR("insert errot\n");
        }
}

int netbios_reply_prep1(job_t *job, void **buf, int wc, int bc, int status)
{
        int ret;
        smb_head_t *req, *rep;
        char *worldcount;
        uint16_t *bytecount;
        uint32_t *length, len;

        req = (void *)job->buf;

        len = sizeof(smb_head_t) + sizeof(char)
                + wc + sizeof(uint16_t);

        ret = mbuffer_init(&job->reply, len + sizeof(uint32_t));
        if (ret)
                GOTO(err_ret, ret);

        length = mbuffer_head(&job->reply);

        *length = htonl(len);

        rep = (void *)(length + 1);

        memcpy(rep, req, sizeof(smb_head_t));

        rep->flags = SMB_FLAG;
        rep->flags2 = SMB_FLAG2;
        rep->status.status = status;

        worldcount = (void *)(rep + 1);

        *worldcount = wc / 2;

        YASSERT(*worldcount * 2 == wc);

        *buf = (void *)(worldcount + 1);

        bytecount = (void *)(*buf) + wc;

        *bytecount = bc;

        return 0;
err_ret:
        return ret;
}

int netbios_reply_send(job_t *job, buffer_t *buf, mbuffer_op_t op)
{
        int ret;
        niocb_t *iocb;

        iocb = &job->iocb;
        iocb->buf = buf;
        iocb->op = op;
        iocb->reply = NULL;

        if (op == KEEP_JOB) {
                iocb->sent = __netbios_reply_finish_handler;
                iocb->error = __netbios_reply_finish_handler;
        } else {
                iocb->sent = NULL;
                iocb->error = NULL;
        }

        ret = sdevents_queue((void *)job->net, job, NULL);
        if (ret) {
                if (op == FREE_JOB)
                        job_destroy(job);
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int netbios_reply_error(job_t *job, int retval)
{
        int ret;
        smb_head_t *rep;
        uint32_t *length;

        (void) retval;

        length = mbuffer_head(&job->reply);
        rep = (void *)(length + 1);
        rep->status.status = (uint32_t) (retval);

        UNIMPLEMENTED(__WARN__);

        ret = netbios_reply_send(job, &job->reply, FREE_JOB);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int netbios_andx_reply_prep(job_t *job)
{
        int ret;
        smb_head_t *req, *rep;
        uint32_t *length, len;

        req = (void *)job->buf;

        len = sizeof(smb_head_t);

        ret = mbuffer_init(&job->reply, len + sizeof(uint32_t));
        if (ret)
                GOTO(err_ret, ret);

        length = mbuffer_head(&job->reply);

        *length = htonl(len);

        rep = (void *)(length + 1);

        memcpy(rep, req, sizeof(smb_head_t));

        rep->flags = SMB_FLAG;
        rep->flags2 = SMB_FLAG2;
        rep->status.status = 0;

        return 0;
err_ret:
        return ret;
}

void netbios_andx_reply_setree(job_t *job, int treeid)
{
        smb_head_t *rep;
        uint32_t *length;

        length = mbuffer_head(&job->reply);
        rep = (void *)(length + 1);
        rep->tid = treeid;
}

int netbios_andx_reply_append_word(job_t *job, void *buf, int add)
{
        int ret;
        uint32_t *length, len;
        char worldcount;

        worldcount = add / 2;

        YASSERT(worldcount * 2 == add);

        ret = mbuffer_appendmem(&job->reply, &worldcount, 1);
        if (ret)
                GOTO(err_ret, ret);

        if (worldcount) {
                ret = mbuffer_appendmem(&job->reply, buf, add);
                if (ret)
                        GOTO(err_ret, ret);
        }

        length = mbuffer_head(&job->reply);

        len = ntohl(*length);

        len += (add + 1);

        *length = htonl(len);

        return 0;
err_ret:
        return ret;
}

//int netbios_andx_reply_append_byte(job_t *job, void *buf, int add)
int netbios_andx_reply_append_byte(job_t *job, int count, ...)
{
        int ret, buflen, i;
        uint32_t *length, len;
        uint16_t bytecount;
        va_list ap;
        void *buf;

        length = mbuffer_head(&job->reply);

        len = ntohl(*length);

        bytecount = 0;
        va_start(ap, count);

        for (i = 0; i < count; i++) {
                buf = va_arg(ap, void *);
                buflen = va_arg(ap, int);

                bytecount += buflen;
        }

        va_end(ap);

        ret = mbuffer_appendmem(&job->reply, &bytecount, 2);
        if (ret)
                GOTO(err_ret, ret);

        va_start(ap, count);

        for (i = 0; i < count; i++) {
                buf = va_arg(ap, void *);
                buflen = va_arg(ap, int);

                DINFO("byte %s\n", (char *)buf);

                ret = mbuffer_appendmem(&job->reply, buf, buflen);
                if (ret)
                        GOTO(err_ret, ret);
        }

        va_end(ap);

        len += (bytecount + 2);

        *length = htonl(len);

        return 0;
err_ret:
        return ret;
}

int netbios_reply_prep(job_t *job, smb_head_t **head)
{
        int ret;
        smb_head_t *req, *rep;
        uint32_t *length, len;

        req = (void *)job->buf;

        len = sizeof(smb_head_t);

        ret = mbuffer_init(&job->reply, len + sizeof(uint32_t));
        if (ret)
                GOTO(err_ret, ret);

        length = mbuffer_head(&job->reply);

        *length = htonl(len);

        rep = (void *)(length + 1);

        memcpy(rep, req, sizeof(smb_head_t));

        rep->flags = SMB_FLAG;
        rep->flags2 = SMB_FLAG2;

        if (head)
                *head = rep;

        return 0;
err_ret:
        return ret;
}

int netbios_reply_append(job_t *job, void **ptr, int add)
{
        int ret;
        uint32_t *length, len;

        length = mbuffer_head(&job->reply);

        len = ntohl(*length);

        ret = mbuffer_extern(&job->reply, ptr, add);
        if (ret)
                GOTO(err_ret, ret);

        len += add;

        *length = htonl(len);

        return 0;
err_ret:
        return ret;
}

int netbios_reply_append_word(job_t *job, void **ptr, int add)
{
        int ret;
        char *worldcount;

        ret = netbios_reply_append(job, (void **)&worldcount,
                                   add + sizeof(*worldcount));
        if (ret)
                GOTO(err_ret, ret);

        *worldcount = add / 2;

        YASSERT(*worldcount * 2 == add);

        *ptr = ((void *)worldcount + sizeof(*worldcount));

        return 0;
err_ret:
        return ret;
}

int netbios_reply_append_byte(job_t *job, void **ptr, int add)
{
        int ret;
        uint16_t *bytecount;

        ret = netbios_reply_append(job, (void **)&bytecount,
                                   add + sizeof(*bytecount));
        if (ret)
                GOTO(err_ret, ret);

        *bytecount = add;

        *ptr = ((void *)bytecount + sizeof(*bytecount));

        return 0;
err_ret:
        return ret;
}

int netbios_reply_append1(job_t *job, void **word, char wc,
                              void **byte, uint16_t bc)
{
        int ret;
        char *wordcount;
        uint16_t *bytecount;
        void *begin;

        ret = netbios_reply_append(job, (void **)&begin,
                                   sizeof(wc) + wc + sizeof(bc) + bc);
        if (ret)
                GOTO(err_ret, ret);

        wordcount = begin;

        *wordcount = wc / 2;
        YASSERT(*wordcount * 2 == wc);
        *word = wordcount + 1;

        bytecount = (*word) + wc;
        *bytecount = bc;
        *byte = bytecount + 1;

        return 0;
err_ret:
        return ret;
}


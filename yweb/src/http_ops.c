

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <stdio.h>

#define DBG_SUBSYS S_YWEB

#include "http_ops.h"
#include "http_state_machine.h"
#include "job_tracker.h"
#include "match.h"
#include "net_global.h"
#include "request.h"
#include "response.h"
#include "yweb_conf.h"
#include "../../ynet/sock/ynet_sock.h"
#include "ylib.h"
#include "dbg.h"

extern jobtracker_t *jobtracker;

static void __http_reply_finish_handler(void *job)
{
        int ret;

        ret = jobtracker_insert((job_t *)job);
        if (ret) {
                DERROR("insert errot\n");
        }
}

int add_headers(struct http_request *req, int status, char *title,
                char *extra_header, char *me, char *mt, off_t bytes, 
                time_t mod, buffer_t *rep, int isclose, int usefilename)
{
        time_t now;
        char timebuf[100], buf[10000];
        int buflen, s100;
        const char* rfc1123_fmt = "%a, %d %b %Y %H:%M:%S GMT";

        buflen = snprintf(buf, sizeof(buf), "%s %d %s\015\012", req->protocol,
                          status, title);
        mbuffer_appendmem(rep, buf, buflen);

        buflen = snprintf(buf, sizeof(buf), "Server: %s\015\012",
                          SERVER_SOFTWARE);
        mbuffer_appendmem(rep, buf, buflen);

        buflen = snprintf(buf, sizeof(buf), "Accept-Ranges: bytes\015\012");
        mbuffer_appendmem(rep, buf, buflen);

        now = time(NULL);
        (void) strftime(timebuf, sizeof(timebuf), rfc1123_fmt, gmtime(&now));
        buflen = snprintf(buf, sizeof(buf), "Date: %s\015\012", timebuf);
        mbuffer_appendmem(rep, buf, buflen);

        s100 = status / 100;
        if (s100 != 2 && s100 != 3) {
                buflen = snprintf(buf, sizeof(buf),
                                  "Cache-Control: no-cache,no-store\015\012");
                mbuffer_appendmem(rep, buf, buflen);
        }

        if (extra_header != NULL && extra_header[0] != '\0') {
                buflen = snprintf(buf, sizeof(buf), "%s\015\012", extra_header);
                mbuffer_appendmem(rep, buf, buflen);
        }

        if (me != NULL && me[0] != '\0') {
                buflen = snprintf(buf, sizeof(buf),
                                  "Content-Encoding: %s\015\012", me);
                mbuffer_appendmem(rep, buf, buflen);
        }

        if (mt != NULL && mt[0] != '\0') {
                buflen = snprintf(buf, sizeof(buf), "Content-Type: %s\015\012",
                                  mt);
                mbuffer_appendmem(rep, buf, buflen);
        }

        if (status == 206 && req->offset > 0) {
                buflen = snprintf(buf, sizeof(buf), 
                                  "Content-Range: bytes %llu-%llu/%llu\015\012", 
                                  (long long unsigned)req->offset,
                                  (long long unsigned)req->stat.st_size-1,
                                  (long long unsigned)req->stat.st_size);
                mbuffer_appendmem(rep, buf, buflen);
        }

        if (bytes > 0) {
                buflen = snprintf(buf, sizeof(buf),
                                  "Content-Length: %llu\015\012",
                                  (long long unsigned)bytes);
                mbuffer_appendmem(rep, buf, buflen);
        }

        if (mod != (time_t)-1) {
                (void) strftime(timebuf, sizeof(timebuf), rfc1123_fmt,
                                gmtime(&mod));
                buflen = snprintf(buf, sizeof(buf), "Last-Modified: %s\015\012",
                                  timebuf);
                mbuffer_appendmem(rep, buf, buflen);
        }

        if (usefilename) {
                buflen = snprintf(buf, sizeof(buf),
                                  "Content-Disposition: attachment; filename=\"%s\"\015\012", 
                                  req->file);
                mbuffer_appendmem(rep, buf, buflen);
        }

        if (isclose) {
                buflen = snprintf(buf, sizeof(buf),
                                  "Connection: close\015\012");
                mbuffer_appendmem(rep, buf, buflen);
        }

        mbuffer_appendmem(rep, "\015\012", _strlen("\015\012"));

        return 0;
}

int http_send_response(struct http_request *req, int code, buffer_t *rep)
{
        off_t bytes = 0;

        switch(code) {
        case 100:
                add_headers(req, code, "Continue", "", "", "", 
                            (off_t)0, (time_t)-1, rep, 0, 0);
                break;
        case 200:
                add_headers(req, code, "Ok", "", "", "", 
                            (off_t)0, (time_t)-1, rep, 1, 0);
                break;
        case 201:
                add_headers(req, code, "Created", "", "", "", 
                            (off_t)0, (time_t)-1, rep, 1, 0);
                break;
        case 206:
                bytes = req->stat.st_size - req->offset;
                add_headers(req, code, "Partial Content", "", "", "", 
                           bytes, (time_t)-1, rep, 1, 0);
                break;
        default:
                return 0;
        }

        return 0;
}

int http_send_ok(struct http_request *req, char *me, char *mt, struct stat *st,
                 buffer_t *rep)
{
        add_headers(req, 200, "Ok", "", me, mt, st->st_size, st->st_mtime,
                    rep, 1, 1);

        return 0;
}

int http_send_nomod(struct http_request *req, char *me, char *mt, time_t mtime,
                    buffer_t *rep)
{
        add_headers(req, 304, "Not Modified", "", me, mt, (off_t) -1, mtime,
                    rep, 1, 0);

        return 0;
}


void add_listhead(struct http_request *req, buffer_t *rep)
{
        char buf[1000];
        int buflen;

        buflen = snprintf(buf, sizeof(buf), 
"<html>\n\
<head><title>Index of %s</title>\n\
<meta http-equiv='Content-Type' content='text/html; charset=utf-8'></meta>\n\
</head>\n\
<body bgcolor=\"#cc9999\" LINK=\"#2020ff\" >\n\
<h1>Index of %s</h1><hr><pre>\n", req->path, req->path); 

        mbuffer_appendmem(rep, buf, buflen);
}

#define HTTP_MAX_NAME_LEN 20

void add_listbody(char *filename, char *date, uint64_t size, buffer_t *rep)
{
        char buf[1000], buf1[MAX_BUF_LEN], *file;
        int buflen, namelen;
        
        namelen = strlen(filename);
        file = buf1;
        memcpy(file, filename, namelen + 1);

        if (namelen > HTTP_MAX_NAME_LEN) {
                sprintf(&file[HTTP_MAX_NAME_LEN - 7] , "...&gt;");
                //DINFO("%s\n", file);
        } else {
                //memset(&file[namelen], ' ', HTTP_MAX_NAME_LEN - namelen);
        }

        file[HTTP_MAX_NAME_LEN] = '\0';

        buflen = snprintf(buf, sizeof(buf), "<a href=\"%s\">%s</a>\t\t\t%s\t%llu KB\n",
                          filename, file, date,
                          (unsigned long long)size);

        mbuffer_appendmem(rep, buf, buflen);
}

void add_errbody(int err, char *title, char *text, buffer_t *rep)
{
        char buf[1000];
        int buflen;

        /* try virtual-host custom error page */

        /* try server-wide custom error page */

        /* send built-in error page */
        buflen = snprintf(buf, sizeof(buf), "\
<HTML>\n\
<HEAD><TITLE>%d %s</TITLE></HEAD>\n\
<BODY BGCOLOR=\"#cc9999\" TEXT=\"#000000\" LINK=\"#2020ff\" VLINK=\"#4040cc\">\n\
<H4>%d %s</H4><pre>\n",
                          err, title, err, title);

        mbuffer_appendmem(rep, buf, buflen);

        buflen = snprintf(buf, sizeof(buf), "%s\n", text);

        mbuffer_appendmem(rep, buf, buflen);
}

void add_errtail(struct http_request *req, buffer_t *rep)
{
        char buf[500];
        int buflen;

        if (match("**MSIE**", req->useragent)) {
                int n;

                buflen = snprintf(buf, sizeof(buf), "<!--\n");
                mbuffer_appendmem(rep, buf, buflen);

                for (n = 0; n < 6; ++n) {
                        buflen = snprintf(buf, sizeof(buf),
                                          "Padding so that MSIE deigns to show this error instead of its own canned one.\n");
                        mbuffer_appendmem(rep, buf, buflen);
                }

                buflen = snprintf(buf, sizeof(buf), "-->\n");
                mbuffer_appendmem(rep, buf, buflen);
        }

        buflen = snprintf(buf, sizeof(buf), "\
</pre><HR>\n\
<center><ADDRESS><A HREF=\"%s\">%s</A></ADDRESS></center>\n\
</BODY>\n\
</HTML>\n",
                          SERVER_URL, SERVER_SOFTWARE);
        mbuffer_appendmem(rep, buf, buflen);
}

int http_send_error(struct http_request *req, int err, char *title,
                    char *extra_header, char *text, buffer_t *rep)
{

	add_headers(req, err, title, extra_header, "", "text/html; charset=%s",
                    (off_t)-1, (time_t)-1, rep, 1, 0);

        add_errbody(err, title, text, rep);

        add_errtail(req, rep);

        return 0;
}

inline int http_send_badreq(struct http_request *req, buffer_t *rep)
{
        return http_send_error(req, 400, "Bad Request", "",
                               "Can't parse request.", rep);
}

int http_reply_send(job_t *job, buffer_t *buf, mbuffer_op_t op)
{
        int ret;
        niocb_t *iocb;

        iocb = &job->iocb;
        iocb->buf = buf;
        iocb->op = op;
        iocb->reply = NULL;

        if (op == KEEP_JOB) {
                iocb->sent = __http_reply_finish_handler; 
                iocb->error = __http_reply_finish_handler;
        } else {
                iocb->sent = NULL;
                iocb->error = NULL;
        }

        YASSERT(buf->len);

        ret = sdevents_queue(&job->net, job);
        if (ret) {
                if (ret == ECONNRESET)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}


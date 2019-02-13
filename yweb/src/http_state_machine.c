

#include <errno.h>
#include <dirent.h>
#include <sys/types.h>
#include <iconv.h>

#define DBG_SUBSYS S_YWEB


#include "http_state_machine.h"
#include "http_ops.h"
#include "job_tracker.h"
#include "job_dock.h"
#include "mime.h"
#include "net_global.h"
#include "sdfs_lib.h"
#include "ynet_rpc.h"
#include "yweb_conf.h"
#include "dbg.h"

#define RATESTR "rate="

static char* charset = YWEB_CHARSET_DEF;
extern jobtracker_t *jobtracker;

extern int http_readdir(struct http_request *http_req, buffer_t *buf);

void dump_http_request(struct http_request *req)
{
        DINFO("method %d path %s protocol %s pathinfo %s file %s"
              " content type %s context len %d host %s\n", 
              req->method, req->path, req->protocol,
              req->pathinfo, req->file,
              req->content_type, req->content_length,
              req->host);
}

void http_events_interrupt(union sigval foo)
{
        int ret;

        ret = jobtracker_insert((job_t *)foo.sival_ptr);
        if (ret)
                DERROR("insert error\n");
}


int get_pathinfo(struct http_request *req)
{
        int ret;

        req->pathinfo = &req->file[strlen(req->file)];

        while (srv_running) {
                do {
                        --req->pathinfo;

                        if (req->pathinfo <= req->file) {
                                req->pathinfo = NULL;
                                ret = ENOENT;
                                GOTO(err_ret, ret);
                        }
                } while (req->pathinfo[0] != '/');

                req->pathinfo[0] = '\0';

                ret = ly_getattr(req->file, &req->stat);
                if (ret == 0) {
                        req->pathinfo++;
                        break;
                } else
                        req->pathinfo[0] = '/';
        }

        return 0;
err_ret:
        return ret;
}

int http_state_machine_error(job_t *job, char *name)
{
	int ret;
	http_job_context_t *context;
	struct http_request *http_req __attribute__((unused));
        (void) *name;
	
	context = (http_job_context_t *) job->context;

	http_req = &context->http_req;


	ret = http_reply_send (job, &job->reply, FREE_JOB);

	if (ret)
		DERROR("error op!");

	end_request(&context->http_req);

	return 0;
	
}

int _iconv(char *outbuf, char *inbuf, const char *to, const char *from)
{
        int ret;
        iconv_t cd;
        size_t inlen, outlen;

        cd = iconv_open(to, from);
        if ((long long)cd == -1) {
                ret = errno;
                DWARN("from %s to %s\n", from, to);
                GOTO(err_ret, ret);
        }

        inlen = strlen(inbuf);
        outlen = MAX_BUF_LEN;

        ret = iconv(cd, &inbuf, &inlen, &outbuf, &outlen);
        if (ret) {
                ret = errno;
                GOTO(err_fd, ret);
        }

        iconv_close(cd);

        return 0;
err_fd:
        iconv_close(cd);
err_ret:
        return ret;
}

int http_lookup(fileid_t *fileid, const char *_path)
{
        int ret;
        char buf[MAX_PATH_LEN];

        ret = sdfs_lookup_recurive(_path, fileid);
        if (ret) {
                if (ret == ENOENT) {
                        ret = _iconv(buf, (void*)_path, "UTF-8", "GBK");
                        if (ret)
                                GOTO(err_ret, ret);

                        ret = sdfs_lookup_recurive(buf, fileid);
                        if (ret) {
                                if (ret == ENOENT) {
                                        ret = _iconv(buf, (void*)_path, "UTF-8", "GB18030");
                                        if (ret)
                                                GOTO(err_ret, ret);

                                        ret = sdfs_lookup_recurive(buf, fileid);
                                        if (ret) {
                                                GOTO(err_ret, ret);
                                        }
                                } else
                                        GOTO(err_ret, ret);
                        }
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int http_state_machine_get(job_t *job, char *name)
{
        int ret, done = 0;
        int name_len;
        struct http_request *http_req;
        http_job_context_t *context;
        char mime_encodings[500], fixed_mime_type[500], *mime_type;
        uint32_t size;

        (void) *name;

#if 0
        const char* index_names[] = {
                "index.html", "index.htm", "index.xhtml", "index.xht",
                "Default.htm"
        };
#endif

        context = (http_job_context_t *)job->context;
        http_req = &context->http_req;

        while (done == 0) {
                switch (job->status) {
                case HTTP_GET_BEGIN:
                        job->status = HTTP_GET_READ;

                        context->fileid.id = -1;
                        context->fileid.volid = -1;

                        mbuffer_init(&job->reply, 0);

                        //dump_http_request(http_req);

                        if (http_req->query != NULL &&
                            strncmp(http_req->query, RATESTR, _strlen(RATESTR)) == 0) {
                                int rate;
                                char *rate_chr;

                                rate_chr = strstr(http_req->query, "=");
                                if (rate_chr != NULL) {
                                        rate_chr++;
                                        rate = atoi(rate_chr);

                                        if (rate > 0) {
                                                rate = rate < 1024 ? 1024 : rate;
                                                
                                                DBUG("file %s, rate is %s\n", http_req->path, rate_chr);

                                                sdevents_limit_rate(&job->net, rate);
                                        }
                                }
                        }

                        if (http_req->path[0] == '\0' 
                        || (http_req->path[0] == '.' && http_req->path[1] == '/'
                            && http_req->path[2] == '\0')){
                                (void) http_send_error(http_req, 400, "Bad Request", "",
                                                "Illegal filename.", &job->reply);

                                job->status = HTTP_GET_ERROR;
                                break;
                        }

                        ret = http_lookup(&context->fileid, http_req->path);
                        if (ret) {
                                DWARN("get %s ret %s\n",
                                      http_req->path, 
                                      strerror(ret));

                                (void) http_send_error(http_req, 
                                                404, "Not Found", "",
                                                "File not found.", 
                                                &job->reply);

                                job->status = HTTP_GET_ERROR;
                                break;
                        }

                        ret = sdfs_getattr(NULL, &context->fileid, &http_req->stat);
                        if (ret)
                                GOTO(err_ret, ret);

                        DBUG("file length %llu\n", (long long unsigned)http_req->stat.st_size);

                        name_len = _strlen(http_req->path);

                        if (!S_ISDIR(http_req->stat.st_mode)) {
                                while (http_req->path[name_len - 1] == '/') {
                                        http_req->path[name_len - 1] = '\0';
                                        --name_len;
                                }

                                //(void) xmit_file(http_req, clisd);
                        } else {
                                //char idx[10000];

                                /*
                                 * the filename is a directory
                                 * is it missing the trailing slash ?
                                 */
                                if (http_req->path[name_len - 1] != '/'
                                    && http_req->pathinfo == NULL) {
                                        char location[10000];
                                        if (http_req->query[0] != '\0')
                                                snprintf(location, sizeof(location),
                                                         "Location: %s/?%s", http_req->path,
                                                         http_req->query);
                                        else
                                                snprintf(location, sizeof(location),
                                                         "Location: %s/", http_req->path);

                                        ret = http_send_error(http_req, 302, "Found", location,
                                                              "Directories must end with a slash.",
                                                              &job->reply);
                                        job->status = HTTP_GET_ERROR;
                                        break;
                                }

                                ret = http_readdir(http_req, &job->reply);
                                if (ret)
                                        GOTO(err_ret, ret);

                                job->status = HTTP_GET_DONE;
                                job_set_ret(job, 0, 0);

                                ret = http_reply_send(job, &job->reply, KEEP_JOB);
                                if (ret)
                                        GOTO(err_ret, ret);

                                done = 1;
                                break;

#if 0
                                /* check for an index file */
                                for (i = 0; i < sizeof(index_names) / sizeof(char *); ++i) {
                                        snprintf(idx, sizeof(idx), "%s%s", http_req->path,
                                                 index_names[i]);

                                        ret = ly_getattr(idx, &http_req->stat);
                                        if (ret == 0) {
                                                http_req->path = idx;

                                                //(void) xmit_file(http_req, clisd);
                                        }
                                }

                                /* nope, no index file, so it's an actual directory request */

                                /* xmit_dir( ); */
                                if (i == (i < sizeof(index_names) / sizeof(char *))) {
                                        ret = http_send_error(http_req, 302, "Found", "",
                                                              "Is a directory.", &job->reply);
                                        job->status = HTTP_GET_ERROR;
                                        break;
                                }
#endif
                        }

                        mime_getype(http_req->path, mime_encodings, sizeof(mime_encodings),
                                    &mime_type);
                        (void)snprintf(fixed_mime_type, sizeof(fixed_mime_type), mime_type,
                                       charset);

                        if (http_req->if_modified_since != (time_t) -1
                            && http_req->if_modified_since >= http_req->stat.st_mtime) {
                                ret = http_send_nomod(http_req, mime_encodings, fixed_mime_type,
                                                      http_req->stat.st_mtime, &job->reply);
                                if (ret)
                                        GOTO(err_ret, ret);
                                else {
                                        done = 1;

                                        break;
                                }
                        }

                        context->offset = http_req->offset;
                        context->size = http_req->stat.st_size;
                        context->size -= context->offset;

                        if (context->offset > 0)
                                ret = http_send_response(http_req, 206, &job->reply);
                        else 
                                ret = http_send_ok(http_req, mime_encodings, fixed_mime_type,
                                                   &http_req->stat, &job->reply);
                        if (ret)
                                GOTO(err_ret, ret);

                        job->status = HTTP_GET_READ;
                        job_set_ret(job, 0, 0);

                        ret = http_reply_send(job, &job->reply, KEEP_JOB);
                        if (ret)
                                GOTO(err_ret, ret);

                        done = 1;
                        break;
                case HTTP_GET_READ:
                        ret = job_get_ret(job, 0);
                        if (ret < 0) {
                                ret = -ret;
                                GOTO(err_ret, ret);
                        }

                        if (http_req->method == METHOD_HEAD
                            ||http_req->stat.st_size == 0) {
                                job->status = HTTP_GET_DONE;
                                break;
                        }

                        mbuffer_init(&context->buf, 0);

                        size = context->size < Y_MSG_MAX ? context->size : Y_MSG_MAX;

                        job->status = HTTP_GET_WAIT_READ;

                        DBUG("read "FID_FORMAT" offset %llu size %u\n", FID_ARG(&context->fileid), (LLU)context->offset, size);

                        ret = sdfs_read_async(&context->fileid, &context->buf,
                                           size, context->offset,
                                           job_resume, job);
                        if (ret)
                                GOTO(err_ret, ret);

                        done = 1;
                        break;
                case HTTP_GET_WAIT_READ:
                        ret = job_get_ret(job, 0);
                        if (ret < 0) {
                                ret = -ret;
                                GOTO(err_ret, ret);
                        }

                        context->size -= ret;
                        context->offset += ret;

                        mbuffer_init(&job->reply, 0);
                        mbuffer_merge(&job->reply, &context->buf);

                        job->status = HTTP_GET_WAIT_SEND;
                        job_set_ret(job, 0, 0);

                        ret = http_reply_send(job, &job->reply, KEEP_JOB);
                        if (ret) {
                                if (ret == ECONNRESET)
                                        goto err_ret;
                                else
                                        GOTO(err_ret, ret);
                        }

                        done = 1;
                        break;
                case HTTP_GET_ERROR:
                        job->status = HTTP_GET_DONE;
                        job_set_ret(job, 0, 0);

                        ret = http_reply_send(job, &job->reply, KEEP_JOB);
                        if (ret)
                                GOTO(err_ret, ret);

                        done = 1;
                        break;
                case HTTP_GET_WAIT_SEND:
                        ret = job_get_ret(job, 0);
                        if (ret) {
                                if (ret == ECONNRESET)
                                        goto err_ret;
                                else
                                        GOTO(err_ret, ret);
                        }

                        mbuffer_free(&job->reply);

                        if (context->size) {
                                job->status = HTTP_GET_READ;

                                break;
                        } else {
                                job->status = HTTP_GET_DONE;

                                break;
                        }
                case HTTP_GET_DONE:
                        end_request(&context->http_req);

                        sdevents_close(&job->net);
                        job_destroy(job);
                        done = 1;
                        break;
                default:
                        DERROR("error op\n");
                }
        }

        return 0;
err_ret:
        end_request(&context->http_req);
        sdevents_close(&job->net);
        job_destroy(job);
        return ret;
}

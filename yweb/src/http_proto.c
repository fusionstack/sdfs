

#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "job_dock.h"
#include "job_tracker.h"
#include "http_state_machine.h"
#include "http_ops.h"
#include "../net/net_events.h"
#include "net_global.h"
#include "str.h"
#include "tdate_parse.h"
#include "ynet_rpc.h"
#include "yweb_conf.h"
#include "dbg.h"

const char http_request[] = "http_request";

extern jobtracker_t *jobtracker;

extern  char *methods_list[];
void dir_dotdot(char **_path)
{
        char *path;
        char *last_slash = '\0';

        path = *_path;

        while(*path) {
                if (*path == '/')
                        last_slash = path;

                path++;
        }

        last_slash += 1;
        if (*last_slash == '\0')
                last_slash = NULL;

        *_path = last_slash;
}


int http_pack_len(void *buf, uint32_t len, int *msg_len, int *io_len)
{
        void *ptr;

        (void)len;
        (void)msg_len;
        (void)io_len;

        ptr = strstr(buf, HTTP_REQ_END1);

        if (ptr) {
                return ptr - buf + _strlen(HTTP_REQ_END1);
        }

        ptr = strstr(buf, HTTP_REQ_END2);

        if (ptr) {
                return ptr - buf + _strlen(HTTP_REQ_END2);
        }

        return 0;
}

int http_pack_handler(void *self, void *_buf)
{
	char msg_unsupport[64];
        int ret;
        ynet_sock_conn_t *sock;
        buffer_t *buf;
        job_t *job;
        http_job_context_t *context;
        struct http_request *http_req;
        char *method_str, *line, *hdr;
        char req[MAX_BUF_LEN];
        uint32_t reqlen;
        net_handle_t *nh;

	int i = 0;

        buf = _buf;
        sock = self;

        ret = job_create(&job, jobtracker, http_request);
        if (ret)
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&context, sizeof(http_job_context_t));
        if (ret)
                GOTO(err_ret, ret);

        job->context = context;
        http_req = &context->http_req;

        start_request(http_req);

        reqlen = buf->len;
        ret = mbuffer_popmsg(buf, req, buf->len);
        if (ret)
                GOTO(err_ret, ret);

        //DINFO("req %s\n", req);

        ret = append_request(http_req, req, reqlen);
        if (ret)
                GOTO(err_ret, ret);

        mbuffer_free(buf);

        init_request(http_req);

        /* parse the first line of the request */
        get_request_line(http_req, &method_str);
        if (method_str == NULL) {
                (void) http_send_badreq(http_req, &job->reply);

                ret = EINVAL;
                GOTO(err_req, ret);
        }
        DBUG("method %s\n", method_str);

        http_req->path = strpbrk(method_str, " \t\012\015");
        if (http_req->path == NULL) {
                (void) http_send_badreq(http_req, &job->reply);

                ret = EINVAL;
                GOTO(err_req, ret);
        }
        *http_req->path++ = '\0';
        http_req->path += strspn(http_req->path, " \t\012\015");
        DBUG("path %s\n", http_req->path);

        http_req->protocol = strpbrk(http_req->path, " \t\012\015");
        if (http_req->protocol == NULL) {
                (void) http_send_badreq(http_req, &job->reply);

                ret = EINVAL;
                GOTO(err_req, ret);
        }
        *http_req->protocol++ = '\0';
        http_req->protocol += strspn(http_req->protocol, " \t\012\015");
        DBUG("protocol %s\n", http_req->protocol);

        http_req->query = strchr(http_req->path, '?');
        if (http_req->query == NULL)
                http_req->query = "";
        else
                *http_req->query++ = '\0';
        DBUG("query %s\n", http_req->query);

        /* parse the rest of the request headers */
        while (srv_running) {
                get_request_line(http_req, &line);

                if (line == NULL)
                        break;
                else if (strncasecmp(line, "Authorization:", 14) == 0) {
                        hdr = &line[14];
                        hdr += strspn(hdr, " \t");
                        http_req->authorization = hdr;
                } else if (strncasecmp(line, "Content-Length:", 15) == 0) {
                        hdr = &line[15];
                        hdr += strspn(hdr, " \t");
                        http_req->content_length = strtoul(hdr, (char **)NULL,
                                                          10);
                } else if (strncasecmp(line, "Content-Type:", 13) == 0) {
                        hdr = &line[13];
                        hdr += strspn(hdr, " \t");
                        http_req->content_type = hdr;
                } else if (strncasecmp(line, "Cookie:", 7) == 0) {
			hdr = &line[7];
                        hdr += strspn(hdr, " \t");
                        http_req->cookie = hdr;
                } else if (strncasecmp(line, "Host:", 5) == 0) {
                        hdr = &line[5];
                        hdr += strspn(hdr, " \t");
                        http_req->host = hdr;

                        if (strchr(http_req->host, '/') != NULL
                            || http_req->host[0] == '.') {
                                (void) http_send_badreq(http_req, &job->reply);

                                ret = EINVAL;
                                GOTO(err_req, ret);
                        }
                } else if (strncasecmp(line, "If-Modified-Since:", 18) == 0) {
                        hdr = &line[18];
                        hdr += strspn(hdr, " \t");
                        (void) tdate_parse(hdr, &http_req->if_modified_since);
                } else if (strncasecmp(line, "Referer:", 8) == 0) {
                        hdr = &line[8];
                        hdr += strspn(hdr, " \t");
                        http_req->referer = hdr;
                } else if (strncasecmp(line, "User-Agent:", 11) == 0) {
                        hdr = &line[11];
                        hdr += strspn(hdr, " \t");
                        http_req->useragent = hdr;
                } else if (strncasecmp(line, "Range:", 6) == 0) {
                        hdr = &line[6];
                        hdr += strspn(hdr, " \t");

                        DBUG("Range: %s\n", hdr);

                        char *str = strstr(hdr, "bytes=");
                        char *endptr = "\r\n";
                        if (str) {
                                str += 6;
                                http_req->offset = strtoull(str, &endptr, 0);
                        } 
                }
        }


	/* all http method:
	   get head put post options delete.
	 */

	i = 0;

	while (i < MAX_METHOD_INDEX) {
		if (strcasecmp(method_str, get_method_str(i)) == 0)
			break;
		i++;
	}


	if (i >= MAX_METHOD_INDEX) {
		(void) http_send_error(http_req, 501, "Not Implemented", "",
                                       "That method is not implemented.",
                                       &job->reply);
                ret = EINVAL;
                GOTO(err_req, ret);
	}

	http_req->method = i;
        DBUG("method %s\n", method_str);

        /* verify request-uri */
        str_decode(http_req->path, http_req->path);

        if (http_req->path[0] != '/') {
                (void) http_send_error(http_req, 400, "Bad Request", "",
                                       "Bad filename.", &job->reply);

                ret = EINVAL;
                GOTO(err_req, ret);
        }

        http_req->file = &http_req->path[0];

        dir_dotdot(&http_req->file);

        nh = &job->net;
        *nh = sock->nh;


        switch (http_req->method) {
        case METHOD_GET:

		job->status = HTTP_GET_BEGIN;

                job->state_machine = http_state_machine_get;

                ret = jobtracker_insert(job);
                if (ret)
                        GOTO(err_req, ret);
                break;

        case METHOD_HEAD:
	case METHOD_PUT:
	case METHOD_POST:
	case METHOD_DELETE:
	case METHOD_OPTIONS:
		goto err_method;
		break;

        default:
                break;
        }
	
        return 0;

err_method:
        mbuffer_init(&job->reply, 0);
#define FORMAT_METHOD_UNSUPPORT "%s is unsupport"

	sprintf(msg_unsupport, FORMAT_METHOD_UNSUPPORT, get_method_str(job->status));

#undef FORMAT_METHOD_UNSUPPORT

	(void) http_send_error (http_req, 400, "Bad Request", "", msg_unsupport, &job->reply);

	
err_req:

	job->state_machine = http_state_machine_error;
	ret = jobtracker_insert(job);

	if (ret)
		GOTO(err_ret, ret);
	

err_ret:
        return ret;
}

int http_accept_handler(int fd, void *context)
{
        int ret;
        net_proto_t proto;
        net_handle_t nh;

        (void) context;

        _memset(&proto, 0x0, sizeof(net_proto_t));

        proto.head_len = 0;
        proto.reader = net_events_handle_read;
        proto.writer = net_events_handle_write;
        proto.pack_len = http_pack_len;
        proto.pack_handler = http_pack_handler;

        ret = sdevents_accept(fd, &nh, &proto,
                              YNET_RPC_NONBLOCK);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdevents_add(&nh, NULL, Y_EPOLL_EVENTS);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int http_listen(const char* port, int nonblock)
{
        int ret;
        net_proto_t proto;
        net_handle_t nh;

        _memset(&proto, 0x0, sizeof(net_proto_t));

        proto.reader = http_accept_handler;

        ret = sdevents_listen(&nh, NULL, port, &proto, nonblock);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdevents_add(&nh, NULL, Y_EPOLL_EVENTS_LISTEN);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}



#include <string.h>

#define DBG_SUBSYS S_YWEB

#include "request.h"
#include "ylib.h"
#include "dbg.h"


char *methods_list[MAX_METHOD_INDEX] = {
  "GET",
  "HEAD",
  "POST",
  "PUT",
  "DELETE",
  "OPTIONS",
};


char * get_method_str(int m)
{
	if (m >= MAX_METHOD_INDEX - 1 || m < 0)
		return NULL;

	return methods_list[m];
}

void start_request(struct http_request *req)
{
        req->size = 0;
        req->idx = 0;
        req->buf = NULL;
        req->len = 0;
        req->offset = 0;
}

int append_request(struct http_request *req, char *buf, uint32_t len)
{
        int ret;
        uint32_t needsize, newsize;
        void *ptr;

        needsize = req->len + len;

        if (needsize >= req->size) {
                if (req->size == 0)
                        newsize = 1024;

                while (newsize < needsize)
                        newsize *= 2;

                ptr = req->buf;
                ret = yrealloc(&ptr, req->size, newsize);
                if (ret)
                        GOTO(err_ret, ret);

                req->buf = ptr;
                req->size = newsize;
        }

        _memmove(&req->buf[req->len], buf, len);
        req->len += len;
        req->buf[req->len] = '\0';

        return 0;
err_ret:
        return ret;
}

void init_request(struct http_request *req)
{
        req->path = NULL;
        req->protocol = NULL;
        req->query = "";
        req->authorization = NULL;
        req->content_length = -1;
        req->content_type = NULL;
        req->cookie = NULL;
        req->host = NULL;
        req->if_modified_since = (time_t) -1;
        req->referer = "";
        req->useragent = "";
        req->offset = 0;
        req->method = -1;
}

void get_request_line(struct http_request *req, char **line)
{
        int i;
        char c;

        *line = NULL;

        for (i = req->idx; req->idx < req->len; ++req->idx) {
                c = req->buf[req->idx];

                if (c == '\012' || c == '\015') {
                        req->buf[req->idx] = '\0';
                        ++req->idx;

                        if (c == '\015' && req->idx < req->len
                            && req->buf[req->idx] == '\012') {
                                req->buf[req->idx] = '\0';
                                ++req->idx;
                        }

                        *line = &req->buf[i];
                        break;
                }
        }
}

void end_request(struct http_request *req)
{
        req->size = 0;
        req->len = 0;
        yfree((void **)&req->buf);
}

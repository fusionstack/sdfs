

#include <unistd.h>
#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_YWEB

#include "ynet_rpc.h"
#include "response.h"
#include "ylib.h"
#include "dbg.h"

void start_response(struct http_response *rep)
{
        rep->size = 0;
        rep->len = 0;
        rep->buf = NULL;
}

int append_response(struct http_response *rep, char *buf, uint32_t len)
{
        int ret;
        uint32_t needsize, newsize;
        void *ptr;

        needsize = rep->len + len;

        if (needsize >= rep->size) {
                if (rep->size == 0)
                        newsize = 1024;

                while (newsize < needsize)
                        newsize *= 2;

                ptr = rep->buf;
                ret = yrealloc(&ptr, rep->size, newsize);
                if (ret)
                        GOTO(err_ret, ret);

                rep->buf = ptr;
                rep->size = newsize;
        }

        _memmove(&rep->buf[rep->len], buf, len);
        rep->len += len;
        rep->buf[rep->len] = '\0';

        return 0;
err_ret:
        return ret;
}

int send_response(struct http_response *rep, int sd)
{
        return _send(sd, rep->buf, rep->len, 0);
}

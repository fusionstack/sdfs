

#include <arpa/inet.h>

#define DBG_SUBSYS S_LIBYLIB

#include "xdr.h"
#include "ylib.h"
#include "errno.h"
#include "dbg.h"

#define LASTUNSIGNED	((u_int) 0-1)

int __xdr_void(void)
{
        return 0;
}

int __xdr_buffer(xdr_t *xdr, char **_buf, uint32_t *size,
			uint32_t max)
{
        int ret;
        buffer_t *buf;
        uint32_t len, tail;
        char blank[4];

        switch (xdr->op) {
        case __XDR_DECODE:
                ret = mbuffer_popmsg(xdr->buf, (void *)&len, sizeof(uint32_t));
                if (ret)
                        GOTO(err_ret, ret);

                len = htonl(len);

                if (len > max) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }

                ret = ymalloc((void **)&buf, sizeof(buffer_t));
                if (ret)
                        GOTO(err_ret, ret);

                mbuffer_init(buf, 0);

                ret = mbuffer_pop(xdr->buf, buf, len);
                if (ret) {
                        GOTO(err_ret, ret);
                }

                *_buf = (void *)buf;
                *size = len;

                break;
        case __XDR_ENCODE:
                buf = (void *)*_buf;

                if (*size != buf->len || *size > max) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }

                len = htonl(*size);
                tail = *size % 4;
                tail = tail ? (4 - tail) : 0;

                ret = mbuffer_appendmem(xdr->buf, (void *)&len, sizeof(uint32_t));
                if (ret)
                        GOTO(err_ret, ret);

#if 0
                DINFO("buffer crc %x\n", mbuffer_crc(buf, 0, buf->len));
#endif

                mbuffer_merge(xdr->buf, buf);

                if (tail) {
                        _memset(blank, 0x0, tail);

                        ret = mbuffer_appendmem(xdr->buf, (void *)blank, tail);
                        if (ret)
                                GOTO(err_ret, ret);
                }

                break;
        case __XDR_FREE:
                buf = (void *)*_buf;

                mbuffer_free(buf);

                yfree((void **)_buf);

                break;
        }

        return 0;
err_ret:
        return ret;
}

int __xdr_bytes(xdr_t *xdr, char **buf, uint32_t *size,
			uint32_t max)
{
        int ret;
        char *ptr;
        uint32_t len;

        switch (xdr->op) {
        case __XDR_DECODE:
                ret = mbuffer_popmsg(xdr->buf, (void *)&len, sizeof(uint32_t));
                if (ret)
                        GOTO(err_ret, ret);

                len = ntohl(len);

                if (len > max) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }

                ret = ymalloc((void **)&ptr, len + 1);
                if (ret)
                        GOTO(err_ret, ret);

                ptr[len] = 0;

                ret = __xdr_opaque(xdr, ptr, len);
                if (ret)
                        GOTO(err_ret, ret);
                
                *size = len;
                *buf = ptr;

                break;
        case __XDR_ENCODE:
                ptr = *buf;

                len = *size;
                len = htonl(len);

                ret = mbuffer_appendmem(xdr->buf, (void *)&len, sizeof(uint32_t));
                if (ret)
                        GOTO(err_ret, ret);

                ret = __xdr_opaque(xdr, ptr, *size);
                if (ret)
                        GOTO(err_ret, ret);

                break;
        case __XDR_FREE:
                yfree((void **)buf);

                break;
        }

        return 0;
err_ret:
        return ret;
}

int __xdr_string (xdr_t *xdr, char **__cpp, u_int __maxsize)
{
        int ret;
        uint32_t len;

        if (xdr->op == __XDR_ENCODE)
                len = _strlen(*__cpp);

        ret = __xdr_bytes(xdr, __cpp, &len, __maxsize);
        if (ret)
                GOTO(err_ret, ret);

#if 0
        if (len != _strlen(*__cpp)) {
                ret = EINVAL;

                DERROR("len %u:%lu op %d\n", len, _strlen(*__cpp), xdr->op);

                GOTO(err_ret, ret);
        }
#endif

        return 0;
err_ret:
        return ret;
}

int __xdr_long (xdr_t *xdr, long *_val)
{
        int ret;
        uint32_t val;

        switch (xdr->op) {
        case __XDR_DECODE:
                ret = mbuffer_popmsg(xdr->buf, (void *)&val, sizeof(uint32_t));
                if (ret)
                        GOTO(err_ret, ret);

                *_val = ntohl(val);

                break;
        case __XDR_ENCODE:
                val = *_val;
                val = htonl(val);

                ret = mbuffer_appendmem(xdr->buf, &val, sizeof(uint32_t));
                if (ret)
                        GOTO(err_ret, ret);

                break;
        case __XDR_FREE:
                break;
        }

        return 0;
err_ret:
        return ret;
}

int __xdr_enum (xdr_t *xdr, enum_t *__ep)
{
        int ret;
        long val;

        val = (long)*__ep;

        ret = __xdr_long(xdr, &val);
        if (ret)
                GOTO(err_ret, ret);

        *__ep = (enum_t)val;

        return 0;
err_ret:
        return ret;
}

int __xdr_uint32 (xdr_t *xdr, uint32_t *__up)
{
        int ret;
        long val;

        val = (long)*__up;

        ret = __xdr_long(xdr, &val);
        if (ret)
                GOTO(err_ret, ret);

        *__up = (uint32_t)val;

        return 0;
err_ret:
        return ret;
}

int __xdr_int(xdr_t *xdr, int *__ip)
{
        int ret;
        long val;

        val = (long)*__ip;

        ret = __xdr_long(xdr, &val);
        if (ret)
                GOTO(err_ret, ret);

        *__ip = (int)val;

        return 0;
err_ret:
        return ret;
}

int __xdr_uint64(xdr_t *xdr, uint64_t *val)
{
        int ret;
        char buf[8];
        uint32_t *top;
        uint32_t *bottom;

        switch (xdr->op) {
        case __XDR_ENCODE:
                buf[0] = (*val >> 56) & 0xFF;
                buf[1] = (*val >> 48) & 0xFF;
                buf[2] = (*val >> 40) & 0xFF;
                buf[3] = (*val >> 32) & 0xFF;
                buf[4] = (*val >> 24) & 0xFF;
                buf[5] = (*val >> 16) & 0xFF;
                buf[6] = (*val >> 8) & 0xFF;
                buf[7] = *val & 0xFF;

                ret = __xdr_opaque(xdr, buf, 8);
                if (ret)
                        GOTO(err_ret, ret);

                break;
        case __XDR_DECODE:
                top = (void *)&buf[0];
                bottom = (void *)&buf[4];

                ret = __xdr_opaque(xdr, buf, 8);
                if (ret)
                        GOTO(err_ret, ret);

                *val = (uint64_t)(ntohl(*top)) << 32 | ntohl(*bottom);
                break;
        case __XDR_FREE:
                break;
        }

        return 0;
err_ret:
        return ret;
}

int __xdr_bool (xdr_t *xdr, int *_val)
{
        int ret;
        long val;

        val = *_val;

        ret = __xdr_long(xdr, &val);
        if (ret)
                GOTO(err_ret, ret);

        *_val = val;

        return 0;
err_ret:
        return ret;
}

int __xdr_opaque (xdr_t *xdr, char *val, uint32_t size)
{
        int ret;
        uint32_t tail;
        char blank[4];

        tail = size % 4;
        tail = tail ? (4 - tail) : 0;

        switch (xdr->op) {
        case __XDR_DECODE:
                ret = mbuffer_popmsg(xdr->buf, val, size);
                if (ret)
                        GOTO(err_ret, ret);

                if (tail) {
                        ret = mbuffer_popmsg(xdr->buf, (void *)blank, tail);
                        if (ret)
                                GOTO(err_ret, ret);
                }

                break;
        case __XDR_ENCODE:
                ret = mbuffer_appendmem(xdr->buf, val, size);
                if (ret)
                        GOTO(err_ret, ret);

                if (tail) {
                        _memset(blank, 0x0, tail);

                        ret = mbuffer_appendmem(xdr->buf, blank, tail);
                        if (ret)
                                GOTO(err_ret, ret);
                }

                break;
        case __XDR_FREE:

                break;
        }

        return 0;
err_ret:
        return ret;
}

int __xdr_reference (xdr_t *xdr, char **pp, uint32_t size, __xdrproc_t proc)
{
        int ret;
        char *loc = *pp;

        if (loc == NULL) {
                switch (xdr->op) {
                case __XDR_FREE:
                        goto out;
    
                case __XDR_DECODE:
                        ret = ymalloc((void **)&loc, size);
                        if (ret)
                                GOTO(err_ret, ret);

                        *pp = loc;
                        break;
                default:
                        break;
                }
        }

        ret = (*proc) (xdr, loc, LASTUNSIGNED);
        if (ret)
                GOTO(err_ret, ret);

        if (xdr->op == __XDR_FREE) {
                yfree((void **)&loc);
                *pp = NULL;
        }

out:
        return 0;
err_ret:
        return ret;
}


int __xdr_pointer (xdr_t *xdr, char **objpp,
			   uint32_t obj_size, __xdrproc_t proc)
{
        int ret, more_data;

        more_data = (*objpp != NULL);
        ret = __xdr_bool(xdr, &more_data);
        if (ret)
                GOTO(err_ret, ret);

        if (!more_data) {
                *objpp = NULL;

                goto out;
        } else
                return __xdr_reference(xdr, objpp, obj_size, proc);

out:
        return 0;
err_ret:
        return ret;
}

int __xdr_array(xdr_t *xdr , void *__addrp, uint32_t *__sizep,
                uint32_t __maxsize, uint32_t __elsize, __xdrproc_t __elproc)
{
        int ret;
        char **ptr;
        uint32_t size, i;

        i = *__sizep;
        ptr = __addrp;
        size = __elsize;

        ret = __xdr_uint32(xdr, __sizep);
        if (ret)
                GOTO(err_ret, ret);

        for (i = 0; i < *__sizep && size < __maxsize; i ++) {
                ret = __elproc(xdr, (void *)*ptr);
                if (ret)
                        GOTO(err_ret, ret);

                ptr += __elsize;
                size += __elsize;
        }

        return 0;
err_ret:
        return ret;
}

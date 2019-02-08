

#define _GNU_SOURCE

#include <linux/aio_abi.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define DBG_SUBSYS S_YFSLIB

#include "sdfs_lib.h"
#include "dbg.h"
#include "ylib.h"

#define MAX_REQ 1024
#define __COUNT__ 128
#define BUFLEN (1024 * 1024)

struct io_event events[MAX_REQ];
struct iocb *ioarray[MAX_REQ], queue[MAX_REQ];
char buf[BUFLEN];
aio_context_t ctx;

int write_async(int fd)
{
        int ret, count = __COUNT__, i, r, off;
        struct iocb *cur;
        struct iovec iov[3];

        ctx = 0;

        ret = io_setup(MAX_REQ, &ctx);
        if (ret) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        iov[0].iov_base = buf;
        iov[0].iov_len = BUFLEN;
        iov[1].iov_base = buf;
        iov[1].iov_len = BUFLEN;
        iov[2].iov_base = buf;
        iov[2].iov_len = BUFLEN;

        off = 0;
        for (i = 0; i < count; i++) {
                cur = &queue[i];

                io_prep_pwritev(cur, fd, iov, 3, off);

                off += (BUFLEN * 3);
                ioarray[i] = cur;
        }

        DINFO("offset %u\n", off);

        ret = io_submit(ctx, count, ioarray);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        while (count > 0) {
                r = io_getevents(ctx, 1, count > MAX_REQ ? MAX_REQ: (long) count,
                                 events, NULL);
                if (r > 0) {
                        count -= r;

                        DINFO("r %u count %u\n", r, count);
                } else {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
                
        }

        return 0;
err_ret:
        return ret;
}

int write_sync(int fd)
{
        int ret, count = __COUNT__, i, r, off;
        struct iocb *cur;
        struct iovec iov[3];

        ctx = 0;

        ret = io_setup(MAX_REQ, &ctx);
        if (ret) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        iov[0].iov_base = buf;
        iov[0].iov_len = BUFLEN;
        iov[1].iov_base = buf;
        iov[1].iov_len = BUFLEN;
        iov[2].iov_base = buf;
        iov[2].iov_len = BUFLEN;

        for (i = 0; i < count; i++) {
                ret = writev(fd, iov, 3);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

int main()
{
        int ret, fd;

        //fd = open("/tmp/test1", O_CREAT | O_RDWR | O_SYNC | O_DIRECT, 0644);
        //fd = open("/tmp/test1", O_CREAT | O_RDWR | O_SYNC, 0644);
        fd = open("/sysy/yfs/cds/2/test1", O_CREAT | O_RDWR | O_SYNC, 0644);
        //fd = open("/tmp/test1", O_CREAT | O_RDWR, 0644);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }


#if 1
        return write_async(fd);
#else
        return write_sync(fd);
#endif

        return 0;
err_ret:
        return ret;
}

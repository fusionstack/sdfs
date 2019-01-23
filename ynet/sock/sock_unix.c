#include "config.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <netdb.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "sock_tcp.h"
#include "net_global.h"
#include "sysutil.h"
#include "sock_unix.h"
#include "configure.h"
#include "ylib.h"
#include "ynet_conf.h"
#include "ynet_sock.h"
#include "dbg.h"

int sock_unix_tuning(int sd)
{
        int ret, xmit_buf, flag;
        struct linger lin __attribute__((unused));
        struct timeval tv;
        socklen_t size;

        flag = fcntl(sd, F_GETFL);
        if (flag < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = fcntl(sd, F_SETFL, flag | O_CLOEXEC);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = sock_setnonblock(sd);
        if (unlikely(ret)) {
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        tv.tv_sec = 30;
        tv.tv_usec = 0;
        ret = setsockopt(sd, SOL_SOCKET, SO_SNDTIMEO, (void *)&tv,
                         sizeof(struct timeval));
        if (ret == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        ret = setsockopt(sd, SOL_SOCKET, SO_RCVTIMEO, (void *)&tv,
                         sizeof(struct timeval));
        if (ret == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        xmit_buf = SO_XMITBUF;
        ret = setsockopt(sd, SOL_SOCKET, SO_SNDBUF, &xmit_buf, sizeof(int));
        if (ret == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        xmit_buf = SO_XMITBUF;
        ret = setsockopt(sd, SOL_SOCKET, SO_RCVBUF, &xmit_buf, sizeof(int));
        if (ret == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        xmit_buf = 0;
        size = sizeof(int);

        ret = getsockopt(sd, SOL_SOCKET, SO_SNDBUF, &xmit_buf, &size);
        if (ret == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        } else if (xmit_buf != SO_XMITBUF * 2)
                DWARN("Can't set tcp send buf to %d (got %d)\n", SO_XMITBUF,
                      xmit_buf);

        DBUG("send buf %u\n", xmit_buf);

        xmit_buf = 0;
        size = sizeof(int);

        ret = getsockopt(sd, SOL_SOCKET, SO_RCVBUF, &xmit_buf, &size);
        if (ret == -1) {
                ret = errno;
                DERROR("%d - %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        } else if (xmit_buf != SO_XMITBUF * 2)
                DWARN("Can't set tcp recv buf to %d (got %d)\n", SO_XMITBUF,
                      xmit_buf);

        DBUG("recv buf %u\n", xmit_buf);

        return 0;
err_ret:
        return ret;
}

int sock_unix_connect(const char *path, int *_sd, struct sockaddr_un *addr)
{
        int ret, sd;
        socklen_t len;

	addr->sun_family = AF_UNIX;
	strncpy(addr->sun_path, path, sizeof(addr->sun_path) - 1);

        len = sizeof(addr->sun_family) + strlen(addr->sun_path) + 1;
	sd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (sd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
	}

        ret = connect(sd, (struct sockaddr *)addr, len);
        if (ret < 0) {
                ret = errno;
                if (ret == ENOENT) {
                        DWARN("%s not exist\n", path);
                        ret = ENONET;
                }

                GOTO(err_ret, ret);
        }

        *_sd = sd;

        return 0;
err_ret:
        return ret;
}

int sock_unix_listen(const char *path, int *_sd, struct sockaddr_un *addr)
{
	int ret, len, sd;
	mode_t mode;

	sd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (sd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
	}

	mode = umask(000);

	memset(addr, 0, sizeof(*addr));
	memset(addr->sun_path, 0, sizeof(addr->sun_path));
	addr->sun_family = AF_UNIX;
	strncpy(addr->sun_path, path, sizeof(addr->sun_path) - 1);
	unlink(path);

	len = sizeof(addr->sun_family) + strlen(path) + 1;

	ret = bind(sd, (struct sockaddr *) addr, len);
        if (ret < 0) {
                ret = errno;
                DERROR("bind to %s errno (%d)%s\n", path, ret, strerror(ret));
                GOTO(err_sd, ret);
	}

	ret = listen(sd, 100);
        if (ret < 0) {
                ret = errno;
                GOTO(err_sd, ret);
	}

	umask(mode);
        *_sd = sd;

	return 0;
err_sd:
        close(sd);
err_ret:
        return ret;
}

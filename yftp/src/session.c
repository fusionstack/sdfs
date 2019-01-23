

#include <sys/epoll.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <errno.h>

#define DBG_SUBSYS S_YFTP

#include "session.h"
#include "ylib.h"
#include "ynet_rpc.h"
#include "dbg.h"

long yftp_session_running = 0;

int session_init(struct yftp_session *ys, int ctrl_fd)
{
        int ret, epoll_fd;
        struct epoll_event ev;

        epoll_fd = epoll_create(1);
        if (epoll_fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        _memset(&ev, 0x0, sizeof(struct epoll_event));
        ev.events = Y_EPOLL_EVENTS;
        ev.data.fd = ctrl_fd;

        ret = _epoll_ctl(epoll_fd, EPOLL_CTL_ADD, ctrl_fd, &ev);
        if (ret == -1) {
                ret = errno;
                GOTO(err_fd, ret);
        }

        _memset(ys, 0x0, sizeof(struct yftp_session));

        ys->epoll_fd = epoll_fd;

        ys->ctrl_fd = ctrl_fd;

        ys->pasv_sd = -1;

        ys->ctrl_line[YFTP_MAX_CMD_LINE] = '\0';
        ys->user[YFTP_MAX_CMD_LINE] = '\0';
        ys->pwd[MAX_PATH_LEN] = '\0';
        ys->rnfr_filename[MAX_PATH_LEN] = '\0';

        ys->pwd[0] = '/';
        ys->pwd[1] = '\0';
        ys->mode   = SESSION_READONLY;/*0=readonly 0xff=readwrite*/
        ys->fakedir = NULL;

        return 0;
err_fd:
        (void) sy_close(epoll_fd);
err_ret:
        return ret;
}

int session_destroy(struct yftp_session *ys)
{
        if (ys->ctrl_fd != -1) {
                (void) sy_close(ys->ctrl_fd);
                ys->ctrl_fd = -1;
        }

        if (ys->epoll_fd != -1) {
                (void) sy_close(ys->epoll_fd);
                ys->epoll_fd = -1;
        }

        if (ys->pasv_sd != -1) {
                (void) sy_close(ys->pasv_sd);
                ys->pasv_sd = 1;
        }

        yftp_session_running--;
        if (ys->fakedir)
                free(ys->fakedir);
        pthread_exit(NULL);

        return 0;
}

int session_clearpasv(struct yftp_session *ys)
{
        if (ys->pasv_sd != -1) {
                (void) sy_close(ys->pasv_sd);
                ys->pasv_sd = -1;
        }

        return 0;
}

int session_pasvactive(struct yftp_session *ys)
{
        return ys->pasv_sd != -1 ? 1 : 0;
}

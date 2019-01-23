

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <string.h>
#include <poll.h>
#include <errno.h>

#define DBG_SUBSYS S_YFTP

#include "ftpcodes.h"
#include "ynet_rpc.h"
#include "session.h"
#include "ylib.h"
#include "dbg.h"

int cmdio_write(struct yftp_session *ys, int status, const char *msg)
{
        int ret;
        char mangle[YFTP_MAX_CMD_LINE], cmd[YFTP_MAX_CMD_LINE];

        _memcpy(mangle, msg, _strlen(msg) + 1);

        /* Escape telnet characters properly */
        str_replace_str(mangle, "\377", "\377\377");

        str_replace_char(mangle, '\n', '\0');

        if (status)
                snprintf(cmd, YFTP_MAX_CMD_LINE, "%d %s\r\n", status, mangle);
        else
                snprintf(cmd, YFTP_MAX_CMD_LINE, "%s\r\n", mangle);
        

        ret = _write(ys->ctrl_fd, cmd, _strlen(cmd));
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        DBUG("cmd (%s) sent\n", cmd);

        return 0;
err_ret:
        return ret;
}

int cmdio_get_cmd_and_arg(struct yftp_session *ys)
{
        int ret, ctrl_fd, nfds;
        char *buf, *pos, *arg;
        uint32_t buflen, left, count, i, len;
        struct epoll_event ev;

        _memset(ys->ctrl_line, 0x0, YFTP_MAX_CMD_LINE);
        buf = ys->ctrl_line;
        buflen = 0;

        ctrl_fd = ys->ctrl_fd;

        pos = buf;
        left = YFTP_MAX_CMD_LINE;

        while (srv_running) {
                nfds = _epoll_wait(ys->epoll_fd, &ev, 1,
                                  YFTP_IDLE_SESSION_TIMEOUT * 1000);
                if (nfds == -1) {
                        ret = errno;
                        if (ret == EINTR)
                                continue;

                        GOTO(err_session, ret);
                } else if (nfds == 0) {
                        (void) cmdio_write(ys, FTP_IDLE_TIMEOUT, "Timeout.");
                        ret = ETIME;
                        GOTO(err_session, ret);
                }

                ret = rpc_peek_sd_sync(ctrl_fd, pos, left,
                                       YFTP_IDLE_SESSION_TIMEOUT);
                if (ret < 0) {
                        ret = -ret;
                        goto err_session;
                } else if (ret == 0) {
                        ret = ENODATA;
                        GOTO(err_session, ret);
                } else
                        count = (uint32_t)ret;

                for (i = 0; i < count; i++) {
                        if (pos[i] == FTP_CMD_END) {
                                len = i + 1;

                                ret = rpc_discard_sd_sync(ctrl_fd, len,
                                                     YFTP_IDLE_SESSION_TIMEOUT);
                                if (ret < 0)
                                        GOTO(err_session, ret);
                                else if (ret == 0) {
                                        ret = ENODATA;
                                        GOTO(err_session, ret);
                                } else {
                                        buflen += len;
                                        goto got_cmd;
                                }
                        }
                }

                ret = rpc_discard_sd_sync(ctrl_fd, count,
                                          YFTP_IDLE_SESSION_TIMEOUT);
                if (ret < 0)
                        GOTO(err_session, ret);
                else if (ret == 0) {
                        ret = ENODATA;
                        GOTO(err_session, ret);
                }

                left -= count;

                if (left == 0) {        /* cmd too long ! */
                        DWARN("Oops, cmd too long ! %s\n", buf);

                        _memset(buf, 0x0, buflen);
                        buflen = 0;

                        pos = buf;
                        left = YFTP_MAX_CMD_LINE;

                        continue;
                }

                buflen += count;
                pos += count;
        }

got_cmd:
        /* if the last char is a \r or \n, strip it */
        while (buflen > 0 && (buf[buflen - 1] == '\r'
               || buf[buflen - 1] == '\n')) {
                buf[buflen - 1] = '\0';
                buflen--;
        }

        /* split command and arguments */
        arg = strchr(buf, ' ');
        if (arg != NULL) {
                ys->arg = arg + 1;
                arg[0] = '\0';
        } else
                ys->arg = NULL;

        ys->cmd = ys->ctrl_line;
        DBUG("got cmd (%s) arg (%s)\n", ys->cmd, ys->arg);

        str_upper(ys->cmd);

        return 0;
err_session:
        (void) session_destroy(ys);

        return ret;
}

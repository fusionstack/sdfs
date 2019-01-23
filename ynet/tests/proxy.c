#include <sys/poll.h>
#include <errno.h>
#include <sys/ioctl.h>

#define DBG_SUBSYS S_YRPC

#include "ynet_rpc.h"
#include "dbg.h"

int main()
{
        int ret, listen1, listen2, listener_sd, speaker_sd, fds[2];
        struct pollfd pfd;
        uint32_t toread, towrite, piece;

        piece = 1024 * 64;

        ret = rpc_hostlisten(&listen1, NULL, "10090", 256, YNET_RPC_BLOCK);
        if (ret)
                GOTO(err_ret, ret);

        printf("listen 10090, waiting for listener...\n");

        pfd.fd = listen1;
        pfd.events = POLLIN;
        pfd.revents = 0;

        ret = poll(&pfd, 1, -1);
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = rpc_accept(&listener_sd, listen1, 1, YNET_RPC_BLOCK);
        if (ret)
                GOTO(err_ret, ret);

        printf("listenr accepted\n");

        ret = rpc_hostlisten(&listen2, NULL, "10091", 256, YNET_RPC_BLOCK);
        if (ret)
                GOTO(err_ret, ret);

        printf("listen 10091, waiting for speaker...\n");

        pfd.fd = listen2;
        pfd.events = POLLIN;
        pfd.revents = 0;

        ret = poll(&pfd, 1, -1);
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = rpc_accept(&speaker_sd, listen2, 1, YNET_RPC_BLOCK);
        if (ret)
                GOTO(err_ret, ret);

        printf("speaker accepted\n");

        ret = pipe(fds);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        printf("waiting for data...\n");

        pfd.fd = speaker_sd;
        pfd.events = POLLIN;
        pfd.revents = 0;


        while (srv_running) {
                ret = poll(&pfd, 1, -1);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                ret = ioctl(speaker_sd, FIONREAD, &toread);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                if (toread == 0) {
                        printf("peer close...\n");
                        break;
                } else
                        printf("got data len %u\n", toread);

                while(toread) {
                        ret = splice(speaker_sd, NULL, fds[1], NULL,
                                     toread < piece ? toread : piece,
                                     SPLICE_F_MOVE);
                        if (ret < 0) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        } else {
                                towrite = ret;
                                toread -= ret;
                        }

                        while(towrite) {
                                ret = splice(fds[0], NULL, listener_sd, NULL, towrite,
                                             SPLICE_F_MOVE);
                                if (ret < 0) {
                                        ret = errno;
                                        GOTO(err_ret, ret);
                                } else {
                                        towrite -= ret;
                                }
                        }
                }
        }

        return 0;
err_ret:
        return ret;
}

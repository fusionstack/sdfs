#include <sys/poll.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <unistd.h>

#define DBG_SUBSYS S_YRPC

#include "ynet_rpc.h"
#include "dbg.h"

//#define MAX_BUF_LEN 65536

int main(int argc, char *argv[])
{
        int ret, proxysd, fd;
        struct pollfd pfd;
        char write_to[128], host[128], buf[MAX_BUF_LEN];
        char c_opt;
        
//        uint32_t toread, towrite;

        while ((c_opt = getopt(argc, argv, "f:h:")) > 0)
                switch (c_opt) {
                case 'f':
                        _strcpy(write_to, optarg);
                        break;
                case 'h':
                        _strcpy(host, optarg);
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        GOTO(err_ret, ret);
                }


        ret = net_hostconnect(&proxysd, host, "10090", YNET_RPC_BLOCK);
        if (ret)
                GOTO(err_ret, ret);

        fd = open(write_to, O_CREAT | O_WRONLY, 0644);
        if (fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        printf("proxy '%s' connected waiting for data...\n", host);
        
        pfd.fd = proxysd;
        pfd.events = POLLIN;
        pfd.revents = 0;

        while (srv_running) {
                ret = poll(&pfd, 1, -1);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                ret = _read(proxysd, buf, MAX_BUF_LEN);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                if (ret == 0) {
                        printf("peer close\n");
                        break;
                }

                ret = write(fd, buf, ret);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        close(fd);
        close(proxysd);

        return 0;
err_ret:
        return ret;
}

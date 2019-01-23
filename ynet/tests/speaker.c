#include <sys/poll.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <sys/sendfile.h>

#define DBG_SUBSYS S_YRPC

#include "ynet_rpc.h"
#include "dbg.h"

//#define MAX_BUF_LEN 65536

int main(int argc, char *argv[])
{
        int ret, proxysd, fd;
        char write_to[128], host[128];
        char c_opt;
        struct stat stbuf;
        
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


        ret = net_hostconnect(&proxysd, host, "10091", YNET_RPC_BLOCK);
        if (ret)
                GOTO(err_ret, ret);

        fd = open(write_to, O_RDONLY);
        if (fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = fstat(fd, &stbuf);
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        printf("proxy '%s' connected begin sendfile\n", host);

        ret = sendfile(proxysd, fd, NULL, stbuf.st_size);
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        printf("send size %u\n", ret);

        sleep(3);

        return 0;
err_ret:
        return ret;
}

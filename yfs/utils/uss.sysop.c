

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#define DBG_SUBSYS S_LIBYLIB

#include "yfs_conf.h"
#include "disk_proto.h"
#include "configure.h"
#include "sdfs_lib.h"
#include "cd_proto.h"
#include "md_proto.h"
#include "md_lib.h"
#include "configure.h"
#include "dbg.h"

void usage(const char *prog)
{
        printf("Send Command:\n"
               "Usage:\n"
               "\t%s [-s] [-g] key [value]\n"
               "\t-----------------------\n"
               "example:\n"
               "\t-g key\n"
               "\t-s key value\n"
               "key/value:\n"
               "\tconsistent_leak: true or false\n"
               , prog);
}

#define SYSOP_SET 1
#define SYSOP_GET 2

int main(int argc, char *argv[])
{
        int ret, cmd = 0;
        const char *key, *value;
        char c_opt, *prog, buf[MAX_BUF_LEN];

        dbg_info(0);

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        cmd = 0;
        while ((c_opt = getopt(argc, argv, "sgh")) > 0) {
                switch (c_opt) {
                case 'h':
                        usage(prog);
                        exit(1);
                        break;
                case 's': 
                        cmd = SYSOP_SET;
                        break;
                case 'g':
                        cmd = SYSOP_GET;
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        exit(1);
                }
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if(ret)
                exit(1);

        ret = ly_init_simple("uss.sysop");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        }

        if (cmd == SYSOP_SET) {
                if (argc != 4) {
                        usage(prog);
                        exit(1);
                }

                key = argv[2];
                value = argv[3];

                ret = md_setopt(key, value);
                if (ret)
                        GOTO(err_ret, ret);

                printf("set %s %s\n", key, value);
        } else if (cmd == SYSOP_GET) {
                if (argc != 3) {
                        usage(prog);
                        exit(1);
                }

                key = argv[2];

                ret = md_getopt(key, buf);
                if (ret)
                        GOTO(err_ret, ret);

                printf("get %s %s\n", key, buf);
        } else {
                usage(prog);
                exit(1);
        }

        return 0;
err_ret:
        return ret;
}

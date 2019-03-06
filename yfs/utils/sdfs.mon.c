#include <sys/socket.h>
#include <getopt.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <dirent.h>

#include "yfs_conf.h"
#include "configure.h"
#include "sdfs_lib.h"
#include "net_global.h"
#include "md_lib.h"
#include "configure.h"
#include "yatomic.h"
#include "network.h"
#include "sdfs_list.h"
#include "redis_util.h"
#include "redis_conn.h"
#include "../../sdfs/io_analysis.h"
#include "nodectl.h"

void usage(const char *prog)
{
        printf("%s --type <cds/nfs/ganesha/samba>\n", prog);
}


int main(int argc, char *argv[])
{
        int ret;
        char c_opt;
        const char *type = NULL;

        while (srv_running) {
                int option_index = 0;

                static struct option long_options[] = {
                        {"type", required_argument, NULL, 't'},
                        {NULL, 0, NULL, 0}
                };

                c_opt = getopt_long(argc, argv, "t:",
                                    long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 't':
                        type = optarg;
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        usage(argv[0]);
                        exit(1);
                }
        }

        if (type == NULL ) {
                fprintf(stderr, "Hoops, wrong op got!\n");
                usage(argv[0]);
                exit(1);
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        gloconf.testing = 0;
        dbg_info(0);
        
        ret = ly_init_simple("mon");
        if (ret)
                GOTO(err_ret, ret);

        ret = io_analysis_dump(type);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}




#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "zk.h"

int main(int argc, char *argv[])
{
        int ret, args, verbose = 0;
        char c_opt, *prog;

        (void) verbose;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;
        dbg_info(0);

        /*if (argc < 2) {*/
                /*fprintf(stderr, "%s [-v]\n", prog);*/
                /*exit(1);*/
        /*}*/

        while ((c_opt = getopt(argc, argv, "v")) > 0)
                switch (c_opt) {
                        case 'v':
                                verbose = 1;
                                args++;
                                break;
                        default:
                                fprintf(stderr, "Hoops, wrong op got!\n");
                                exit(1);
                }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple2("uss.zk");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        }

        ret = zk_init("myid", gloconf.zk_hosts);
        if (ret)
                GOTO(err_ret, ret);

        ret = zk_connect();
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        exit(ret);
        return ret;
}

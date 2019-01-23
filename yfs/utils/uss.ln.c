#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <dirent.h>
#include <sys/types.h>

#include "configure.h"
#include "ylib.h"
#include "sdfs_lib.h"

#define PROG    "ln"

int verbose;

static void usage()
{
        fprintf(stderr, "usage: uss.ln [-vh] TARGET LINK_NAME\n"
                        "\t-v --verbose         Show verbose message\n"
                        "\t-h --help            Show this help\n"
               );
}

int main(int argc, char *argv[])
{
        int ret;
        char c_opt;
        char *target, *link_name;
        uid_t uid=0;
        gid_t gid=0;

        dbg_info(0);

        while (srv_running) {
                int option_index = 0;

                static struct option long_options[] = {
                        { "verbose", 0, 0, 'v' },
                        { "help",    0, 0, 'h' },
                        { 0, 0, 0, 0 },
                };

                c_opt = getopt_long(argc, argv, "vh", long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                        case 'v':
                                verbose = 1;
                                break;
                        case 'h':
                                usage();
                                exit(0);
                        default:
                                usage();
                                exit(EINVAL);
                }
        }

        if (argc - optind != 2) {
                usage();
                exit(1);
        }

        target = argv[optind++];
        link_name = argv[optind++];

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init_simple(PROG);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_link(target, link_name);
        if (ret)
                GOTO(err_ret, ret);

        uid = getuid();
        gid = getgid();
        ret = ly_chown(link_name, uid, gid);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        exit(ret);
}

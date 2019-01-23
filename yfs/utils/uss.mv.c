

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <dirent.h>

#include "configure.h"
#include "ylib.h"
#include "sdfs_lib.h"

#define PROG    "mv"

enum {
        PATH_HOST,
        PATH_USS,
};

int verbose;

static void usage()
{
        fprintf(stderr, "\nusage: uss.mv [-vh] </from/path> </to/path>\n"
                "\t-v --verbose         Show verbose message\n"
                "\t-h --help            Show this help\n"
               );
}

int main(int argc, char *argv[])
{
        int ret;
        /*char from[MAX_PATH_LEN], to[MAX_PATH_LEN];*/
        char c_opt;
        char *from, *to;

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

        from = argv[optind++];
        to = argv[optind++];

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init_simple(PROG);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_rename(from, to);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        exit(ret);
}

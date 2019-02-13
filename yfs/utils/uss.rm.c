

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <dirent.h>

#include "configure.h"
#include "ylib.h"
#include "sdfs_lib.h"

static void usage(const char *prog)
{
        fprintf(stderr, "usage: %s [-v] [-h] PATH\n"
                "\t-v --verbose         Show verbose message\n"
                "\t-h --help            Show this help\n",
                prog);
}

int main(int argc, char *argv[])
{
        int ret, verbose;
        char c_opt, *prog, *path;
        fileid_t parent;
        char name[MAX_NAME_LEN];

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        verbose = 0;

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
                        usage(prog);
                        exit(0);
                default:
                        usage(prog);
                        exit(1);
                }
        }

        if (optind >= argc) {
                usage(prog);
                exit(1);
        }

        path = argv[optind];

        if (verbose)
                printf("%s\n", path);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        dbg_info(0);

        ret = ly_init_simple("uss.rm");
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_splitpath(path, &parent, name);
        if (ret) {
                if (ret == ENOENT) {
                        printf("cannot remove %s, %s\n", path, strerror(ret));
                        goto err_ret;
                } else
                        GOTO(err_ret, ret);
        }

        ret = sdfs_unlink(NULL, &parent, name);
        if (ret) {
                if (ret == ENOENT) {
                        printf("cannot remove %s, %s\n", path, strerror(ret));
                        goto err_ret;
                } else if(ret == EISDIR) {
                        printf("cannot remove ‘%s/’: Is a directory\n", name);
                        goto err_ret;
                } else {
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

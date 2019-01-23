

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "ylib.h"
#include "sdfs_lib.h"

int main(int argc, char *argv[])
{
        int ret, args, verbose = 0;
        char c_opt, *prog, *path;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;

        if (argc < 2) {
                fprintf(stderr, "%s [-v] <dirpath>\n", prog);
                exit(1);
        }

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

        if (argc - args != 1) {
                fprintf(stderr, "%s [-v] <dirpath>\n", prog);
                exit(1);
        } else
                path = argv[args++];

        if (verbose)
                printf("%s %s\n", prog, path);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        dbg_info(0);
        
        ret = ly_init_simple("uss.rmdir");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_init()'ed\n");

        ret = ly_rmdir(path);
        if (ret) {
                fprintf(stderr, "ly_rmdir(%s) %s\n", path, strerror(ret));
                exit(1);
        } else if (verbose)
                printf("dir %s removed\n", path);

        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ly_destroy() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_destroy()'ed\n");

        return 0;
}

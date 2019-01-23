

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "sdfs_lib.h"

int main(int argc, char *argv[])
{
        int ret, args, verbose = 0;
        char c_opt, *prog, *user, *passwd;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;

        if (argc < 3) {
                fprintf(stderr, "%s [-v] USERNAME PASSWD\n", prog);
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

        user = argv[args++];
        passwd = argv[args++];

        if (verbose)
                printf("%s %s %s\n", prog, user, passwd);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("ylogin");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_init()'ed\n");

        ret = ly_login(user, passwd);
        if (ret) {
                fprintf(stderr, "Err: user(%s) passwd(%s) %s\n", user, passwd,
                        strerror(ret));
                exit(1);
        } else if (verbose)
                printf("user login ok\n");

        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ly_destroy() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_destroy()'ed\n");

        return 0;
}

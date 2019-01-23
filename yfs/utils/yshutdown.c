

#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include "configure.h"
#include "chk_proto.h"
#include "sdfs_lib.h"

void usage(const char *prog)
{
        printf("%s [-h] [-v] <-c|-m> <-s> [p]\n", prog);
}

int main(int argc, char *argv[])
{
        int ret, port = -1, verbose = 0;
        char c_opt, *prog, *server = NULL;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        while ((c_opt = getopt(argc, argv, "hvs:p:")) > 0)
                switch (c_opt) {
                case 'h':
                        usage(prog);
                        exit(1);
                        break;
                case 'v':
                        verbose = 1;
                        break;
                case 's':
                        server = optarg;
                        break;
                case 'p':
                        port = atoi(optarg);
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        exit(1);
                }

        if (server == NULL) {
                fprintf(stderr, "Err, shutdown which server ?\n");
                exit(1);
        } else if (port == -1) {
                fprintf(stderr, "Err, shutdown cds' which port ?\n");
                exit(1);
        }

        if (verbose) {
                printf("%s cds %s\n", prog, server);
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("yshutdown");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_init()'ed\n");

        /* query mds for port */

        /* send shutdown command to cds */
        ret = ly_shutdowncds(server, port);
        if (ret) {
                fprintf(stderr, "ly_shutdowncds %s(%d), %s\n", server, port,
                        strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_shutdowncds %s(%d) finished\n", server, port);

        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ly_destroy() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_destroy()'ed\n");

        return 0;
}

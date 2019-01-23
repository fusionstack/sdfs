

#include <unistd.h>
#include <sys/types.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "job_dock.h"

int main(int argc, char *argv[])
{
        int ret, args, verbose = 0;
        char c_opt, *prog, *path;
        uint64_t length;
        fileid_t fileid;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;

        if (argc < 3) {
                fprintf(stderr, "%s [-v] PATH LENGTH\n", prog);
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

        path = argv[args++];
        length = atoll(argv[args++]);

        DINFO("size %llu\n", (LLU)length);

        if (verbose)
                printf("%s %s %llu\n", prog, path, (unsigned long long)length);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("ytruncate");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_init()'ed\n");

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret) {
                fprintf(stderr, "lookup() %s\n", strerror(ret));
                exit(1);
        }

        ret = raw_dalloc(&fileid, length, NULL);
        if (ret) {
                fprintf(stderr, "dlloc() %s\n", strerror(ret));
                exit(1);
        }

#if 0
        ret = ly_truncate(path, length);
        if (ret) {
                fprintf(stderr, "Err: path(%s) length(%lld) %s\n", path,
                        (unsigned long long)length, strerror(ret));
                exit(1);
        } else if (verbose)
                printf("file truncate ok\n");
#endif
        return 0;
}

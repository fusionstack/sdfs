

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

#include "ylib.h"

int main(int argc, char *argv[])
{
        int cascade, rorate, path2id, args, verbose = 0, ret;
        char *prog, c_opt;
        uint64_t id;
        uint32_t version;
        char path[MAX_PATH_LEN];

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;

        if (argc < 3) {
                fprintf(stderr, "%s [-v] <-c|-r|-i> <number>\n", prog);
                EXIT(1);
        }

        cascade = 0;
        rorate = 0;
        path2id = 0;

        while ((c_opt = getopt(argc, argv, "vicr")) > 0)
                switch (c_opt) {
                case 'v':
                        verbose = 1;
                        args++;
                        break;
                case 'c':
                        cascade = 1;
                        args++;
                        break;
                case 'r':
                        rorate = 1;
                        args++;
                        break;
                case 'i':
                        path2id = 1;
                        args++;
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        EXIT(1);
                }

        if (verbose)
                printf("%s (%s) to path\n",
                       cascade ? "Cascade" : "Rorate", argv[args]);

        if (cascade == 1) {
                id = (uint64_t) strtoull(argv[args], NULL, 10);

                cascade_id2path(path, MAX_PATH_LEN, id);

                printf("id %llu, path %s\n", (unsigned long long)id, path);

        } else if (rorate == 1) {
                rorate_id2path(path, MAX_PATH_LEN, 5 /* MDS_NODEID_PATHLEVEL */,
                               argv[args]);

                printf("id %s, path %s\n", argv[args], path);

        } else if (path2id == 1) {
                ret = cascade_path2idver(argv[args], &id, &version);
                if (ret) {
                        fprintf(stderr, "Oops, Invalid input \n");
                        return 1;
                }
                printf("id %llu, version %u\n", (unsigned long long)id,
                       version);
        }

        return 0;
}

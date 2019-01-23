

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
        fprintf(stderr, "usage: %s [-v] [-h] [-s size] PATH\n"
                "\t-v --verbose         Show verbose message\n"
                "\t-h --help            Show this help\n"
                "\t-s --size=size       Set the lun's size when create a lun\n"
                "\t                     Valid unit: [b|B] [k|K] [m|M] [g|G]\n"
                "\t-o --solid           Create a solid file\n",
                prog);
}

static int get_size(char *str, uint64_t *_size)
{
        int ret;
        uint64_t size = 0;
        char unit;

        if (strlen(str) < 2) {
                ret = EINVAL;
                goto err_ret;
        }

        unit = str[strlen(str) - 1];
        str[strlen(str) - 1] = 0;

        size = atoll(str);

        switch (unit) {
        case 'b':
        case 'B':
                break;
        case 'k':
        case 'K':
                size *= 1024;
                break;
        case 'm':
        case 'M':
                size *= (1024 * 1024);
                break;
        case 'g':
        case 'G':
                size *= (1024 * 1024 * 1024);
                break;
        default:
                fprintf(stderr, "size unit must be specified, see help for detail\n");
                ret = EINVAL;
                goto err_ret;
        }

        *_size = size;

        return 0;
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret, verbose, solid;
        char c_opt, *prog, *path;
        fileid_t fid;
        uint64_t size;
        struct stat stbuf;

        dbg_info(0);

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        size = 0;
        solid = 0;
        verbose = 0;

        while (srv_running) {
                int option_index = 0;

                static struct option long_options[] = {
                        { "verbose", 0, 0, 'v' },
                        { "help",    0, 0, 'h' },
                        { "size",    1, 0, 's' },
                        { "solid",   0, 0, 'o' },
                        { 0, 0, 0, 0 },
                };

                c_opt = getopt_long(argc, argv, "vhos:", long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 'v':
                        verbose = 1;
                        break;
                case 'h':
                        usage(prog);
                        exit(0);
                case 'o':
                        solid = 1;
                        break;
                case 's':
                        ret = get_size(optarg, &size);
                        if (ret || !size)
                                GOTO(opt_err, ret);
                        break;
opt_err:
                default:
                        usage(prog);
                        exit(1);
                }
        }

        if (size == 0) {
                usage(prog);
                exit(1);
        }

        if (optind >= argc) {
                usage(prog);
                exit(1);
        }

        path = argv[optind];

        if (verbose)
                printf("%s, %llu\n", path, (LLU)size);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init_simple(prog);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_lookup_recurive(path, &fid);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_getattr(&fid, &stbuf);
        if (ret)
                GOTO(err_ret, ret);

        if (!S_ISREG(stbuf.st_mode)) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        ret = sdfs_truncate(&fid, size);
        if (ret)
                GOTO(err_ret, ret);

        if (solid) {
                uint32_t cnt, split = 1024000; /* 1M */
                uint64_t off = 0;
                buffer_t buf;


                while (off < size) {
                        mbuffer_init(&buf, 0);
                        mbuffer_appendzero(&buf, split);

                        cnt = sdfs_write_sync(&fid, &buf, split, off);
                        if (cnt != split) {
                                /*
                                 * Only report a warning here.
                                 */
                                fprintf(stderr, "pwrite error: (%u, %u)\n",
                                        cnt, split);
                        }

                        off += cnt;
                        mbuffer_free(&buf);
                }
        }

        return 0;
err_ret:
        return ret;
}

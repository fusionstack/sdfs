

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <dirent.h>

#include "configure.h"
#include "ylib.h"
#include "sdfs_lib.h"
//#include "leveldb_util.h"

static void usage(const char *prog)
{
        fprintf(stderr, "usage: %s [-v] [-h] PATH\n"
                "\t-v --verbose         Show verbose message\n"
                "\t-h --help            Show this help\n",
                prog);
}

int main(int argc, char *argv[])
{
        int ret, verbose, retry;
        char c_opt, *prog, *path;
        struct stat stbuf;
        uint64_t left, offset, size;
        char buf[MAX_BUF_LEN+1];

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
        
        ret = ly_init_simple(prog);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_getattr(path, &stbuf);
        if (ret)
                GOTO(err_ret, ret);

        offset = 0;
        left = stbuf.st_size;
        while (left > 0) {
                size = left < MAX_BUF_LEN ? left : MAX_BUF_LEN;
                retry = 0;
        retry2:
                ret = ly_read(path, buf, size, offset);
                if (ret < 0) {
                        ret = -ret;
                        if (NEED_EAGAIN(ret)) {
                                SLEEP_RETRY(err_ret, ret, retry2, retry);
                        } else
                                GOTO(err_ret, ret);
                }
                
                YASSERT((uint64_t)ret == size);
                buf[size] = '\0';
                printf("%s", buf);
                fflush(stdout);

                offset += size;
                left -= size;
        }

        return 0;
err_ret:
        return ret;
}

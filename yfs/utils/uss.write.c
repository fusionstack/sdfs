

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "ylib.h"
#include "schedule.h"
#include "sdfs_lib.h"

int main(int argc, char *argv[])
{
        int ret, args, verbose = 0, retry;
        char c_opt, *prog;
        const char *from = NULL, *to = NULL;

        (void) verbose;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;

        if (argc < 2) {
                fprintf(stderr, "%s [-v] <file ctx> <file>\n", prog);
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

        if (argc - args != 2) {
                fprintf(stderr, "%s [-v] <from> <to>\n", prog);
                exit(1);
        } else {
                from = argv[args++];
                to = argv[args++];
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        dbg_info(0);
        
        ret = ly_init_simple("ywrite");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        }

        DINFO("write %s to %s, len: %d\n", from, to, (int)strlen(from)+1);
        
        retry = 0;
retry:
        ret = ly_write(to, from, strlen(from)+1, 0);
        if (ret < 0) {
                ret = -ret;
                if (NEED_EAGAIN(ret)) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 30, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        fprintf(stderr, "write fail, %s \n", strerror(ret));
        return ret;
}

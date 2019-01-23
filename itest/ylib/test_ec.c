

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "sdfs_ec.h"

int test_ec()
{
        int ret, i, j, m, k;
        void *buf;
        char *buffs[EC_MMAX];
        unsigned char *tmp[EC_MMAX];
        unsigned char src_in_err[EC_MMAX];

        m = 5;
        k = 3;

        memset(src_in_err, 0, sizeof(unsigned char)*EC_MMAX);

        for (i = 0; i < EC_MMAX; i++) {
                if (posix_memalign(&buf, STRIP_ALIGN, STRIP_BLOCK)) {
                        printf("alloc error: Fail");
                        return -1;
                }
                tmp[i] = buf;
        }

        for (i = 0; i < EC_MMAX; i++) {
                if (posix_memalign(&buf, STRIP_ALIGN, STRIP_BLOCK)) {
                        printf("alloc error: Fail");
                        return -1;
                }
                buffs[i] = buf;
        }

        //random data
        for (i = 0; i < k; i++) {
                for (j = 0; j < STRIP_BLOCK; j++) {
                        buffs[0][j] = rand();          
                }
        }

        ret = ec_encode(buffs, &buffs[k], STRIP_BLOCK, m, k);
        if (ret) {
                GOTO(err_ret, ret);
        }

        for (i = 0; i < m; i++) {
                memcpy(tmp[i], buffs[i], STRIP_BLOCK);
        }

        for (i = 0; i < m; i++) {
                src_in_err[i] = 0;
        }

        for (i = 0; i < (m-k); i++) {
                DINFO("set %d error\n", k+i);
                src_in_err[k+i] = 1;
        }

        for (i = 0; i < m; i++) {
                if (src_in_err[i]) {
                        DINFO("erase data: %d\n", i);
                        for (j = 0; j < STRIP_BLOCK; j++) {
                                buffs[i][j] = rand();
                        }
                }
        }

        printf("sizeof char * %d, u char * %d\n", (int)sizeof(char *), (int)sizeof(unsigned char *));
#if 1
        ret = ec_decode(src_in_err, buffs, &buffs[k], STRIP_BLOCK, m, k);
        if (ret)
                GOTO(err_ret, ret);
#endif

        for (i = 0; i < m; i++) {
                if (memcmp(buffs[i], tmp[i], STRIP_BLOCK) != 0) {
                        printf("cmp %d error\n", i);
                        ret = 1;
                        GOTO(err_ret, ret);
                }
        }

        printf("ok\n");

        EXIT(1);

        return 0;
err_ret:
        EXIT(1);
        return ret;
}

int main(int argc, char *argv[])
{
        int ret, args, verbose = 0;
        char c_opt, *prog;
        const char *from = NULL, *to = NULL;

        (void) verbose;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];


        args = 1;
        /*dbg_info(0);*/

        if (argc < 2) {
                fprintf(stderr, "%s [-v] <file ctx> <file>\n", prog);
                EXIT(1);
        }

        while ((c_opt = getopt(argc, argv, "v")) > 0)
                switch (c_opt) {
                        case 'v':
                                verbose = 1;
                                args++;
                                break;
                        default:
                                fprintf(stderr, "Hoops, wrong op got!\n");
                                EXIT(1);
                }

        if (argc - args != 2) {
                fprintf(stderr, "%s [-v] <from> <to>\n", prog);
                EXIT(1);
        } else {
                from = argv[args++];
                to = argv[args++];
        }

        printf("write %s to %s, len: %d\n", from, to, (int)strlen(from)+1);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                EXIT(1);

        ret = ly_init_simple("ywrite");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                EXIT(1);
        }

        test_ec();

        return 0;
}

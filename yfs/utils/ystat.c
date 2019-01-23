

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "file_proto.h"

void show_help(char *prog) 
{
        fprintf(stderr, "%s [-v] path\n", prog);
        fprintf(stderr, "%s (id)_v(version)\n", prog);
}

int main(int argc, char *argv[])
{
        int ret, args, verbose __attribute__((unused)) = 0;
        fileid_t fileid;
        char c_opt, *prog, *arg;
        struct stat stbuf;

        dbg_info(0);

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;

        if (argc < 2) {
                show_help(prog);
                exit(1);
        }

        while ((c_opt = getopt(argc, argv, "vf")) > 0)
                switch (c_opt) {
                case 'v':
                        verbose = 1;
                        args++;
                        break;
                default:
                        show_help(prog);
                        exit(1);
                }

        arg = argv[1];

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret) {
                fprintf(stderr, "conf_init() %s\n", strerror(ret));
                exit(1);
        }

        ret = ly_init_simple("ystat");
        if (ret)
                GOTO(err_ret, ret);

        if (arg[0] == '/') {
                ret = sdfs_lookup_recurive(arg, &fileid);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
#ifdef __x86_64__
                ret = sscanf(arg, "%lu_v%u", &fileid.id, &fileid.version);
#else
                ret = sscanf(arg, "%llu_v%u", &fileid.id, &fileid.version);
#endif

                if (ret != 2) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }

        ret = sdfs_getattr(&fileid, &stbuf);
        if (ret)
                GOTO(err_ret, ret);

        printf("%s mode %s/%s %o, %llu link:%lu\n", arg,
               S_ISDIR((stbuf).st_mode) ? "d" : "",
               S_ISREG((stbuf).st_mode) ? "f" : "", stbuf.st_mode & 00777,
               (unsigned long long)(stbuf).st_size,
               (unsigned long)stbuf.st_nlink);

        if (S_ISDIR((stbuf).st_mode)) {
                printf("    fileid %llu_v%u\n", (LLU)fileid.id, fileid.version);
                goto out;       /* not file */
        }

        ret = raw_printfile(&fileid, -1);
        if (ret)
                GOTO(err_ret, ret);

out:
        return 0;
err_ret:
        return ret;
}

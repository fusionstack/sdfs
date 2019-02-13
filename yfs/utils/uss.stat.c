

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "file_proto.h"
#include "md_lib.h"

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
        fileinfo_t *md;
        char buf[MAX_BUF_LEN], ec[MAX_NAME_LEN];
        uint64_t size = 0;

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

        arg = argv[args];
        if(arg == NULL) {
                printf("%s: missing path argument\n", prog);
                show_help(prog);
                exit(1);
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret) {
                fprintf(stderr, "conf_init() %s\n", strerror(ret));
                exit(1);
        }

        ret = ly_init_simple("uss.stat");
        if (ret)
                GOTO(err_ret, ret);

        if (arg[0] == '/') {
                ret = sdfs_lookup_recurive(arg, &fileid);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
#ifdef __x86_64__
                ret = sscanf(arg, "%lu_v%lu", &fileid.id, &fileid.volid);
#else
                ret = sscanf(arg, "%llu_v%llu", &fileid.id, &fileid.volid);
#endif

                if (ret != 2) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }

        DBUG(""FID_FORMAT" name %s\n", FID_ARG(&fileid), arg);

        fileid.idx = 0;
        md = (void *)buf;
        ret = md_getattr(&fileid, (void *)md);
        if (ret)
                GOTO(err_ret, ret);

        MD2STAT(md, &stbuf);

        if (S_ISREG((stbuf).st_mode)) {
                ret = sdfs_getattr(NULL, &fileid, &stbuf);
                if (ret) {
                        DERROR("getattr ret: %d\n", ret);
                        size = 0;
                } else {
                        size = stbuf.st_size;
                }
        }

        if (md->plugin == PLUGIN_NULL) {
                strcpy(ec, "replica");
        } else {
                snprintf(ec, MAX_NAME_LEN, "%d+%d", md->k, md->m-md->k);
        }

        printf("path: %s; fileid: %llu_v%llu; mode: %s/%s %o; size:%llu; link:%lu; layout: %s\n",
               arg, (LLU)fileid.id, (LLU)fileid.volid,
               S_ISDIR((stbuf).st_mode) ? "d" : "",
               S_ISREG((stbuf).st_mode) ? "f" : "", stbuf.st_mode & 00777,
               (LLU)size, (unsigned long)stbuf.st_nlink, ec);

        if (S_ISDIR((stbuf).st_mode)) {
                goto out;       /* not file */
        }

        if (verbose) {
                printf("==================\n");
                ret = raw_printfile(&fileid, -1);
                if (ret)
                        GOTO(err_ret, ret);
        }

out:
        return 0;
err_ret:
        return ret;
}



#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include <stdio.h>
#include <dirent.h>
#include <errno.h>
#include <sys/mman.h>

#define DBG_SUBSYS S_LIBYLIB

#include "yfs_conf.h"
#include "disk_proto.h"
#include "configure.h"
#include "sdfs_lib.h"
#include "cd_proto.h"
#include "md_proto.h"
#include "configure.h"
#include "c60_lib.h"
#include "dbg.h"

#define DBG_SYNC_MDS_JNL_ON 0

void usage(const char *prog)
{
        printf("*** Write Mds Journal to C60 ***\n"
               "Usage:\n"
               "\t%s [-h]\n"
               "\t%s -f <jfile>\n"
               "\t%s -d <jdirectory>\n"
               , prog, prog, prog);

        exit(-1);
}

#ifdef __x86_64__
#if 0
static inline int __digitsort(const void *a, const void *b)
{
        return (atol((*(struct dirent **)a)->d_name) - atol((*(struct dirent **)b)->d_name));
}
#endif
static inline int __digitsort(const struct dirent **a, const struct dirent **b)
{
        return (atol((*a)->d_name) - atol((*b)->d_name));
}
#else
static inline int __digitsort(const struct dirent **a, const struct dirent **b)
{
        return (atol((*a)->d_name) - atol((*b)->d_name));
}
#endif

static inline int __filter(const struct dirent *dp)
{
        int ret;
        unsigned int idx;

        ret = 1;

        for (idx = 0; idx < strlen(dp->d_name); ++idx) {
                if (dp->d_name[idx] < '0' || dp->d_name[idx] > '9') {
                        ret = 0;
                        break;
                }
        }

        return ret;
}


int sync_jnl(const char *path, int isdir)
{
        int ret, idx, total;
        struct dirent **namelist;
        uint64_t lrn_count, lrn_total;

        /* path is a jnl file */
        if (!isdir) {
                ret = c60_jnlsyncfile(path, &lrn_count);
                if (ret)
                        GOTO(err_ret, ret);
                goto out_ret;
        }

        /* path is a jnl dir */
        fprintf(stdout, "Scaning dir: %s\n========================================\n", path);

        total = scandir(path, &namelist, __filter, __digitsort);
        if (total == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = chdir(path);
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        lrn_total = 0;
        for (idx = 0; idx < total - 1; ++idx) {
                ret = c60_jnlsyncfile(namelist[idx]->d_name, &lrn_count);
                if (ret)
                        GOTO(err_ret, ret);

                lrn_total += lrn_count;
        }

        if (lrn_total > 0) {
                uint32_t flags = C60_FLAG_OVERWRITE | C60_FLAG_PERSISTENT;
                ret = c60_set(KEY_JFILE_LRN, &lrn_total, sizeof(uint64_t), flags);
                if (ret)
                        GOTO(err_ret, ret);
        }

out_ret:
        return 0;
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret;
        char c_opt, *prog, *path;
        int isdir;

        dbg_info(0);

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        path = NULL;

        ret = conf_init(YFS_CONFIGURE_FILE);
        if(ret)
                exit(1);

        ret = ly_init_simple("ysync_mds_jnl");
        if (ret)
                GOTO(err_ret, ret);

        ret = c60_init(NULL, NULL);
        if (ret)
                GOTO(err_ret, ret);

        while ((c_opt = getopt(argc, argv, "d:f:h")) != -1) {
                switch (c_opt) {
                        case 'd':
                                path = optarg;
                                isdir = YLIB_ISDIR;

                                break;
                        case 'f':
                                path = optarg;
                                isdir = YLIB_NOTDIR;

                                break;
                        case 'h':
                                usage(prog);

                                break;
                        default:
                                usage(prog);
                }
        }

        if (!path)
                usage(prog);

        ret = sync_jnl(path, isdir);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <getopt.h>
#include <stdio.h>

#include "ylib.h"
#include "sdfs_id.h"
#include "configure.h"
#include "sdfs_lib.h"
#include "sdfs_worm.h"

#define INVALID_MOD (mode_t)-1

static void usage(IN const char *prog)
{
        fprintf(stderr, "usage: %s [OPTION] PATH\n"
                "\t-m, --mode mod     new mode for the file, the mode is octal number in(000~777)\n"
                "\t-h, --help         help information\n",
                prog);
}

static inline void chmod_usage_error(void)
{
        fprintf(stderr, "uss.chmod: invalid argument.\n"
                        "Try 'uss.chmod --help' for more information.\n");
}

static int mode_check(IN const char *mod_str)
{
        int ret = ERROR_FAILED;
        int i;
        int len;

        len = strlen(mod_str);
        if (len > 3)
                goto err_ret;

        for (i = 0; i < len; i++) {
                if ((mod_str[i] < '0') || (mod_str[i] > '7'))
                        break;
        }

        if (i < len)
                goto err_ret;

        return ERROR_SUCCESS;

err_ret:
        return ret;
}

static struct option chmod_options[] = {
        {"mode",  required_argument, 0, 'm'},
        {"help",  no_argument,       0, 'h'},
        {0,       0,                 0,  0 },
};

int main(int argc, char *argv[])
{
        int ret;
        char c_opt, *prog, *path;
        char normal_path[MAX_PATH_LEN];
        mode_t mod = INVALID_MOD;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

	dbg_info(0);

        while (1) {
                int option_index = 0;

                c_opt = getopt_long(argc, argv, "m:h", chmod_options, &option_index);
                if (c_opt == -1)
                        break;
                switch (c_opt) {
                case 'm':
                        if (optarg) {
                                if (ERROR_SUCCESS != mode_check(optarg)) {
                                        fprintf(stderr, "mode %s is error.\n", optarg);
                                        exit(1);
                                }
                                mod = (mode_t)strtol(optarg, NULL, 8);
                        }
                        break;
                case 'h':
                        usage(prog);
                        exit(0);
                default:
                        chmod_usage_error();
                        exit(1);
                }
        }
        if (optind >= argc) {
                chmod_usage_error();
                exit(1);
        }

        if (INVALID_MOD == mod)
                exit(0);

        path = argv[optind];
        YASSERT(path[0] == '/');
        ret = path_normalize(path, normal_path, sizeof(normal_path));
        if (ret)
                exit(1);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("uss.chmod");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        }

        ret = ly_chmod(normal_path, mod);
        if (ret) {
                fprintf(stderr, "ly_chmod(%s,...) %s\n", path, strerror(ret));
                exit(1);
        }

        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ly_destroy() %s\n", strerror(ret));
                exit(1);
        }

        return 0;
}

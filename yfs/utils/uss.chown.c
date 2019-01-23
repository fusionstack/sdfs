/*Date   : 2017.07.25
*Author : Yang
*uss.worm : the command for chown
*/
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <dirent.h>
#include <ctype.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>

#include "configure.h"
#include "ylib.h"
#include "ynet.h"
#include "sdfs_lib.h"
#include "sdfs_id.h"
#include "sdfs_worm.h"


static void usage(IN const char *prog)
{
        fprintf(stderr, "usage: %s [OPTION] PATH\n"
                "\t-u, --user USER    user name\n"
                "\t-g, --group GROUP  group name\n"
                "\t-h, --help         help information\n",
                prog);
}

static inline void chown_usage_error(void)
{
        fprintf(stderr, "uss.chown: invalid argument.\n"
                        "Try 'uss.chown --help' for more information.\n");
}

static struct option chown_options[] = {
        {"user",  required_argument, 0, 'u'},
        {"group", required_argument, 0, 'g'},
        {"help",  no_argument,       0, 'h'},
        {0,       0,                 0,  0 },
};


int main(int argc, char *argv[])
{
        int ret;
        char c_opt, *prog, *path;
        char normal_path[MAX_PATH_LEN];
        uid_t uid = INVALID_UID;
        gid_t gid = INVALID_GID;
        struct passwd *user_info = NULL;
        struct group *group_info = NULL;

        dbg_info(0);

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        while (1) {
                int option_index = 0;

                c_opt = getopt_long(argc, argv, "u:g:h", chown_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 'u':
                        if (optarg) {
                                user_info = getpwnam(optarg);
                                if (NULL == user_info) {
                                        fprintf(stderr, "user %s is not exist.\n", optarg);
                                        exit(1);
                                }
                                uid = user_info->pw_uid;
                        }
                        break;
                case 'g':
                        if (optarg) {
                                group_info = getgrnam(optarg);
                                if (NULL == group_info) {
                                        fprintf(stderr, "group %s is not exist.\n", optarg);
                                        exit(1);
                                }
                                gid = group_info->gr_gid;
                        }
                        break;
                case 'h':
                        usage(prog);
                        exit(0);
                default:
                        chown_usage_error();
                        exit(1);
                }
        }

        if ((INVALID_UID == uid) && (INVALID_GID == gid)) {
                chown_usage_error();
                exit(1);
        }

        if (optind >= argc) {
                chown_usage_error();
                exit(1);
        }

        path = argv[optind];
        YASSERT(path[0] == '/');
        ret = path_normalize(path, normal_path, sizeof(normal_path));
        if (ret)
                GOTO(err_ret, ret);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init_simple("uss.chown");
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_chown(normal_path, uid, gid);
        if (ret)
                GOTO(err_ret, ret);

        return 0;

err_ret:
        return ret;
}


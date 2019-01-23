

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "yfs_conf.h"
#include "disk_proto.h"
#include "configure.h"
#include "sdfs_lib.h"
#include "cd_proto.h"
#include "md_proto.h"
#include "configure.h"
#include "dbg.h"

void usage(const char *prog)
{
        printf("*** Logical volume management\n"
               "Usage:\n"
               "\t%s [-h]\n"
               "\t%s --list\n"
               "\t%s --create <name> <size>\n"
               "\t%s --resize <name> <size>\n"
               , prog, prog, prog, prog);
}

uint64_t get_size(const char *arg)
{
        char _size[MAX_NAME_LEN], unit;
        uint64_t size;

        strcpy(_size, arg);
        unit = _size[strlen(_size) - 1];
        _size[strlen(_size) - 1] = '\0';
        size = atol(_size);

        if (size < 10) {
                fprintf(stderr, "error:min size is 10G\n");
                exit(100);
        }

        if (unit == 'G') {
                size *= (1024 * 1024 * 1024);
        } else {
                fprintf(stderr, "error:unit is G\n");
                exit(100);
        }

        return size;
}

int main(int argc, char *argv[])
{
        int ret, cmd = 0;
        uint64_t size;
        char c_opt, *prog, *name;
#if 0
        char buf[MAX_BUF_LEN];
        uint32_t buflen;
#endif

        dbg_info(0);

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];


        while (srv_running) {
                int option_index = 0;

                static struct option long_options[] = {
                        {"create", 0, 0, 0},
                        {"list", 0, 0, 0},
                        {"resize", 0, 0, 0},
                };
                
                c_opt = getopt_long(argc, argv, "h",
                        long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 0:
                        switch (option_index) {
                        case 0:
                                cmd = 1;
                                break;
                        case 1:
                                cmd = 2;
                                break;
                        case 2:
                                cmd = 3;
                                break;
                        default:
                                fprintf(stderr, "Hoops, wrong op got!\n");
                        }

                        break;
                case 'h':
                        usage(prog);
                        exit(1);
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op (%c) got!\n", c_opt);
                        exit(1);
                }
        }

#if 0
        /* subcommand */
        if (strcmp(argv[optind], "resize") == 0) {
                cmd = 1;
        } else if (strcmp(argv[optind], "list") == 0) {
                cmd = 2;
        } else if (strcmp(argv[optind], "rename") == 0) {
                cmd = 3;
        } else {
                fprintf(stderr, "ERROR: No such subcommand (%s).\n", argv[optind]);
                usage(prog);
                exit(1);
        }
#endif

        ret = conf_init(YFS_CONFIGURE_FILE);
        if(ret)
                exit(1);

        /* exec */
        ret = ly_init_simple("ylvm");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        }

        switch (cmd) {
        case 1:
                name = argv[optind];
                if (strstr(name, "/") != NULL) {
                        ret = EINVAL;
                        fprintf(stderr,"name %s \n",name);
                        GOTO(err_ret, ret);
                }

                if (argc == 3)
                        size = 0;
                else
                        size = get_size(argv[optind + 1]);

                if (strlen(argv[optind]) > MAX_LVNAME) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                ret = ly_lvcreate(argv[optind], size);
                if (ret)
                        GOTO(err_ret, ret);
                break;
        case 2:
                ret = ly_lvlist();
                if (ret)
                        GOTO(err_ret, ret);
                break;
        case 3:
                name = argv[optind];
                if (strstr(name, "/") != NULL) {
                        fprintf(stderr,"name %s \n",name);
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }

                if (argc == 3)
                        size = 0;
                else
                        size = get_size(argv[optind + 1]);

                ret = ly_lvset(argv[optind], size);
                if (ret)
                        GOTO(err_ret, ret);
                break;
        default:
                fprintf(stderr, "ERROR: No such subcommand (%s).\n", argv[optind]);
                usage(prog);
                break;
        }

        return 0;
err_ret:
        return ret;
}



#include <sys/types.h>
#include <sys/wait.h>
#include <getopt.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSMDS

#include "configure.h"
#include "net_global.h"
#include "job_dock.h"
#include "get_version.h"
#include "shadow.h"
#include "ylib.h"
#include "yfsmds_conf.h"
#include "sdfs_lib.h"
#include "ylog.h"
#include "mds.h"
#include "dbg.h"
#include "fnotify.h"
#include "sdfs_quota.h"
#include "flock.h"
#include "mds_main.h"

static inline void usage()
{
        printf("yfs_mds [-cfxr] [-n no] [-h pasv_addr] [-m netmask]\n");
        printf("\t-c core file.\n");
        printf("\t-f foreground.\n");
        printf("\t-x show this help.\n");
        printf("\t-r reload MDS journal from C60.\n");
        printf("\t-v get version info.\n");
        printf("\t-n no [1|2|3|...].\n");
}

int main(int argc, char *argv[])
{
        int ret, metano, daemon = 1, maxcore = 0;
        const char *prog;
        char c_opt;
        election_type_t type;
        char name[MAX_NAME_LEN], home[MAX_PATH_LEN];
        mds_args_t mds_args;

        (void) maxcore;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        metano = -1;

        type = ELECTION_NORMAL;

        while (srv_running) {//(c_opt = getopt(argc, argv, "cfrn:t:h:m:v")) > 0)
                int option_index = 0;

                static struct option long_options[] = {
                        {"init", 0, 0, 0},
                        {"sync", 0, 0, 0},
                };

                c_opt = getopt_long(argc, argv, "cfrn:t:h:m:v",
                        long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 0:
                        switch (option_index) {
                        case 0:
                                DINFO("init mds\n");

                                type = ELECTION_INIT;
                                daemon = 2;
                                break;
                        case 1:
                                DINFO("sync mds\n");

                                type = ELECTION_SYNC;
                                break;
                        default:
                                fprintf(stderr, "Hoops, wrong op got!\n");
                        }

                        break;
                case 'c':
                        maxcore = 1;
                        break;
                case 'f':
                        daemon = 2;
                        break;
                case 'n':
                        metano = atoi(optarg);
                        break;
                case 'v':
                        get_version();
                        EXIT(0);
                case 'x':
                        usage();
                        EXIT(0);
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        usage();
                        EXIT(1);
                }
        }

        if (metano == -1) {
                DERROR("Should be : %s <-n metano> ! Please Retry\n", prog);
                usage();
                return 1;
        }

        mds_info.metano = metano;

        snprintf(name, MAX_NAME_LEN, "mond/%d", metano);
        ret = ly_prep(daemon, name, -1);
        if (ret)
                GOTO(err_ret, ret);

        snprintf(home, MAX_PATH_LEN, "%s/%s", gloconf.workdir, name);
        mds_args.daemon = daemon;
        mds_args.metano = metano;
        mds_args.type = type;

        signal(SIGUSR1, mds_monitor_handler);
        signal(SIGUSR2, mds_monitor_handler);
        signal(SIGTERM, mds_monitor_handler);
        signal(SIGHUP, mds_monitor_handler);
        signal(SIGKILL, mds_monitor_handler);
        signal(SIGINT, mds_monitor_handler);

        if (daemon == 2) {
                ret = mds_run((void *)&mds_args);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                ret = ly_run(home, mds_run, (void *)&mds_args);
                if (ret) {
                        if (srv_running) {
                                DWARN("mds exit\n");
                                GOTO(err_ret, ret);
                        } else
                                goto err_ret;
                }
        }

        return 0;
err_ret:
        return ret;
}

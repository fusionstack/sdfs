

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <sys/vfs.h>


#define DBG_SUBSYS S_YFSCDS

#include "ylib.h"
#include "get_version.h"
#include "yfscds_conf.h"
#include "configure.h"
#include "ylog.h"
#include "dbg.h"
#include "../../ynet/sock/sock_buffer.h"
#include "cds.h"
#include "sdfs_lib.h"

int main(int argc, char *argv[])
{
        int ret, diskno, daemon = 1, maxcore __attribute__((unused)) = 0, c_opt;
        const char *prog;
        char home[MAX_PATH_LEN], name[MAX_NAME_LEN];
        int64_t max_object;
        cds_args_t cds_args;
        //char dpath[MAX_PATH_LEN];
        struct stat stbuf;
        struct statvfs fsbuf;
        char dpath[MAX_PATH_LEN], path[MAX_PATH_LEN];

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        diskno = -1;

        while ((c_opt = getopt(argc, argv, "cfn:h:m:v")) > 0)
                switch (c_opt) {
                case 'c':
                        maxcore = 1;
                        break;
                case 'f':
                        daemon = 2;
                        break;
                case 'n':
                        diskno = atoi(optarg);
                        break;
                case 'v':
                        get_version();
                        EXIT(0);
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        EXIT(1);
                }

        if (diskno == -1) {
                DERROR("Should be : %s <-n diskno> ! Please Retry\n", prog);
                return 1;
        }

        //DBUG("daemon %d, diskno %d, maxcore %d\n", daemon, diskno, maxcore);

        //snprintf(logpath, MAX_PATH_LEN, YFS_CDS_LOGFILE, diskno);


        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

#if 0
        (void) ylog_init(daemon ? YLOG_FILE : YLOG_STDERR, logpath);

        (void) daemonlize(daemon, maxcore, NULL, ylog ? ylog->logfd: -1);
        snprintf(lockname, MAX_PATH_LEN, "%s_%d", prog, diskno);

        server_run(daemon, lockname, diskno ,cds_run, &diskno);
#endif
        
        sprintf(name, "cds/%d", diskno);
        snprintf(home, MAX_PATH_LEN, "%s/%s", gloconf.workdir, name);

        ret = path_validate(home, YLIB_ISDIR, YLIB_DIRCREATE);
        if (ret)
                GOTO(err_ret, ret);
        
        sprintf(dpath, "%s/skip", home);
        ret = stat(dpath, &stbuf);
        if (ret == 0) {
                DWARN("cds exit by %s\n", dpath);
                EXIT(100);
        }

        ret = statvfs(home, &fsbuf);
        if (ret == -1) {
                ret = errno;
                DERROR("statvfs(%s, ...) ret (%d) %s\n", path,
                       ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        max_object = (LLU)fsbuf.f_blocks * fsbuf.f_bsize / YFS_CHK_LEN_DEF;
        ret = ly_prep(daemon, name, max_object * 2 + MAX_OPEN_FILE);
        if (ret)
                GOTO(err_ret, ret);

        cds_args.daemon = daemon;
        cds_args.diskno = diskno;

        signal(SIGUSR1, cds_monitor_handler);
        signal(SIGUSR2, cds_monitor_handler);
        signal(SIGTERM, cds_monitor_handler);
        signal(SIGHUP, cds_monitor_handler);
        signal(SIGKILL, cds_monitor_handler);
        signal(SIGINT, cds_monitor_handler);

        ret = ly_run(home, cds_run, &cds_args);
        if (ret)
                GOTO(err_ret, ret);

        (void) ylog_destroy();

        return 0;
err_ret:
        return ret;
}



#include <sys/types.h>
#include <sys/wait.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <getopt.h>
#include <semaphore.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#define DBG_SUBSYS S_YFTP

#include "get_version.h"
#include "md_lib.h"
#include "prelogin.h"
#include "session.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "yftp_conf.h"
#include "ylog.h"
#include "ynet_rpc.h"
#include "configure.h"
#include "io_analysis.h"
#include "network.h"
#include "job_dock.h"
#include "proc.h"
#include "dbg.h"
#include "sdfs_quota.h"

sem_t clisd_sem;
static int ftp_srv_running = 0;

typedef struct {
        int daemon;
        char service[MAX_INFO_LEN];
        char home[MAX_PATH_LEN];
}yftp_args_t;

static void signal_handler(int sig)
{
        (void) sig;
        //DINFO("got signal %d, load %llu\n", sig, (LLU)jobdock_load());

        if (ftp_srv_running == 0)
                return;
        
        //inode_proto_dump();
        jobdock_iterator();
        netable_iterate();
        analysis_dumpall();
}

void ftp_exit_handler(int sig)
{
        DWARN("got signal %d, exiting\n", sig);

        ftp_srv_running = 0;
}


void *ftp_handler(void *arg)
{
        int ret, clisd;
        struct yftp_session ys;

        clisd = *(int *)arg;
        sem_post(&clisd_sem);

        ret = session_init(&ys, clisd);
        if (ret)
                GOTO(err_ret, ret);

        ret = emit_greeting(&ys);
        if (ret)
                GOTO(err_session, ret);

        (void) ftp_handle(&ys);

        YASSERT(0 == "should not get here:(\n");

        return NULL;
err_session:
        (void) session_destroy(&ys);
err_ret:
        return NULL;
}

static void *__ftp_run(void *args)
{
        int ret, yftp_sd, epoll_fd = -1, cli_sd, nfds, i;
        struct epoll_event ev, *events;
        uint32_t len;
        void *ptr;
        pthread_t th;
        pthread_attr_t ta;
        yftp_args_t *yftp_args;
        char service[MAX_INFO_LEN];

        yftp_args = (yftp_args_t *)args;
        memset(service, 0, MAX_INFO_LEN);
        strcpy(service, yftp_args->service);

        (void) sem_init(&clisd_sem, 0, 0);
        
        epoll_fd = epoll_create(YFTP_EPOLL_SIZE);
        if (epoll_fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = rpc_hostlisten(&yftp_sd, NULL, service, YFTP_QLEN_DEF, YNET_RPC_BLOCK);
        if (ret)
                GOTO(err_fd, ret);

        _memset(&ev, 0x0, sizeof(struct epoll_event));
        ev.events = Y_EPOLL_EVENTS;
        ev.data.fd = yftp_sd;

        ret = _epoll_ctl(epoll_fd, EPOLL_CTL_ADD, yftp_sd, &ev);
        if (ret == -1) {
                ret = errno;
                GOTO(err_sd, ret);
        }

        len = sizeof(struct epoll_event) * YFTP_EPOLL_SIZE;

        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_sd, ret);

        events = (struct epoll_event *)ptr;

        while (ftp_srv_running) {
                nfds = _epoll_wait(epoll_fd, events, YFTP_EPOLL_SIZE, 1 * 1000);
                if (nfds == -1) {
                        ret = errno;
                        if (ret == EINTR) {
                                DBUG("EINTR from epoll\n");
                                continue;
                        } else {
                                GOTO(err_ptr, ret);
                        }
                }

                DBUG("got %d events\n", nfds);

                for (i = 0; i < nfds; i++) {
                        if (events[i].data.fd == yftp_sd) {
                                /*
                                ret = rpc_accept(&cli_sd, yftp_sd, 1, YNET_RPC_BLOCK);
                                if (ret) {
                                        DERROR("ret (%d) %s\n", ret,
                                               strerror(ret));
                                        continue;
                                }
                                */

                                while ((ret = rpc_accept(&cli_sd, yftp_sd, 1, YNET_RPC_BLOCK)) == 0) {

                                        if (yftp_session_running > YFTP_THR_MAX) {
                                                ret = sy_close(cli_sd);
                                                if (ret)
                                                        DERROR("ret (%d) %s\n", ret,
                                                               strerror(ret));

                                                DBUG("too many connects %ld\n",
                                                     yftp_session_running);
                                                break;
                                        }

                                        ret = pthread_create(&th, &ta, ftp_handler,
                                                             (void *)&cli_sd);
                                        if (ret) {
                                                DERROR("ret (%d) %s\n", ret,
                                                       strerror(ret));

                                                ret = sy_close(cli_sd);
                                                if (ret)
                                                        DERROR("ret (%d) %s\n", ret,
                                                               strerror(ret));
                                                break;
                                        }

                                        _sem_wait(&clisd_sem);
                                        yftp_session_running++;

                                }

                                if(ret == -1) {
                                        if(errno != EAGAIN && errno != ECONNABORTED
                                                        && errno != EPROTO && errno != EINTR) {
                                                DERROR("ret (%d) %s\n", ret, strerror(ret));
                                        }
                                }

                                continue;

                        } else
                                DWARN("fd %d\n", events[i].data.fd);
                }
        }

        (void) sem_destroy(&clisd_sem);

        yfree((void **)&ptr);

        (void) sy_close(yftp_sd);

        (void) sy_close(epoll_fd);
        
        return NULL;
err_ptr:
        yfree((void **)&ptr);
err_sd:
        (void) sy_close(yftp_sd);
err_fd:
        (void) sy_close(epoll_fd);
err_ret:
        return NULL;
}

int ftp_srv(void *args)
{
        int ret;
        int daemon;
        yftp_args_t *yftp_args;
        char path[MAX_PATH_LEN];
        char service[MAX_INFO_LEN];

        yftp_args = (yftp_args_t *)args;
        if(yftp_args == NULL) {
                DERROR("argument must not be empty !!!\n");
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        daemon = yftp_args->daemon;
        memset(service, 0, MAX_INFO_LEN);
        strcpy(service, yftp_args->service);

        snprintf(path, MAX_NAME_LEN, "%s/status/status.pid", yftp_args->home);
        ret = daemon_pid(path);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init(daemon, "ftp/0", -1);
        if (ret)
                GOTO(err_ret, ret);

        ret = io_analysis_init("ftp", 0);
        if (ret)
                GOTO(err_ret, ret);
        
        ret = rpc_start(); /*begin serivce*/
        if (ret)
                GOTO(err_ret, ret);

retry:
        ret = network_connect_mond(1);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        sleep(5);
                        goto retry;
                } else
                        GOTO(err_ret, ret);
        }

        ftp_srv_running = 1;
        
        DBUG("yftp_srv started ...\n");

        ret = sy_thread_create2(__ftp_run, args, "__conn_worker");
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_update_status("running", -1);
        if (ret)
                GOTO(err_ret, ret);
        
        while (ftp_srv_running) {
                sleep(1);
        }
        
        ret = ly_update_status("stopping", -1);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_update_status("stopped", -1);
        if (ret)
                GOTO(err_ret, ret);

        (void) ly_destroy();

        return 0;
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret, daemon = 1, maxcore __attribute__((unused)) = 0;
        char *service, c_opt;
        char name[MAX_NAME_LEN], *home = NULL;
        yftp_args_t yftp_args;

        service = YFTP_SERVICE_DEF;

        while (1) {
                int option_index = 0;

                static struct option long_options[] = {
                        { "home", required_argument, 0, 'h'},
                };

                c_opt = getopt_long(argc, argv, "cfp:h:m:v",
                                    long_options, &option_index);
                if (c_opt == -1)
                        break;
                
                switch (c_opt) {
                case 'c':
                        maxcore = 1;
                        break;
                case 'f':
                        daemon = 2;
                        break;
                case 'p':
                        service = optarg;
                        break;
                case 'v':
                        get_version();
                        EXIT(0);
                case 'h':
                        home = optarg;
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        EXIT(1);
                }
        }

        strcpy(name, "ftp/0");
        ret = ly_prep(daemon, name, -1);
        if (ret)
                GOTO(err_ret, ret);

        snprintf(home, MAX_PATH_LEN, "%s/%s", gloconf.workdir, name);
        yftp_args.daemon = daemon;
        strcpy(yftp_args.service, service);
        strcpy(yftp_args.home, home);

        signal(SIGIO,  signal_handler);
        signal(SIGUSR1,  signal_handler);
        signal(SIGUSR2, ftp_exit_handler);

        ret = ly_run(home, ftp_srv, &yftp_args);
        if (ret)
                GOTO(err_ret, ret);

        (void) ylog_destroy();

        return 0;
err_ret:
        return ret;
}

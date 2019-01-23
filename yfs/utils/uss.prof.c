
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <dirent.h>
#include <ctype.h>
#include <regex.h>


#include "configure.h"
#include "adt.h"
#include "net_table.h"
/*#include "prof.h"*/
#include "sysutil.h"
#include "net_global.h"
#include "dbg.h"
#include "schedule.h"
#include "variable.h"
#include "main_loop.h"
#include "sdfs_lib.h"

typedef enum {
        OP_NULL,
        OP_RPC,
        OP_NET,
        OP_VM,
        OP_DIO,
        OP_LICHBD,
        //OP_DIRECT,
        OP_SCHEDULE,
} admin_op_t;


static void usage()
{
        fprintf(stderr, "\nusage:\n"
                "lich.prof --schedule\n"
                );
}

typedef struct {
        uint64_t io;
} prof_schedule_ctx_t;

static void __prof_schedule_exec(void *arg)
{
        prof_schedule_ctx_t *ctx = arg;

        ctx->io ++;

        /*schedule_sleep("sleep 1", 1);*/
        //schedule_task_new("prof_task", __prof_schedule_exec, arg);
}

static void __prof_schedule_dump(prof_schedule_ctx_t *ctx, int threads, uint64_t used)
{
        int i;
        uint64_t io;

        io = 0;
        for (i = 0; i < threads; i++) {
                io += ctx[i].io;
                ctx[i].io = 0;
        }
                        
        printf("iops : %llu\n", (LLU)io * 1000 * 1000 / used);
}

int prof_schedule(int threads)
{
        int ret, efd, i;
        schedule_t *schedule;
        prof_schedule_ctx_t *array;
        time_t last, now;
        struct timeval _last, _now;
        uint64_t used;

        ret = variable_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = schedule_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = schedule_create(&efd, "prof_schedule", NULL, &schedule);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&array, sizeof(*array) * threads);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        //printf("init thread\n");

        for (i = 0; i < threads; i++) {
                array[i].io = 0;

                schedule_task_new("prof_task", __prof_schedule_exec, &array[i], -1);
        }

        last = gettime();
        _gettimeofday(&_last, NULL);
#if 0
#else

        while (1) {
                schedule_run();

                for (i = 0; i < threads; i++) {
                        schedule_task_new("prof_task", __prof_schedule_exec, &array[i], -1);
                }
                
                now = gettime();
                if (now - last >= 1) {
                        //printf("dump iops\n");
                        _gettimeofday(&_now, NULL);

                        used = _time_used(&_last, &_now);

                        __prof_schedule_dump(array, threads, used);

                        last = gettime();
                        _gettimeofday(&_last, NULL);
                }
        }
#endif

        return 0;
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret, op = OP_NULL, direct = 0;
        const char *nodename = NULL, *sendsize = "0", *recvsize = "0",
                *thread = "1", *runtime = "30", *rw = NULL,
                *size = "4096", *volume = "/lichbd_prof/pool/device0";
        char c_opt;

        /*dbg_info(0);*/

        while (srv_running) {
                int option_index = 0;

                static struct option long_options[] = {
                        { "sendsize", required_argument, 0, 0},
                        { "recvsize", required_argument, 0, 0},
                        { "thread", required_argument, 0, 0},
                        { "runtime", required_argument, 0, 0},
                        { "direct", no_argument, 0, 0},
                        { "volume", required_argument, 0, 0},
                        { "schedule", no_argument, 0, 's'},
                        { "rpc", required_argument, 0, 'r'},
                        { "net", required_argument, 0, 'n'},
                        { "vm", required_argument, 0, 'm'},
                        { "dio", required_argument, 0, 'd'},
                        { "lichbd", required_argument, 0, 'l'},
                        { "verbose", 0, 0, 'v' },
                        { "help",    0, 0, 'h' },
                        { 0, 0, 0, 0 },
                };

                c_opt = getopt_long(argc, argv, "vhn:", long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 0:
                        switch (option_index) {
                        case 0:
                                sendsize = optarg;
                                break;
                        case 1:
                                recvsize = optarg;
                                break;
                        case 2:
                                thread = optarg;
                                break;
                        case 3:
                                runtime = optarg;
                                break;
                        case 4:
                                direct = 1;
                                break;
                        case 5:
                                volume = optarg;
                                break;
                        default:
                                fprintf(stderr, "Hoops, wrong op got!\n");
                                YASSERT(0); 
                        }

                        break;
                case 'r':
                        op = OP_RPC;
                        nodename = optarg;
                        break;
                case 'n':
                        op = OP_NET;
                        nodename = optarg;
                        break;
                case 'm':
                        op = OP_VM;
                        nodename = optarg;
                        break;
                case 'd':
                        op = OP_DIO;
                        nodename = optarg;
                        break;
                case 's':
                        op = OP_SCHEDULE;
                        break;
                case 'l':
                        op = OP_LICHBD;
                        rw = optarg;
                        YASSERT(strlen(rw) == 1);
                        YASSERT(rw[0] == 'r' || rw[0] == 'w');
                        break;
                }
        }

        (void)volume;
        (void)size;
        (void)runtime;
        (void)recvsize;
        (void)sendsize;
        (void)nodename;
        (void)direct;

#if 0
        ret = env_init_simple("lich.prof");
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif

        ret = timer_init();
        if (ret)
                GOTO(err_ret, ret);

        switch (op) {
        case OP_SCHEDULE:
                ret = prof_schedule(atoi(thread));
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                break;
        default:
                usage();
                exit(EINVAL);
        }

        //DBUG("test...........\n");

        return 0;
err_ret:
        exit(ret);
}

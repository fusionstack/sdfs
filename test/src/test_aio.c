#include <limits.h>
#include <time.h>
#include <string.h>
#include <sys/epoll.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBSCHEDULE

#include "sysutil.h"
#include "net_proto.h"
#include "ylib.h"
#include "sdevent.h"
#include "../net/xnect.h"
#include "net_table.h"
#include "mem_cache.h"
#include "rpc_table.h"
#include "configure.h"
#include "vm.h"
#include "schedule.h"
#include "bh.h"
#include "core.h"
#include "aio.h"
#include "sdfs_lib.h"
#include "net_global.h"
#include "cpuset.h"
#include "adt.h"
#include "dbg.h"

#define AIO_LOCAL 0

#if AIO_LOCAL
extern int test_aio_create(const char *name, int cpu);
extern int test_aio_commit(struct iocb *iocb, size_t size, int prio, int mode);
extern void test_aio_submit();
#endif

extern int __aio_sche__;

#if AIO_LOCAL
int test_aio_basic()
{
        int ret, fd;
        sem_t sem;
        char *ptr;
        struct iovec iov;
        struct iocb iocb;

        DINFO("aio test\n");

        __aio_sche__ = 0;

        ret = test_aio_create("test_aio", -1);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        fd = open("./test_aio_file", O_CREAT | O_DIRECT | O_RDWR, 0777);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = sem_init(&sem, 0, 0);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = posix_memalign((void **)&ptr, 4096, 4096);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        iov.iov_base = ptr;
        iov.iov_len = 4096;
        
        io_prep_pwritev(&iocb, fd, &iov, 1, 0);

        iocb.aio_reqprio = 0;
        iocb.aio_data = (__u64)&sem;

        ret = test_aio_commit(&iocb, 4096, 0, AIO_MODE_DIRECT);
        if (ret)
                GOTO(err_ret, ret);

        test_aio_submit();
        
        ret = sem_wait(&sem);
        if (ret)
                GOTO(err_ret, ret);

        DINFO("aio test successful\n");
        
        return 0;
err_ret:
        return ret;
}
#endif

static int __test_aio(va_list ap)
{
        int ret, fd;
        char *ptr;
        struct iovec iov;
        struct iocb iocb;
        task_t task;
        int *testing = va_arg(ap, int *);

        va_end(ap);

        DINFO("--------------aio test func------------\n");
        
        fd = open("./test_aio_file", O_CREAT | O_DIRECT | O_RDWR, 0777);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = posix_memalign((void **)&ptr, 4096, 4096);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        memset(ptr, 0x0, 4096);
        iov.iov_base = ptr;
        iov.iov_len = 4096;
        
        io_prep_pwritev(&iocb, fd, &iov, 1, 0);

        task = schedule_task_get();
        iocb.aio_reqprio = 0;
        iocb.aio_data = (__u64)&task;

        DINFO("----------aio test commit---------\n");

#if AIO_LOCAL
        ret = test_aio_commit(&iocb, 4096, 0, AIO_MODE_DIRECT);
        if (ret)
                GOTO(err_ret, ret);

        test_aio_submit();
#else
        ret = aio_commit(&iocb, 4096, 0, AIO_MODE_DIRECT);
        if (ret)
                GOTO(err_ret, ret);

        aio_submit();
#endif
        
        DINFO("----------aio test success---------\n");

        *testing = 0;
        
        return 0;
err_ret:
        return ret;
}

int test_aio_sdfs()
{
        int ret;

        ret = ly_prep(0, "test_aio", -1);
        if(ret)
                GOTO(err_ret, ret);

        //ly_set_daemon();

        ret = ly_init(0, "test_aio", 524288);
        if (ret)
                GOTO(err_ret, ret);

        ret = core_init(1, 1, CORE_FLAG_PASSIVE | CORE_FLAG_AIO);
        if (ret)
                GOTO(err_ret, ret);
        
        int testing = 1;

        sleep(12);
        
        DINFO("--------------aio test request-------------\n");
        ret = core_request(0, -1, "replica_read", __test_aio, &testing);
        if (ret)
                GOTO(err_ret, ret);
        
        while (testing) {
                sleep(1);
        }
        
        return 0;
err_ret:
        return ret;
}

typedef struct {
        schedule_t *schedule;
        sem_t sem;
} aio_sche_t;

static void *__aio_schedule(void *arg)
{
        int ret, interrupt_eventfd;
        char name[MAX_NAME_LEN];
        aio_sche_t *aio_sche = arg;

        snprintf(name, sizeof(name), "aio");
        ret = schedule_create(&interrupt_eventfd, name, NULL, &aio_sche->schedule, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

#if AIO_LOCAL
        ret = test_aio_create(name, -1);
#else
        ret = aio_create(name, -1, 0);
#endif
        if (unlikely(ret))
                GOTO(err_ret, ret);

        sem_post(&aio_sche->sem);
        
        while (1) {
                ret = eventfd_poll(interrupt_eventfd, 1, NULL);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                DBUG("poll return\n");

                schedule_run(aio_sche->schedule);

#if AIO_LOCAL
                test_aio_submit();
#else
                aio_submit();
#endif

                schedule_scan(aio_sche->schedule);
        }

        pthread_exit(NULL);
err_ret:
        UNIMPLEMENTED(__DUMP__);
        pthread_exit(NULL);
}


typedef struct {
        task_t task;
        sem_t sem;
        func_va_t exec;
        va_list ap;
        int type;
        int retval;
} arg1_t;

#define REQUEST_SEM 1
#define REQUEST_TASK 2

static void __test_aio_request__(void *_ctx)
{
        arg1_t *ctx = _ctx;

        ctx->retval = ctx->exec(ctx->ap);

        if (ctx->type == REQUEST_SEM) {
                sem_post(&ctx->sem);
        } else {
                schedule_resume(&ctx->task, 0, NULL);
        }
}

static int __test_aio_request(aio_sche_t *aio_sche, func_va_t exec, ...)
{
        int ret;
        schedule_t *schedule = aio_sche->schedule;
        arg1_t ctx;

        ctx.exec = exec;
        va_start(ctx.ap, exec);

        if (schedule_running()) {
                ctx.type = REQUEST_TASK;
                ctx.task = schedule_task_get();
        } else {
                ctx.type = REQUEST_SEM;
                ret = sem_init(&ctx.sem, 0, 0);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);
        }

        ret = schedule_request(schedule, -1, __test_aio_request__, &ctx, "redis_request");
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (schedule_running()) {
                ret = schedule_yield1("redis_request", NULL, NULL, NULL, -1);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        } else {
                ret = _sem_wait(&ctx.sem);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        }

        return ctx.retval;
err_ret:
        return ret;
}

int test_aio_schedule()
{
        int ret;
        aio_sche_t aio_sche;

        __aio_sche__ = 1;
        
        ret = ly_prep(0, "test_aio", -1);
        if(ret)
                GOTO(err_ret, ret);

        //ly_set_daemon();

        ret = ly_init(0, "test_aio", 524288);
        if (ret)
                GOTO(err_ret, ret);

        ret = sem_init(&aio_sche.sem, 0, 0);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }
                
        ret = sy_thread_create2(__aio_schedule, &aio_sche, "aio_schedule");
        if(ret)
                GOTO(err_ret, ret);

        ret = _sem_wait(&aio_sche.sem);
        if(ret) {
                GOTO(err_ret, ret);
        }

        sleep(5);
        
        int testing = 1;

        DINFO("--------------aio test request-------------\n");
        ret = __test_aio_request(&aio_sche, __test_aio, &testing);
        if(ret)
                GOTO(err_ret, ret);

        while (testing) {
                sleep(1);
        }

        DINFO("--------------aio test success-------------\n");
        
        return 0;
err_ret:
        return ret;
}

int main()
{
        int ret;

#if 0
        ret = test_aio_basic();
        if (ret)
                GOTO(err_ret, ret);

#endif

#if 0
        ret = test_aio_sdfs();
        if (ret)
                GOTO(err_ret, ret);
#endif

#if 1
        ret = test_aio_schedule();
        if (ret)
                GOTO(err_ret, ret);
#endif
        
        return 0;
err_ret:
        return ret;
}

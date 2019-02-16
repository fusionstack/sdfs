

#include <limits.h>
#include <time.h>
#include <string.h>
#include <sys/epoll.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <stdarg.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBSCHEDULE

#include "sysutil.h"
#include "net_proto.h"
#include "ylib.h"
#include "sdevent.h"
#include "../net/xnect.h"
#include "net_table.h"
#include "nodectl.h"
#include "timer.h"
#include "mem_cache.h"
#include "mem_hugepage.h"
#include "rpc_table.h"
#include "configure.h"
#include "core.h"
#include "redis_co.h"
#if ENABLE_CORENET
#include "corenet_maping.h"
#include "corenet_connect.h"
#include "corenet.h"
#include "corerpc.h"
#endif
#include "sdfs_aio.h"
#include "schedule.h"
#include "bh.h"
#include "net_global.h"
#include "cpuset.h"
#include "variable.h"
#include "ylib.h"
#include "adt.h"
#include "dbg.h"
#include "mem_pool.h"

#define CORE_MAX 256

typedef struct {
        struct list_head hook;
        sockid_t sockid;
        buffer_t buf;
} core_fwd_t;

typedef struct {
        struct list_head hook;
        char name[MAX_NAME_LEN];
        func1_t func;
        void *opaque;
} core_check_t;

typedef struct {
        task_t task;
        sem_t sem;
        func_va_t exec;
        va_list ap;
        int type;
        int retval;
} arg1_t;

#if 1
typedef struct {
        time_t last_update;
        uint64_t used;
        uint64_t count;
} core_latency_t;

typedef struct {
        struct list_head hook;
        uint64_t used;
        uint64_t count;
} core_latency_update_t;

typedef struct {
        sy_spinlock_t lock;
        struct list_head list;
        uint64_t count;
        uint64_t used;
        double last_result;
} core_latency_list_t;

static __thread core_latency_t *core_latency = NULL;
static core_latency_list_t *core_latency_list;

static int __core_latency_init();
static int __core_latency_private_init();
#if ENABLE_CORE_PIPELINE
static void  __core_pipeline_forward();
static int __core_pipeline_create();
#endif
#endif

static core_t *__core_array__[256];
static int __polling_core__ = -1;
static int __polling_timeout__ = -1;

extern __thread struct list_head private_iser_dev_list;

core_t *core_self()
{
        return variable_get(VARIABLE_CORE);
}

#if ENABLE_CORENET
static void __core_interrupt_eventfd_func(void *arg)
{
        int ret;
        char buf[MAX_BUF_LEN];

        (void) arg;

        ret = read(core_self()->interrupt_eventfd, buf, MAX_BUF_LEN);
        if (ret < 0) {
                ret = errno;
                UNIMPLEMENTED(__DUMP__);
        }
}
#endif

#define CORE_CHECK_KEEPALIVE_INTERVAL 1
#define CORE_CHECK_CALLBACK_INTERVAL 5
#define CORE_CHECK_HEALTH_INTERVAL 30
#define CORE_CHECK_HEALTH_TIMEOUT 180

STATIC void *__core_check_health__(void *_arg)
{
        int ret, i;
        core_t *core = NULL;
        time_t now;
        (void)_arg;

        while (1) {
                sleep(CORE_CHECK_HEALTH_INTERVAL);

                now = gettime();
                for (i = 0; i < cpuset_useable(); i++) {
                        core = core_get(i);
                        if (unlikely(core == NULL))
                                continue;

                        ret = sy_spin_lock(&core->keepalive_lock);
                        if (unlikely(ret))
                                continue;

                        if (unlikely(now - core->keepalive > CORE_CHECK_HEALTH_TIMEOUT)) {
                                sy_spin_unlock(&core->keepalive_lock);
                                DERROR("polling core[%d] block !!!!!\n", core->idx);
                                YASSERT(0);
                                EXIT(EAGAIN);
                        }

                        sy_spin_unlock(&core->keepalive_lock);
                }
        }
}

static void __core_check_keepalive(core_t *core, time_t now)
{
        int ret;

        if (now - core->keepalive < CORE_CHECK_KEEPALIVE_INTERVAL) {
                return;
        }

        ret = sy_spin_lock(&core->keepalive_lock);
        if (unlikely(ret))
                return;

        core->keepalive  = now;

        sy_spin_unlock(&core->keepalive_lock);
}

static void __core_check_callback(core_t *core, time_t now)
{
        struct list_head *pos;
        core_check_t *core_check;

        if (now - core->last_check < CORE_CHECK_CALLBACK_INTERVAL) {
                return;
        }

        core->last_check  = now;

        list_for_each(pos, &core->check_list) {
                core_check = (void *)pos;

                core_check->func(core_check->opaque, core_check->name);
        }
}

static void __core_check(core_t *core)
{
        time_t now;

        now = gettime();

        __core_check_keepalive(core, now);
        __core_check_callback(core, now);
}

static inline void IO_FUNC __core_worker_run(core_t *core, void *ctx)
{
#if ENABLE_CORENET
        int tmo = core->main_core ? 0 : 1;
        corenet_tcp_poll(ctx, tmo);
#endif

        schedule_run(core->schedule);

#if ENABLE_CORE_PIPELINE
        __core_pipeline_forward();
#endif
        
#if ENABLE_CORENET
        corenet_tcp_commit(ctx);
        schedule_run(core->schedule);
#endif

#if ENABLE_COREAIO
        if (core->flag & CORE_FLAG_AIO) {
                aio_submit();
        }
#endif

#if ENABLE_CORERPC
        corerpc_scan();
#endif

        redis_co_run(ctx);
        
        schedule_scan(core->schedule);

#if ENABLE_CORENET
        if (!gloconf.rdma || sanconf.tcp_discovery) {
                corenet_tcp_check();
        }
#endif

        __core_check(core);

        gettime_refresh(ctx);
        timer_expire(ctx);
        analysis_merge(ctx);
}

static int __core_worker_init(core_t *core)
{
        int ret;
        char name[MAX_NAME_LEN];

        DINFO("core[%u] init begin\n", core->hash);

        if (ng.daemon && __polling_timeout__ == 0) {
                cpuset_getcpu(&core->main_core, &core->aio_core);
                if (core->main_core) {
                        snprintf(name, sizeof(name), "core[%u]", core->hash);
                        ret = cpuset(name, core->main_core->cpu_id);
                        if (unlikely(ret)) {
                                DWARN("set cpu fail\n");
                        }

                        DINFO("core[%u] cpu set\n", core->hash);
                } else {
                        DINFO("core[%u] skip cpu set\n", core->hash);
                }
        } else {
                core->main_core = NULL;
        }
        
#if ENABLE_CORENET
        int *interrupt = !core->main_core ? &core->interrupt_eventfd : NULL;

        snprintf(name, sizeof(name), "core");
        ret = schedule_create(interrupt, name, &core->idx, &core->schedule, NULL);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        ret = corenet_tcp_init(32768, (corenet_tcp_t **)&core->tcp_net);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (interrupt) {
                sockid_t sockid;
                sockid.sd = core->interrupt_eventfd;
                sockid.seq = _random();
                sockid.type = SOCKID_CORENET;
                sockid.addr = 123;
                ret = corenet_tcp_add(core->tcp_net, &sockid, NULL, NULL, NULL, NULL,
                                      __core_interrupt_eventfd_func, "interrupt_fd");
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }
#else 
        snprintf(name, sizeof(name), "core");
        ret = schedule_create(NULL, name, &core->idx, &core->schedule, NULL);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }
#endif

        DINFO("core[%u] schedule inited\n", core->hash);

        ret = timer_init(1, core->main_core ? 1 : 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DINFO("core[%u] timer inited\n", core->hash);

        ret = gettime_private_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);

#if ENABLE_CO_WORKER
        if (core->flag & CORE_FLAG_PRIVATE) {
                ret = mem_cache_private_init();
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                ret = mem_hugepage_private_init();
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        
                DINFO("core[%u] mem inited\n", core->hash);

                snprintf(name, sizeof(name), "core[%u]", core->idx);
                ret = analysis_private_create(name);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        }
#else
        ret = mem_cache_private_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = mem_hugepage_private_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        DINFO("core[%u] mem inited\n", core->hash);

        snprintf(name, sizeof(name), "core[%u]", core->idx);
        ret = analysis_private_create(name);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        DINFO("core[%u] analysis inited\n", core->hash);
#endif

#if 0
        ret = fastrandom_private_init();
        if (unlikely(ret)) {
                UNIMPLEMENTED(__DUMP__);
        }

        DINFO("core[%u] fastrandom inited\n", core->hash);
#endif

#if ENABLE_CORERPC
        ret = corerpc_init(name, core);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DINFO("core[%u] rpc inited\n", core->hash);

        if (core->flag & CORE_FLAG_ACTIVE) {
                corenet_maping_t *maping;
                ret = corenet_maping_init(&maping);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                YASSERT(maping);

                DINFO("core[%u] maping inited\n", core->hash);

                core->maping = maping;
        } else {
                core->maping = NULL;
        }
#endif

#if 1
        ret = __core_latency_private_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif

        DINFO("core[%u] latency inited\n", core->hash);

#if ENABLE_COREAIO
        if (core->flag & CORE_FLAG_AIO) {
                snprintf(name, sizeof(name), "aio[%u]", core->hash);
                ret = aio_create(name, core->aio_core);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                DINFO("core[%u] aio inited\n", core->hash);
        }
#endif

#if ENABLE_REDIS_CO
        if (core->flag & CORE_FLAG_REDIS) {
                ret = redis_co_init();
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }
#endif
        
        variable_set(VARIABLE_CORE, core);
        //core_register_tls(VARIABLE_CORE, private_mem);

#if ENABLE_CORE_PIPELINE
        ret = __core_pipeline_create();
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif
        
        sem_post(&core->sem);

        return 0;
err_ret:
        return ret;
}

static void * IO_FUNC __core_worker(void *_args)
{
        int ret;
        core_t *core = _args;

        DINFO("start core[%d] name %s idx %d\n",
              core->hash, core->name, core->idx);

        ret = __core_worker_init(core);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        void *ctx = variable_get_ctx();
        YASSERT(ctx);
        
        while (1) {
                __core_worker_run(core, ctx);
        }

        DFATAL("name %s idx %d hash %d\n", core->name, core->idx, core->hash);
        return NULL;
}

typedef struct {
        task_t task;
        sem_t sem;
        func_t exec;
        void *ctx;
        int type;
} arg_t;

#define REQUEST_SEM 1
#define REQUEST_TASK 2

int core_create(core_t **_core, int hash, int flag)
{
        int ret;
        core_t *core;

        ret = ymalloc((void **)&core, sizeof(*core));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        memset(core, 0x0, sizeof(*core));

        core->idx = -1;
        core->hash = hash;
        core->flag = flag;
        core->keepalive = gettime();

        INIT_LIST_HEAD(&core->check_list);

        ret = sy_spin_init(&core->keepalive_lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        ret = sem_init(&core->sem, 0, 0);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        ret = sy_thread_create2(__core_worker, core, "__core_worker");
        if (ret == -1) {
                ret = errno;
                GOTO(err_free, ret);
        }

        *_core = core;

        return 0;
err_free:
        yfree((void **)&core);
err_ret:
        return ret;
}

int core_init(int polling_core, int polling_timeout, int flag)
{
        int ret, i;
        core_t *core = NULL;

        __polling_timeout__ = polling_timeout;
        __polling_core__ = polling_core;

        ret = cpuset_init(polling_core);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);
        
        DINFO("core init begin\n");
        YASSERT(cpuset_useable() > 0 && cpuset_useable() < 64);

#if 0
        ret = global_private_mem_init();
        if (ret)
                UNIMPLEMENTED(__DUMP__);
#endif

        DINFO("core global private mem inited\n");

#if 1
        ret = __core_latency_init();
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);
#endif

        DINFO("core global latency inited\n");
        for (i = 0; i < cpuset_useable(); i++) {
                ret = core_create(&core, i, flag);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

                __core_array__[i] = core;

                DINFO("core %d hash %d idx %d\n",
                      i, core->hash, core->idx);
        }

        for (i = 0; i < cpuset_useable(); i++) {
                core = __core_array__[i];
                ret = _sem_wait(&core->sem);
                if (unlikely(ret)) {
                        UNIMPLEMENTED(__DUMP__);
                }
        }

#if ENABLE_CORERPC
        if (flag & CORE_FLAG_PASSIVE) {
                ret = corenet_tcp_passive();
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

#if ENABLE_RDMA
                if (gloconf.rdma) {
                        ret = corenet_rdma_passive();
                        if (unlikely(ret))
                                UNIMPLEMENTED(__DUMP__);
                }
#endif
        }
#endif

        ret = sy_thread_create2(__core_check_health__, NULL, "core_check_health");
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        //DINFO("flag %d cpuset_useable %d\n", flag, cpuset_useable());

        return 0;
}

int core_hash(const fileid_t *fileid)
{
        return fileid->id % cpuset_useable();
}

#if ENABLE_CORENET
int core_attach(int hash, const sockid_t *sockid, const char *name,
                void *ctx, core_exec func, func_t reset, func_t check)
{
        int ret;
        core_t *core;
        corenet_tcp_t *corenet;

        DINFO("attach hash %d fd %d name %s\n", hash, sockid->sd, name);

        core = __core_array__[hash % cpuset_useable()];
        YASSERT(core);

        corenet = core->tcp_net;
        //ctx->nid->id = 0;
        ret = corenet_tcp_add(corenet, sockid, ctx, func, reset, check, NULL, name);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        schedule_post(core->schedule);

        return 0;
err_ret:
        return ret;
}
#endif

core_t *core_get(int hash)
{
        return __core_array__[hash % cpuset_useable()];
}

static void __core_request(void *_ctx)
{
        arg1_t *ctx = _ctx;

        ctx->retval = ctx->exec(ctx->ap);

        if (ctx->type == REQUEST_SEM) {
                sem_post(&ctx->sem);
        } else {
                schedule_resume(&ctx->task, 0, NULL);
        }
}

int core_request(int hash, int priority, const char *name, func_va_t exec, ...)
{
        int ret;
        core_t *core;
        schedule_t *schedule;
        arg1_t ctx;

        if (unlikely(__core_array__[0] == NULL)) {
                ret = ENOSYS;
                GOTO(err_ret, ret);
        }
        
        core = __core_array__[hash % cpuset_useable()];
        schedule = core->schedule;
        if (unlikely(schedule == NULL)) {
                ret = ENOSYS;
                GOTO(err_ret, ret);
        }

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

        ret = schedule_request(schedule, priority, __core_request, &ctx, name);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (schedule_running()) {
                ret = schedule_yield1(name, NULL, NULL, NULL, -1);
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

int core_request_new(core_t *core, int priority, const char *name, func_va_t exec, ...)
{
        int ret;
        schedule_t *schedule;
        arg1_t ctx;

        schedule = core->schedule;
        if (unlikely(schedule == NULL)) {
                ret = ENOSYS;
                GOTO(err_ret, ret);
        }

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

        ret = schedule_request(schedule, priority, __core_request, &ctx, name);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (schedule_running()) {
                ret = schedule_yield1(name, NULL, NULL, NULL, -1);
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

void core_check_register(core_t *core, const char *name, void *opaque, func1_t func)
{
        int ret;
        core_check_t *core_check;

        YASSERT(strlen(name) < MAX_NAME_LEN);

        ret = ymalloc((void **)&core_check, sizeof(*core_check));
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

	DINFO("%s register core check %p\n", name, opaque);
        core_check->func = func;
        core_check->opaque = opaque;
        strcpy(core_check->name, name);
        list_add_tail(&core_check->hook, &core->check_list);
}

void core_check_dereg(const char *name, void *opaque)
{
	core_t  *core = core_self();
	struct list_head *pos, *n;
	core_check_t *core_check;
	int found = 0;

	list_for_each_safe(pos, n, &core->check_list) {
		core_check = list_entry(pos, core_check_t, hook);

		if (core_check->opaque == opaque) {
			DWARN("deregister %s corecheck %p name len %lu\n", name, opaque, strlen(name));
			YASSERT(memcmp(core_check->name, name, strlen(name)) == 0);
			list_del(&core_check->hook);
			found = 1;
			yfree((void **)&core_check);
			break;
		}
	}

	if (found == 0)
		YASSERT(0);
}

void core_register_tls(int type, void *ptr)
{
        core_t *core = core_self();

        if (core == NULL)
                YASSERT(0);

        core->tls[type] = ptr;
}

void core_iterator(func1_t func, const void *opaque)
{
        int i;
        core_t *core;

        for (i = 0; i < cpuset_useable(); i++) {
                core = __core_array__[i];
                func(core, (void *)opaque);
        }
}

#if 1
static int __core_latency_private_init()
{
        int ret;

        YASSERT(core_latency == NULL);
        ret = ymalloc((void **)&core_latency, sizeof(*core_latency));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        memset(core_latency, 0x0, sizeof(*core_latency));

        return 0;
err_ret:
        return ret;
}

static void __core_latency_private_destroy()
{
        YASSERT(core_latency);
        yfree((void **)&core_latency);
}

static int __core_latency_worker__()
{
        int ret;
        struct list_head list, *pos, *n;
        core_latency_update_t *core_latency_update;
        char path[MAX_PATH_LEN], buf[MAX_BUF_LEN];
        double latency;

        INIT_LIST_HEAD(&list);

        ret = sy_spin_lock(&core_latency_list->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        list_splice_init(&core_latency_list->list, &list);

        sy_spin_unlock(&core_latency_list->lock);

        list_for_each_safe(pos, n, &list) {
                list_del(pos);
                core_latency_update = (void *)pos;
                core_latency_list->used += core_latency_update->used;
                core_latency_list->count += core_latency_update->count;
                yfree((void **)&pos);
        }

        if (core_latency_list->count) {
                core_latency_list->last_result
                        = ((double)(core_latency_list->used + core_latency_list->last_result)
                           / (core_latency_list->count + 1));
        } else
                core_latency_list->last_result /= 2;

        latency = core_latency_list->last_result / (1000);
        core_latency_list->used = 0;
        core_latency_list->count = 0;

        snprintf(path, MAX_PATH_LEN, "latency/10");
        snprintf(buf, MAX_PATH_LEN, "%fms\n", latency);
        //DINFO("latency %s", buf);

        DBUG("latency %llu\n", (LLU)core_latency_list->last_result);

        nodectl_set(path, buf);

        return 0;
err_ret:
        return ret;
}

static void *__core_latency_worker(void *arg)
{
        int ret;

        (void) arg;

        while (1) {
                sleep(4);

                ret = __core_latency_worker__();
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);
        }

        return NULL;
}

static int __core_latency_init()
{
        int ret;

        YASSERT(core_latency_list == NULL);
        ret = ymalloc((void **)&core_latency_list, sizeof(*core_latency_list));
        if (unlikely(ret))
                GOTO(err_ret, ret);


        INIT_LIST_HEAD(&core_latency_list->list);

        ret = sy_spin_init(&core_latency_list->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sy_thread_create2(__core_latency_worker, NULL, "__core_latency_worker");
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __core_latency_update()
{
        int ret;
        time_t now = gettime();
        core_latency_update_t *core_latency_update;
        core_t *core;

        if (now - core_latency->last_update < 2) {
                return 0;
        }

        core = core_self();
        DBUG("%s update latency\n", core->name);

        ret = ymalloc((void **)&core_latency_update, sizeof(*core_latency_update));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        core_latency_update->used = core_latency->used;
        core_latency_update->count = core_latency->count;
        core_latency->used = core_latency->used / core_latency->count;
        core_latency->count = 1;
        core_latency->last_update = now;

        ret = sy_spin_lock(&core_latency_list->lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        list_add_tail(&core_latency_update->hook, &core_latency_list->list);

        sy_spin_unlock(&core_latency_list->lock);

        return 0;
err_ret:
        return ret;
}

void core_latency_update(uint64_t used)
{
        if (core_latency == NULL) {
                return;
        }

        core_latency->used += used;
        core_latency->count++;

        DBUG("latency %llu / %llu\n", (LLU)core_latency->used, (LLU)core_latency->count);
        __core_latency_update();
}

uint64_t IO_FUNC core_latency_get()
{
        if (core_latency && core_latency->count) {
                DBUG("latency %llu / %llu\n", (LLU)core_latency->used, (LLU)core_latency->count);
                return core_latency->used / core_latency->count;
        } else if (core_latency_list) {
                DBUG("latency %llu\n", (LLU)core_latency_list->last_result);
                return core_latency_list->last_result;
        } else
                return 0;
}

#endif

typedef struct {
        func_t init;
        void *ctx;
        sem_t sem;
} arg2_t;

static void __core_init_attach(void *_ctx)
{
        arg2_t *ctx = _ctx;

        ctx->init(ctx->ctx);

        sem_post(&ctx->sem);
}

int core_init_register(func_t init, void *_ctx, const char *name)
{
        int ret, i, count;
        arg2_t ctx;
        core_t *core;

        YASSERT(!schedule_running());

        ctx.init = init;
        ctx.ctx = _ctx;
        ret = sem_init(&ctx.sem, 0, 0);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        count = cpuset_useable();
        for (i = 0; i < count; i++) {
                core = __core_array__[i];

                ret = schedule_request(core->schedule, -1, __core_init_attach, &ctx, name);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        for (i = 0; i < count; i++) {
                ret = _sem_wait(&ctx.sem);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}


void __core_dump_memory(void *_core, void *_arg)
{
        core_t *core = _core;
        uint64_t *memory = _arg;

        schedule_t *schedule = core->schedule;
        *memory += sizeof(core_t) +
                   sizeof(schedule_t) +
                   (sizeof(taskctx_t) + DEFAULT_STACK_SIZE) * schedule->size;
}

/**
 * 获取内存使用量
 *
 * @return
 */
int core_dump_memory(uint64_t *memory)
{
        *memory = 0;

        core_iterator(__core_dump_memory, memory);

        return 0;
}

#if 0
int core_poller_register(core_t *core, const char *name, void (*poll)(void *,void*), void *user_data)
{
        sub_poller_t *poller;

        int ret = huge_malloc((void **)&poller, sizeof(sub_poller_t));
        if(ret)
                return ret;

        strncpy(poller->name, name, 64);
        poller->poll = poll;
        poller->user_data = user_data;
        list_add_tail(&poller->list_entry, &core->poller_list);

        DINFO("register sub poller, core:%d, ptr=%p, name: %s\r\n", core->idx, poller, poller->name);
        
        return 0;
}

int core_poller_unregister(core_t *core, void (*poll)(void *,void*))
{
        sub_poller_t *entry;
        sub_poller_t *n;

        /*all sub-poller should be registered in the list.*/
	list_for_each_entry_safe(entry, n, &core->poller_list, list_entry) {
                if(entry->poll == poll) {
                        DINFO("unregister sub poller, ptr=%p, name: %s\r\n", entry, entry->name);
                        list_del(&entry->list_entry);
                        huge_free((void **)&entry);
                }
        }
        
        return 0;
}
#endif 

#if ENABLE_CORE_PIPELINE
typedef struct __vm {
        /*forward*/
        struct list_head forward_list;
        /*aio cb*/
        //aio_context_t  ioctx;
        int iocb_count;
        struct iocb *iocb[TASK_MAX];
} vm_t;

typedef struct {
        struct list_head hook;
        sockid_t sockid;
        buffer_t buf;
} vm_fwd_t;

static __thread vm_t *__vm__ = NULL;

int core_pipeline_send(const sockid_t *sockid, buffer_t *buf, int flag)
{
        int ret, found = 0;
        vm_fwd_t *vm_fwd;
        struct list_head *pos;

        if (__vm__ == NULL) {
                ret = ENOSYS;
                goto err_ret;
        }

#if 1
        ret = sdevent_check(sockid);
        if (unlikely(ret)) {
                DWARN("append forward to %s fail\n",
                      _inet_ntoa(sockid->addr));
                GOTO(err_ret, ret);
        }
#endif

        YASSERT(flag == 0);

        DBUG("core_pipeline_send\n");

        list_for_each(pos, &__vm__->forward_list) {
                vm_fwd = (void *)pos;

                if (sockid_cmp(sockid, &vm_fwd->sockid) == 0) {
                        DBUG("append forward to %s @ %u\n",
                             _inet_ntoa(sockid->addr), sockid->sd);
                        mbuffer_merge(&vm_fwd->buf, buf);
                        found = 1;
                        break;
                }
        }

        if (found == 0) {
                DBUG("new forward to %s @ %u\n",
                      _inet_ntoa(sockid->addr), sockid->sd);

#ifdef HAVE_STATIC_ASSERT
                static_assert(sizeof(*vm_fwd)  < sizeof(mem_cache128_t), "vm_fwd_t");
#endif

                vm_fwd = mem_cache_calloc(MEM_CACHE_128, 0);
                YASSERT(vm_fwd);
                vm_fwd->sockid = *sockid;
                mbuffer_init(&vm_fwd->buf, 0);
                mbuffer_merge(&vm_fwd->buf, buf);
                list_add_tail(&vm_fwd->hook, &__vm__->forward_list);
        }

        return 0;
err_ret:
        return ret;
}

static void  __core_pipeline_forward()
{
        int ret;
        net_handle_t nh;
        struct list_head *pos, *n;
        vm_fwd_t *vm_fwd;

        YASSERT(__vm__);
        list_for_each_safe(pos, n, &__vm__->forward_list) {
                vm_fwd = (void *)pos;
                list_del(pos);

                DBUG("forward to %s @ %u\n",
                     _inet_ntoa(vm_fwd->sockid.addr), vm_fwd->sockid.sd);

                sock2nh(&nh, &vm_fwd->sockid);
                ret = sdevent_queue(&nh, &vm_fwd->buf, 0);
                if (unlikely(ret)) {
                        DWARN("forward to %s @ %u fail\n",
                              _inet_ntoa(vm_fwd->sockid.addr), vm_fwd->sockid.sd);
                        mbuffer_free(&vm_fwd->buf);
                }

                mem_cache_free(MEM_CACHE_128, vm_fwd);
        }
}

static int __core_pipeline_create()
{
        int ret;
        vm_t *vm;

        ret = ymalloc((void **)&vm, sizeof(*vm));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        memset(vm, 0x0, sizeof(*vm));

        INIT_LIST_HEAD(&vm->forward_list);

        __vm__ = vm;

        return 0;
err_ret:
        return ret;
}
#endif

int core_request_async(int hash, int priority, const char *name, func_t exec, void *arg)
{
        int ret;
        core_t *core;
        schedule_t *schedule;

        if (unlikely(__core_array__ == NULL)) {
                ret = ENOSYS;
                GOTO(err_ret, ret);
        }
        
        core = __core_array__[hash % cpuset_useable()];
        schedule = core->schedule;
        if (unlikely(schedule == NULL)) {
                ret = ENOSYS;
                GOTO(err_ret, ret);
        }

        ret = schedule_request(schedule, priority, exec, arg, name);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

#if 1
void core_worker_exit(core_t *core)
{
        DINFO("core[%u] destroy begin\n", core->hash);

        corenet_tcp_destroy(&core->tcp_net);
        gettime_private_destroy();

#if ENABLE_COREAIO
        if (core->flag & CORE_FLAG_AIO) {
                aio_destroy();
        }
#endif

        if (core->flag & CORE_FLAG_REDIS) {
                redis_co_destroy();
        }
        
        if (core->main_core) {
                cpuset_unset(core->main_core->cpu_id);
        }
        

#if ENABLE_CORERPC
        corerpc_destroy((void *)&core->rpc_table);

        if (core->flag & CORE_FLAG_ACTIVE) {
                corenet_maping_destroy((corenet_maping_t **)&core->maping);
        }
#endif

        __core_latency_private_destroy();

#if ENABLE_CORE_PIPELINE
        UNIMPLEMENTED(__DUMP__);
#endif

        variable_unset(VARIABLE_CORE);

        timer_destroy();

        if (core->flag & CORE_FLAG_PRIVATE) {
                analysis_private_destroy();
                mem_cache_private_destroy();
                mem_hugepage_private_destoy();
        }

        schedule_destroy(core->schedule);
}
#endif

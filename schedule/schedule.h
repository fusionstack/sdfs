#ifndef __SCHEDULE_H__
#define __SCHEDULE_H__

/** @file scheduler
 *
 * 协程是一种非连续执行的机制，每个core thread一个调度器
 *
 * 目前调度器的调度策略：公平调度，不支持优先级调度
 * 对并发运行的任务数有一定限制：1024
 *
 * 一些约束：
 * - 每个task有64K的stack，所以不能声明太大的stack上数据，特别是数量多的数组，或大对象。
 * - 一个任务的总执行时间不能超过180s，否则会timeout，导致进程退出
 * - IDLE状态的代码，不能加锁，会形成deadlock (@see __rpc_table_check)
 */

#include <ucontext.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>

#include "ylib.h"
#include "dbg.h"
#include "sdfs_list.h"
#include "schedule_thread.h"
#include "configure.h"
#include "analysis.h"

#define SCHEDULE_CHECK_RUNTIME ENABLE_SCHEDULE_CHECK_RUNTIME

#define TASK_MAX (8192)

#define REQUEST_QUEUE_STEP 128
#define REQUEST_QUEUE_MAX TASK_MAX * 4

#define REPLY_QUEUE_STEP 128
#define REPLY_QUEUE_MAX TASK_MAX

#define NEW_SCHED

#ifndef NEW_SCHED
#define swapcontext1 swapcontext
#endif

#define SCHEDULE_STATUS_NONE 0
#define SCHEDULE_STATUS_IDLE 1
#define SCHEDULE_STATUS_RUNNING 2

typedef enum {
        TASK_STAT_FREE = 10, //0
        TASK_STAT_RUNNABLE,  //1
        TASK_STAT_RUNNING,   //2        //previous was conflicating with spdk.
        TASK_STAT_BACKTRACE, //3
        TASK_STAT_SUSPEND,   //4
        //TIMEOUT, //5
} taskstate_t;

struct cpu_ctx {
        void *rsp;
        void *rbp;
        void *rip;
        void *rbx;
        void *r12;
        void *r13;
        void *r14;
        void *r15;
        void *rdi;
};

typedef enum {
        TASK_VALUE_LEASE = 0,
        TASK_VALUE_REPLICA = 1,
        TASK_VALUE_MAX,
} taskvalue_t;

typedef struct __task {
        // must be first member
        struct list_head hook;

#ifdef NEW_SCHED
        struct cpu_ctx main;
        struct cpu_ctx ctx;
#else
        //sy_spinlock_t lock;
        ucontext_t ctx;          // coroutine context
        ucontext_t main;         // scheduler context(core thread)
#endif

        // for schedule->running_task_list;
        struct list_head running_hook;

        char name[MAX_NAME_LEN];
        int id;
        task_t parent;
        struct timeval ctime; /*create time*/
#if SCHEDULE_CHECK_RUNTIME
        struct timeval rtime; /*running time*/
#endif

#if ENABLE_SCHEDULE_LOCK_CHECK
        int lock_count;
        int ref_count;
#endif

        taskstate_t state;
        char pre_yield;
        char sleeping;
        int8_t step;
        int8_t priority;
        int8_t wait_tmo;
        int32_t retval;

        func_t func;
        void *arg;
        buffer_t buf;

        //uint32_t fingerprint_prev;
        uint32_t fingerprint;

        time_t wait_begin;

        uint32_t value[TASK_VALUE_MAX];

        const char *wait_name;
        const void *wait_opaque;

        // last member?
        void *stack;
        void *schedule;
} taskctx_t;

typedef struct {
        void (*exec)(void *buf);
        void *buf;
        int8_t priority;
        task_t parent;
        char name[32];
} request_t;

typedef struct {
        sy_spinlock_t lock;
        int count;
        int max;
        request_t *requests;
} request_queue_t;

typedef struct {
        task_t task;
        int retval;
        buffer_t *buf;
} reply_t;

typedef struct {
        sy_spinlock_t lock;
        int count;
        int max;
        reply_t *replys;
} reply_queue_t;

#if 1
#define SCHEDULE_PRIORITY0 0
#define SCHEDULE_PRIORITY1 1
#define SCHEDULE_PRIORITY2 2
#define SCHEDULE_PRIORITY_MAX 3

#else

#define SCHEDULE_PRIORITY0 0
#define SCHEDULE_PRIORITY1 0
#define SCHEDULE_PRIORITY_MAX 1

#endif

typedef struct schedule_t {
        // scheduler
        char name[32];
        int id;

        // scheduler status
        int eventfd;
        int running;
        int suspendable;

        // coroutine
        void *private_mem;
        taskctx_t *tasks;
        void *stack_addr;
        int size;

        // no free task count
        int task_count;
        int running_task;
        int cursor;

        // running task list
        struct list_head running_task_list;

        // free task list
        struct list_head free_task_list;

        // 若tasks满，缓存到wait_task
        count_list_t wait_task;

        // core_request的请求，先放入队列，而后才生成task
        request_queue_t request_queue;

        // 当前可调度的任务队列
        count_list_t runable[SCHEDULE_PRIORITY_MAX];

        // resume相关, local是本调度器上的任务，remote是跨core任务(需要MT同步）
        reply_queue_t reply_local;
        reply_queue_t reply_remote;

        // backtrace
        uint32_t sequence;
        uint32_t scan_seq;
        time_t last_scan;
        int backtrace;
} schedule_t;

// API

// scheduler相关

typedef enum {
        TASK_SCHEDULE = 10,
        TASK_THREAD,
} task_type_t;

int schedule_init();
void schedule_destroy();

int schedule_create(int *eventfd, const char *name, int *idx, schedule_t **_schedule, void *private_mem);

void schedule_run(schedule_t *_schedule);

schedule_t *schedule_self();
int schedule_running();
int schedule_suspend();
int schedule_stat(int *sid, int *taskid, int *rq, int *runable, int *wait_task, int *task_count);


// task/coroutine相关, 切换task状态
int schedule_request(schedule_t *schedule, int priority, func_t exec, void *buf, const char *name);

void schedule_task_new(const char *name, func_t func, void *arg, int priority);

/** 有副作用，两次schedule_task_get调用之间，必须有schedule_yield
 *
 * @return
 */
task_t schedule_task_get();
void schedule_task_given(task_t *task);

int schedule_yield(const char *name, buffer_t *buf, void *opaque);

/**
 *
 * @param name
 * @param buf
 * @param opaque
 * @param func
 * @param _tmo
 * @return
 */
int schedule_yield1(const char *name, buffer_t *buf, void *opaque, func_t func, int _tmo);

/**
 *
 * @param task
 * @param retval
 * @param buf
 */
void schedule_resume(const task_t *task, int retval, buffer_t *buf);
void schedule_resume1(const task_t *task, int retval, buffer_t *buf);

/**
 *
 * @param name
 * @param usec
 * @return
 */
int schedule_sleep(const char *name, suseconds_t usec);

// 当前任务相关函数
int schedule_taskid();

void schedule_task_setname(const char *name);

// internals

void schedule_post(schedule_t *schedule);

void schedule_scan(schedule_t *schedule);
void schedule_backtrace();

/** task stack overflow，影响性能
 *
 */
void schedule_stack_assert(schedule_t *schedule);
// task/coroutine

void schedule_dump(schedule_t *schedule, int block);

void schedule_task_run(schedule_t *_schedule);
void schedule_reply_local_run();

void schedule_task_reset();

void schedule_task_sleep(task_t *task);
void schedule_task_wakeup(task_t *task);

#if ENABLE_SCHEDULE_LOCK_CHECK
void schedule_lock_set(int lock, int ref);
#endif

int schedule_assert_retry();
int schedule_status();

void schedule_value_set(int key, uint32_t value);
int schedule_task_uptime();
void schedule_value_get(int key, uint32_t *value);
int schedule_priority(schedule_t *_schedule);

#if 1

# define SCHEDULE_LEASE_SET()                          \
        do {                                                            \
                schedule_value_set(TASK_VALUE_LEASE, gettime());       \
        } while (0)

# define SCHEDULE_LEASE_CHECK(label, __ret__)                           \
        do {                                                            \
                uint32_t __lease__, diff; \
                schedule_value_get(TASK_VALUE_LEASE, &__lease__);       \
                YASSERT(__lease__);                                     \
                diff = gettime() - __lease__;                          \
                if (__lease__!= -1 && diff > gloconf.lease_timeout) {   \
                        __ret__ = ETIMEDOUT;                            \
                        DERROR("Process leaving via %s ret %d lease %u timeout %u %u\n", \
                               #label, __ret__, __lease__, gloconf.lease_timeout, diff); \
                        goto label;                                     \
                }                                                       \
        } while (0)


#else

# define SCHEDULE_LEASE_SET()
# define SCHEDULE_LEASE_CHECK(label, __ret__)

#endif

#endif

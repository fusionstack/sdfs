#include <ucontext.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <uuid/uuid.h>
#include <unistd.h>
#include <setjmp.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>   /* For SYS_xxx definitions */
#include <stdint.h>

#define DBG_SUBSYS S_LIBSCHEDULE

#include "ylib.h"
#include "sdfs_aio.h"
#include "schedule.h"
#include "timer.h"
#include "net_global.h"
#include "mem_cache.h"
#include "core.h"
#include "job_dock.h"
#include "cpuset.h"
#include "variable.h"
#include "sdfs_buffer.h"
#include "cpuset.h"

#define __TASK_MAX__ (TASK_MAX * SCHEDULE_MAX)
#define SCHEDULE_RUNNING_TASK_LIST TRUE

/**
//gdb loop

define loop_schedule
set $count= schedule->size 
set $i = 0
while ($i < $count)
if (schedule->tasks[$i].state != TASK_STAT_FREE)
print  schedule->tasks[$i].name
print  schedule->tasks[$i].wait_name
print $i
end
set $i = $i + 1
end
end
**/


/**
 * polling core execute flow graph by Gabe:
 *
 *                        swapcontext    swapcontext
 *                  |user space|schedule space|user space|
 * .--.             |          `----.     .---`          |
 * |  V             `---------.     v     v     .--------`
 * | poll()                   +-----+     +-----+          +-----+
 * |  |     .--reply_local--> |taks0| --> |taks1| --.      |taks2|
 * |  |     |                 +-----+     +-----+   |      +-----+
 * |  |     |                 .---------------------`      ^
 * |  |     |                 |     +-----+                |           +------------+
 * |  |     |                 `---> |taks2|                |           |another core|
 * |  |     |                       +-----+ ---> yield  ---`           +------------+
 * |  |     |                   .---------------`      ^                     |
 * |  |     |                   |                      |                     |
 * |  |     |                   |   +-----+            |                     |
 * |  |     |                   `-> |taks3| --resume---`                     |
 * |  |     |                       +-----+ ---> yield  <-----resume---------`
 * |  |     |  .--------------------------------`       `--.
 * |  v     |  |                                           V
 * | run()--`  |              +-----+     +-----+          +-----+
 * |  .-----.  reply_remote-> |taks4| --> |taks5| --.      |taks3|
 * |  |     |                 +-----+     +-----+   |      +-----+
 * |  |     |  .------------------------------------`
 * |  |     |  |
 * |  |     |  |              +-----+     +-----+          +-----+
 * |  |     |  runable------> |taks6| --> |taks7| --.      |taks8|
 * |  |     |                 +-----+     +-----+   |      +-----+
 * |  |     |  .------------------------------------`      ^
 * |  |     |  |                                           |
 * |  |     |  |              +-----+ -----------new-------`
 * |  |     |  request------> |req1 | --.
 * |  |     |                 +-----+   |
 * |  |     `---------------------------`
 * |  v
 * | scan()
 * `--`
 */

typedef struct {
        func_t func;
        void *arg;
} schedule_task_ctx_t;

typedef struct {
        struct list_head hook;
        char name[MAX_NAME_LEN];
        func_t func;
        void *arg;
        task_t parent;
} wait_task_t;

static __thread job_t *__running_job__ = NULL;

//globe data
static schedule_t **__schedule_array__ = NULL;
static sy_spinlock_t __schedule_array_lock__;

static int __schedule_isfree(taskctx_t *taskctx);
static void __schedule_fingerprint_new(schedule_t *schedule, taskctx_t *taskctx);
static taskctx_t *__schedule_task_new(const char *name, func_t func, void *arg, int timeout, task_t *_parent, int priority);
static void __schedule_backtrace__(const char *name, int id, int idx, uint32_t seq);
static void __schedule_backtrace_set(taskctx_t *taskctx);


#ifdef NEW_SCHED
int swapcontext1(struct cpu_ctx *cur_ctx, struct cpu_ctx *new_ctx);
__asm__ (
"       .text                                                           \n"
"       .p2align 4,,15                                                  \n"
"       .globl swapcontext1                                             \n"
"       .globl swapcontext1                                             \n"
"swapcontext1:                                                          \n"
"swapcontext1:                                                          \n"
"       movq %rsp, 0(%rdi)      # save stack_pointer                    \n"
"       movq %rbp, 8(%rdi)      # save frame_pointer                    \n"
"       movq (%rsp), %rax       # save insn_pointer                     \n"
"       movq %rax, 16(%rdi)                                             \n"
"       movq %rbx, 24(%rdi)     # save rbx,r12-r15                      \n"
"       movq 24(%rsi), %rbx                                             \n"
"       movq %r15, 56(%rdi)                                             \n"
"       movq %r14, 48(%rdi)                                             \n"
"       movq %rdi, 64(%rdi)     #was a lack, to backup rdi              \n"
"       movq 48(%rsi), %r14                                             \n"
"       movq 56(%rsi), %r15                                             \n"
"       movq %r13, 40(%rdi)                                             \n"
"       movq %r12, 32(%rdi)                                             \n"
"       movq 32(%rsi), %r12                                             \n"
"       movq 40(%rsi), %r13     # restore rbx,r12-r15                   \n"
"       movq 0(%rsi), %rsp      # restore stack_pointer                 \n"
"       movq 16(%rsi), %rax     # restore insn_pointer                  \n"
"       movq 8(%rsi), %rbp      # restore frame_pointer                 \n"
"       movq 64(%rsi), %rdi     # save taskctx to rdi tramp function    \n" 
"       movq %rax, (%rsp)                                               \n"
"       ret                                                             \n"    
);
#endif

//used by schedule_stack_assert
static char zerobuf[KEEP_STACK_SIZE] = {0};

inline IO_FUNC schedule_t *schedule_self()
{
        return variable_get(VARIABLE_SCHEDULE);
}

static inline IO_FUNC schedule_t *__schedule_self(schedule_t *schedule)
{
        if (likely(schedule)) {
#if ENABLE_SCHEDULE_SELF_DEBUG
                YASSERT(schedule == schedule_self());
#endif
                return schedule;
        } else {
                return schedule_self();
        }
}

static int __schedule_runable(const schedule_t *schedule)
{
        int count = 0, i;

        for (i = 0; i < SCHEDULE_PRIORITY_MAX; i++) {
                count += schedule->runable[i].count;
        }

        return count;
}

#if SCHEDULE_CHECK_RUNTIME
static inline void __schedule_check_running_used(schedule_t *schedule, taskctx_t *taskctx, uint64_t used)
{
        if (unlikely(used > 100 * 1000)) {
                __schedule_backtrace__(schedule->name, schedule->id, taskctx->id, schedule->scan_seq);
                DWARN("task[%u] %s running used %fs\n", taskctx->id, taskctx->name, (double)used / (1000 * 1000));
        } else if (used > 10 * 1000) {
                DWARN("task[%u] %s running used %fs\n", taskctx->id, taskctx->name, (double)used / (1000 * 1000));
        }
}
#endif

static inline void __schedule_check_yield_used(schedule_t *schedule, taskctx_t *taskctx, uint64_t used)
{
        if (unlikely(used < (uint64_t)1000 * 1000 * (gloconf.lease_timeout / 2 + 1))) {
                return;
        }

        if (unlikely(used > (uint64_t)1000 * 1000 * (gloconf.rpc_timeout * 2))) {
                __schedule_backtrace__(schedule->name, schedule->id, taskctx->id, schedule->scan_seq);

                if (unlikely(used > (uint64_t)1000 * 1000 * (gloconf.rpc_timeout * 2))) {
                        DERROR("%s[%u][%u] %s.%s wait %fs, total %lu, retval %u\n",
                              schedule->name, schedule->id, taskctx->id,
                              taskctx->name, taskctx->wait_name, (double)used / (1000 * 1000),
                              gettime() - taskctx->ctime.tv_sec, taskctx->retval);
                } else if (unlikely(used > (uint64_t)1000 * 1000 * (gloconf.rpc_timeout * 1))) {
                        DWARN("%s[%u][%u] %s.%s wait %fs, total %lu, retval %u\n",
                              schedule->name, schedule->id, taskctx->id,
                              taskctx->name, taskctx->wait_name, (double)used / (1000 * 1000),
                              gettime() - taskctx->ctime.tv_sec, taskctx->retval);
                } else {
                        DINFO("%s[%u][%u] %s.%s wait %fs, total %lu, retval %u\n",
                              schedule->name, schedule->id, taskctx->id,
                              taskctx->name, taskctx->wait_name, (double)used / (1000 * 1000),
                              gettime() - taskctx->ctime.tv_sec, taskctx->retval);
                }
        }
}

static inline void __schedule_check_scan_used(schedule_t *schedule, taskctx_t *taskctx, uint64_t time_used)
{
        /* notice: time_used is seconds */
        if (unlikely(taskctx->sleeping
                     || time_used < (uint64_t)(gloconf.lease_timeout / 2 + 1))) {
                return;
        }
        
        if (unlikely((taskctx->wait_tmo != -1 && time_used > (uint64_t)taskctx->wait_tmo))
                        || time_used > (uint64_t)gloconf.rpc_timeout) {
                DINFO("%s[%u][%u] %s status %u time_used %lus\n", schedule->name,
                                schedule->id, taskctx->id, taskctx->name, taskctx->state, time_used);

                if (!taskctx->sleeping)
                        __schedule_backtrace_set(taskctx);
        } else if (time_used > (uint64_t)gloconf.rpc_timeout / 2) {
                DWARN("%s[%u][%u] %s status %u time_used %lus\n", schedule->name,
                                schedule->id, taskctx->id, taskctx->name, taskctx->state, time_used);
        } else if (time_used > 1) {
                DBUG("%s[%u][%u] %s status %u time_used %lus\n", schedule->name, schedule->id, taskctx->id,
                                taskctx->name, taskctx->state, time_used);
        }
}

int schedule_suspend()
{
        schedule_t *schedule = schedule_self();
        if (schedule == NULL)
                return 0;

        if (schedule->running_task == -1 && !schedule->suspendable)
                return 1;
        else
                return 0;
}

#if SCHEDULE_RUNNING_TASK_LIST
static int __schedule_task_hasfree(schedule_t *schedule)
{
        return !list_empty(&schedule->free_task_list);
}
#else
static int __schedule_task_hasfree(schedule_t *schedule)
{
        int idx = 0, i;
        taskctx_t *tasks;

        if (likely(schedule == NULL)) {//run in temporary worker;
                return 1;
        }

        tasks = schedule->tasks;

        if (likely(schedule->size)) {
                for (i = 0; i < schedule->size; ++i) {
                        idx = (i + schedule->cursor) % schedule->size;
                        if (__schedule_isfree(&tasks[idx]))
                                break;
                }

                if (i == TASK_MAX) {
                        return 0;
                }
        }

        return 1;
}
#endif

static void __schedule_wait_task_resume(schedule_t *schedule)
{
        wait_task_t *wait_task;

        if (list_empty(&schedule->wait_task.list)) {
                return;
        }

        if (!__schedule_task_hasfree(schedule)) {
                return;
        }

        wait_task = (void *)schedule->wait_task.list.next;
        count_list_del(&wait_task->hook, &schedule->wait_task);

        DBUG("resume wait task %s\n", wait_task->name);

        __schedule_task_new(wait_task->name, wait_task->func, wait_task->arg, -1, &wait_task->parent, -1);
        yfree((void **)&wait_task);
}

static void __schedule_trampoline(taskctx_t *taskctx)
{
        schedule_t *schedule = taskctx->schedule;
#if SCHEDULE_CHECK_RUNTIME
        struct timeval now;
        uint64_t used;
#endif

        //ANALYSIS_BEGIN(0);

#if ENABLE_SCHEDULE_DEBUG
        DINFO("start task[%u] %s\n", taskctx->id, taskctx->name);
#else
        DBUG("start task[%u] %s\n", taskctx->id, taskctx->name);
#endif

#if SCHEDULE_CHECK_RUNTIME
        _gettimeofday(&taskctx->rtime, NULL);
#endif
        YASSERT(schedule->running_task != -1);

        taskctx->func(taskctx->arg);

#if ENABLE_SCHEDULE_DEBUG
        DINFO("finish task[%u] %s\n", taskctx->id, taskctx->name);
#else
        DBUG("finish task[%u] %s\n", taskctx->id, taskctx->name);
#endif

#if SCHEDULE_CHECK_RUNTIME
        _gettimeofday(&now, NULL);
        used = _time_used(&taskctx->rtime, &now);
        __schedule_check_running_used(schedule, taskctx, used);
#endif

        //ANALYSIS_QUEUE(0, IO_WARN, taskctx->name);

        __schedule_wait_task_resume(schedule);

        DBUG("free task %s, id [%u][%u]\n", taskctx->name, schedule->id, taskctx->id);

#if ENABLE_SCHEDULE_LOCK_CHECK
        YASSERT(taskctx->lock_count == 0);
        YASSERT(taskctx->ref_count == 0);
#endif

        //taskctx->fingerprint_prev = taskctx->fingerprint;
        taskctx->fingerprint = 0;
        taskctx->parent.scheduleid = -1;
        taskctx->parent.taskid = -1;
        taskctx->parent.fingerprint = 0;
        taskctx->func = NULL;
        taskctx->arg = NULL;
        taskctx->state = TASK_STAT_FREE;
        schedule->task_count--;
        YASSERT(schedule->task_count >= 0);
        list_del(&taskctx->running_hook);
        /* list_add better than list_add_tail here, otherwise statck will malloc for each task */
        list_add(&taskctx->running_hook, &schedule->free_task_list);

#ifdef NEW_SCHED
        swapcontext1(&taskctx->ctx, &taskctx->main);
#endif
}

#ifdef NEW_SCHED
static void __schedule_makecontext(schedule_t *schedule, taskctx_t *taskctx)
{
        (void) schedule;
        char *stack_top = (char *)taskctx->stack +  DEFAULT_STACK_SIZE;
        void **stack = NULL;
        stack = (void **)stack_top;

        stack[-3] = NULL;
        taskctx->ctx.rdi = (void *)taskctx;
        taskctx->ctx.rsp = (void *) (stack_top - (4 * sizeof(void *)));
        taskctx->ctx.rbp = (void *) (stack_top - (3 * sizeof(void *)));
        taskctx->ctx.rip = (void *)__schedule_trampoline;
}
#else
static void __schedule_makecontext(schedule_t *schedule, taskctx_t *taskctx)
{
        (void) schedule;
        getcontext(&(taskctx->ctx));

        taskctx->ctx.uc_stack.ss_sp = taskctx->stack;
        taskctx->ctx.uc_stack.ss_size = DEFAULT_STACK_SIZE;
        taskctx->ctx.uc_stack.ss_flags = 0;
        taskctx->ctx.uc_link = &(taskctx->main);
        makecontext(&(taskctx->ctx), (void (*)(void))(__schedule_trampoline), 1, taskctx);
}
#endif

static void __schedule_exec_queue(schedule_t *schedule, taskctx_t *taskctx, int retval, buffer_t *buf)
{
        YASSERT(retval <= INT32_MAX);
        taskctx->retval = retval;
        taskctx->state = TASK_STAT_RUNNABLE;

        mbuffer_init(&taskctx->buf, 0);
        if (buf && buf->len) {
                mbuffer_merge(&taskctx->buf, buf);
        }

        count_list_add_tail(&taskctx->hook, &schedule->runable[taskctx->priority]);
}

static void __schedule_exec__(schedule_t *schedule, taskctx_t *taskctx)
{
#if SCHEDULE_CHECK_RUNTIME
        struct timeval now;
        uint64_t used;
#endif

#if 0
        ANALYSIS_BEGIN(0);
#endif

        YASSERT(taskctx->state != TASK_STAT_FREE && taskctx->state != TASK_STAT_RUNNING);
        YASSERT(schedule->running_task == -1);
        schedule->running_task = taskctx->id;
        taskctx->state = TASK_STAT_RUNNING;

        DBUG("swap in task[%u] %s\n", taskctx->id, taskctx->name);

#if SCHEDULE_CHECK_RUNTIME
        _gettimeofday(&taskctx->rtime, NULL);
#endif

        swapcontext1(&(taskctx->main), &(taskctx->ctx));

#if SCHEDULE_CHECK_RUNTIME
        _gettimeofday(&now, NULL);
        used = _time_used(&taskctx->rtime, &now);
        __schedule_check_running_used(schedule, taskctx, used);
#endif

        schedule->running_task = -1;

#if 0
        char *name, _name[MAX_NAME_LEN];
        name = _name;
        snprintf(name, MAX_NAME_LEN, "schd:%s", taskctx->name);
        ANALYSIS_QUEUE(0, 1000 * 10, name);
#endif
        //ANALYSIS_ASSERT(0, 1000 * 10, name);
}

void schedule_stack_assert(schedule_t *_schedule)
{
        schedule_t *schedule = __schedule_self(_schedule);
        taskctx_t *taskctx;

        if (unlikely(!(schedule && schedule->running_task != -1))) {
                return;
        }

        taskctx = &schedule->tasks[schedule->running_task];
        YASSERT(taskctx->state == TASK_STAT_RUNNING);

        // ?
        DBUG("size %u\n", (int)((uint64_t)&taskctx - (uint64_t)taskctx->stack));
        YASSERT((int)((uint64_t)&taskctx - (uint64_t)taskctx->stack) > DEFAULT_STACK_SIZE / 4);

#if 0
        int i;
        for (i = 0; i < KEEP_STACK_SIZE; i++) {
                if (unlikely(*(char *)(taskctx->stack + i) != 0)) {
                        YASSERT(0);
                }
        }
#else
        //堆向上 栈向下
        YASSERT(!memcmp(taskctx->stack, zerobuf, KEEP_STACK_SIZE));
#endif
}

static inline void __schedule_backtrace__(const char *name, int id, int idx, uint32_t seq)
{
        char info[MAX_INFO_LEN];

        snprintf(info, MAX_INFO_LEN, "%s[%u][%u] seq[%u]", name, id, idx, seq);
        _backtrace(info);
}


static void __schedule_backtrace_exec(schedule_t *schedule, taskctx_t *taskctx,
                                      const char *name, void *opaque, func_t func)
{
        int used = gettime() - taskctx->wait_begin;

        __schedule_backtrace__(schedule->name, schedule->id, taskctx->id, schedule->scan_seq);

        if (func && taskctx->wait_tmo != -1 && used > taskctx->wait_tmo) {
                func(opaque);
        }

        YASSERT(gettime() -  taskctx->wait_begin < gloconf.rpc_timeout * 6 * 1000 * 1000);
        
        DINFO("%s[%u][%u] %s.%s wait %ds, total %lu s\n",
              schedule->name, schedule->id, taskctx->id,
              taskctx->name, name, used,
              gettime() - taskctx->ctime.tv_sec);

#if 0
        if (!strcmp(name, "rdplock") || !strcmp(name, "wrplock")) {
                plock_t *rwlock = opaque;
#if ENABLE_LOCK_DEBUG
                DWARN("lock %p, writer %d, readers %u count %u, write locked, last %u\n", rwlock,
                      rwlock->writer, rwlock->readers, rwlock->count, rwlock->last_unlock);
#else
                DWARN("lock %p, writer %d, readers %u, write locked\n", rwlock,
                      rwlock->writer, rwlock->readers);
#endif
        }
#endif

#if ENABLE_SCHEDULE_LOCK_CHECK
        if ((taskctx->lock_count || taskctx->ref_count)
            && used > gloconf.rpc_timeout * 1 * 1000 * 1000) {
                DERROR("%s[%u][%u] %s.%s wait %ds, total %lu s, "
                       "retval %u, lock %u, ref %u\n",
                       schedule->name, schedule->id, taskctx->id,
                       taskctx->name, name, used,
                       gettime() - taskctx->ctime.tv_sec, taskctx->retval,
                       taskctx->lock_count, taskctx->ref_count);
        }
#endif
}


/**
 * 记录了任务执行时间.
 *
 * @param name
 * @param buf
 * @param opaque
 * @param func
 * @param _tmo
 * @return
 */
int IO_FUNC schedule_yield1(const char *name, buffer_t *buf, void *opaque, func_t func, int _tmo)
{
        schedule_t *schedule = schedule_self();
        taskctx_t *taskctx;
#if SCHEDULE_CHECK_RUNTIME
        struct timeval now;
#endif
        struct timeval t1, t2;
        uint64_t used;

        YASSERT(_tmo < 1000);

#if ENABLE_SCHEDULE_STACK_ASSERT
        schedule_stack_assert(schedule);
#endif

        YASSERT(schedule->running_task != -1);
        _gettimeofday(&t1, NULL);

        taskctx = &schedule->tasks[schedule->running_task];
        YASSERT(taskctx->state == TASK_STAT_RUNNING);
        taskctx->pre_yield = 0;
        taskctx->state = TASK_STAT_SUSPEND;
        taskctx->wait_begin = gettime();
        taskctx->wait_tmo = _tmo;
        taskctx->wait_name = name;
        taskctx->wait_opaque = opaque;
        taskctx->step++;

retry:
#if SCHEDULE_CHECK_RUNTIME
        _gettimeofday(&now, NULL);
        used = _time_used(&taskctx->rtime, &now);
        __schedule_check_running_used(schedule, taskctx, used);
#endif

#if ENABLE_SCHEDULE_DEBUG
        DINFO("swap out task[%u] %s yield @ %s\n", taskctx->id, taskctx->name, name);
#endif

        // 交错执行scheduler和task代码，现在切换回调度器
        swapcontext1(&(taskctx->ctx), &(taskctx->main));

#if ENABLE_SCHEDULE_DEBUG
        DINFO("swap in task[%u] %s yield @ %s\n", taskctx->id, taskctx->name, name);
#endif

#if SCHEDULE_CHECK_RUNTIME
        _gettimeofday(&taskctx->rtime, NULL);
#endif

#if 1
        if (unlikely(taskctx->state == TASK_STAT_BACKTRACE)) {
                __schedule_backtrace_exec(schedule, taskctx, name,  opaque, func);
                goto retry;
        }
#endif

        DBUG("task[%u] %s return @ %s\n", taskctx->id, taskctx->name, name);

        YASSERT(schedule == schedule_self());
        taskctx->wait_name = NULL;
        taskctx->wait_opaque = NULL;
        YASSERT(schedule->running_task != -1);
        
        __schedule_fingerprint_new(schedule, taskctx);

        if (taskctx->buf.len) {
                YASSERT(buf);
                mbuffer_merge(buf, &taskctx->buf);
        }

        // 任务等待时间，过大说明调度器堵塞，或在此期间别的被调度任务堵塞
        // 如write等同步过程，不运行出现在调度器循环里

        _gettimeofday(&t2, NULL);
        used = _time_used(&t1, &t2);
        __schedule_check_yield_used(schedule, taskctx, used);

        return taskctx->retval;
}

int IO_FUNC schedule_yield(const char *name, buffer_t *buf, void *opaque)
{
        return schedule_yield1(name, buf, opaque, NULL, -1);
}

static void  __schedule_backtrace_set(taskctx_t *taskctx)
{
        schedule_t *schedule = taskctx->schedule;

        DBUG("run task %s, id %u\n", taskctx->name, taskctx->id);

        YASSERT(schedule->running_task == -1);

        if (taskctx->state == TASK_STAT_SUSPEND) {
                taskctx->state = TASK_STAT_BACKTRACE;

                swapcontext1(&(taskctx->main), &(taskctx->ctx));

                taskctx->state = TASK_STAT_SUSPEND;
        }
}

#if 0
static void  __schedule_timeout(taskctx_t *taskctx)
{
        int ret;
        schedule_t *schedule = taskctx->schedule;

        DBUG("run task %s, id %u\n", taskctx->name, taskctx->id);

        YASSERT(schedule->running_task == -1);

        if (taskctx->state == SUSPEND) {
                taskctx->state = TIMEOUT;
                swapcontext(&(taskctx->main), &(taskctx->ctx));
        }
}
#endif

static int __schedule_exec(schedule_t *schedule, const task_t  *task, int retval, buffer_t *buf, int warn)
{
        int ret;
        taskctx_t *taskctx;

        YASSERT(task->taskid >= 0 && task->taskid < TASK_MAX);
        taskctx = &schedule->tasks[task->taskid];

        DBUG("run task %s, id [%u][%u]\n", taskctx->name, schedule->id, taskctx->id);

        // TODO core
        YASSERT(task->fingerprint);
        if (unlikely(task->fingerprint != taskctx->fingerprint)) {
                DERROR("run task[%u] %s already destroyed\n", taskctx->id, taskctx->name);
                ret = ESTALE;
                GOTO(err_ret, ret);
        }

        if (unlikely(warn)) {
                DBUG("run task[%u] %s wait %s resume remote\n", taskctx->id,
                      taskctx->name, taskctx->wait_name);
        }

        YASSERT(taskctx->state == TASK_STAT_SUSPEND);
        __schedule_exec_queue(schedule, taskctx, retval, buf);

        return 0;
err_ret:
        return ret;
}

/** called before yield
 *
 * @return
 */
task_t schedule_task_get()
{
        task_t taskid;
        schedule_t *schedule = schedule_self();
        taskctx_t *taskctx;

        YASSERT(schedule);
        YASSERT(schedule->running_task != -1);
        taskctx = &schedule->tasks[schedule->running_task];
        YASSERT(taskctx->state == TASK_STAT_RUNNING);
        YASSERT(taskctx->pre_yield == 0);
        taskctx->pre_yield = 1;

        __schedule_fingerprint_new(schedule, taskctx);

        taskid.scheduleid = schedule->id;
        taskid.taskid = schedule->running_task;
        taskid.fingerprint = taskctx->fingerprint;
        YASSERT(taskid.fingerprint);

        return taskid;
}

void schedule_task_sleep(task_t *task)
{
        schedule_t *schedule = schedule_self();
        taskctx_t *taskctx;

        YASSERT(schedule);
        taskctx = &schedule->tasks[task->taskid];
        YASSERT(taskctx->sleeping == 0);
        taskctx->sleeping = 1;
}

void schedule_task_wakeup(task_t *task)
{
        schedule_t *schedule = __schedule_array__[task->scheduleid];
        taskctx_t *taskctx;

        YASSERT(schedule);
        taskctx = &schedule->tasks[task->taskid];
        YASSERT(taskctx->sleeping == 1);
        taskctx->sleeping = 0;
}

void schedule_task_reset()
{
        schedule_t *schedule = schedule_self();
        taskctx_t *taskctx;

        YASSERT(schedule);
        YASSERT(schedule->running_task != -1);
        taskctx = &schedule->tasks[schedule->running_task];
        YASSERT(taskctx->state == TASK_STAT_RUNNING);
        YASSERT(taskctx->pre_yield == 1);
        taskctx->pre_yield = 0;
}

inline int schedule_taskid()
{
        schedule_t *schedule = schedule_self();

        YASSERT(schedule);
        return schedule->running_task;
}

inline int schedule_stat(int *sid, int *taskid, int *rq, int *runable, int *wait, int *count)
{
        schedule_t *schedule = schedule_self();
        if (schedule) {
                *sid = schedule->id;
                *rq = schedule->request_queue.count;
                *runable = __schedule_runable(schedule);
                *wait = schedule->wait_task.count;
                *taskid = schedule->running_task;
                *count = schedule->task_count;
        } else {
                *sid = 0;
                *rq = 0;
                *taskid = 0;
                *runable = 0;
                *wait = 0;
                *count = 0;
        }

        return 0;
}

inline int schedule_running()
{
        schedule_t *schedule = schedule_self();

        if (schedule == NULL)
                return 0;

        if (schedule->running_task == -1)
                return 0;

        return 1;
}

inline int schedule_status()
{
        schedule_t *schedule = schedule_self();

        if (schedule == NULL)
                return SCHEDULE_STATUS_NONE;

        if (schedule->running_task == -1)
                return SCHEDULE_STATUS_IDLE;

        return SCHEDULE_STATUS_RUNNING;
}

inline static void __schedule_task_stm(job_t *job, int *keep)
{
        schedule_task_ctx_t *ctx;

        *keep = 0;

        ctx = job->context;

        YASSERT(__running_job__ == NULL);
        __running_job__ = job;
        ctx->func(ctx->arg);
        __running_job__ = NULL;

        //yfree((void **)&ctx-arg);
}

static int  __schedule_task2job(const char *name, func_t func, void *arg)
{
        (void) name;
        (void) func;
        (void) arg;

        UNIMPLEMENTED(__DUMP__);

        return 0;
#if 0
        int ret;
        job_t *job;
        schedule_task_ctx_t *ctx;

        ret = job_create(&job, &jobtracker, name);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = job_context_create(job, sizeof(*ctx));
        if (unlikely(ret))
                GOTO(err_job, ret);

        job->state_machine = __schedule_task_stm;
        ctx = job->context;
        ctx->arg = arg;
        ctx->func = func;

        job_exec(job_handler(job, 0), 0, EXEC_INDIRECT);

        return 0;
err_job:
        job_destroy(job);
err_ret:
        return ret;
#endif
}

/** append to schedule->wait_task
 *
 * @param name
 * @param func
 * @param arg
 * @return
 */
static int  __schedule_wait_task(const char *name, func_t func, void *arg, task_t *parent)
{
        int ret;
        wait_task_t *wait_task;
        schedule_t *schedule = schedule_self();

        DWARN("wait task %s\n", name);

        ret = ymalloc((void **)&wait_task, sizeof(*wait_task));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        wait_task->arg = arg;
        wait_task->func = func;
        wait_task->parent = *parent;
        strcpy(wait_task->name, name);

        count_list_add_tail(&wait_task->hook, &schedule->wait_task);

        return 0;
err_ret:
        return ret;
}

static inline void __schedule_fingerprint_new(schedule_t *schedule, taskctx_t *taskctx)
{
	(void)schedule;
	taskctx->fingerprint++;
        
	/*int rand, retry = 0;
        while (1) {
                rand = ++schedule->sequence;
                if (taskctx->fingerprint_prev != (uint32_t)rand && rand != 0) {
                        taskctx->fingerprint = rand;
                        break;
                } else {
                        YASSERT(retry < 100);
                        retry++;
                }
        } */
}

static inline int __schedule_isfree(taskctx_t *taskctx)
{
        if (taskctx->state == TASK_STAT_FREE) {
                return 1;
        } else
                return 0;
}

int schedule_priority(schedule_t *_schedule)
{
        schedule_t *schedule;

        schedule = _schedule ? _schedule : schedule_self();

        if (schedule == NULL)
                return SCHEDULE_PRIORITY0;

        if (schedule->running_task == -1) {
                return SCHEDULE_PRIORITY0;
        }

        return schedule->tasks[schedule->running_task].priority;
}
        

static taskctx_t *__schedule_task_new(const char *name, func_t func, void *arg,
                                      int timeout, task_t *_parent, int _priority)
{
        int ret, idx = 0, i, priority;
        schedule_t *schedule = schedule_self();
        taskctx_t *taskctx, *tasks;
        task_t parent;
        //task_t *task, _task;

        priority = _priority != -1 ?
                _priority : schedule_priority(schedule);

        DBUG("create task %s priority %u\n", name, priority);
        
        YASSERT(priority >= SCHEDULE_PRIORITY0 && priority < SCHEDULE_PRIORITY_MAX);
        YASSERT(timeout == -1 || (timeout > 0 && timeout < 20));

        //ANALYSIS_BEGIN(0);

        if (unlikely(schedule == NULL)) {//run in temporary worker;
                ret = __schedule_task2job(name, func, arg);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

                return NULL;
        }

        YASSERT(schedule);
        //YASSERT(schedule->running);
        tasks = schedule->tasks;

        if (unlikely(_parent)) {
                parent = *_parent;
        } else {
                schedule_task_given(&parent);
        }

#if SCHEDULE_RUNNING_TASK_LIST
        (void) i;
        (void) tasks;
        if (list_empty(&schedule->free_task_list)) {
                ret = __schedule_wait_task(name, func, arg, &parent);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

                return NULL;
        } else {
                taskctx = list_entry(schedule->free_task_list.next, taskctx_t, running_hook);

                list_del(&taskctx->running_hook);
        }

        if (taskctx->stack == NULL) {
                if (likely(schedule->private_mem)) {
                        taskctx->stack = schedule->stack_addr + taskctx->id * DEFAULT_STACK_SIZE;
                } else {
                        ret = ymalloc((void **)&taskctx->stack, DEFAULT_STACK_SIZE);
                        if (unlikely(ret))
                                UNIMPLEMENTED(__DUMP__);
                }

                memset(taskctx->stack, 0x0, KEEP_STACK_SIZE);
        }
#else
        if (likely(schedule->size)) {
                for (i = 0; i < schedule->size; ++i) {
                        idx = (i + schedule->cursor) % schedule->size;
                        if (__schedule_isfree(&tasks[idx]))
                                break;
                }

                DBUG("id %u size %u\n", idx, schedule->size);

                if (unlikely(i == TASK_MAX)) {
                        ret = __schedule_wait_task(name, func, arg, &parent);
                        if (unlikely(ret))
                                UNIMPLEMENTED(__DUMP__);

                        return NULL;
                }
        } else {
                idx = 0;
                i = 0;
        }

        if (unlikely(i == schedule->size)) {
                taskctx = &(tasks[schedule->size]);

                if (likely(schedule->private_mem)) {
                        taskctx->stack = schedule->stack_addr + taskctx->id * DEFAULT_STACK_SIZE;
                        YASSERT(i == taskctx->id);
                } else {
                        ret = ymalloc((void **)&taskctx->stack, DEFAULT_STACK_SIZE);
                        if (unlikely(ret))
                                UNIMPLEMENTED(__DUMP__);
                }

                memset(taskctx->stack, 0x0, KEEP_STACK_SIZE);

                DBUG("cursor %u, size %u, retry %u\n", schedule->cursor, schedule->size, i);
                idx = schedule->size;
                schedule->size++;
        }

        taskctx = &(tasks[idx]);
#endif

        DBUG("%s\n", name);
        strcpy(taskctx->name, name);
        taskctx->state = TASK_STAT_RUNNABLE;
        taskctx->func = func;
        taskctx->arg = arg;
        taskctx->step = 0;
        taskctx->pre_yield = 0;
        taskctx->sleeping = 0;
        taskctx->parent = parent;
        taskctx->wait_begin = 0;
        taskctx->wait_tmo = 0;
        taskctx->schedule = schedule;
        taskctx->priority = priority;
        schedule->task_count++;

        list_add_tail(&taskctx->running_hook, &schedule->running_task_list);

#if ENABLE_SCHEDULE_LOCK_CHECK
        taskctx->lock_count = 0;
        taskctx->ref_count = 0;
#endif

        DBUG("new task[%d] %s count:%d\n", idx, name, schedule->task_count);

        __schedule_fingerprint_new(schedule, taskctx);

        _gettimeofday(&taskctx->ctime, NULL);

#if SCHEDULE_RUNNING_TASK_LIST
#else
        schedule->cursor = (idx + 1) % schedule->size;

        if (i > schedule->size / 2) {
                DBUG("cursor %u, size %u, retry %u\n", schedule->cursor, schedule->size, i);
        }
#endif

        count_list_add_tail(&taskctx->hook, &schedule->runable[taskctx->priority]);

        __schedule_makecontext(schedule, taskctx);

        //ANALYSIS_QUEUE(0, 1000 * 100, "schedule_task_new");
        return taskctx;
}

static void __schedule_task_run_now(schedule_t *schedule, taskctx_t *newtask)
{
        taskctx_t *taskctx;

        taskctx = &schedule->tasks[schedule->running_task];
        taskctx->state = TASK_STAT_SUSPEND;

        schedule->running_task = -1;

        __schedule_exec__(schedule, newtask);

        schedule->running_task = taskctx->id;
        taskctx->state = TASK_STAT_RUNNING;
}

void schedule_task_new1(const char *name, func_t func, void *arg, int priority)
{
        schedule_t *schedule = schedule_self();
        taskctx_t *newtask;
        
        newtask =  __schedule_task_new(name, func, arg, -1, NULL, priority);

        if (newtask == NULL)
                return;

        count_list_t *list = &schedule->runable[newtask->priority];
        count_list_del(&newtask->hook, list);
        __schedule_task_run_now(schedule, newtask);

#if 0
        taskctx_t *taskctx;
        schedule_t *schedule = schedule_self();
        count_list_t *list = &schedule->runable[newtask->priority];

        taskctx = &schedule->tasks[schedule->running_task];
        taskctx->state = TASK_STAT_SUSPEND;

        schedule->running_task = -1;

        count_list_del(&newtask->hook, list);
        __schedule_exec__(schedule, newtask);

        schedule->running_task = taskctx->id;
        taskctx->state = TASK_STAT_RUNNING;
#endif
}

void schedule_task_new(const char *name, func_t func, void *arg, int priority)
{
        (void) __schedule_task_new(name, func, arg, -1, NULL, priority);
}

#ifdef ENABLE_SCHEDULE_RUN
#else
#endif

static int __schedule_create__(schedule_t **_schedule, const char *name, int idx, void *private_mem, int *_eventfd)
{
        int ret, fd, i;
        schedule_t *schedule;
        taskctx_t *taskctx;

#if 1
        (void) private_mem;
        
        ret = ymalloc((void **)&schedule, sizeof(*schedule));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        memset(schedule, 0x0, sizeof(*schedule));
        
#else
        if (private_mem) {
                schedule = (schedule_t *) register_private_static_stor_area(private_mem,
                                                                            sizeof(schedule_t),
                                                                            VARIABLE_SCHEDULE);
        	memset(schedule, 0x0, sizeof(*schedule));
                schedule->private_mem = private_mem;
                schedule->stack_addr = mempages_alloc(private_mem, TASK_MAX * DEFAULT_STACK_SIZE);
                if (unlikely(!schedule->stack_addr)) {
                        ret = ENOMEM;
                        GOTO(err_ret, ret);
                }

                DINFO("schedule stack size: %d, mem hugepage count: %d\n", TASK_MAX * DEFAULT_STACK_SIZE,
                      TASK_MAX * DEFAULT_STACK_SIZE / MEMPAGE_SIZE);
                //     core_register_tls(VARIABLE_SCHEDULE, (void *)schedule);
        } else {
                ret = ymalloc((void **)&schedule, sizeof(*schedule));
                if (unlikely(ret))
                        GOTO(err_ret, ret);

	        memset(schedule, 0x0, sizeof(*schedule));

        }
#endif

        if (_eventfd) {
                fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
                if (fd < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                *_eventfd = fd;
        } else {
                fd = -1;
        }

        ret = sy_spin_init(&schedule->request_queue.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

#if SCHEDULE_REPLY_NEW
        ret = sy_spin_init(&schedule->reply_remote_lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        INIT_LIST_HEAD(&schedule->reply_remote_list);
#else

        ret = sy_spin_init(&schedule->reply_remote.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif
        
        schedule->running_task = -1;
        schedule->task_count = 0;
        schedule->id = idx;
        schedule->eventfd = fd;
        schedule->suspendable = 0;
        strcpy(schedule->name, name);

#if 1
        ret = ymalloc((void **)&taskctx, sizeof(*taskctx) * TASK_MAX);
        if (unlikely(ret))
                GOTO(err_ret, ret);
#else
        if (private_mem) {
                int count = 0;
                count = (sizeof(*taskctx) * TASK_MAX) / MEMPAGE_SIZE;
                if ((sizeof(*taskctx) * TASK_MAX) % MEMPAGE_SIZE) {
                        count = count + 1;
                }
                DINFO("taskctx size: %lu, mem hugepage count: %d\n", sizeof(*taskctx), count);
                taskctx = mempages_alloc(private_mem, count * MEMPAGE_SIZE);
        } else {

                ret = ymalloc((void **)&taskctx, sizeof(*taskctx) * TASK_MAX);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }
#endif

        YASSERT(taskctx);
        memset(taskctx, 0x0, sizeof(*taskctx) * TASK_MAX);

        INIT_LIST_HEAD(&schedule->running_task_list);
        INIT_LIST_HEAD(&schedule->free_task_list);

        for (i = 0; i < TASK_MAX; ++i) {
                taskctx[i].id = i;
                taskctx[i].stack = NULL;
                taskctx[i].state = TASK_STAT_FREE;
                list_add_tail(&taskctx[i].running_hook, &schedule->free_task_list);
        }

        count_list_init(&schedule->wait_task);
        for (i = 0; i < SCHEDULE_PRIORITY_MAX; i++) {
                count_list_init(&schedule->runable[i]);
        }

        schedule->tasks = taskctx;
        schedule->running = 1;
        schedule->size = TASK_MAX;

        variable_set(VARIABLE_SCHEDULE, schedule);
        if (_schedule)
                *_schedule = schedule;

        return 0;
err_ret:
        return ret;
}

static void __schedule_destroy(schedule_t *schedule)
{
        reply_queue_t *reply_local = &schedule->reply_local;
        request_queue_t *request_queue = &schedule->request_queue;
        taskctx_t *taskctx, *tasks = schedule->tasks;

#if SCHEDULE_REPLY_NEW
#else
        reply_queue_t *reply_remote = &schedule->reply_remote;
        
        if (reply_remote->replys)
                yfree((void **)&reply_remote->replys);
#endif

        if (reply_local->replys)
                yfree((void **)&reply_local->replys);
        if (request_queue->requests)
                yfree((void **)&request_queue->requests);

        YASSERT(list_empty(&schedule->wait_task.list));
#if SCHEDULE_RUNNING_TASK_LIST
        YASSERT(list_empty(&schedule->running_task_list));
        list_for_each_entry_safe(taskctx, tasks, &schedule->free_task_list, running_hook) {
                YASSERT(taskctx->state == TASK_STAT_FREE);
                if (taskctx->stack) {
                        if (schedule->private_mem == NULL)
                                yfree((void **)&taskctx->stack);
                        else
                                taskctx->stack = NULL;
                }
        }
#else
        int i;
        for (i = 0; i < schedule->size; ++i) {
                taskctx = &tasks[i];
                YASSERT(taskctx->state == FREE);
                if (schedule->private_mem == NULL)
                        yfree((void **)&schedule->tasks[i].stack);
                else
                        schedule->tasks[i].stack = NULL;
        }
#endif

        variable_unset(VARIABLE_SCHEDULE);
        
        close(schedule->eventfd);

        if (schedule->private_mem == NULL) {
                yfree((void **)&schedule->tasks);
                yfree((void **)&schedule);
        }
}

static int __schedule_create(int *eventfd, const char *name, int idx, schedule_t **_schedule, void *private_mem)
{
        int ret;
        schedule_t *schedule = NULL;

        YASSERT(__schedule_array__);

        ret = sy_spin_lock(&__schedule_array_lock__);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (__schedule_array__[idx]) {
                ret = EEXIST;
                goto err_lock;
        }

        DINFO("create schedule[%d] name %s\n", idx, name);

        if (gloconf.solomode) {
                (void)private_mem;
                ret = __schedule_create__(&schedule, name, idx, NULL, eventfd);
        } else {
                ret = __schedule_create__(&schedule, name, idx, private_mem, eventfd);
        }
        if (unlikely(ret))
                GOTO(err_lock, ret);

        __schedule_array__[idx] = schedule;
        if (_schedule)
                *_schedule = schedule;

        sy_spin_unlock(&__schedule_array_lock__);

        return 0;
err_lock:
        sy_spin_unlock(&__schedule_array_lock__);
err_ret:
        return ret;
}

int schedule_create(int *eventfd, const char *name, int *idx, schedule_t **_schedule, void *private_mem)
{
        int ret, i;

        if (!private_mem) {
                ret = variable_newthread();
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        for (i = 0; i < SCHEDULE_MAX; i++) {
                ret = __schedule_create(eventfd, name, i, _schedule, private_mem);
                if (unlikely(ret)) {
                        if (ret == EEXIST)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }

                if (idx)
                        *idx = i;
                break;
        }

        return 0;
err_ret:
        return ret;
}

static int __schedule_request_queue_finished(schedule_t *_schedule)
{
        const schedule_t *schedule = __schedule_self(_schedule);

        if (schedule->request_queue.count == 0)
                DBUG("retval finished %u\n", schedule->request_queue.count);

        return !schedule->request_queue.count;
}

static int __schedule_task_finished(schedule_t *_schedule)
{
        const schedule_t *schedule = __schedule_self(_schedule);

        return !__schedule_runable(schedule);
}

static int __schedule_reply_remote_finished(schedule_t *_schedule)
{
        const schedule_t *schedule = __schedule_self(_schedule);

#if SCHEDULE_REPLY_NEW
        return list_empty(&schedule->reply_remote_list);
#else
        if (schedule->reply_remote.count == 0)
                DBUG("retval finished %u\n", schedule->reply_remote.count);


        return !schedule->reply_remote.count;
#endif
}

static int __schedule_reply_local_finished(schedule_t *_schedule)
{
        const schedule_t *schedule = __schedule_self(_schedule);

        if (schedule->reply_local.count == 0)
                DBUG("retval finished %u\n", schedule->reply_local.count);

        return !schedule->reply_local.count;
}


int schedule_finished(schedule_t *schedule)
{
        if (__schedule_request_queue_finished(schedule) == 0)
                return 0;
        else if (__schedule_reply_local_finished(schedule) == 0)
                return 0;
        else if (__schedule_reply_remote_finished(schedule) == 0)
                return 0;
        else if (__schedule_task_finished(schedule) == 0)
                return 0;

        return 1;
}

static taskctx_t * __schedule_task_pop(schedule_t *schedule)
{
        int i;
        count_list_t *list;
        taskctx_t *taskctx;

        for (i = 0; i < SCHEDULE_PRIORITY_MAX; i++) {
                list = &schedule->runable[i];

                if (!list_empty(&list->list)) {
                        taskctx = (void *)list->list.next;
                        count_list_del(&taskctx->hook, list);

                        DBUG("run task %s, priority %u\n", taskctx->name, i);

                        return taskctx;
                }
        }

        return NULL;
}


static int __schedule_task_run(schedule_t *_schedule)
{
        int count = 0;
        schedule_t *schedule = __schedule_self(_schedule);
        taskctx_t *taskctx;

        while (1) {
                taskctx = __schedule_task_pop(schedule);
                if (taskctx == NULL) {
                        break;
                }

                count++;
                YASSERT(taskctx->state == TASK_STAT_RUNNABLE);
                __schedule_exec__(schedule, taskctx);
        }

        return count;
}

void IO_FUNC schedule_task_run(schedule_t *_schedule)
{
        __schedule_task_run(_schedule);
}

static void __schedule_request_queue_run(schedule_t *_schedule)
{
        int ret, count, i;
        schedule_t *schedule = __schedule_self(_schedule);
        request_queue_t *request_queue = &schedule->request_queue;
        request_t _request[REQUEST_QUEUE_MAX], *request;

        if (request_queue->count) {
                ret = sy_spin_lock(&request_queue->lock);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

                memcpy(_request, request_queue->requests, sizeof(request_t) * request_queue->count);
                count = request_queue->count;
                request_queue->count = 0;
                sy_spin_unlock(&request_queue->lock);

                for (i = 0; i < count; ++i) {
                        request = &_request[i];
                        __schedule_task_new(request->name, request->exec,
                                            request->buf, -1, &request->parent,
                                            request->priority);
                }
        }
}

#if SCHEDULE_REPLY_NEW
static int __schedule_reply_remote_run(schedule_t *_schedule)
{
        int ret;
        schedule_t *schedule = __schedule_self(_schedule);
        struct list_head list, *pos, *n;
        reply_remote_t *reply;
        
        if (list_empty(&schedule->reply_remote_list)) {
                return 0;
        }
        
        // schedule_resume 是生产者，本函数是消费者，需要MT同步

        INIT_LIST_HEAD(&list);
        
        ret = sy_spin_lock(&schedule->reply_remote_lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        list_splice_init(&schedule->reply_remote_list, &list);

        sy_spin_unlock(&schedule->reply_remote_lock);

        list_for_each_safe(pos, n, &list) {
                list_del(pos);
                reply = (void *)pos;
        
                ret = __schedule_exec(schedule, &reply->task, reply->retval, &reply->buf, 0);
                if (unlikely(ret)) {
                        YASSERT(ret == ESTALE);
                }

                mem_cache_free(MEM_CACHE_4K, reply);
        }

        return 1;
}
#else
static int __schedule_reply_remote_run(schedule_t *_schedule)
{
        int ret, count, i;
        schedule_t *schedule = __schedule_self(_schedule);
        reply_queue_t *reply_remote = &schedule->reply_remote;
        reply_t _reply[REPLY_QUEUE_MAX], *reply;
        //core_t *core = core_self();

        if (schedule->running == 0) {
                DINFO("reply queue %u\n",
                      reply_remote->count);
        }

        if (reply_remote->count == 0) {
                return 0;
        } else {
                // schedule_resume 是生产者，本函数是消费者，需要MT同步

                ret = sy_spin_lock(&reply_remote->lock);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

                YASSERT(reply_remote->count <= REPLY_QUEUE_MAX);
                memcpy(_reply, reply_remote->replys, sizeof(reply_t) * reply_remote->count);
                count = reply_remote->count;
                reply_remote->count = 0;
                sy_spin_unlock(&reply_remote->lock);

                for (i = 0; i < count; ++i) {
                        reply = &_reply[i];
                        DBUG("**** rep %u\n", reply->task.taskid);

                        ret = __schedule_exec(schedule, &reply->task, reply->retval, reply->buf, 0);
                        if (unlikely(ret)) {
                                YASSERT(ret == ESTALE);
                        }

                        mem_cache_free(MEM_CACHE_4K, reply->buf);
                }
        }

        if (schedule->running == 0) {
                DINFO("reply queue %u\n",
                      reply_remote->count);
        }

        return 1;
}
#endif

static int __schedule_reply_local_run(schedule_t *_schedule)
{
        int ret, count, i;
        schedule_t *schedule = __schedule_self(_schedule);
        reply_queue_t *reply_local = &schedule->reply_local;
        reply_t _reply[REPLY_QUEUE_MAX], *reply;

        if (schedule->running == 0) {
                DINFO("reply queue %u\n", reply_local->count);
        }

        if (reply_local->count == 0) {
                return 0;
        } else {
                YASSERT(reply_local->count <= REPLY_QUEUE_MAX);
                memcpy(_reply, reply_local->replys, sizeof(reply_t) * reply_local->count);
                count = reply_local->count;
                reply_local->count = 0;

                for (i = 0; i < count; ++i) {
                        reply = &_reply[i];
                        DBUG("**** rep %u\n", reply->task.taskid);
                        ret = __schedule_exec(schedule, &reply->task, reply->retval, reply->buf, 0);
                        if (unlikely(ret)) {
                                YASSERT(ret == ESTALE);
                        }

                        mem_cache_free(MEM_CACHE_4K, reply->buf);
                }
        }

        if (schedule->running == 0) {
                DINFO("reply queue %u\n", reply_local->count);
        }

        return 1;
}

void schedule_reply_local_run(schedule_t *_schedule)
{
        __schedule_reply_local_run(_schedule);
}

static void __schedule_run(schedule_t *_schedule)
{
        int ret;

#if SCHEDULE_CHECK_RUNTIME
        const schedule_t *schedule = __schedule_self(_schedule);
        int64_t used;
        struct timeval t1, t2;
#endif

        while (1) {
#if SCHEDULE_CHECK_RUNTIME
                _gettimeofday(&t1, NULL);
#endif

                ret = (__schedule_reply_local_run(_schedule)
                       || __schedule_reply_remote_run(_schedule)
                       || __schedule_task_run(_schedule));

#if SCHEDULE_CHECK_RUNTIME
                _gettimeofday(&t2, NULL);
                used = _time_used(&t1, &t2);
                if (used > 1000 * 1000) {
                        DERROR("schedule[%u] %u %u %u %u running used %fs\n",
                                        schedule->id,
                                        schedule->reply_local.count,
                                        schedule->reply_remote.count,
                                        __schedule_runable(schedule);
                                        schedule->request_queue.count,
                                        (double)used / (1000 * 1000));
                }
#endif

                if (ret == 0) {
                        break;
                }
        }

        __schedule_request_queue_run(_schedule);
}

void schedule_run(schedule_t *_schedule)
{
#if SCHEDULE_CHECK_RUNTIME
        struct timeval t1, t2;
        int64_t used;
        _gettimeofday(&t1, NULL);
#endif

        while (!schedule_finished(_schedule)) {
                //ANALYSIS_BEGIN(0);

                DBUG("running\n");
                __schedule_run(_schedule);

                //ANALYSIS_QUEUE(0, IO_WARN, "schedule_run");
        }

#if SCHEDULE_CHECK_RUNTIME
        _gettimeofday(&t2, NULL);
        used = _time_used(&t1, &t2);
        if (unlikely(used > 1000 * 1000)) {
                DWARN("schedule running used %fs\n", (double)used / (1000 * 1000));
        }
#endif
}

void schedule_post(schedule_t *schedule)
{
        int ret;
        uint64_t e = 1;

        DBUG("eventfd %d\n", schedule->eventfd);
        if (unlikely(schedule->eventfd != -1)) {
                ret = write(schedule->eventfd, &e, sizeof(e));
                if (ret < 0) {
                        ret = errno;
                        DERROR("errno %u\n", ret);
                        YASSERT(0);
                }
        }
}

void schedule_task_given(task_t *task)
{
        if (likely(schedule_running())) {
                schedule_t *schedule = schedule_self();

                task->scheduleid = schedule->id;
                task->taskid = schedule->running_task;
                task->fingerprint = TASK_SCHEDULE;
        } else {
                task->taskid = __gettid();
                task->scheduleid = getpid();
                task->fingerprint = TASK_THREAD;
        }
}

int schedule_request(schedule_t *schedule, int priority, func_t exec, void *buf, const char *name)
{
        int ret;
        request_queue_t *request_queue = &schedule->request_queue;
        request_t *request;

        YASSERT(strlen(name) + 1 <= SCHE_NAME_LEN);
        
        ret = sy_spin_lock(&request_queue->lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        if (request_queue->count == request_queue->max) {
                if (request_queue->count + REQUEST_QUEUE_STEP == REQUEST_QUEUE_MAX) {
                        ret = ENOSPC;
                        UNIMPLEMENTED(__DUMP__);
                        GOTO(err_lock, ret);
                }

                DINFO("new request_queue array %u\n", request_queue->max + REQUEST_QUEUE_STEP);

                ret = yrealloc((void **)&request_queue->requests,
                               sizeof(*request) * request_queue->max,
                               sizeof(*request) * (request_queue->max + REQUEST_QUEUE_STEP));
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

                request_queue->max += REQUEST_QUEUE_STEP;
        }

        DBUG("count %u max %u\n", request_queue->count, request_queue->max);

        request = &request_queue->requests[request_queue->count];
        request->exec = exec;
        request->buf = buf;
        request->priority = priority;
        schedule_task_given(&request->parent);
        snprintf(request->name, SCHE_NAME_LEN, "%s", name);
        request_queue->count++;

        sy_spin_unlock(&request_queue->lock);

        schedule_post(schedule);

        return 0;
err_lock:
        sy_spin_unlock(&request_queue->lock);
//err_ret:
        return ret;
}

static void __schedule_resume(schedule_t *schedule, reply_queue_t *reply_queue,
                              const task_t *task, int retval, buffer_t *buf)
{
        int ret;
        buffer_t *mem = mem_cache_calloc(MEM_CACHE_4K, 1);
        reply_t *reply;

        YASSERT(task->scheduleid >= 0 && task->scheduleid <= SCHEDULE_MAX);
        YASSERT(task->taskid >= 0 && task->taskid < TASK_MAX);
        YASSERT(task->fingerprint);

        (void) schedule;
#if 0
        taskctx_t *taskctx;
        taskctx = &schedule->tasks[task->taskid];
#if 0
        YASSERT(taskctx->state != RUNNING);
        YASSERT(taskctx->state != FREE);
        YASSERT(task->fingerprint == taskctx->fingerprint);
#endif
        DINFO("resume core[%u][%u] name %s\n", task->scheduleid, task->taskid, taskctx->name);
#endif

        if (unlikely(reply_queue->count == reply_queue->max)) {
                if (unlikely(reply_queue->count + REPLY_QUEUE_STEP > REPLY_QUEUE_MAX)) {
                        ret = ENOSPC;
                        GOTO(err_ret, ret);
                }

                DBUG("new reply_queue array %u\n", reply_queue->max + REPLY_QUEUE_STEP);

                // @note 会导致buffer.list失效, 所以reply.buffer需要是指针类型
                ret = yrealloc((void **)&reply_queue->replys,
                               sizeof(*reply) * reply_queue->max,
                               sizeof(*reply) * (reply_queue->max + REPLY_QUEUE_STEP));
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

                reply_queue->max += REPLY_QUEUE_STEP;
        }

        reply = &reply_queue->replys[reply_queue->count];
        reply->task = *task;
        reply->retval = retval;
        reply->buf = mem;
        mbuffer_init(reply->buf, 0);
        if (buf && buf->len) {
                mbuffer_merge(reply->buf, buf);
        }

        reply_queue->count++;

        return;
err_ret:
        UNIMPLEMENTED(__DUMP__);
        return;
}

#if 0
/**
 * task_t里包含scheduleid，所以可以在外部线程里调用，也可定位到任务所在scheduler
 *
 * @param task
 * @param retval
 * @param buf
 */
static void __schedule_resume1(schedule_t *schedule, const task_t *task, int retval, buffer_t *buf)
{
        taskctx_t *taskctx;
        taskctx = &schedule->tasks[task->taskid];

        YASSERT(task->fingerprint);
        if (unlikely(task->fingerprint != taskctx->fingerprint)) {
                DERROR("run task[%u] %s already destroyed\n", taskctx->id, taskctx->name);
                return;
        }

        taskctx->retval = retval;
        taskctx->state = TASK_STAT_RUNNABLE;

        mbuffer_init(&taskctx->buf, 0);
        if (buf && buf->len) {
                mbuffer_merge(&taskctx->buf, buf);
        }

        __schedule_task_run_now(schedule, taskctx);
}

void schedule_resume1(const task_t *task, int retval, buffer_t *buf)
{
        int ret;
        schedule_t *schedule = __schedule_array__[task->scheduleid];
        reply_queue_t *reply_queue;

        if (schedule == schedule_self()) {
                __schedule_resume1(schedule, task, retval, buf);
        } else {
                // 由外部线程唤醒，需要MT同步机制
                // 如AIO，异步sqlite等使用场景
                reply_queue = &schedule->reply_remote;

                ret = sy_spin_lock(&reply_queue->lock);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

                __schedule_resume(schedule, reply_queue, task, retval, buf);

                sy_spin_unlock(&reply_queue->lock);

                schedule_post(schedule);
        }
}
#endif

#if SCHEDULE_REPLY_NEW
static void __schedule_resume_remote(schedule_t *schedule,
                              const task_t *task, int retval, buffer_t *buf)
{
        int ret;
        reply_remote_t *reply = mem_cache_calloc(MEM_CACHE_4K, 1);

        YASSERT(task->scheduleid >= 0 && task->scheduleid <= SCHEDULE_MAX);
        YASSERT(task->taskid >= 0 && task->taskid < TASK_MAX);
        YASSERT(task->fingerprint);

        (void) schedule;
#if 0
        taskctx_t *taskctx;
        taskctx = &schedule->tasks[task->taskid];
#if 0
        YASSERT(taskctx->state != RUNNING);
        YASSERT(taskctx->state != FREE);
        YASSERT(task->fingerprint == taskctx->fingerprint);
#endif
        DINFO("resume core[%u][%u] name %s\n", task->scheduleid, task->taskid, taskctx->name);
#endif
        reply->task = *task;
        reply->retval = retval;
        mbuffer_init(&reply->buf, 0);
        if (buf && buf->len) {
                mbuffer_merge(&reply->buf, buf);
        }

        ret = sy_spin_lock(&schedule->reply_remote_lock);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        list_add_tail(&reply->hook, &schedule->reply_remote_list);
        
        sy_spin_unlock(&schedule->reply_remote_lock);
        
        return;
}
#endif

void schedule_resume(const task_t *task, int retval, buffer_t *buf)
{
        schedule_t *schedule = __schedule_array__[task->scheduleid];

        if (schedule == schedule_self()) {
                __schedule_resume(schedule, &schedule->reply_local, task, retval, buf);

                schedule_post(schedule);
        } else {
#if SCHEDULE_REPLY_NEW
                __schedule_resume_remote(schedule, task, retval, buf);
#else
                int ret;
                reply_queue_t *reply_queue;
                // 由外部线程唤醒，需要MT同步机制
                // 如AIO，异步sqlite等使用场景
                reply_queue = &schedule->reply_remote;

                ret = sy_spin_lock(&reply_queue->lock);
                if (unlikely(ret))
                        UNIMPLEMENTED(__DUMP__);

                __schedule_resume(schedule, reply_queue, task, retval, buf);

                sy_spin_unlock(&reply_queue->lock);
#endif

                schedule_post(schedule);
        }
}

#ifdef ENABLE_SCHEDULE_RUN
#else
#endif

void schedule_task_setname(const char *name)
{
        schedule_t *schedule = schedule_self();
        taskctx_t *taskctx;

#if 0
        if (schedule == NULL) {
                YASSERT(__running_job__);
                job_setname(__running_job__, name);
        } else {
                YASSERT(schedule->running_task != -1);
                taskctx = &schedule->tasks[schedule->running_task];
                YASSERT(taskctx->state == TASK_STAT_RUNNING);
                strcpy(taskctx->name, name);
        }
#else
        YASSERT(schedule->running_task != -1);
        taskctx = &schedule->tasks[schedule->running_task];
        YASSERT(taskctx->state == TASK_STAT_RUNNING);
        strcpy(taskctx->name, name);
#endif
}

void schedule_dump(schedule_t *schedule, int block)
{
        int i, count = 0;
        taskctx_t *taskctx;
        struct timeval t1;
        uint64_t used;
        taskctx_t *tasks = schedule->tasks;

        _gettimeofday(&t1, NULL);

#if SCHEDULE_RUNNING_TASK_LIST
        list_for_each_entry_safe(taskctx, tasks, &schedule->running_task_list, running_hook) {
                i = taskctx->id;
#else
        for (i = 0; i < schedule->size; ++i) {
                taskctx = &tasks[i];
#endif

                if (taskctx->state != TASK_STAT_FREE) {
                        count ++;
                        used = _time_used(&taskctx->ctime, &t1);
                        DINFO("%s[%u] %s status %u used %fms\n", schedule->name, i,
                              taskctx->name, taskctx->state, (double)used / 1000);
                }
        }

        DINFO("%s used %u\n", schedule->name, count);
        schedule->backtrace = 1;
        schedule_post(schedule);

        while (block) {
                usleep(1000);
                if (schedule->backtrace == 0) {
                        break;
                }
        }
}
        
void IO_FUNC schedule_scan(schedule_t *_schedule)
{
        int i, time_used, used = 0;
        time_t now;
        schedule_t *schedule = __schedule_self(_schedule);
        taskctx_t *taskctx, *tasks = schedule->tasks;

        YASSERT(schedule);

        now = gettime();
        if (unlikely(now - schedule->last_scan < gloconf.rpc_timeout / 2)) {
                return;
        }

        schedule->last_scan = now;
        schedule->scan_seq++;

        (void) i;
#if SCHEDULE_RUNNING_TASK_LIST
        list_for_each_entry_safe(taskctx, tasks, &schedule->running_task_list, running_hook) {
                i = taskctx->id;
#else
        for (i = 0; i < schedule->size; ++i) {
                taskctx = &tasks[i];
                DBUG("scan %u, size %u\n", i, schedule->size);
#endif

                if (taskctx->state != TASK_STAT_FREE) {
                        time_used = gettime() - taskctx->wait_begin;
                        used++;

                        DBUG("task %s\n", taskctx->name);
                        __schedule_check_scan_used(schedule, taskctx, time_used);
                }
        }

        DINFO("%s[%u] %u/%u/%u\n", schedule->name, schedule->id,
              TASK_MAX, schedule->size, used);
}

void schedule_backtrace()
{
        int i;
        schedule_t *schedule = schedule_self();
        taskctx_t *taskctx, *tasks = schedule->tasks;

        if (!schedule->backtrace) {
                return;
        }

        schedule->backtrace = 0;

        YASSERT(schedule);

#if SCHEDULE_RUNNING_TASK_LIST
        list_for_each_entry_safe(taskctx, tasks, &schedule->running_task_list, running_hook) {
                i = taskctx->id;
#else
        for (i = 0; i < schedule->size; ++i) {
                taskctx = &tasks[i];
                DBUG("scan %u, size %u\n", i, schedule->size);
#endif

                int time_used = gettime() - taskctx->wait_begin;
                if (taskctx->state != TASK_STAT_FREE) {
                        DINFO("%s[%u][%u] %s status %u time_used %us\n", schedule->name,
                              schedule->id, i, taskctx->name, taskctx->state, time_used);
                        __schedule_backtrace_set(taskctx);
                }
        }
}

typedef struct {
        task_t task;
        int free;
} slp_task_t;

inline static void  __schedule_sleep(void *arg)
{
        slp_task_t *slp = arg;
        schedule_task_wakeup(&slp->task);
        schedule_resume(&slp->task, 0, NULL);

        if (slp->free) {
                yfree((void **)&arg);
        } else {
                mem_cache_free(MEM_CACHE_128, slp);
        }
}

int schedule_sleep(const char *name, suseconds_t usec)
{
        int ret;
        slp_task_t *slp;

        if (schedule_running()) {
                YASSERT(usec < 180 * 1000 * 1000);

                if (usec > 1000 * 100) {
                        ret = ymalloc((void **)&slp, sizeof(*slp));
                        if (unlikely(ret))
                                UNIMPLEMENTED(__DUMP__);

                        slp->free = 1;
                } else {
                        slp = mem_cache_calloc(MEM_CACHE_128, 1);
#ifdef HAVE_STATIC_ASSERT
                        static_assert(sizeof(*slp) < sizeof(mem_cache128_t), "corerpc_ctx_t");
#endif
                        slp->free = 0;
                }

                slp->task = schedule_task_get();

                schedule_task_sleep(&slp->task);
                timer_insert(name, slp, __schedule_sleep, usec);

                ret = schedule_yield(name, NULL, &slp->task);
                if (unlikely(ret)) {
                        if (ret == ESTALE) {
                                GOTO(err_ret, ret);
                        } else {
                                UNIMPLEMENTED(__DUMP__);
                        }
                }
        } else {
                usleep(usec);
        }
        
        return 0;
err_ret:
        return ret;
}

static int __schedule_task_cleanup()
{
        int i, waiting = 0;
        const schedule_t *schedule = schedule_self();
        taskctx_t *taskctx, *tasks = schedule->tasks;

#if SCHEDULE_RUNNING_TASK_LIST
        list_for_each_entry_safe(taskctx, tasks, &schedule->running_task_list, running_hook) {
                i = taskctx->id;
#else
        for (i = 0; i < schedule->size; ++i) {
                taskctx = &tasks[i];
#endif
                if (taskctx->state != TASK_STAT_FREE) {
                        DINFO("task[%u] name %s wait %s state %u\n",
                              i, tasks[i].name, tasks[i].wait_name, tasks[i].state);
                        __schedule_backtrace_set(&tasks[i]);
                        waiting++;
                }
        }

        DBUG("task finished\n");

        return !waiting;
}

void schedule_destroy(schedule_t *_schedule)
{
        int ret, retry = 0;
        schedule_t *schedule = __schedule_self(_schedule);

        schedule->running = 0;

        while (1) {
                DBUG("running\n");
                if (retry > 100) {
                        DINFO("schedule[%u] exist, retry %u %u\n",
                              schedule->id, retry,
                              schedule->reply_local.count);
                }

                __schedule_run(schedule);

                if (__schedule_task_cleanup()) {
                        break;
                }else {
                        usleep(1000 * 100);
                        retry++;

                        YASSERT(retry < 1800);
                }
        }

        DINFO("schedule[%u] exit, retry %u\n", schedule->id, retry);

        ret = sy_spin_lock(&__schedule_array_lock__);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        __schedule_array__[schedule->id] = NULL;

        sy_spin_unlock(&__schedule_array_lock__);

        __schedule_destroy(schedule);

        variable_exit();
}

int schedule_init()
{
        int ret;
        schedule_t **schedule;

        DINFO("schedule malloc %lu\n", sizeof(**schedule) * SCHEDULE_MAX);

        ret = ymalloc((void **)&schedule, sizeof(**schedule) * SCHEDULE_MAX);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        memset(schedule, 0x0, sizeof(**schedule) * SCHEDULE_MAX);

        ret = sy_spin_init(&__schedule_array_lock__);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        __schedule_array__ = schedule;

        ret = schedule_thread_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

#if ENABLE_SCHEDULE_LOCK_CHECK

void schedule_lock_set(int lock, int ref)
{
        schedule_t *schedule = schedule_self();
        taskctx_t *taskctx;

        if (schedule == NULL)
                return;

        YASSERT(schedule);
        YASSERT(schedule->running_task != -1);
        taskctx = &schedule->tasks[schedule->running_task];

        if (lock) {
                taskctx->lock_count += lock;
                YASSERT(taskctx->lock_count >= 0);
        }

        if (ref) {
                taskctx->ref_count += ref;
                YASSERT(taskctx->ref_count >= 0);
        }
}

int schedule_assert_retry()
{
        schedule_t *schedule = schedule_self();
        taskctx_t *taskctx;

        (void) taskctx;

        if (!(schedule && schedule->running_task != -1)) {
                return 0;
        }

        taskctx = &schedule->tasks[schedule->running_task];
        // TODO core
        YASSERT(taskctx->lock_count == 0);
        if (taskctx->ref_count) {
                DERROR("task %s ref count %d\n", taskctx->name, taskctx->ref_count);
        }

#if 0
        if (gloconf.performance_analysis) {
                YASSERT(used < 1000 * 1000 * (gloconf.hb_timeout * 2));
        }
#endif

        return 0;
}
#endif

void schedule_value_set(int key, uint32_t value)
{ 
        schedule_t *schedule = schedule_self();
        taskctx_t *taskctx;

        YASSERT(key < TASK_VALUE_MAX);
        
        if (unlikely(!(schedule && schedule->running_task != -1))) {
        } else {
                taskctx = &schedule->tasks[schedule->running_task];
                taskctx->value[key] = value;
        }
}

void schedule_value_get(int key, uint32_t *value)
{ 
        schedule_t *schedule = schedule_self();
        taskctx_t *taskctx;

        YASSERT(key < TASK_VALUE_MAX);
        
        if (!(schedule && schedule->running_task != -1)) {
                *value = -1;
        } else {
                taskctx = &schedule->tasks[schedule->running_task];
                *value = taskctx->value[key];
        }
}


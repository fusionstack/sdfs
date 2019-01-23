#ifndef __YLIB_DBG_H__
#define __YLIB_DBG_H__

#include <time.h>
#include <sys/types.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/syscall.h>
#include <assert.h>
#include <stdlib.h>
#include <syslog.h>
#include <errno.h>
#include <unistd.h>

#include "ylog.h"
#include "dbg_proto.h"
#include "dbg_message.h"
#include "configure.h"

/* for gettime, localtime_safe etc */
#include "sysutil.h"
#include "removed.h"

// DEBUG
#define DEBUG_MODE       ENABLE_DEBUG_MODE


#define DGOTO_PATH            SHM_ROOT"/msgctl/backtrace"
#define DLEVEL_PATH           SHM_ROOT"/msgctl/level"
#define SYSLOG_INTEVAL_PATH   SHM_ROOT"/msgctl/syslog_inteval"

#define DBUG_PATH             SHM_ROOT"/msgctl/dbug"
#define DBUG_YLIB_PATH        SHM_ROOT"/msgctl/sub/ylib"
#define DBUG_YNET_PATH        SHM_ROOT"/msgctl/sub/ynet"
#define DBUG_CLUSTER_PATH     SHM_ROOT"/msgctl/sub/cluster"
#define DBUG_INTERFACE_PATH   SHM_ROOT"/msgctl/sub/interface"
#define DBUG_SCHEDULE_PATH   SHM_ROOT"/msgctl/sub/schedule"
#define DBUG_LSV_PATH         SHM_ROOT"/msgctl/sub/lsv"

#define DBUG_REPLICA_PATH   SHM_ROOT"/msgctl/sub/replica"
#define DBUG_CHUNK_PATH   SHM_ROOT"/msgctl/sub/chunk"
#define DBUG_CONTROL_PATH   SHM_ROOT"/msgctl/sub/control"
#define DBUG_STORAGE_PATH     SHM_ROOT"/msgctl/sub/storage"
#define DBUG_TASK_PATH   SHM_ROOT"/msgctl/sub/task"

extern int __d_info__;
extern int __d_goto__;
extern int __syslog_interval__;
extern int __d_bug__;
extern int __d_level__;
extern int __shutdown__ ;

static inline pid_t __gettid(void)
{
        return syscall(SYS_gettid);
}

void dbg_sub_init();
void dbg_info(int on);
void dbg_goto(int on);
void dbg_bug(int on);
void dbg_level(int level);

#if 0
static inline void __msg_free(void *ptr, int stat)
{
        if (stat)
                mem_cache_free(MEM_CACHE_4K, ptr);
        else
                __free(ptr);
}

static inline void *__msg_calloc(size_t nmemb, size_t size, char *stat);
{
        char *stat = mem_cache_inited();
        if (*stat)
                return mem_cache_calloc(MEM_CACHE_4K, 1);
        else
                return  calloc(1, 4096);
}
#endif

static inline void __free(void *ptr)
{
        free(ptr);
}

static inline void *__malloc(size_t size)
{
        return malloc(size);
}

int schedule_stat(int *sid, int *taskid, int *rq, int *runable, int *wait_task, int *count_task);

#if defined(CMAKE_SOURCE_PATH_SIZE)

#define __FILENAME__ (__FILE__ + CMAKE_SOURCE_PATH_SIZE)

#else

#define __FILENAME__ (__FILE__)

#endif

#define __MSG__(mask) (((mask) & (__d_bug__ | __d_info__ | __D_WARNING | __D_ERROR | __D_FATAL)) \
                               || ((ylib_dbg & (mask)) && (ylib_sub & DBG_SUBSYS)))
#define __LEVEL__(mask) ((mask) & (__d_level__))

void  __attribute__((noinline)) dbg_ylog_write(const int logtype, const int size, const int mask,
                const char *filename, const int line, const char *function,
                const char *format, ...);
                
#define __D_MSG__(logtype, size, mask, format, a...)                    \
        if (unlikely(__MSG__(mask)) && unlikely(__LEVEL__(mask))) {     \
                dbg_ylog_write(logtype, size, mask, __FILENAME__, __LINE__, __FUNCTION__, format, ##a); \
        }


#define D_MSG(mask, format, a...)  __D_MSG__(YLOG_TYPE_STD, 4096 - 128, mask, format, ## a)
#define DINFO(format, a...)        D_MSG(__D_INFO, "INFO: "format, ## a)
#define DINFO1(size, format, a...) __D_MSG__(YLOG_TYPE_STD, size, __D_INFO, "INFO: "format, ## a)
#define DWARN(format, a...)        D_MSG(__D_WARNING, "WARNING: "format, ## a)
#define DWARN1(size, format, a...) __D_MSG__(YLOG_TYPE_STD, size, __D_WARNING, "WARNING: "format, ## a)
#define DERROR(format, a...)       D_MSG(__D_ERROR, "ERROR: "format, ## a)
#define DFATAL(format, a...)       D_MSG(__D_FATAL, "FATAL: "format, ## a)
#define DBUG(format, a...)         D_MSG(__D_BUG, "DBUG: "format, ## a)

#define DINFO_PERF(format,a...)    __D_MSG__(YLOG_TYPE_PERF, 4069 - 128, __D_INFO, "INFO:"format, ## a)
#define DWARN_PERF(format,a...)    __D_MSG__(YLOG_TYPE_PERF, 4069 - 128, __D_WARNING, "WARNING:"format, ## a)

#define DINFO_BALANCE(format,a...)    __D_MSG__(YLOG_TYPE_BALANCE, 4069 - 128, __D_INFO, "INFO:"format, ## a)
#define DWARN_BALANCE(format,a...)    __D_MSG__(YLOG_TYPE_BALANCE, 4069 - 128, __D_WARNING, "WARNING:"format, ## a)

#define DINFO_RAMDISK(format,a...)    __D_MSG__(YLOG_TYPE_RAMDISK, 4069 - 128, __D_INFO, "INFO:"format, ## a)
#define DWARN_RAMDISK(format,a...)    __D_MSG__(YLOG_TYPE_RAMDISK, 4069 - 128, __D_WARNING, "WARNING:"format, ## a)

#define D_MSG_RAW(logtype, mask, format, a...)                                   \
        do {                                                            \
                if (__MSG__(mask)) {                                    \
                        char __d_msg_buf[2 * 1024];                     \
                        snprintf(__d_msg_buf, 2 * 1024, format, ##a);   \
                                                                        \
                        (void) ylog_write(logtype, __d_msg_buf);        \
                }                                                       \
        } while (0);

#define DBUG_RAW(format, a...)         D_MSG_RAW(YLOG_TYPE_STD, __D_BUG, format, ## a)

#define _SYSLOG(size, level, format, a...) \
    do {                                                        \
            char *__d_msg_buf;              \
                                                        \
            __d_msg_buf = __malloc(size);             \
            snprintf(__d_msg_buf, size,                     \
                    format, \
                    ##a);      \
            syslog(level, "%s", __d_msg_buf);                 \
            __free(__d_msg_buf);                            \
    } while (0);

#define SYSLOG(mark, level, format, a...) \
        do {                                                        \
                syslogt2##mark = gettime(); \
                syslogused##mark = syslogt2##mark - syslogt1##mark;                  \
                if ((syslogused##mark > __syslog_interval__) || (__syslog_interval__ == 0)) { \
                        _SYSLOG(4096, level, format, ##a);\
                        syslogt1##mark = syslogt2##mark;        \
                } else {        \
                        DBUG("syslog limit "format, ##a); \
                }           \
        } while (0);

#if 0
#define SMARK(mark) \
        static __thread int syslogt1##mark;                \
        int syslogt2##mark, syslogused##mark;


#define SINFO(mark, format, a...)                       \
        SMARK(mark);                                    \
        SYSLOG(mark, LOG_INFO, "INFO: "format, ##a);

#define SWARN(mark, format, a...) \
        SMARK(mark); \
        SYSLOG(mark, LOG_WARNING, "WARNING: "format, ##a);

#define SERROR(mark, format, a...) \
        SMARK(mark); \
        SYSLOG(mark, LOG_ERR, "ERROR: "format, ##a);

#else

#define SMARK(mark)

#define SINFO(mark, format, a...)

#define SWARN(mark, format, a...) 

#define SERROR(mark, format, a...)

#endif


#define SY_HALT_ON_ERR 0

#ifdef YFS_DEBUG
# define YASSERT(exp)                                                   \
        do {                                                            \
                if (unlikely(!(exp) && __shutdown__ == 0)) {                      \
                        __shutdown__ = 1;                               \
                        DERROR("!!!!!!!!!!assert fail!!!!!!!!!!!!!!!\n"); \
                        SERROR(200, "%s !!!!!!!!!!assert fail!!!!!!!!!!!!!!!\n", M_FUSIONSTOR_ASSERT_WARN); \
                        if (srv_running && gloconf.coredump) {                              \
                            abort(); \
                        } else {                                        \
                                if (gloconf.restart) {                  \
                                        EXIT(EAGAIN);                   \
                                } else {                                \
                                        EXIT(100);                      \
                                }                                       \
                        }                                               \
                }                                                       \
        } while (0)
#else
# define YASSERT(exp) {}
#endif

#ifdef YFS_DEBUG
# define YASSERT_NOLOG(exp)                                             \
        do {                                                            \
                if (!(exp)) {                                           \
                        __shutdown__ = 1;                               \
                        if (srv_running) {                              \
                             abort(); \
                        } else {                                        \
                                EXIT(100);                              \
                        }                                               \
                }                                                       \
        } while (0)
#else
# define YASSERT_NOLOG(exp) {}
#endif

#if SY_HALT_ON_ERR
# define GOTO(label, ret)                                               \
        do {                                                            \
                if (__d_goto__) {                                       \
                        DERROR("Process halting via (%d)%s\n", ret, strerror(ret)); \
                }                                                       \
                sleep(2);                                               \
                goto label;                                             \
        } while (0)
#else
# define GOTO(label, ret)                                               \
        do {                                                            \
                if (__d_goto__) {                                       \
                        DWARN("Process leaving via (%d)%s\n", ret, strerror(ret)); \
                }                                                       \
                goto label;                                             \
        } while (0)
#endif

# define UNLIKELY_GOTO(label, ret) \
        if (unlikely(ret)) {       \
                GOTO(label, ret);  \
        }

#define __NULL__ 0
#define __WARN__ 1
#define __DUMP__ 2

#define UNIMPLEMENTED(__arg__)                                          \
        do {                                                            \
                if (__arg__ == __WARN__ || __arg__ == __DUMP__) {       \
                        DWARN("unimplemented yet\n");                   \
                        if (__arg__ == __DUMP__) {                      \
                                YASSERT(0);                             \
                        }                                               \
                }                                                       \
        } while (0)

extern uint32_t ylib_loglevel;
extern uint32_t ylib_dbg;
extern uint32_t ylib_sub;

#define FATAL_RETVAL 255

#define FATAL_EXIT(__arg__)                                     \
        do {                                                    \
                DERROR("fatal error:  %s!!!!\n", __arg__);      \
                exit(FATAL_RETVAL);                             \
        } while (0)

#define EXIT(__ret__)                             \
        do {       					\
		rdma_running = 0;                               \
		sleep(2);					\
                DWARN("exit worker (%u) %s\n", __ret__, strerror(__ret__)); \
                exit(__ret__);                                          \
        } while (0)

#if 1
#define STATIC
#else
#define STATIC static
#endif

#ifndef CHECK_RETURN
#define CHECK_RETURN(ret) do { \
        if (unlikely(ret)) { \
                DWARN("ret %d\n", ret); \
        } \
        return ret; \
} while(0)
#endif

#define CHECK_CONCURRENCY_BEGIN() \
        static int __enter_count = 0; \
        __enter_count ++;             \
        YASSERT(__enter_count == 1)

#define CHECK_CONCURRENCY_END() \
        __enter_count --

#define DINFO_NOP(format, a...)
#define DWARN_NOP(format, a...)
#define DERROR_NOP(format, a...)
#define DFATAL_NOP(format, a...)

#if 1
        #define DDEV DINFO
#else
        #define DDEV DBUG
#endif

int dmsg_init();

#endif

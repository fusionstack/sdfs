#ifndef __ADT_H__
#define __ADT_H__

#include <time.h>
#include <stdint.h>
#include <stdarg.h>

#include "configure.h"

#define ENABLE_ANALYSIS 1

#define ARRAY_POP(__head__, __count__, __total__)                         \
        do {                                                            \
                memmove(&(__head__), &(__head__) + __count__,           \
                        sizeof(__head__) * ((__total__) - (__count__))); \
                memset(&(__head__) + (__total__) - (__count__), 0x0,    \
                       sizeof(__head__) * (__count__));                 \
        } while (0);

#define ARRAY_PUSH(__head__, __count__, __len__)                        \
        do {                                                            \
                memmove(&(__head__) + __count__, &(__head__),           \
                        sizeof(__head__) * (__len__));                  \
                memset(&(__head__), 0x0,                                \
                       sizeof(__head__) * (__count__));                 \
        } while (0);

#define ARRAY_SORT(__head__, __count__, __cmp__)                        \
        do {                                                            \
                qsort(__head__, __count__, sizeof(__head__[0]), __cmp__); \
        } while (0);


#define ARRAY_COPY(__src__, __dist__, __len__)                         \
        do {                                                            \
                memcpy(&(__src__), &(__dist__),                         \
                       sizeof(__src__) * (__len__));                    \
        } while (0);

#if 0
#define dir_for_each(buf, buflen, de, off)                      \
        YASSERT(sizeof(off) == sizeof(uint64_t));               \
        for (de = (void *)(buf);                                \
             (void *)de < (void *)(buf) + buflen ;              \
             off = de->d_off, de = (void *)de + de->d_reclen)
#endif

#if ENABLE_ANALYSIS
#define ANALYSIS_BEGIN(mark)                    \
        struct timeval t1##mark, t2##mark;      \
        int used##mark;                         \
                                                \
        if (unlikely(gloconf.performance_analysis)) {\
                _gettimeofday(&t1##mark, NULL); \
        }

#define ANALYSIS_START(mark, __str)             \
        struct timeval t1##mark, t2##mark;      \
        int used##mark;                         \
                                                \
        if (gloconf.performance_analysis) {\
                DWARN_PERF("analysis %s start\n", (__str) ? (__str) : ""); \
                _gettimeofday(&t1##mark, NULL); \
        }                                       \

#define ANALYSIS_RESET(mark)                    \
        if (gloconf.performance_analysis) {\
                _gettimeofday(&t1##mark, NULL); \
        }

#define ANALYSIS_TIMED_END(mark, __str)                                                \
        if (gloconf.performance_analysis) {                                                   \
                _gettimeofday(&t2##mark, NULL);                                               \
                used##mark = _time_used(&t1##mark, &t2##mark);                                \
                DERROR(""#mark" time %ju us %s\n", used##mark, (__str) ? (__str) : "");  \
        }

#define ANALYSIS_END(mark, __usec, __str)                               \
        if (unlikely(gloconf.performance_analysis)) {                             \
                _gettimeofday(&t2##mark, NULL);                         \
                used##mark = _time_used(&t1##mark, &t2##mark);          \
                if (used##mark > (__usec)) {                            \
                        if (used##mark > 1000 * 1000 * gloconf.rpc_timeout) {            \
                                DWARN_PERF("analysis used %f s %s, timeout\n", (double)(used##mark) / 1000 / 1000, (__str) ? (__str) : ""); \
                        } else {                                        \
                                DINFO_PERF("analysis used %f s %s\n", (double)(used##mark) / 1000 / 1000, (__str) ? (__str) : ""); \
                        }                                               \
                } \
        }

#define ANALYSIS_ASSERT(mark, __usec, __str)                               \
        if (gloconf.performance_analysis) {                             \
                _gettimeofday(&t2##mark, NULL);                         \
                used##mark = _time_used(&t1##mark, &t2##mark);          \
                YASSERT(used##mark < (__usec));                         \
        }                                                             \
        
#define ANALYSIS_QUEUE(mark, __usec, __str)                               \
        if (unlikely(gloconf.performance_analysis)) {                   \
                _gettimeofday(&t2##mark, NULL);                         \
                used##mark = _time_used(&t1##mark, &t2##mark);          \
                analysis_private_queue(__str ? __str : __FUNCTION__, NULL, used##mark); \
                if (used##mark > (__usec)) {                            \
                        if (used##mark > 1000 * 1000 * gloconf.rpc_timeout) { \
                                DWARN_PERF("analysis used %f s %s, timeout\n", (double)(used##mark) / 1000 / 1000, (__str) ? (__str) : ""); \
                        } else {                                        \
                                DINFO_PERF("analysis used %f s %s\n", (double)(used##mark) / 1000 / 1000, (__str) ? (__str) : ""); \
                        }                                               \
                }                                                       \
        }

#define ANALYSIS_UPDATE(mark, __usec, __str)                               \
        if (gloconf.performance_analysis) {                             \
                _gettimeofday(&t2##mark, NULL);                         \
                used##mark = _time_used(&t1##mark, &t2##mark);          \
                core_latency_update(used##mark);                        \
                if (used##mark > (__usec)) {                            \
                        if (used##mark > 1000 * 1000 * gloconf.rpc_timeout) { \
                                DWARN_PERF("analysis used %f s %s, timeout\n", (double)(used##mark) / 1000 / 1000, (__str) ? (__str) : ""); \
                        } else {                                        \
                                DINFO_PERF("analysis used %f s %s\n", (double)(used##mark) / 1000 / 1000, (__str) ? (__str) : ""); \
                        }                                               \
                }                                                       \
        }



#else
#define ANALYSIS_BEGIN(mark)  {}
#define ANALYSIS_RESET(mark)   {}
#define ANALYSIS_QUEUE(mark, __usec, __str)     \
        do { \
        (void) __str; \
        } while (0);

#define ANALYSIS_END(mark, __usec, __str) \
        do { \
        (void) __str; \
        } while (0);

#define ANALYSIS_ASSERT(mark, __usec, __str)

#endif

typedef struct {
        uint32_t len;
        char buf[0];
} str_t;

#define str_for_each(str, p)                    \
        for (p = (str)->buf;                    \
             p < (str)->buf + (str)->len ;      \
             p = p + strlen(p) + 1)

#define str_append(str, p)                                      \
        do {                                                    \
                if ((str)->len) {                               \
                        strcpy(&(str)->buf[(str)->len + 1], p); \
                        (str)->len += strlen(p) + 1;            \
                } else {                                        \
                        strcpy((str)->buf, p);                  \
                        (str)->len += strlen(p);                \
                }                                               \
        } while (0);

#if ENABLE_SCHEDULE_LOCK_CHECK
#define USLEEP_RETRY(__err_ret__, __ret__, __labal__, __retry__, __max__, __sleep__) \
        if ((__retry__)  < __max__) {                                   \
                if (__retry__ % 10 == 0) {                              \
                        DINFO("retry %u/%u\n", (__retry__), __max__);   \
                }                                                       \
                                                \
                schedule_assert_retry();                                \
                schedule_sleep("none", __sleep__);                      \
                __retry__++;                                            \
                goto __labal__;                                 \
        } else                                                  \
                GOTO(__err_ret__, __ret__);

#else
#define USLEEP_RETRY(__err_ret__, __ret__, __labal__, __retry__, __max__, __sleep__) \
        if ((__retry__)  < __max__) {                                   \
                if (__retry__ % 10 == 0 && __retry__ > 1) {             \
                        DINFO("retry %u/%u\n", (__retry__), __max__);   \
                }                                                       \
                schedule_sleep("none", __sleep__);                      \
                __retry__++;                                            \
                goto __labal__;                                 \
        } else                                                  \
                GOTO(__err_ret__, __ret__);
#endif

#define USLEEP_RETRY1(__err_ret__, __ret__, __labal__, __retry__, __max__, __sleep__) \
        if (__retry__  < __max__) {                                     \
                DWARN("retry %u/%u\n", __retry__, __max__);             \
                                                                        \
                schedule_sleep("none", __sleep__);                      \
                __retry__++;                                            \
                goto __labal__;                                 \
        } else                                                  \
                GOTO(__err_ret__, __ret__);

typedef void (*func_t)(void *arg);
typedef void (*func1_t)(void *, void *);
typedef void (*func2_t)(void *, void *, void *);
typedef void (*func3_t)(void *, void *, void *, void *);

typedef int (*func_int_t)(void *arg);
typedef int (*func_int1_t)(void *, void *);
typedef int (*func_int2_t)(void *, void *, void *);
typedef int (*func_int3_t)(void *, void *, void *, void *);

typedef int (*func_va_t)(va_list ap);

typedef void * (*thread_proc)(void *);

#endif

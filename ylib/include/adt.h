#ifndef __ADT_H__
#define __ADT_H__

#include <configure.h>
#include <stdint.h>
#include <stdarg.h>

#define ARRAY_POP(__head__, __count__, __total__)                         \
        do {                                                            \
                memmove(&(__head__), &(__head__) + __count__,           \
                        sizeof(__head__) * ((__total__) - (__count__)));  \
                memset(&(__head__) + (__total__) - (__count__), 0x0,      \
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

#define ANALYSIS_BEGIN(mark)                    \
        struct timeval t1##mark, t2##mark;      \
        int used##mark;                         \
                                                \
        if (gloconf.performance_analysis) {\
                _gettimeofday(&t1##mark, NULL); \
        }


#define ANALYSIS_RESET(mark)                    \
        if (gloconf.performance_analysis) {\
                _gettimeofday(&t1##mark, NULL); \
        }

#define ANALYSIS_END(mark, __usec, __str)                               \
        if (gloconf.performance_analysis) {                             \
                _gettimeofday(&t2##mark, NULL);                         \
                used##mark = _time_used(&t1##mark, &t2##mark);          \
                if (used##mark > (__usec)) {                            \
                        if (used##mark > 1000 * 1000 * gloconf.rpc_timeout) {            \
                                DWARN("analysis used %f s %s, timeout\n", (double)(used##mark) / 1000 / 1000, (__str) ? (__str) : ""); \
                        } else {                                        \
                                DINFO("analysis used %f s %s\n", (double)(used##mark) / 1000 / 1000, (__str) ? (__str) : ""); \
                        }                                               \
                } \
        }

#define ANALYSIS_QUEUE(mark, __usec, __str)                               \
        if (gloconf.performance_analysis) {                             \
                _gettimeofday(&t2##mark, NULL);                         \
                used##mark = _time_used(&t1##mark, &t2##mark);          \
                if (gloconf.performance_analysis) {            \
                        analysis_queue(&default_analysis, __str ? __str : __FUNCTION__, used##mark); \
                }                                                       \
                if (used##mark > (__usec)) {                            \
                        if (used##mark > 1000 * 1000 * gloconf.rpc_timeout) { \
                                DWARN("analysis used %f s %s, timeout\n", (double)(used##mark) / 1000 / 1000, (__str) ? (__str) : ""); \
                        } else {                                        \
                                DINFO("analysis used %f s %s\n", (double)(used##mark) / 1000 / 1000, (__str) ? (__str) : ""); \
                        }                                               \
                }                                                       \
        }


typedef struct {
        uint32_t len;
        char buf[0];
} str_t;

#define SLEEP_RETRY(__err_ret__, __ret__, __labal__, __retry__) \
        if (__retry__ < 50) {                               \
                DINFO("retry time %u by ret %u\n", __retry__, __ret__); \
                usleep(100 * 1000);                                     \
                __retry__++;                                            \
                goto __labal__;                                 \
        } else                                                  \
                GOTO(__err_ret__, __ret__);


#define SLEEP_RETRY1(__err_ret__, __ret__, __labal__, __retry__) \
        if (__retry__ < MAX_RETRY * 10) {                               \
                if (__retry__ > 2)                                      \
                        DWARN("retry time %u by ret %u\n", __retry__, __ret__); \
                sleep(1);                                               \
                __retry__++;                                    \
                goto __labal__;                                 \
        } else                                                  \
                GOTO(__err_ret__, __ret__);

#define SLEEP_RETRY2(__err_ret__, __ret__, __labal__, __retry__, __max__)  \
        if (__retry__ < __max__ * 10) {                               \
                if (__retry__ > __max__ * 5 && __retry__ % 10 == 0)     \
                        DWARN("retry time %u by ret %u\n", __retry__, __ret__); \
                usleep(100 * 1000);                                     \
                __retry__++;                                            \
                goto __labal__;                                         \
        } else                                                          \
                GOTO(__err_ret__, __ret__);


#define SLEEP_RETRY3(__err_ret__, __ret__, __labal__, __retry__, __max__)  \
        if (__retry__ < __max__ ) {                               \
                if (__retry__ > __max__ / 2)                            \
                        DWARN("retry time %u by ret %u\n", __retry__, __ret__); \
                sleep(1);                                               \
                __retry__++;                                            \
                goto __labal__;                                         \
        } else                                                          \
                GOTO(__err_ret__, __ret__);

#if 1

#define USLEEP_RETRY(__err_ret__, __ret__, __labal__, __retry__, __max__, __sleep__) \
        if ((__retry__)  < __max__) {                                   \
                if (__retry__ % 10 == 0) {                              \
                        DBUG("retry %u/%u\n", (__retry__), __max__);    \
                }                                                       \
                                                \
                schedule_sleep("none", __sleep__);                      \
                __retry__++;                                            \
                goto __labal__;                                 \
        } else                                                  \
                GOTO(__err_ret__, __ret__);

#else
#define USLEEP_RETRY(__err_ret__, __ret__, __labal__, __retry__, __max__, __sleep__) \
        if (__retry__  < __max__) {                                     \
                usleep(__sleep__);                                               \
                __retry__++;                                            \
                goto __labal__;                                 \
        } else                                                  \
                GOTO(__err_ret__, __ret__);
#endif

typedef void (*func_t)(void *arg);
typedef void (*func1_t)(void *, void *);
typedef void (*func2_t)(void *, void *, void *);
typedef void (*func3_t)(void *, void *, void *, void *);
typedef int (*func0_t)(void *arg);
typedef int (*func_va_t)(va_list ap);

#endif

#ifndef __TIMER_H__
#define __TIMER_H__

#include "ytime.h"
#include "worker.h"
#include "adt.h"

typedef int (*resume_func)(void *ctx, int retval);

typedef int (*timer_exec_t)(void *);

int timer_init(int private, int polling);
int timer_insert(const char *name, void *ctx, func_t func, suseconds_t usec);
void timer_expire();

int timer1_create(worker_handler_t *handler, const char *name, timer_exec_t exec, void *_ctx);

int timer1_settime(const worker_handler_t *handler, uint64_t nsec);
int timer1_settime_retry(const worker_handler_t *handler, uint64_t nsec, int retry);

#if 0
typedef struct {
        worker_handler_t handler;
        char name[MAX_NAME_LEN];
        timer_exec_t func;
        void *arg;
        int free_arg;
        uint64_t nsec;
        int count;
} easy_timer_t;

int easy_timer_init(easy_timer_t *timer, const char *name, timer_exec_t func, void *arg,
                    uint64_t nsec, int count, int free_arg);
#endif

#endif

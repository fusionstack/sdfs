#ifndef __WORKER_H__
#define __WORKER_H__

#include <pthread.h>
#include <semaphore.h>

#include "sdfs_list.h"
#include "job.h"
#include "analysis.h"
#include "ylock.h"

#define WORKER_TYPE_TIMER 1
#define WORKER_TYPE_SEM 2

typedef struct {
        int fd;
        int type;
} worker_handler_t;

typedef int (*worker_exec_t)(void *);
typedef int (*worker_queue_t)(void *, const void *);

//extern worker_handler_t jobtracker;

int worker_init(void);
int worker_settime(const worker_handler_t *handler, uint64_t nsec);
int worker_queue(const worker_handler_t *handler, const void *arg);
int worker_post(const worker_handler_t *handler);
int worker_create(worker_handler_t *handler, const char *name,
                  worker_exec_t exec, worker_queue_t queue, void *ctx,
                  int type, int multi);

#endif



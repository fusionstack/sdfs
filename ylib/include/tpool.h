#ifndef __TPOLL_H__
#define __TPOLL_H__

#include <semaphore.h>
#include <pthread.h>

#include "sdfs_list.h"
#include "ylib.h"
#include "ylock.h"

typedef void *(*tpool_worker)(void *);

typedef struct {
        sem_t sem;
        time_t last_left;
        sy_spinlock_t lock;
        int total;
        int left;
        int idle;
        time_t last_threads;
        char name[MAX_NAME_LEN];
        tpool_worker worker;
        void *context;
} tpool_t;


int tpool_init(tpool_t *tpool, tpool_worker worker, void *context, const char *name, int idle_thread);
int tpool_left(tpool_t *tpool);
void tpool_return(tpool_t *tpool);
void tpool_increase(tpool_t *tpool);
int tpool_wait(tpool_t *tpool);

#endif /* __TPOOL_H__ */

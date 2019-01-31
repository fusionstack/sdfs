#ifndef __PLOCK_H__
#define __PLOCK_H__

#include <pthread.h>
#include <stdint.h>

#include "sdfs_list.h"

#if 1
#define PLOCK_NEW
#endif

#ifdef PLOCK_NEW

typedef struct {
        //pthread_rwlock_t lock;


        struct list_head queue;

        int writer;
        int readers;
        uint32_t priority;
        int thread;
#if LOCK_DEBUG
        int32_t count;
        uint32_t last_unlock;
        char name[MAX_LOCK_NAME];
#endif
} plock_t;

#else

typedef struct {
        pthread_rwlock_t lock;
        struct list_head queue;
        sy_spinlock_t spin;
        uint32_t priority;
        int thread;
#if LOCK_DEBUG
        int32_t count;
        uint32_t last_unlock;
#endif
} plock_t;

#endif

/* plock.c */
extern int plock_init(plock_t *rwlock, const char *name);
extern int plock_destroy(plock_t *rwlock);
extern int plock_rdlock(plock_t *rwlock);
extern int plock_tryrdlock(plock_t *rwlock);
extern int plock_wrlock(plock_t *rwlock);
extern int plock_trywrlock(plock_t *rwlock);
extern void plock_unlock(plock_t *rwlock);
extern int plock_timedwrlock(plock_t *rwlock, int sec);
extern int plock_wrlock_prio(plock_t *rwlock, int prio);

#endif

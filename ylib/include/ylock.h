#ifndef __YLOCK_H__
#define __YLOCK_H__

#include <pthread.h>
#include <stdint.h>

#include "sdfs_list.h"
#include "sdfs_id.h"

#define MAX_LOCK_NAME 128

#if 1

#define sy_spinlock_t pthread_spinlock_t

#define sy_spin_init(__spin__) \
        pthread_spin_init(__spin__, PTHREAD_PROCESS_PRIVATE)

#define sy_spin_destroy pthread_spin_destroy
#define sy_spin_lock pthread_spin_lock
#define sy_spin_trylock pthread_spin_trylock
#define sy_spin_unlock pthread_spin_unlock

#endif

#define SCHEDULE

#ifdef SCHEDULE

#if 1
#define LOCK_DEBUG 0
#endif

typedef struct {
        pthread_rwlock_t lock;
        struct list_head queue;
        sy_spinlock_t spin;
        uint32_t priority;
#ifdef LOCK_DEBUG
        int32_t count;
        uint32_t last_unlock;
        task_t writer;
        char name[MAX_LOCK_NAME];
#endif
} sy_rwlock_t;

#endif

#endif

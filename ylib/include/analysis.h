#ifndef __ANALYSIS_H__
#define __ANALYSIS_H__

#include "hash_table.h"
#include "job.h"
#include "ylock.h"

#define ANALYSIS_QUEUE_MAX (8192 * 10)

typedef struct {
        int count;
        struct {
                char name[MAX_NAME_LEN];
                uint64_t time;
        } array[ANALYSIS_QUEUE_MAX];
} analysis_queue_t;

typedef struct {
        struct list_head hook;
        char name[MAX_NAME_LEN];
        hashtable_t tab;
        analysis_queue_t *queue;
        analysis_queue_t *new_queue;
        sy_spinlock_t queue_lock;
        sy_spinlock_t tab_lock;
} analysis_t;

extern analysis_t default_analysis;

int analysis_create(analysis_t *ana, const char *name);
int analysis_dump();
int analysis_queue(analysis_t *ana, const char *name, uint64_t _time);
int analysis_init();

#endif

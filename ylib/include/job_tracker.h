#ifndef __JOBTRACKER_H__
#define __JOBTRACKER_H__

#include <pthread.h>
#include <semaphore.h>

#include "sdfs_list.h"
#include "job.h"
#include "analysis.h"
#include "ylock.h"


typedef struct {
        struct list_head list;
        void *parent;
        sem_t sem;
        sem_t exit_sem;
        int idx;
        int length;              /* NOT required */
        sy_spinlock_t lock;
        int pipe[2];
} job_head_t;

typedef struct {
        int size;
        int running;
        char name[MAX_NAME_LEN];
        analysis_t ana;
        job_head_t array[0];
} jobtracker_t;

extern jobtracker_t *jobtracker;

int  jobtracker_create(jobtracker_t **jobtracker, int size, const char *name);
int  jobtracker_insert(job_t *job);
int  jobtracker_jobnum(jobtracker_t *jobtracker, uint32_t *jobnum);
int jobtracker_exit_wait(jobtracker_t *jobtracker);

#endif

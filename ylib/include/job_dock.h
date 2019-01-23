#ifndef __JOB_DOCK_H__
#define __JOB_DOCK_H__

#include <sys/timeb.h>

#include "job_tracker.h"

#define JOBDOCK_TIMEOUT         30
#define JOBDOCK_SCAN_INTERVAL   10

typedef int (*net_cmp_func)(const void *, const void *);
typedef void (*net_reset_func)(void *);
typedef const char *(*net_print_func)(const void *);
typedef void (*net_notify_func)(const void *);
typedef int (*net_revoke_func)(job_t *);
typedef int (*remove_job_func)(job_t *job, sy_spinlock_t *lock);

extern int  jobdock_init(net_print_func net_print);
extern void jobdock_destroy(void);

extern int  job_create(job_t **job, jobtracker_t *jobtracker, const char *name);
extern int job_used(int *used, int *total);
extern void job_destroy(job_t *job);
extern void jobdock_iterator();
extern void jobdock_checktmo(uint16_t nethash);
int jobdock_resume(jobid_t *id, buffer_t *buf, void *nh, int retval);
void jobdock_setmo(const sockid_t *, const char *peer);
uint64_t jobdock_load();
int job_context_create(job_t *job, size_t size);
int job_timedwait(job_t *job, int sec);
job_t *jobdock_find(const jobid_t *id);
void job_set_child(job_t *job, int count);
void job_get_res(job_t *job, int count, int *res);
int job_get_ret(job_t *job, int idx);
void job_set_ret(job_t *job, int idx, int res);
int job_timermark(job_t *job, const char *stage);
void job_wait_init(job_t *job);
int job_lock(job_t *job);
int job_unlock(job_t *job);
void jobdock_block(job_t *job);
void jobdock_unblock(job_t *job);
void jobdock_destroy();
void jobdock_load_set(uint64_t load);

#endif

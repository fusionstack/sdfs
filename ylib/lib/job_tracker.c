


#include <stdint.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "sysutil.h"
#include "job_tracker.h"
#include "ylib.h"
#include "job_dock.h"
#include "configure.h"
#include "adt.h"
#include "dbg.h"

#define JOBTRACKER_MIN_HASH 1
#define JOBTRACKER_MAX_HASH 128
#define JOB_ARRAY_LEN 512

//#define JOB_PREVIEW

#ifndef __CYGWIN__
#include <sys/syscall.h>
#endif

pid_t gettid()
{
        return syscall(SYS_gettid);
}

#define USE_SEM

jobtracker_t *jobtracker;

void *__jobtracker_worker(void *arg)
{
        int ret, idx __attribute__((unused)), more_job __attribute__((unused)), used;
        job_t *job;
        job_head_t *head;
        char name[MAX_BUF_LEN], wname[MAX_BUF_LEN];
        struct timeval t1, t2;
        jobtracker_t *tracker;

        head = arg;
        idx = head->idx;
        tracker = head->parent;

        while (tracker->running) {
                ret = sy_spin_lock(&head->lock);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);

                DBUG("%s len %u\n", tracker->name, head->length);

                if (list_empty(&head->list)) {
                        sy_spin_unlock(&head->lock);

#ifdef USE_SEM
                        ret = _sem_wait(&head->sem);
                        if (ret)
                                UNIMPLEMENTED(__DUMP__);
#else
                        int ret;
                        char buf[MAX_BUF_LEN];
                        ret = read(head->pipe[0], buf, MAX_BUF_LEN);
                        if (ret < 0) {
                                ret = errno;
                                UNIMPLEMENTED(__DUMP__);
                        }
#endif
                        if (tracker->running)
                                continue;
                        else
                                break;
                }

                job = (void *)head->list.next;

                ret = job_lock(job);
                if (ret)
                        YASSERT(0);

                list_del_init(&job->hook);
                job->in_queue = 0;
                YASSERT(job->state_machine);

                job_unlock(job);

                head->length--;

                sy_spin_unlock(&head->lock);

                more_job = 0;

                strcpy(name, job->name);

                _gettimeofday(&t1, NULL);
                used = _time_used(&job->queue_time, &t1);

#if 0
                if (t1.tv_sec - job->queue_time > 10) {
                        DINFO("job %s wait time %llu at %s, queue len %u\n", job->name,
                              (LLU)(t1.tv_sec - job->queue_time), tracker->name, head->length);
                }
#endif

                (void) job->state_machine(job, name);

                snprintf(wname, MAX_NAME_LEN, "%s.wait", name);
                //analysis_queue(&tracker->ana, wname, used);

                if (used > 1000 * 2000) {
                        DBUG("job %s time used %u queue len %u at %s\n",
                             name, used, head->length, tracker->name);
                }

                _gettimeofday(&t2, NULL);
                used = _time_used(&t1, &t2);
#if 0
                if (used > 100 * 1000) {
                        DINFO("job %s time used %u queue len %u at %s\n",
                             name, used, head->length, tracker->name);
                }
#endif
        }

        DINFO("job tracker %s[%u] exit\n", tracker->name, head->idx);

        sem_post(&head->exit_sem);

        pthread_exit(NULL);
}

int jobtracker_create(jobtracker_t **jobtracker, int size, const char *name)
{
        int ret, i, newsize, len;
        void *ptr;
        jobtracker_t *tracker;
        job_head_t *head;
        pthread_t th;
        pthread_attr_t ta;
        char newname[MAX_NAME_LEN];

        if (*jobtracker != NULL) {
                DINFO("jobtracker %s already inited\n", (*jobtracker)->name);
                return 0;
        }

        newsize = size;

        if (newsize < JOBTRACKER_MIN_HASH)
                newsize = JOBTRACKER_MIN_HASH;

        if (newsize > JOBTRACKER_MAX_HASH)
                newsize = JOBTRACKER_MAX_HASH;

        DBUG("create size %d:%d\n", size, newsize);

        len = sizeof(jobtracker_t) + sizeof(job_head_t) * newsize;

        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_ret, ret);

        _memset(ptr, 0x0, len);

        tracker = ptr;
        tracker->size = newsize;
        tracker->running = 1;

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);
        pthread_attr_setstacksize(&ta, 1<<21);

        for (i = 0; i < newsize; i++) {
                head = &tracker->array[i];
                sy_spin_init(&head->lock);
                INIT_LIST_HEAD(&head->list);
                head->idx = i;

                sem_init(&head->sem, 0, 0);
                sem_init(&head->exit_sem, 0, 0);

                head->parent = tracker;

#ifdef USE_SEM
                sem_init(&head->sem, 0, 0);
#else
                ret = pipe(head->pipe);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
#endif

                ret = pthread_create(&th, &ta, __jobtracker_worker,
                                     (void *)head);
                if (ret)
                        GOTO(err_ret, ret);
        }

        strcpy(tracker->name, name);
        snprintf(newname, MAX_NAME_LEN, "jobtracker.%s", name);

        *jobtracker = tracker;


        return 0;
err_ret:
        return ret;
}

/**
 * @param job
 *
 * net event -> job
 * @sa sdevents_add (job -> net event)
 */
int jobtracker_insert(job_t *job)
{
        int ret, hash, post = 0;
        job_head_t *head;
        struct list_head *list;
        jobtracker_t *__jobtracker;

        YASSERT(job->state_machine);
        YASSERT(job->jobtracker);

        __jobtracker = job->jobtracker;
        hash = job->key % __jobtracker->size;

        if (__jobtracker->running == 0) {
                DWARN("srv stoped job %s\n", job->name);
                return 0;
        }

        DBUG("insert job %s[%d] seq %d status %d into worker %s, "
             "worker count %d key %llu\n",
              job->name, job->jobid.idx, job->jobid.seq, job->status,
             __jobtracker->name, __jobtracker->size, (LLU)job->key);

        head = &__jobtracker->array[hash];

        list = &head->list;

        ret = sy_spin_lock(&head->lock);
        if (ret)
                GOTO(err_ret, ret);

        ret = job_lock(job);
        if (ret)
                GOTO(err_lock, ret);

        YASSERT(!job->in_queue);
        YASSERT(list_empty(&job->hook));

        if (job->steps >= STEP_MAX * 10) {
                YASSERT(job->steps < STEP_MAX * 100);
                DWARN("job %s steps %u\n", job->name, job->steps);
        }

        job->in_queue = 1;
        job->in_wait = 0;
        job->steps++;
        _gettimeofday(&job->queue_time, NULL);

        list_add_tail(&job->hook, list);

        job_unlock(job);

        if (head->length > QUEUE_MAX) {
                DWARN("jobtracker %s insert %s busy\n", __jobtracker->name, job->name);
        }

        if (head->length == 0)
                post = 1;

        DBUG("len %u\n", head->length);

        head->length++;

        sy_spin_unlock(&head->lock);

        ANALYSIS_BEGIN(0);

        if (post) {
#ifdef USE_SEM
                sem_post(&head->sem);
#else
                int ret;
                ret = write(head->pipe[1], "\n", 1);
                if (ret != 1) {
                        ret = errno;
                        UNIMPLEMENTED(__DUMP__);
                }
#endif
        }

        ANALYSIS_END(0, 1000 * 100, job->name);

        return 0;
err_lock:
        sy_spin_unlock(&head->lock);
err_ret:
        return ret;
}

int jobtracker_jobnum(jobtracker_t *tracker, uint32_t *jobnum)
{
        int ret, i, num;
        job_head_t *head;

        *jobnum = 0;

        for (i = 0; i < (int)tracker->size; ++i) {
                head = &tracker->array[i];

                ret = sem_getvalue(&head->sem, &num);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                *jobnum += num;
        }

        DINFO("size %u %u\n", tracker->size, *jobnum);

        return 0;
err_ret:
        return ret;
}

int jobtracker_exit_wait(jobtracker_t *__jobtracker)
{
        int ret, i;
        job_head_t *head;

        DINFO("wait for jobtracker %s destroy...\n", __jobtracker->name);

        __jobtracker->running = 0;

        for (i = 0; i < (int)__jobtracker->size; i++) {
                head = &__jobtracker->array[i];

                sem_post(&head->sem);
        }

        for (i = 0; i < (int)__jobtracker->size; i++) {
                head = &__jobtracker->array[i];

                while (1) {
                        ret = _sem_wait(&head->exit_sem);
                        if (ret)
                                GOTO(err_ret, ret);

                        break;
                }

                DBUG("jobtracker %s[%u] destroyed\n", __jobtracker->name, i);
        }

        DINFO("jobtracker %s destroyed\n", __jobtracker->name);
        __jobtracker->name[0] = '\0';

        return 0;
err_ret:
        return ret;
}

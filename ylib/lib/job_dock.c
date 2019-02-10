

#include <limits.h>
#include <time.h>
#include <string.h>
#include <semaphore.h>
#include <pthread.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "configure.h"
#include "job_dock.h"
#include "job_tracker.h"
#include "skiplist.h"
#include "ylib.h"
#include "sysutil.h"
#include "ylock.h"
#include "dbg.h"

#if USE_EPOLL
#include <sys/epoll.h>
#endif

typedef struct {
        job_t job;
        int events;
        int events_total;
        int block;
        jobid_t id; /*idx in job dock, only set then dock init*/
        sy_spinlock_t used; //be careful, only sy_spin_trylock can use on this lock;
        sy_spinlock_t lock;
        int res[YFS_CHK_REP_MAX];
        int finished[YFS_CHK_REP_MAX];
} dock_entry_t;

typedef struct {
        int size; /*max size of the array*/
        int used; /*current used in the array*/
        uint64_t time_used;
        uint32_t prev_time;
        uint32_t last_use;
        uint32_t seq;
        uint32_t timeout;
        sy_spinlock_t lock;
        net_print_func net_print;
        net_revoke_func net_revoke; /*revoke job from sock buffer*/
        int idx;
        dock_entry_t array[0];
} job_dock_t;

typedef struct {
        job_t *job;
        int idx;
        int retval;
} job_resume_arg_t;

extern jobtracker_t *jobtracker;
static job_dock_t *jobdock;

static int __job_resume(job_t *job, int retval, int idx);

static inline void __jobdock_resume(job_t *job, int retval)
{
        niocb_t *iocb;

        iocb = &job->iocb;

        job_set_ret(job, 0, retval);
        job->timeout = 0;
        job->jobid.seq = 0;
        job->nethash = -1;

        if (retval) {
                YASSERT(iocb->error);
                iocb->error(job);
        } else {
                YASSERT(iocb->reply);
                iocb->reply(job);
        }

        //jobdock->net_reset((void *)(JOB_STACK((job_t *)job)->net));
}

job_t *jobdock_find(const jobid_t *id)
{
        int ret;
        job_t *job;
        dock_entry_t *entry;

        if (id->idx >= (uint32_t)jobdock->size) {
                DWARN("repno %u invalid\n", id->idx);

                return NULL;
        }

        entry = &jobdock->array[id->idx];
        job = &entry->job;

        ret = sy_spin_lock(&entry->lock);
        if (ret)
                YASSERT(0);

        if (job->jobid.seq == id->seq) { /*if job timeout, this will be unequal*/
                ret = sy_spin_trylock(&entry->used);
                if (ret == 0) {
                        (void) sy_spin_unlock(&entry->used);

                        DERROR("table %d un-alloc'ed\n", job->jobid.idx);

                        goto err_ret;
                }
        } else
                goto err_ret;

        sy_spin_unlock(&entry->lock);

        return job;
err_ret:
        sy_spin_unlock(&entry->lock);
        return NULL;
}

void __job_destroy(job_t *job)
{
        int ret;
        uint64_t time_used;
        dock_entry_t *entry;
        struct timeval now;

        DBUG("destroy job %p %s %u %u\n", job, job->name, job_idx(job), job->jobid.seq);

        YASSERT(!job->in_queue);

        entry = (void *)job;

        ret = sy_spin_trylock(&entry->used);
        if (ret == 0) {
                (void) sy_spin_unlock(&entry->used);

                DERROR("job %d un-alloc'ed\n", entry->id.idx);

                return;
        }

        if (job->free)
                job->free(job);

        if (job->context && job->context != job->__context__) {
                yfree((void **)&job->context);
        }

        if (job->dumpinfo) {
                yfree((void **)&job->dumpinfo);
        }

        mbuffer_free(&job->request);
        mbuffer_free(&job->reply);

        entry->id.seq = 0;

        job->timeout = 0;
        job->state_machine = 0;
        job->jobid.idx = 0;
        job->jobid.seq = 0;
        job->in_queue = 0;
        job->nethash = -1;
        net_handle_reset(&job->net);

        _gettimeofday(&now, NULL);
        time_used = _time_used(&job->timer.create, &now);

        if (time_used > UINT64_MAX / 2)
                time_used = UINT64_MAX / 10;

        sy_spin_lock(&jobdock->lock);

        if (job->update_load) {
                jobdock->time_used += ((float)time_used * job->update_load);
                DBUG("load %f * %f = %f\n", (float)time_used,  job->update_load,
                     (float)time_used * job->update_load);
        }

        jobdock->used--;
        jobdock->last_use = now.tv_sec;

        sy_spin_unlock(&jobdock->lock);

        job->update_load = 0;

        sy_spin_unlock(&entry->used);

        if (time_used > 1024 * 1024 * 5) {
                DWARN("job %s time used %lld\n", job->name, (long long)time_used);
        }

        job->name[0] = '\0';

        //DINFO("%u\n", time_used);

        return;
}

void __job_timeout_nolock(job_t *job, int finished[], int events_total)
{
        int i, ret;

        return ;
        for (i=0; i<events_total; i++) {
                if (finished[i] == 0) {
                        ret = __job_resume(job, ETIMEDOUT, i);
                        if (ret)
                                YASSERT(0);
                }
        }
}

static void* __jobdock_watcher(void *arg)
{
        (void )arg;
        
        int ret, i, timeout __attribute((unused)), tmocnt;
        dock_entry_t *entry;
        time_t now;
        job_t *job;
        static time_t prev = 0;
        int finished[YFS_CHK_REP_MAX], events_total;

        if (prev == 0)
                prev = time(NULL);

        while (srv_running) {
                sleep(3);
                now = time(NULL);

                if (now - prev >= JOBDOCK_SCAN_INTERVAL) {
                        prev = now;

                        tmocnt = 0;
                        for (i = 0; i < jobdock->size; i++) {
                                entry = &jobdock->array[i];

                                job = &entry->job;

                                if (job->in_wait && job->timeout && now - job->timeout > 0) {
                                        ret = sy_spin_lock(&entry->lock);
                                        if (ret)
                                                YASSERT(0);

                                        now = time(NULL);

                                        if (job->in_wait && job->timeout && now - job->timeout > 0) {
                                                YASSERT(job->nethash != -1);
                                                DBUG("job %s idx %u status %u %s "
                                                                "timeout %lu(%llu) id %u_%u peer %s\n",
                                                                job->name, entry->id.idx, job->status, job->name,
                                                                job->timeout ? (now - job->timeout) : 0, (LLU)job->timeout,
                                                                job->jobid.idx, job->jobid.seq,
                                                                jobdock->net_print(&job->net));

                                                tmocnt ++;
                                        }

                                        events_total = entry->events_total;
                                        memcpy(finished, entry->finished, entry->events_total);

                                        sy_spin_unlock(&entry->lock);

                                        __job_timeout_nolock(job, finished, events_total);
                                }
                        }

                        if (tmocnt > 0) {
                                _fence_test1();
                        }

                        jobdock->timeout = tmocnt;
                }
        }

        return NULL;
}

inline static int __jobdock_get_empty(job_t **job)
{
        int ret, curno, no;
        dock_entry_t *entry;

        jobdock->idx = (jobdock->idx + 1) % jobdock->size;

        curno = jobdock->idx;
        no = curno;
        while (1) {
                ret = sy_spin_trylock(&jobdock->array[no].used);
                if (ret != 0) {
                        no = (no + 1) % jobdock->size;
                        if (no == curno)  /* not found */
                                break;

                        continue;
                } else  /* free table */
                        goto found;
        }

        return ENOENT;
found:
        entry = &jobdock->array[no];
        sy_spin_lock(&jobdock->lock);

#if 0
        if (jobdock->used > ((JOB_DOCK_SIZE * 3) / 4)) {
                DWARN("job dock busy %u %u\n", jobdock->used, JOB_DOCK_SIZE);

                //jobdock_iterator();

                usleep(jobdock->used);
        }
#endif

        jobdock->used++;
        entry->id.seq = jobdock->seq++;
        entry->block = 0;
        sy_spin_unlock(&jobdock->lock);

        entry->events = 1; //当前还有多少events 没有返回
        entry->events_total = 1; // job的events总数
        memset(entry->finished, 0x0, sizeof(int)*YFS_CHK_REP_MAX);
        *job = &entry->job;

        DBUG("used job %p %u %u\n", *job, job_idx(*job), (*job)->msgid.idx);

        return 0;
}

inline uint32_t job_idx(job_t *job)
{
        return ((dock_entry_t *)job)->id.idx;
}

inline uint32_t job_seq(job_t *job)
{
        return ((dock_entry_t *)job)->id.seq;
}

int jobdock_init(net_print_func net_print)
{
        int ret, i;
        uint32_t len;
        void *ptr;
        pthread_t th;
        pthread_attr_t ta;

        len = sizeof(dock_entry_t) * gloconf.jobdock_size + sizeof(job_dock_t);

        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_ret, ret);

        jobdock = ptr;

        jobdock->size = gloconf.jobdock_size;
        jobdock->idx = 0;
        jobdock->seq = 0;
        jobdock->timeout = 0;
        jobdock->net_print = net_print;
        jobdock->prev_time = time(NULL);
        jobdock->time_used = 1;

        YASSERT(jobdock->time_used);

        (void) sy_spin_init(&jobdock->lock);

        for (i = 0; i < gloconf.jobdock_size; i++) {
                jobdock->array[i].id.idx = i;
                (void) sem_init(&jobdock->array[i].job.sem, 0, 0);
                (void) sy_spin_init(&jobdock->array[i].used);
                (void) sy_spin_init(&jobdock->array[i].lock);
        }

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        ret = pthread_create(&th, &ta, __jobdock_watcher, NULL);
        if (ret)
                GOTO(err_ret, ret);


        return 0;
err_ret:
        return ret;
}

void jobdock_destroy()
{
        int ret, i;
        job_t *job;
        dock_entry_t *entry;

        for (i = 0; i < jobdock->size; i++) {
                entry = &jobdock->array[i];
                job = &entry->job;

                ret = sy_spin_lock(&entry->lock);
                if (ret)
                        YASSERT(0);

                if (job->timeout) {
                        DINFO("job %s force destroy\n", job->name);

                        list_del_init(&job->hook);

                        job->timeout = 0;
                        job->jobid.seq = 0;

                        sy_spin_unlock(&entry->lock);

                        if (job->iocb.op == FREE_JOB)
                                __job_destroy(job);
                        else
                                __jobdock_resume(job, ETIMEDOUT);

                        continue;
                } else
                        sy_spin_unlock(&entry->lock);
        }
}

int job_create(job_t **_job, jobtracker_t *__jobtracker, const char *name)
{
        int ret, retry = 0;
        job_t *job = NULL;

        //YASSERT(strlen(name) < JOB_NAME_LEN / 2);

retry:
        ret = __jobdock_get_empty(&job);
        if (unlikely(ret)) {
                if (ret == ENOENT) {
                        DWARN("jobdock busy, %s used %u\n", name, jobdock->used);

                        if (retry < MAX_RETRY * 10) {
                                retry++;
                                sleep(1);
                                goto retry;
                        } else
                                UNIMPLEMENTED(__DUMP__);
                } else
                        GOTO(err_ret, ret);
        }

        job->context = 0;
        strncpy(job->name, name, JOB_NAME_LEN);
        job->jobid.idx = job_idx(job);
        job->jobid.seq  = job_seq(job);
        job->timeout = 0;
        job->in_queue = 0;
        job->in_wait = 0;
        job->jobtracker = __jobtracker;
        job->state_machine = NULL;
        job->free = NULL;
        job->context = NULL;
        job->dump = job_dump;
        job->dumpinfo = NULL;
        job->retry = 0;
        job->nethash = -1;
        job->steps = 0;
        job->update_load = 0;
        job->timeout_handler = NULL;
        _gettimeofday(&job->timer.create, NULL);
        job->timer.step = job->timer.create;
        job->sock.sd = -1;
        job->sock.addr = -1;
        job->sock.seq = -1;
        net_handle_reset(&job->net);

        mbuffer_init(&job->request, 0);
        mbuffer_init(&job->reply, 0);
        INIT_LIST_HEAD(&job->hook);

        job->uid = 0;
        job->gid = 0;

        *_job = job;

        return 0;
err_ret:
        return ret;
}

int job_used(int *used, int *total)
{
        int ret;

        ret = sy_spin_lock(&jobdock->lock);
        if (ret)
                YASSERT(0);

        *used = jobdock->used;
        *total = jobdock->size;

        ret = sy_spin_unlock(&jobdock->lock);
        if (ret)
                YASSERT(0);

        return 0;
}

int job_context_create(job_t *job, size_t size)
{
        int ret;

        //YASSERT(job->context == NULL);

        if (size <= JOB_CONTEXT_LEN) {
                job->context = job->__context__;
                return 0;
        }

        DWARN("job %s large context %llu\n", job->name, (LLU)size);

        ret = ymalloc(&job->context, size);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

void job_destroy(job_t *job)
{
        int ret, retry = 0;
        dock_entry_t *entry;

        DBUG("destroy job %p %s %u %u\n", job, job->name, job_idx(job), job->jobid.seq);

        entry = (void *)job;

retry:
        ret = sy_spin_lock(&entry->lock);
        if (ret)
                YASSERT(0);

        if (!list_empty(&job->hook)) {
                YASSERT(retry < MAX_RETRY);
                sy_spin_unlock(&entry->lock);

                //先为移动测试临时跳过
                if (strcmp(job->name, "netable_hb1") == 0) {
                        DWARN("skip destroy job %p %s %u %u\n", job, job->name, job_idx(job), job->jobid.seq);
                        goto out;
                } else {
                        DWARN("destroy job %p %s %u %u\n", job, job->name, job_idx(job), job->jobid.seq);
                        sleep(10);

                        retry++;
                        goto retry;
                }
        }

        __job_destroy(job);

        sy_spin_unlock(&entry->lock);

out:
        return;
}

void jobdock_iterator()
{
        int i;
        job_t *job;
        time_t now __attribute__((unused));
        
        now = time(NULL);

        DINFO("used %u\n", jobdock->used);

        for (i = 0; i < jobdock->size; i++) {
                job = &jobdock->array[i].job;

                if (job->name[0] != '\0') {
                        DINFO("%s peer %s sock %s/%u\n", job->dump(job, __MSG__(Y_INFO)),
                              jobdock->net_print(&job->net),
                              _inet_ntoa(job->sock.addr), job->sock.seq);
                }
        }
}

/*only in __sdevents_worker, nethash for thread safe*/
/*todo job 的超时检查需要移动到rpc_table中*/
void jobdock_checktmo(uint16_t nethash)
{
        int ret, i;
        job_t *job;
        dock_entry_t *entry;
        time_t now;

        if (jobdock->timeout) {
                DBUG("timeout count %u hash %u\n", jobdock->timeout, nethash);
                now = time(NULL);
                for (i = 0; i < jobdock->size; i++) {
                        entry = &jobdock->array[i];
                        job = &entry->job;

                        if (job->nethash == -1 && (job->timeout && now - job->timeout > 0)) {
                                DWARN("job %s (%u, %u) not in queue\n",
                                      job->name, job->jobid.idx, job->jobid.seq);
                                continue;
                        }

                        if (job->in_wait && (job->nethash == nethash)
                            && (job->timeout && now - job->timeout > 0)) {
                                ret = sy_spin_lock(&entry->lock);
                                if (ret)
                                        YASSERT(0);

                                if (job->timeout_handler == NULL && !list_empty(&job->hook) ) {
                                        sy_spin_unlock(&entry->lock);
                                        continue;
                                }

                                if (job->in_wait && (job->nethash == nethash || job->nethash == -1)
                                    && (job->timeout && now - job->timeout > 0)) {
                                        DINFO("job[%u] %s status %u timeout"
                                              " %lu peer %s sock %s/%u\n",
                                              entry->id.idx, job->name, job->status,
                                              job->timeout,
                                              jobdock->net_print(&job->net),
                                              _inet_ntoa(job->sock.addr), job->sock.seq);

                                        if (job->timeout_handler) {
                                                sy_spin_unlock(&entry->lock);
                                                job->timeout_handler(job);
                                        } else {
                                                list_del_init(&job->hook);

                                                job->timeout = 0;
                                                job->jobid.seq = 0;

                                                sy_spin_unlock(&entry->lock);

                                                if (job->iocb.op == FREE_JOB)
                                                        __job_destroy(job);
                                                else
                                                        __jobdock_resume(job, ETIMEDOUT);
                                        }

                                        continue;
                                } else
                                        sy_spin_unlock(&entry->lock);
                        }
                }

#if 0
		ret = _fence_test();
		if (ret) {
			DWARN("fence_test fail\n");
			exit(ret);
		}
#endif
        }
}

/*set orphan job as timeout for jobdock_checktmo, just for thread safe*/
void jobdock_setmo(const sockid_t *sock, const char *peer)
{
        int ret, i;
        job_t *job;
        dock_entry_t *entry;
        time_t now;
        int tmocnt;

        now = time(NULL);
        tmocnt = 0;
        for (i = 0; i < jobdock->size; i++) {
                entry = &jobdock->array[i];
                job = &entry->job;

                if (sockid_cmp(&job->sock, sock) == 0) {
                        ret = sy_spin_trylock(&entry->lock);
                        if (ret)
                                continue;

                        if (sockid_cmp(&job->sock, sock) == 0
                            && job->timeout != 0) {

                                job->timeout = (now - 1);
                                tmocnt ++;

                                DINFO("set job[%u] %s status %u"
                                      " timeout %lu peer %s\n",
                                      entry->id.idx, job->name, job->status,
                                      job->timeout, peer);
                        }

                        sy_spin_unlock(&entry->lock);
                }
        }

        jobdock->timeout = tmocnt;
}

int jobdock_resume(jobid_t *id, buffer_t *buf, void *from, int retval)
{
        int ret, retry = 0;
        job_t *job;
        dock_entry_t *entry;

        if (id->idx >= (uint32_t)jobdock->size) {
                DWARN("repno %u invalid\n", id->idx);

                ret = ENOENT;
                goto err_ret;
        }

        entry = &jobdock->array[id->idx];
        job = &entry->job;

retry:
        ret = sy_spin_lock(&entry->lock); /*__timeout*/
        if (ret)
                YASSERT(0);

        if (entry->block) {
                if (retry > 5)
                        DWARN("job %u %s blocked, retry %u\n", id->idx, job->name, retry);
                sy_spin_unlock(&entry->lock);
                usleep(10);
                retry++;
                goto retry;
        }

        //job_timermark(job, "resume");

        if (job->jobid.seq == id->seq) { /*if job timeout, this will be unequal*/
                ret = sy_spin_trylock(&entry->used);
                if (ret == 0) {
                        (void) sy_spin_unlock(&entry->used);

                        ret = EINVAL;

                        DERROR("table %d un-alloc'ed\n", job->jobid.idx);

                        goto out;
                }

                (void) from;
#if 0
                if (from) {
                        if (jobdock->net_cmp(job->net, from)) {
                                ret = EINVAL;

                                DWARN("job %s %s from %s\n",
                                      job->name,
                                      jobdock->net_print(job->net),
                                      jobdock->net_print(from));

                                goto out;
                        }

                        DBUG("job %s %d replied from %s\n", job->name, job_idx(job),
                             jobdock->net_print(from));
                }
#endif

                if (buf) {
                        if (retval == 0)
                                mbuffer_merge(&job->reply, buf);
                        else
                                mbuffer_free(buf);
                }

                DBUG("reply job %p %d_%d ret %u\n", job, job->jobid.idx, job->jobid.seq, retval);

                job->timeout = 0;

                sy_spin_unlock(&entry->lock);
                __jobdock_resume(job, retval);
                return 0;
        } else {
                ret = ENOENT;
                goto err_lock;
        }

out:
        sy_spin_unlock(&entry->lock);
        if (buf)
                mbuffer_free(buf);

        return 0;
err_lock:
        sy_spin_unlock(&entry->lock);
err_ret:
        if (buf)
                mbuffer_free(buf);
        return ret;
}

uint64_t jobdock_load()
{
        uint64_t load;
        uint32_t now;

        sy_spin_lock(&jobdock->lock);

        now = time(NULL);

        if (now > jobdock->prev_time) {
                DBUG("diff %u\n", now - jobdock->prev_time);

                jobdock->time_used = jobdock->time_used / (now - jobdock->prev_time + 1);
                jobdock->prev_time = now;
        }

        load = jobdock->time_used;

        DBUG("load %llu\n", (LLU)load);

        sy_spin_unlock(&jobdock->lock);

        return load;
}

int job_timedwait(job_t *job, int sec)
{
        int ret;
        struct timespec ts;
        time_t t;

        if (sec == 0)
                sec = gloconf.rpc_timeout * 10;

        _memset(&ts, 0x0, sizeof(struct timespec));

        t = time(NULL);
        if (t == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ts.tv_sec = t + sec;
        ts.tv_nsec = 0;

        ret = _sem_timedwait(&job->sem, &ts);
        if (ret) {
                if (sec >= gloconf.rpc_timeout) {
                        DERROR("job %s no %u seq %u timeout peer %s\n",
                               job->name, job->jobid.idx, job->jobid.seq,
                               jobdock->net_print((void *)&job->net));
                }

                if (ret == ETIMEDOUT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        ret = job_get_ret(job, 0);
        if (ret) {
                if (ret == ENOENT || ret == EEXIST || ret == EINVAL)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

void job_set_child(job_t *job, int count)
{
        dock_entry_t *entry;

        YASSERT(count <= YFS_CHK_REP_MAX);

        entry = &jobdock->array[job_idx(job)];

        entry->events = count;
        entry->events_total = count;

        memset(entry->res, 0x0, sizeof(int) * count);
}

void job_get_res(job_t *job, int count, int *res)
{
        dock_entry_t *entry;

        YASSERT(count <= YFS_CHK_REP_MAX);

        entry = &jobdock->array[job_idx(job)];

        memcpy(res, entry->res, sizeof(int) * count);
}

int job_get_ret(job_t *job, int idx)
{
        dock_entry_t *entry;

        entry = &jobdock->array[job_idx(job)];

        return entry->res[idx];
}

void job_set_ret(job_t *job, int idx, int res)
{
        dock_entry_t *entry;

        YASSERT(idx < YFS_CHK_REP_MAX);

        entry = &jobdock->array[job_idx(job)];

        entry->res[idx] = res;
}

static int __job_resume(job_t *job, int retval, int idx)
{
        int ret;
        dock_entry_t *entry;

        //YASSERT(retval >= 0);
        YASSERT(idx >= 0);

        YASSERT(idx < YFS_CHK_REP_MAX);
        
        entry = &jobdock->array[job_idx(job)];

        ret = sy_spin_lock(&entry->lock);
        if (ret)
                YASSERT(0);

        DBUG("resume job[%u] %s  status %u retval %u, event %u\n",
              job->jobid.idx, job->name, job->status, retval, entry->events);

        if (idx != -1) {
                entry->res[idx] = retval;
                entry->finished[idx] = 1;
        }

        YASSERT(entry->events > 0);

        entry->events--;

        if (entry->events == 0) {
                DBUG("all event back\n");

                entry->events = 1; /*default event count*/
                job->nethash = -1;
                job->timeout = 0;

                sy_spin_unlock(&entry->lock);
                ret = jobtracker_insert(job);
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        } else {
                DBUG("event left %u\n", entry->events);
        }

        sy_spin_unlock(&entry->lock);

out:
        return 0;
err_ret:
        return ret;
}

static void __job_resume1(void *obj)
{
        int ret;
        job_resume_arg_t *resume = obj;

        ret = __job_resume(resume->job, resume->retval, resume->idx);
        if (ret)
                GOTO(err_ret, ret);

        yfree((void **)&resume);

        return;
err_ret:
        return;
}

int job_resume1(job_t *job, int retval, int idx, uint64_t slp)
{
        int ret;
        uint64_t tmo;
        job_resume_arg_t *resume;

        YASSERT(idx < YFS_CHK_REP_MAX);
        YASSERT(job);
        
        DBUG("resume job %s %p retval %u\n", job->name, job, retval);

        if (slp) {
                tmo = ytime_gettime();
                tmo += slp;

                ret = ymalloc((void **)&resume, sizeof(job_resume_arg_t));
                if (ret)
                        GOTO(err_ret, ret);

                resume->idx = idx;
                resume->retval  = retval;
                resume->job = job;
                timer_insert("noname", resume, __job_resume1, tmo);
        } else {
                ret = __job_resume(job, retval, idx);
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int job_resume(void *obj, int retval)
{
        return job_resume1(obj, retval, 0, 0);
}

int job_exec(job_t *job, int retval)
{
        return job_resume1(job, retval, 0, 0);
}

int job_timermark(job_t *job, const char *stage)
{
        struct timeval now;
        char name[MAX_NAME_LEN];
        int used;

        if (job == NULL)
                return 0;

        if (gloconf.performance_analysis != 2) {
                return 0;
        }

        _gettimeofday(&now, NULL);
        used = _time_used(&job->timer.step, &now);
        job->timer.step = now;
#if 0
        if (used < 1000 * 100)
                // if (used < 100)
                return 0;
#endif

        snprintf(name, MAX_NAME_LEN, "%s.%s", job->name, stage);

        if (used > 1000 * 1000)
                DWARN("job %s.%s used %u\n", job->name, stage, used);

        return used;
}

int __job_block_resume(job_t *job, char *name)
{
        (void) *name;

        sem_post(&job->sem);

        return 0;
}

void job_wait_init(job_t *job)
{
        YASSERT(job->state_machine == NULL);
        (void) sem_init(&job->sem, 0, 0);
        job->state_machine = __job_block_resume;
}

int job_lock(job_t *job)
{
        int ret;
        dock_entry_t *entry;

        entry = &jobdock->array[job_idx(job)];

        ret = sy_spin_trylock(&entry->used);
        if (ret == 0) {
                (void) sy_spin_unlock(&entry->used);
                ret = EINVAL;
                DERROR("table %d un-alloc'ed\n", job->jobid.idx);
                YASSERT(0);
        }

        ret = sy_spin_lock(&entry->lock);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int job_unlock(job_t *job)
{
        int ret;
        dock_entry_t *entry;

        entry = &jobdock->array[job_idx(job)];

        ret = sy_spin_unlock(&entry->lock);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

void jobdock_block(job_t *job)
{
        dock_entry_t *entry;

        entry = &jobdock->array[job_idx(job)];

        entry->block = 1;
}

void jobdock_unblock(job_t *job)
{
        dock_entry_t *entry;

        entry = &jobdock->array[job_idx(job)];

        entry->block = 0;
}

void jobdock_load_set(uint64_t load)
{
        sy_spin_lock(&jobdock->lock);

        jobdock->prev_time = time(NULL);

        if (jobdock->time_used < load)
                jobdock->time_used = load;

        sy_spin_unlock(&jobdock->lock);
}


static void __job_resume_func(void *obj)
{
        job_t *job;

        job = obj;

        job_resume(job, 0);
}

void job_sleep(job_t *job, int usec, int hash)
{
        uint64_t tmo;

        (void) hash;
        
        tmo = ytime_gettime();
        tmo += usec;

        timer_insert(job->name, job, __job_resume_func, tmo);
}



#include <limits.h>
#include <time.h>
#include <string.h>
#include <semaphore.h>
#include <pthread.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "configure.h"
#include "analysis.h"
#include "skiplist.h"
#include "adt.h"
#include "bh.h"
#include "ylib.h"
#include "ylock.h"
#include "dbg.h"

#if USE_EPOLL
#include <sys/epoll.h>
#endif

typedef struct {
        char name[JOB_NAME_LEN];
        uint64_t time;
        uint32_t count;
} entry_t;

struct {
        struct list_head list;
        sy_spinlock_t lock;
        sem_t sem;
        int inited;
} analysis_list;

analysis_t default_analysis;

static int __analysis_count(analysis_t *ana, const char *name, uint64_t _time)
{
        int ret;
        entry_t *ent;

        DBUG("count %s time %llu\n", name, (LLU)_time);

        ret = sy_spin_lock(&ana->tab_lock);
        if (ret)
                GOTO(err_ret, ret);

        ent = hash_table_find(ana->tab, (void *)name);
        if (ent == NULL) {
                ret = ymalloc((void **)&ent, sizeof(entry_t));
                if (ret)
                        GOTO(err_lock, ret);

                strcpy(ent->name, name);

                ent->time = _time;
                ent->count = 1;

                ret = hash_table_insert(ana->tab, (void *)ent, ent->name, 0);
                if (ret)
                        GOTO(err_lock, ret);
        } else {
                ent->time += _time;
                ent->count ++;
        }
 
        sy_spin_unlock(&ana->tab_lock);

        return 0;
err_lock:
        sy_spin_unlock(&ana->tab_lock);
err_ret:
        return ret;
}

int analysis_queue(analysis_t *ana, const char *name, uint64_t _time)
{
        int ret;

        if (gloconf.performance_analysis == 0) {
                return 0;
        }

        DBUG("queue %s time %llu\n", name, (LLU)_time);

        ret = sy_spin_lock(&ana->queue_lock);
        if (ret)
                GOTO(err_ret, ret);

        YASSERT(ana->queue->count <= ANALYSIS_QUEUE_MAX);

        if (ana->queue->count == ANALYSIS_QUEUE_MAX) {
                sy_spin_unlock(&ana->queue_lock);
                DBUG("analysis queue %s busy, when %s\n", ana->name, name);
                sem_post(&analysis_list.sem);
                goto out;
        }

        _strncpy(ana->queue->array[ana->queue->count].name, name, JOB_NAME_LEN);
        ana->queue->array[ana->queue->count].name[JOB_NAME_LEN - 1] = '\0';
        ana->queue->array[ana->queue->count].time = _time;
        ana->queue->count++;

        sy_spin_unlock(&ana->queue_lock);

out:
        return 0;
err_ret:
        return ret;
}

static int dump(void *arg, void *_ent)
{
        entry_t *ent = _ent;
        char *name = arg;

        if (ent->count == 0) {
                DBUG("%s task %s count %u total %f avg %f\n", name,
                     ent->name, ent->count, (double)ent->time, (double)0);
        } else {
                DINFO("%8s %-36s %8u %10.3f %15.3f\n", name,
                      ent->name, ent->count,
                      ((double)((LLU)ent->time / ent->count)) / (1000),
                      (double)ent->time / 1000);
                //DINFO("%s task %s count %u total %f avg %f\n", name,
                //ent->name, ent->count, (double)ent->time / 1000, ((double)ent->time / ent->count) / 1000);
        }

        ent->count = 0;
        ent->time = 0;

        return 0;
}

int __analysis_dump1(analysis_t *ana)
{
        int ret;

        ret = sy_spin_lock(&ana->tab_lock);
        if (ret)
                GOTO(err_ret, ret);

        hash_iterate_table_entries(ana->tab, (void *)dump, ana->name);

        sy_spin_unlock(&ana->tab_lock);

        return 0;
err_ret:
        return ret;
}

int analysis_dump()
{
        int ret;
        analysis_t *ana;
        struct list_head *pos;

        ret = sy_spin_lock(&analysis_list.lock);
        if (ret)
                GOTO(err_ret, ret);

        list_for_each(pos, &analysis_list.list) {
                ana = (void *)pos;

                __analysis_dump1(ana);
        }

        sy_spin_unlock(&analysis_list.lock);

        return 0;
err_ret:
        return ret;
}

static int __analysis(analysis_t *ana)
{
        int ret, i;
        analysis_queue_t *tmp;

        DBUG("%s\n", ana->name);

        ANALYSIS_BEGIN(0);

        ret = sy_spin_lock(&ana->queue_lock);
        if (ret)
                GOTO(err_ret, ret);

        tmp = ana->queue;
        ana->queue = ana->new_queue;
        ana->new_queue = tmp;

        sy_spin_unlock(&ana->queue_lock);

        for (i = 0; i < ana->new_queue->count; i++) {
                ret = __analysis_count(ana, ana->new_queue->array[i].name,
                                       ana->new_queue->array[i].time);
                if (ret)
                        GOTO(err_ret, ret);
        }

        ana->new_queue->count = 0;

        ANALYSIS_END(0, 1000 * 100, ana->name);

        return 0;
err_ret:
        return ret;
}

static int __analysis_cmp(const void *v1, const void *v2)
{
        const entry_t *ent = v1;
        const char *name = v2;

        return strcmp(ent->name, name);
}

static uint32_t __analysis_key(const void *i)
{
        return hash_str((char *)i);
}

static void *__worker(void *_args)
{
        int ret;
        analysis_t *ana;
        struct list_head *pos;

        (void) _args;

        while (1) {
                ret = _sem_timedwait1(&analysis_list.sem, 1);
                if (ret) {
                        if (ret == ETIMEDOUT) {
                        } else
                                GOTO(err_ret, ret);
                }

                ret = sy_spin_lock(&analysis_list.lock);
                if (ret)
                        GOTO(err_ret, ret);

                list_for_each(pos, &analysis_list.list) {
                        ana = (void *)pos;

                        __analysis(ana);
                }
                
                sy_spin_unlock(&analysis_list.lock);
        }

        return NULL;
err_ret:
        return NULL;
}

int analysis_init()
{
        int ret;
        pthread_t th;
        pthread_attr_t ta;

        YASSERT(analysis_list.inited == 0);

        INIT_LIST_HEAD(&analysis_list.list);

        sy_spin_init(&analysis_list.lock);

        sem_init(&analysis_list.sem, 0, 0);

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        ret = pthread_create(&th, &ta, __worker, NULL);
        if (ret)
                GOTO(err_ret, ret);

        analysis_list.inited = 1;

        return 0;
err_ret:
        return ret;
}

static int __analysis_register(analysis_t *ana)
{
        int ret;

        ret = sy_spin_lock(&analysis_list.lock);
        if (ret)
                GOTO(err_ret, ret);

        list_add_tail(&ana->hook, &analysis_list.list);
                
        sy_spin_unlock(&analysis_list.lock);

        return 0;
err_ret:
        return ret;
}

int analysis_create(analysis_t *ana, const char *_name)
{
        int ret;
        char name[MAX_NAME_LEN];

        YASSERT(analysis_list.inited);

        snprintf(name, MAX_NAME_LEN, "%s", _name);

        strncpy(ana->name, name, MAX_NAME_LEN);

        ana->tab = hash_create_table(__analysis_cmp, __analysis_key, name);
        if (ana->tab == NULL) {
                ret = ENOMEM;
                DERROR("ret (%d) %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        ret = ymalloc((void **)&ana->queue, sizeof(analysis_queue_t));
        if (ret)
                GOTO(err_ret, ret);

        ana->queue->count = 0;

        ret = ymalloc((void **)&ana->new_queue, sizeof(analysis_queue_t));
        if (ret)
                GOTO(err_ret, ret);

        ana->new_queue->count = 0;

        ret = sy_spin_init(&ana->queue_lock);
        if (ret)
                GOTO(err_ret, ret);

        ret = sy_spin_init(&ana->tab_lock);
        if (ret)
                GOTO(err_ret, ret);

        ret = __analysis_register(ana);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

void analysis_destroy(analysis_t *ana)
{
        sy_spin_lock(&analysis_list.lock);

        list_del(&ana->hook);

        sy_spin_unlock(&analysis_list.lock);
}

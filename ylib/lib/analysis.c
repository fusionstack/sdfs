#include <limits.h>
#include <time.h>
#include <string.h>
#include <sys/epoll.h>
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

typedef struct {
        char name[JOB_NAME_LEN * 2];
        uint64_t time;
        uint32_t count;
        time_t prev;
} entry_t;

struct {
        struct list_head list;
        sy_spinlock_t lock;
        sem_t sem;
        int inited;
} analysis_list;

analysis_t *default_analysis = NULL;
static __thread analysis_t *__private_analysis__ = NULL;

static int __analysis_count(analysis_t *ana, const char *name, uint64_t _time)
{
        int ret;
        entry_t *ent;

        DBUG("count %s time %llu\n", name, (LLU)_time);

        ret = sy_spin_lock(&ana->tab_lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ent = hash_table_find(ana->tab, (void *)name);
        if (ent == NULL) {
                ret = ymalloc((void **)&ent, sizeof(entry_t));
                if (unlikely(ret))
                        GOTO(err_lock, ret);

                strncpy(ent->name, name, JOB_NAME_LEN * 2);

                ent->time = _time;
                ent->count = 1;
                ent->prev = gettime();

                ret = hash_table_insert(ana->tab, (void *)ent, ent->name, 0);
                if (unlikely(ret))
                        GOTO(err_lock, ret);
        } else {
                ent->time += _time;
                ent->count++;
        }

        DBUG("%s: (%s, count)\n", ana->name, ent->name, ent->count);
        
        sy_spin_unlock(&ana->tab_lock);

        return 0;
err_lock:
        sy_spin_unlock(&ana->tab_lock);
err_ret:
        return ret;
}

int analysis_queue(analysis_t *ana, const char *_name, const char *type, uint64_t _time)
{
        int ret;
        char tmp[MAX_NAME_LEN];
        const char *name;

        if (gloconf.performance_analysis == 0) {
                return 0;
        }

        if (analysis_list.inited == 0) {
                return 0;
        }

        //DBUG("queue %s time %llu\n", name, (LLU)_time);

        if (type) {
                sprintf(tmp, "%s.%s", _name, type);
                name = tmp;
        } else {
                name = _name;
        }

        if (unlikely(!ana->private)) {
                ret = sy_spin_trylock(&ana->queue_lock);
                if (unlikely(ret)) {
                        if (ret == EBUSY) {
                                DBUG("queue %s %f busy\n", name, (double)_time / 1000 / 1000);
                                return 0;
                        } else
                                GOTO(err_ret, ret);
                }
        }

        YASSERT(ana->queue->count <= ANALYSIS_QUEUE_MAX);

        if (ana->queue->count == ANALYSIS_QUEUE_MAX) {
                if (unlikely(!ana->private)) {
                        sy_spin_unlock(&ana->queue_lock);
                        sem_post(&analysis_list.sem);
                }

                DWARN("analysis queue %s busy, when %s\n", ana->name, name);
                goto out;
        }

        _strncpy(ana->queue->array[ana->queue->count].name, name, JOB_NAME_LEN);
        ana->queue->array[ana->queue->count].name[JOB_NAME_LEN - 1] = '\0';
        ana->queue->array[ana->queue->count].time = _time;
        ana->queue->count++;

        DBUG("%s, count %u\n", ana->name, ana->queue->count);
        
        if (unlikely(!ana->private)) {
                sy_spin_unlock(&ana->queue_lock);
        }

out:
        return 0;
err_ret:
        return ret;
}

static void __analysis(void *arg, void *_ent)
{
        entry_t *ent = _ent;
        char *name = arg;

        if (ent->count == 0) {
                //DBUG("%s %s count %u avg %llu s\n", name,
                //ent->name, ent->count, (LLU)ent->time, (LLU)0);
        } else {
                DINFO("%8s %-36s %8u %10.3f %15.3f\n", name,
                      ent->name, ent->count,
                      ((double)((LLU)ent->time / ent->count)) / (1000),
                      (double)ent->time / 1000);
        }

        ent->count = 0;
        ent->time = 0;
        ent->prev = gettime();
}

int __analysis_dump1(analysis_t *ana)
{
        int ret;

        ret = sy_spin_lock(&ana->tab_lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DINFO("begin {{{\n");
        hash_iterate_table_entries(ana->tab, __analysis, ana->name);
        DINFO("}}} \n");

        sy_spin_unlock(&ana->tab_lock);

        return 0;
err_ret:
        return ret;
}

int analysis_dumpall()
{
        int ret;
        analysis_t *ana;
        struct list_head *pos;

        if (analysis_list.inited == 0) {
                return 0;
        }

        ret = sy_spin_lock(&analysis_list.lock);
        if (unlikely(ret))
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

static int __analysis__(analysis_t *ana, int *count)
{
        int ret, i;
        analysis_queue_t *tmp;
        
        ANALYSIS_BEGIN(0);

        if (unlikely(!ana->private)) {
                ret = sy_spin_lock(&ana->queue_lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        tmp = ana->queue;
        ana->queue = ana->new_queue;
        ana->new_queue = tmp;

        if (unlikely(!ana->private)) {
                sy_spin_unlock(&ana->queue_lock);
        }

        DBUG("%s, count %u\n", ana->name, ana->new_queue->count);

        for (i = 0; i < ana->new_queue->count; i++) {
                ret = __analysis_count(ana, ana->new_queue->array[i].name,
                                       ana->new_queue->array[i].time);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        if (count) {
                *count = ana->new_queue->count;
        }

        ana->new_queue->count = 0;

        ANALYSIS_END(0, 1000 * 100, NULL);

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

        (void) _args;

        while (1) {
                ret = _sem_timedwait1(&analysis_list.sem, 1);
                if (unlikely(ret)) {
                        if (ret == ETIMEDOUT) {
                                DBUG("analysis timeout\n");
                        } else
                                GOTO(err_ret, ret);
                }

                DBUG("analysis collect\n");

                __analysis__(default_analysis, NULL);
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
        if (unlikely(ret))
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
        if (unlikely(ret))
                GOTO(err_ret, ret);

        list_add_tail(&ana->hook, &analysis_list.list);

        sy_spin_unlock(&analysis_list.lock);

        return 0;
err_ret:
        return ret;
}

int analysis_create(analysis_t **_ana, const char *_name, int private)
{
        int ret;
        char name[MAX_NAME_LEN];
        analysis_t *ana;

        if (analysis_list.inited == 0) {
                return 0;
        }

        DINFO("analysis queue %s create\n", _name);

        ret = ymalloc((void **)&ana, sizeof(*ana));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        snprintf(name, MAX_NAME_LEN, "%s", _name);
        strncpy(ana->name, name, MAX_NAME_LEN);

        ana->tab = hash_create_table(__analysis_cmp, __analysis_key, name);
        if (ana->tab == NULL) {
                ret = ENOMEM;
                DERROR("ret (%d) %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        ret = ymalloc((void **)&ana->queue, sizeof(analysis_queue_t));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ana->queue->count = 0;

        ret = ymalloc((void **)&ana->new_queue, sizeof(analysis_queue_t));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ana->new_queue->count = 0;
        ana->private = private;

        if (unlikely(!ana->private)) {
                ret = sy_spin_init(&ana->queue_lock);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        ret = sy_spin_init(&ana->tab_lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __analysis_register(ana);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        *_ana = ana;
        
        return 0;
err_ret:
        return ret;
}

int analysis_private_create(const char *_name)
{
        int ret;
        analysis_t *ana;

#if 1
        DWARN("analysis private disabled\n");
        return 0;
#endif
        
        YASSERT(__private_analysis__ == NULL);
        
        ret = analysis_create(&ana, _name, 1);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        __private_analysis__ = ana;

        return 0;
err_ret:
        return ret;
}

int analysis_private_queue(const char *_name, const char *type, uint64_t _time)
{
        analysis_t *ana = __private_analysis__;

        if (ana) {
                return analysis_queue(ana, _name, type, _time);
        } else {
                return analysis_queue(default_analysis, _name, type, _time);
        }
}



void analysis_destroy(analysis_t *ana)
{
        if (analysis_list.inited == 0) {
                return;
        }

        sy_spin_lock(&analysis_list.lock);

        list_del(&ana->hook);

        sy_spin_unlock(&analysis_list.lock);
}

typedef struct {
        char *name;
        uint64_t total;
        uint64_t count;
        time_t prev;
        int ret;
} __arg_t;

static void __analysis_dump2(void *_arg, void *_ent)
{
        entry_t *ent = _ent;
        __arg_t *arg = _arg;

        if (strcmp(arg->name, ent->name) == 0) {
                arg->total = ent->time;
                arg->count = ent->count;
                arg->prev = ent->prev;
                arg->ret = 0;
                ent->count = 0;
                ent->time = 0;
                ent->prev = gettime();
        }
}

int analysis_dump(const char *tab, const char *name,  char *buf)
{
        int ret;
        analysis_t *ana;
        struct list_head *pos;
        __arg_t arg;

        if (analysis_list.inited == 0) {
                return 0;
        }

        arg.name = (void *)name;
        arg.ret = ENOENT;

        ret = sy_spin_lock(&analysis_list.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        list_for_each(pos, &analysis_list.list) {
                ana = (void *)pos;

                if (strcmp(tab, ana->tab->name) == 0) {

                        ret = sy_spin_lock(&ana->tab_lock);
                        if (unlikely(ret))
                                GOTO(err_lock, ret);

                        hash_iterate_table_entries(ana->tab, __analysis_dump2, &arg);

                        sy_spin_unlock(&ana->tab_lock);
                        //__analysis_dump1(ana);
                }
        }

        sy_spin_unlock(&analysis_list.lock);

        if (arg.ret == ENOENT || arg.count == 0) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        snprintf(buf, MAX_NAME_LEN, "%llu %llu %llu", (LLU)(gettime() - arg.prev), (LLU)arg.total,
                 (LLU)arg.count);

        return 0;
err_lock:
        sy_spin_unlock(&analysis_list.lock);
err_ret:
        return ret;
}

void analysis_merge()
{
        if (likely(__private_analysis__)) {
                __analysis__(__private_analysis__, NULL);
        }
}


static void __analysis_destroy(analysis_t *ana)
{
        hash_destroy_table(ana->tab, NULL, NULL);
        yfree((void **)&ana->queue);
        yfree((void **)&ana->new_queue);
        yfree((void **)&ana);
}

void analysis_private_destroy()
{
        if (__private_analysis__ == NULL) {
                DWARN("analysis private disabled\n");
                return;
        }

        __analysis_destroy(__private_analysis__);
        __private_analysis__ = NULL;
}



#include <errno.h>
#include <stdint.h>

#define DBG_SUBSYS S_LIBYLIB

#include "sysutil.h"
#include "ylib.h"
#include "dbg.h"
#include "bh.h"

typedef struct {
        struct list_head hook;
        char name[MAX_NAME_LEN];
        int (*exec)(void *arg);
        void *arg;
        int step;
        time_t tmo;
} entry_t;

typedef struct {
        struct list_head list;
        sy_spinlock_t lock;
} bh_t;

static bh_t bh;
static int inited = 0;

static int __exec(entry_t *ent, time_t now)
{
        int used = 0;
        struct timeval t1, t2;

        if (ent->tmo < now) {
                _gettimeofday(&t1, NULL);

                ent->exec(ent->arg);

                _gettimeofday(&t2, NULL);
                used = _time_used(&t1, &t2);

                if (used > 100 * 1000) {
                        DINFO("bh %s used %u ms\n", ent->name, 100);
                }

                ent->tmo = now + ent->step;
        }

        return used;
}

static void *__worker(void *_args)
{
        int ret;
        entry_t *ent;
        struct list_head *pos;
        time_t now;
        uint64_t total_used, a;

        (void) _args;

        total_used = 0;
        while (1) {
                sleep(1);
                now = time(NULL);

                total_used = 0;
                ret = sy_spin_lock(&bh.lock);
                if (ret)
                        GOTO(err_ret, ret);

                list_for_each(pos, &bh.list) {
                        ent = (void *)pos;

                        DBUG("%s step %u now %u tmo %u\n", ent->name,
                              ent->step, (int)now, (int)ent->tmo);

                        a = __exec(ent, now);
                        total_used += a;
                }
                
                sy_spin_unlock(&bh.lock);

                if (total_used > 1000 * 1000)
                        DWARN("prev op used %llu ms\n", (LLU)total_used / 1000);
        }

        return NULL;
err_ret:
        return NULL;
}

int bh_init()
{
        int ret;
        pthread_t th;
        pthread_attr_t ta;

        YASSERT(inited == 0);

        INIT_LIST_HEAD(&bh.list);

        ret = sy_spin_init(&bh.lock);
        if (ret)
                GOTO(err_ret, ret);

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        ret = pthread_create(&th, &ta, __worker, NULL);
        if (ret)
                GOTO(err_ret, ret);

        inited = 1;
        
        return 0;
err_ret:
        return ret;
}

int bh_register(const char *name, int (*_exec)(void *), void *args, int step)
{
        int ret;
        entry_t *ent;

        YASSERT(inited);

        ret = ymalloc((void *)&ent, sizeof(entry_t));
        if (ret)
                GOTO(err_ret, ret);

        strcpy(ent->name, name);
        ent->exec = _exec;
        ent->arg = args;
        ent->step = step;
        ent->tmo = time(NULL) + step;

        ret = sy_spin_lock(&bh.lock);
        if (ret)
                GOTO(err_ret, ret);

        list_add_tail(&ent->hook, &bh.list);

        sy_spin_unlock(&bh.lock);

        return 0;
err_ret:
        return ret;
}



#include <limits.h>
#include <time.h>
#include <string.h>
#include <sys/epoll.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBSCHEDULE

#include "sysutil.h"
#include "net_proto.h"
#include "ylib.h"
#include "sdevent.h"
#include "../net/xnect.h"
#include "net_table.h"
#include "mem_cache.h"
#include "rpc_table.h"
#include "configure.h"
#include "core.h"
#include "corenet_maping.h"
#include "corenet_connect.h"
#include "corenet.h"
#include "corerpc.h"
#include "sdfs_aio.h"
#include "schedule.h"
#include "bh.h"
#include "net_global.h"
#include "cpuset.h"
#include "ylib.h"
#include "variable.h"
#include "adt.h"
#include "dbg.h"

#define THREAD_MAX 100

typedef struct {
        int used;
        void *array[VARIABLE_MAX];
} variable_t;

/**
 * core线程启动时，调用variable_thread初始化该变量
 */
static __thread int __thread_id__ = -1;

static variable_t *__array__;
static sy_spinlock_t __lock__;
static int __thread_idx__;


void * IO_FUNC variable_get(variable_type_t type)
{
        int id = __thread_id__;
        
        if (id == -1)
                return NULL;
        else
                return __array__[id].array[type];
}

void * IO_FUNC variable_get_byctx(void *ctx, int type)
{
        if (ctx) {
                variable_t *variable = ctx;
                return variable->array[type];
        } else {
                return variable_get(type);
        }
}

void * IO_FUNC variable_get_ctx()
{
        if (__thread_id__ == -1) {
                UNIMPLEMENTED(__DUMP__);
                return NULL;
        } else
                return &__array__[__thread_id__];
}

int variable_thread()
{
        //YASSERT(__thread_id__ != -1);
        return __thread_id__;
}

void variable_set(variable_type_t type, void *variable)
{
        YASSERT(__thread_id__ != -1);
        YASSERT(__thread_id__ < THREAD_MAX);
        YASSERT(variable);
        int a = type, b = VARIABLE_MAX;
        (void) a;
        (void) b;
        YASSERT(type < VARIABLE_MAX);

        YASSERT(__array__[__thread_id__].array[type] == NULL);
        __array__[__thread_id__].array[type] = variable;
}

void variable_unset(variable_type_t type)
{
        YASSERT(__thread_id__ != -1);
        YASSERT(__thread_id__ < THREAD_MAX);
        YASSERT(type < VARIABLE_MAX);

        YASSERT(__array__[__thread_id__].array[type]);
        __array__[__thread_id__].array[type] = NULL;
}


void variable_exit()
{
        int ret, i;

        ret = sy_spin_lock(&__lock__);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        
        for (i = 0; i < VARIABLE_MAX; i++) {
                YASSERT(__array__[__thread_id__].array[i] == NULL);
        }

        __array__[__thread_id__].used = 0;
        
        sy_spin_unlock(&__lock__);

        DINFO("exit thread id %u\n", __thread_id__);
}

int variable_newthread()
{
        int ret, i;

        YASSERT(__thread_id__ == -1);

        ret = sy_spin_lock(&__lock__);
        if (ret)
                GOTO(err_ret, ret);

#if 1
        for (i = 0; i < THREAD_MAX; i++) {
                if (!__array__[i].used) {
                        __thread_id__ = i;
                        __array__[i].used = 1;
                        break;
                }
        }

        YASSERT(i != THREAD_MAX);
#else
        __thread_id__ = __thread_idx__;
        __thread_idx__++;
#endif

        sy_spin_unlock(&__lock__);

        DINFO("new thread id %u\n", __thread_id__);

        return 0;
err_ret:
        return ret;
}

int variable_init()
{
        int ret;

        ret = ymalloc((void **)&__array__, sizeof(*__array__) * THREAD_MAX);
        if (ret)
                GOTO(err_ret, ret);

        memset(__array__, 0x0, sizeof(*__array__) * THREAD_MAX);

        ret = sy_spin_init(&__lock__);
        if (ret)
                GOTO(err_ret, ret);

        __thread_idx__ = 0;

        return 0;
err_ret:
        return ret;
}

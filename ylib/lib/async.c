

#include <errno.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/socket.h>

#define DBG_SUBSYS S_LIBYLIB

#include "sysutil.h"
#include "ylib.h"
#include "dbg.h"

#define MAX_QUEUE_LEN 100

typedef struct {
        struct list_head hook;
        void *addr;
        uint32_t len;
} async_node_t;

typedef struct {
        int len;
        sem_t sem;
        sy_spinlock_t lock;
        struct list_head list;
} async_t;

async_t async;

void *__async_worker(void *args)
{
        int ret;
        async_node_t *node;

        (void) args;

        while (srv_running) {
                ret = _sem_wait(&async.sem);
                if (ret)
		        GOTO(err_ret, ret);

                sy_spin_lock(&async.lock);

                YASSERT(!(list_empty(&async.list)));

                node = (async_node_t *)async.list.next;

                list_del(&node->hook);

                async.len --;

                sy_spin_unlock(&async.lock);

                msync(node->addr, node->len, MS_ASYNC);
                munmap(node->addr, node->len);

                yfree((void **)&node);
        }

        return NULL;
err_ret:
        return NULL;
}

int async_init()
{
        int ret;
        pthread_t th;
        pthread_attr_t ta;

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);
        pthread_attr_setstacksize(&ta, 1<<21);

        INIT_LIST_HEAD(&async.list);

        sem_init(&async.sem, 0, 0);
        sy_spin_init(&async.lock);
        async.len = 0;

        ret = pthread_create(&th, &ta, __async_worker, NULL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int async_push(void *addr, uint32_t len)
{
        int ret, slp = 0;
        async_node_t *node;

        ret = ymalloc((void **)&node, sizeof(async_node_t));
        if (ret)
                GOTO(err_ret, ret);

        node->addr = addr;
        node->len = len;

        sy_spin_lock(&async.lock);

        list_add(&node->hook, &async.list); 

        sem_post(&async.sem);

        async.len ++;
        slp = async.len - MAX_QUEUE_LEN;

        sy_spin_unlock(&async.lock);

        if (slp > 0) {
                if (slp > MAX_QUEUE_LEN) {
                        DWARN("usleep %u\n", slp);
                }

                usleep(slp);
        }

        return 0;
err_ret:
        return ret;
}


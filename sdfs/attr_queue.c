#include <unistd.h>
#include <string.h>
#include <sys/eventfd.h>
#include <errno.h>
#include <dirent.h>

#define DBG_SUBSYS S_YFSLIB

#include "ylib.h"
#include "net_global.h"
#include "network.h"
#include "core.h"
#include "main_loop.h"
#include "schedule.h"
#include "md_lib.h"
#include "io_analysis.h"
#include "attr_queue.h"
#include "xattr.h"
#include "dbg.h"

#define ATTR_OP_EXTERN    0x00001
#define ATTR_OP_SETTIME   0x00002
#define ATTR_OP_TRUNCATE  0x00004

#define ATTR_OP_MAX 512

typedef struct {
        sy_spinlock_t lock;
        sem_t sem;
        fileid_t fileid;
        int op;
        size_t size;
        __set_time atime;
        __set_time btime;
        __set_time ctime;
        __set_time mtime;
} attr_op_t;

typedef struct {
        sy_spinlock_t lock;
        struct list_head wait_list;
        int eventfd;
        int begin;
        int len;
        attr_op_t *queue;
} attr_queue_t;

static int __count__ = 0;
static attr_queue_t *__attr_queue__ = NULL;

typedef struct {
        struct list_head hook;
        task_t task;
} wait_t;
        

static void __attr_queue_set(attr_op_t *attr_op, const fileid_t *fileid, int op, const void *arg)
{
        int ret;

        ret = sy_spin_lock(&attr_op->lock);
        YASSERT(ret == 0);

        attr_op->fileid = *fileid;
        if (op == ATTR_OP_EXTERN) {
                const uint64_t *size = arg;
                attr_op->size = *size;
        } else if (op == ATTR_OP_SETTIME) {
                const setattr_t *setattr = arg;
                attr_op->atime = setattr->atime;
                attr_op->btime = setattr->btime;
                attr_op->ctime = setattr->ctime;
                attr_op->mtime = setattr->mtime;
        } else if (op == ATTR_OP_TRUNCATE) {
                const uint64_t *size = arg;
                attr_op->size = *size;
        } else {
                UNIMPLEMENTED(__DUMP__);
        }

        attr_op->op = op;
        
        sy_spin_unlock(&attr_op->lock);
}

static void __attr_queue_unset(attr_op_t *attr_op)
{
        int ret;

        ret = sy_spin_lock(&attr_op->lock);
        YASSERT(ret == 0);

        memset(&attr_op->fileid, 0x0, sizeof(attr_op->fileid));
        attr_op->op = 0;

        sy_spin_unlock(&attr_op->lock);
}

static int __attr_queue(const fileid_t *fileid, int op, const void *arg)
{
        int ret, retry = 0;
        attr_queue_t *attr_queue = &__attr_queue__[fileid_hash(fileid) % __count__];
        wait_t wait;

        DBUG("queue "CHKID_FORMAT"\n", CHKID_ARG(fileid));
        
retry:
        ret = sy_spin_lock(&attr_queue->lock);
        if (ret)
                GOTO(err_ret, ret);

        if (attr_queue->len + 1 > ATTR_OP_MAX) {
                wait.task = schedule_task_get();
                list_add_tail(&wait.hook, &attr_queue->wait_list);
                sy_spin_unlock(&attr_queue->lock);

                DBUG("attr queue wait\n");
                ret = schedule_yield1("attr_queue_wait", NULL, NULL, NULL, -1);
                if (ret) {
                        GOTO(err_ret, ret);
                }

                DBUG("attr queue wait return\n");
                
                if (retry > 10) {
                        DWARN("retry %u\n", retry);
                }

                retry++;
                goto retry;
        }

        if (retry > 5) {
                DINFO("retry %u success\n", retry);
        }

        DBUG("attr queue len %u\n", attr_queue->len);
        attr_op_t *attr_op = &attr_queue->queue[(attr_queue->begin + attr_queue->len)
                                                % ATTR_OP_MAX];
        __attr_queue_set(attr_op, fileid, op, arg);
        attr_queue->len++;
        
        sy_spin_unlock(&attr_queue->lock);

        uint64_t e = 1;
        ret = write(attr_queue->eventfd, &e, sizeof(e));
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }
                
        
        return 0;
err_ret:
        return ret;
}

int attr_queue_settime(const fileid_t *fileid, const setattr_t *setattr)
{
        return __attr_queue(fileid, ATTR_OP_SETTIME, setattr);
}

int attr_queue_extern(const fileid_t *fileid, uint64_t size)
{
        return __attr_queue(fileid, ATTR_OP_EXTERN, &size);
}

int attr_queue_truncate(const fileid_t *fileid, uint64_t size)
{
        return __attr_queue(fileid, ATTR_OP_TRUNCATE, &size);
}

int attr_queue_update(const fileid_t *fileid, md_proto_t *md)
{
        int ret, begin, count;
        attr_queue_t *attr_queue = &__attr_queue__[fileid_hash(fileid) % __count__];
        attr_op_t *attr_op;

        DBUG("update "CHKID_FORMAT"\n", CHKID_ARG(fileid));
        
        begin = attr_queue->begin;
        count = attr_queue->len;

        for (int i = 0; i < count; i++) {
                attr_op = &attr_queue->queue[(i + begin) % ATTR_OP_MAX];

                if (fileid_cmp(fileid, &attr_op->fileid) != 0) {
                        continue;
                }

                ret = sy_spin_lock(&attr_op->lock);
                if (ret)
                        GOTO(err_ret, ret);

                if (fileid_cmp(fileid, &attr_op->fileid) != 0) {
                        DBUG("update "CHKID_FORMAT" skiped\n", CHKID_ARG(&attr_op->fileid));
                        sy_spin_unlock(&attr_op->lock);
                        continue;
                }
                
                if (attr_op->op == ATTR_OP_EXTERN) {
                        md->at_size = (md->at_size > attr_op->size)
                                ? md->at_size : attr_op->size;
                        DBUG("update "CHKID_FORMAT" size\n", CHKID_ARG(&attr_op->fileid));
                } else if (attr_op->op == ATTR_OP_SETTIME) {
                        setattr_t setattr;
                        setattr_init(&setattr, -1, -1, NULL, -1, -1, -1);
                        setattr.atime = attr_op->atime;
                        setattr.btime = attr_op->btime;
                        setattr.ctime = attr_op->ctime;
                        setattr.mtime = attr_op->mtime;
                        md_attr_update(md, &setattr);
                        DBUG("update "CHKID_FORMAT" time\n", CHKID_ARG(&attr_op->fileid));
                } else if (attr_op->op == ATTR_OP_TRUNCATE) {
                        md->at_size = attr_op->size;
                } else {
                        UNIMPLEMENTED(__DUMP__);
                }
                
                sy_spin_unlock(&attr_op->lock);
        }
        
        return 0;
err_ret:
        return ret;
}

static void __attr_queue_run__(void *arg)
{
        int ret, retry = 0;
        attr_op_t *attr_op = arg;
        setattr_t setattr;

retry:
        if (attr_op->op == ATTR_OP_EXTERN) {
                ret = md_extend(&attr_op->fileid, attr_op->size);
        } else if (attr_op->op == ATTR_OP_SETTIME) {
                setattr_init(&setattr, -1, -1, NULL, -1, -1, -1);
                setattr.atime = attr_op->atime;
                setattr.btime = attr_op->btime;
                setattr.ctime = attr_op->ctime;
                setattr.mtime = attr_op->mtime;
                
                ret = md_setattr(&attr_op->fileid, &setattr, 1);
        } else if (attr_op->op == ATTR_OP_TRUNCATE) {
                ret = md_truncate(&attr_op->fileid, attr_op->size);
        } else {
                UNIMPLEMENTED(__DUMP__);
        }

        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        sem_post(&attr_op->sem);
        
        return;
err_ret:
        DWARN("set "CHKID_FORMAT" fail\n", CHKID_ARG(&attr_op->fileid));
        sem_post(&attr_op->sem);
        return;
}

static int __attr_queue_run(attr_queue_t *attr_queue)
{
        int ret, begin, count;
        attr_op_t *attr_op;
        wait_t *wait;

        begin = attr_queue->begin;
        count = attr_queue->len;

        DBUG("run[%d, %d]\n", begin, begin + count);
        
        for (int i = 0; i < count; i++) {
                attr_op = &attr_queue->queue[(i + begin) % ATTR_OP_MAX];

                ret = core_request_async(fileid_hash(&attr_op->fileid), -1,
                                         "attr_queue_run", __attr_queue_run__,
                                         attr_op);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);
        }

        for (int i = 0; i < count; i++) {
                attr_op = &attr_queue->queue[(i + begin) % ATTR_OP_MAX];

                ret = sem_wait(&attr_op->sem);
                if (ret)
                        GOTO(err_ret, ret);

                DBUG("set "CHKID_FORMAT" success\n", CHKID_ARG(&attr_op->fileid));
                
                __attr_queue_unset(attr_op);

                ret = sy_spin_lock(&attr_queue->lock);
                if (ret)
                        GOTO(err_ret, ret);

                attr_queue->begin = (attr_queue->begin + 1) % ATTR_OP_MAX;
                attr_queue->len--;

                if (!list_empty(&attr_queue->wait_list)) {
                        wait = (void *)attr_queue->wait_list.next;
                        list_del(&wait->hook);
                } else {
                        wait = NULL;
                }
                
                sy_spin_unlock(&attr_queue->lock);

                if (wait) {
                        schedule_resume(&wait->task, 0, NULL);
                }
        }
        
        return 0;
err_ret:
        return ret;
}


static void *__attr_queue_worker(void *arg)
{
        attr_queue_t *attr_queue = arg;

        while (1) {
                eventfd_poll(attr_queue->eventfd, 1, NULL);

                while (attr_queue->len) {
                        __attr_queue_run(attr_queue);
                }
        }

        pthread_exit(NULL);
}


static int __attr_queue_init(attr_queue_t *attr_queue)
{
        int ret;

        INIT_LIST_HEAD(&attr_queue->wait_list);

        ret = sy_spin_init(&attr_queue->lock);
        if (ret)
                GOTO(err_ret, ret);
        
        ret = ymalloc((void **)&attr_queue->queue, sizeof(attr_op_t) * ATTR_OP_MAX);
        if (ret)
                GOTO(err_ret, ret);

        memset(attr_queue->queue, 0x0, sizeof(attr_op_t) * ATTR_OP_MAX);
        attr_queue->begin = 0;
        attr_queue->len = 0;

        for (int i = 0; i < ATTR_OP_MAX; i++) {
                ret = sy_spin_init(&attr_queue->queue[i].lock);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);

                ret = sem_init(&attr_queue->queue[i].sem, 0, 0);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);
        }

        int fd = eventfd(0, EFD_CLOEXEC);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        attr_queue->eventfd = fd;

        ret = sy_thread_create2(__attr_queue_worker, attr_queue, "attr_queue");
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int attr_queue_init()
{
        int ret, count = gloconf.polling_core * 4;
        attr_queue_t *attr_queue;

        ret = ymalloc((void **)&__attr_queue__, sizeof(*__attr_queue__) * count);
        if (ret)
                GOTO(err_ret, ret);
        
        for (int i = 0; i < count; i++) {
                attr_queue = &__attr_queue__[i];

                ret = __attr_queue_init(attr_queue);
                if (ret)
                        GOTO(err_ret, ret);
        }

        __count__ = count;
        
        return 0;
err_ret:
        return ret;
}

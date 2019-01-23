#ifndef __JOB_H__
#define __JOB_H__

#include <stdint.h>
#include <sys/types.h>
#include <stdint.h>
#include <semaphore.h>
#ifndef __CYGWIN__
#include <aio.h>
#endif

#include "ytime.h"
#include "timer.h"
#include "sdfs_list.h"
#include "sdfs_id.h"
#include "ylock.h"
#include "sdfs_buffer.h"
#include "timer.h"
#include "sdfs_conf.h"
#include "dbg.h"

extern int ymalloc(void **ptr, size_t size);

#define JOBLIST_DEFAULT_LEN  1000
#define JOB_MAX_DELAY    10
#define JOB_BUF_LEN    256
#define NET_HANDLE_LEN 32
#define MAX_STACK 2

typedef enum {
        //STATUS_NULL,
        STATUS_PREP,
        STATUS_SEND,
        STATUS_WAIT,
        STATUS_ERROR,
        STATUS_DONE,
} job_status_t;

typedef enum {
        NIO_NULL,
        NIO_NONBLOCK,
        NIO_BLOCK,
} nio_type_t;

typedef enum {
        NIO_NORMAL,
        NIO_PRIORITY,
} nio_priority_t;

typedef void (*handler_t)(void *);

/*network iocb*/
typedef struct {
        nio_type_t type;
        nio_priority_t priority;
        int __pad__;
        mbuffer_op_t op;
        uint32_t offset; /*msg send offset*/
        uint32_t steps;
        buffer_t *buf;
        handler_t reply; /*handler when reply*/
        handler_t error; /*handler when error*/
        handler_t sent; /*handler after sent*/
} niocb_t;

typedef enum {
        JOB_NULL,
        JOB_WAITING,
        JOB_IN_QUEUE,
        JOB_IN_PROGRESS,
} job_queue_status;

#pragma pack(8)
typedef struct {
        uint32_t idx; /*position at jobdock*/
        uint32_t seq;
} jobid_t;
#pragma pack()

#define JOB_NAME_LEN 36
#define DUMP_BUF_LEN 512
#define JOB_CONTEXT_LEN 8096

typedef struct __job {
        struct list_head hook;
        uint32_t status;
        //int32_t ret; //retval of previous op
        uint16_t retry;
        int16_t nethash; /*net hash*/
        uint64_t key; /*hash key*/
        msgid_t msgid;
        jobid_t jobid;
        niocb_t iocb;

        /**
         * nfs_job_context_t
         * yfs_job_context_write_t
         * yfs_job_context_read_t
         */
        void *context;
        void *jobtracker;
        int (*state_machine)(struct __job *, char *name);
        void (*free)(struct __job*);
        const char *(*dump)(struct __job*, int mask);
        void (*timeout_handler)(struct __job *);

        char *dumpinfo;
        char name[JOB_NAME_LEN];
        char buf[JOB_BUF_LEN];
        char __context__[JOB_CONTEXT_LEN];

        buffer_t request;
        buffer_t reply;
        //char net[NET_HANDLE_LEN];
        net_handle_t net;
        sockid_t sock;
        sem_t sem;
        char in_queue; //remove me
        char in_wait;
        uint16_t steps;
        float update_load;
        time_t sleep;
        time_t timeout;
        struct timeval queue_time;
        uint32_t uid; //nfs创建文件时，会通过cred 来传递uid、gid。暂时通过job_t来传递。 todo 优化
        uint32_t gid;
        struct {
                struct timeval create; /*create time*/
                struct timeval step; /*create time*/
        } timer;
} job_t;

extern int job_resume(void *obj, int retval);
extern int job_exec(job_t *job, int retval);

extern uint32_t job_idx(job_t *job);
extern uint32_t job_seq(job_t *job);

static inline const char *job_dump(job_t *job, int x)
{
        int ret;

        if (x) {
                if (job->dumpinfo == NULL) {
                        ret = ymalloc((void **)&job->dumpinfo, MAX_BUF_LEN);
                        if (ret)
                                return NULL;
                }

                snprintf(job->dumpinfo, DUMP_BUF_LEN, "job[%u] %s seq %u" \
                         " status %u tmo %u ",
                         job_idx(job), job->name, job_seq(job),
                         job->status, (int)(job->timeout));
        }

        return job->dumpinfo;
}

extern void job_sleep(job_t *job, int usec, int hash);
static inline void job_setname(job_t *job, const char *name)
{
        if (job) {
                strncpy(job->name, name, JOB_NAME_LEN);
        }
}

typedef int (*state_machine_t)(job_t *, char *name);
typedef int (*event_job_t)(job_t *);
typedef void (*append_func)(job_t *, struct list_head *);
int job_resume1(job_t *job, int retval, int idx, uint64_t slp);

#endif

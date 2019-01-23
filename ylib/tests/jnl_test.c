

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "jnl_proto.h"
#include "sysutil.h"
#include "job_tracker.h"
#include "job_dock.h"
#include "dbg.h"

jobtracker_t *test_jobtracker;

typedef struct {
        sem_t sem;
        int   ret;
} __block_t;

int __resume_sem(job_t *job, char *name)
{
        __block_t *blk;

        (void) *name;

        blk = job->context;
        sem_post(&blk->sem);

        return 0;
}

int jnl_flush(jnl_handle_t *jnl)
{
        int ret;
        job_t *job;
        __block_t blk;

        ret = job_create(&job, test_jobtracker, "jnl_flush");
        if (ret)
                GOTO(err_ret, ret);

        job->context = &blk;
        job->state_machine = __resume_sem;
 	sem_init(&blk.sem, 0, 0);
        blk.ret = 0;

        ret = jnl_append(jnl, NULL, 0, job, 0, NULL);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        ret = _sem_wait(&blk.sem);
        if (ret)
                GOTO(err_ret, ret);

        job->context = NULL;
        job_destroy(job);

        return 0;
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret, i, max, len;
        char c_opt;
        char *home, buf[MAX_BUF_LEN];
        jnl_handle_t jnl;
        int64_t offset;

        if (argc < 3) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        while ((c_opt = getopt(argc, argv, "c:")) > 0)
                switch (c_opt) {
                case 'c':
                        max = atoi(optarg);
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        EXIT(1);
                }

        home = argv[argc - 1];

        DINFO("cycle %u home %s\n", max, home);

        analysis_init();
        
        ret = jobdock_init(NULL, NULL, NULL);
        if (ret)
                GOTO(err_ret, ret);

        ret = jobtracker_create(&test_jobtracker, 1, "test");

        ret = jnl_open(home, &jnl, O_SYNC | O_RDWR);
        if (ret)
                GOTO(err_ret, ret);

        for (i = 0; i < max; i ++) {
                len = random();
                len = len % 300;

                if (len == 0)
                        len = 100;

                DBUG("idx %u len %u\n", i, len);

                YASSERT(len);

                offset = jnl_append(&jnl, buf, len, NULL, 0, NULL);
                if (offset < 0) {
                        ret = -offset;
                        GOTO(err_ret, ret);
                }
        }

        jnl_flush(&jnl);

        DINFO("jnl offset %llu\n", (LLU)jnl.offset);

        return 0;
err_ret:
        return ret;
}

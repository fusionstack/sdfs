

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <dirent.h>

#include "configure.h"
#include "schedule.h"
#include "sdfs_lib.h"

#define PROG    "ycp"

enum {
        PATH_HOST,
        PATH_USS,
};

struct clone_path {
        int type;
        char *path;
        union {
                int fd;
                fileid_t fileid;
        } id;
        uint64_t size;
};

struct clone_args {
        struct clone_path source;
        struct clone_path target;
};

/*
 * Global variable
 */
int verbose;

static void usage()
{
        fprintf(stderr, "\nusage: ycp [-vh] SOURCE DIST\n"
                "\t-v --verbose         Show verbose message\n"
                "\t-h --help            Show this help\n"
                "\n"
                "copy from:\n"
                "       a) host to uss\n"
                "       b) uss to host\n"
                "       c) uss to uss\n"
                "\n"
                "host format: `/path/of/host/file'\n"
                " uss format: `:/path/of/uss/file\n"
                "\n"
               );
}

static int __clone_path_init_host_source(struct clone_args *cargs)
{
        int ret, fd;
        struct stat stbuf;

        fd = open(cargs->source.path, O_RDONLY);
        if (fd < 0) {
                fprintf(stderr, "can't open host file: %s, %s\n",
                        cargs->source.path, strerror(errno));

                ret = errno;
                goto err_ret;
        }

        ret = fstat(fd, &stbuf);
        if (ret) {
                fprintf(stderr, "can't stat host file: %s, %s\n",
                        cargs->source.path, strerror(errno));

                ret = errno;
                goto err_ret;
        }

        if (!S_ISREG(stbuf.st_mode)) {
                fprintf(stderr, "not a regular file\n");

                ret = EINVAL;
                goto err_ret;
        }

        cargs->source.id.fd = fd;
        cargs->source.size = stbuf.st_size;
        cargs->source.type = PATH_HOST;

        return 0;
err_ret:
        return ret;
}

static int __clone_path_init_host_target(struct clone_args *cargs)
{
        int ret, fd;

        fd = open(cargs->target.path, O_WRONLY | O_CREAT | O_EXCL, 0644);
        if (fd < 0) {
                fprintf(stderr, "can't open host file: %s, %s\n",
                        cargs->target.path, strerror(errno));

                ret = errno;
                goto err_ret;
        }

        ret = ftruncate(fd, cargs->source.size);
        if (ret) {
                fprintf(stderr, "can't truncate host file: %s, %s\n",
                        cargs->target.path, strerror(errno));

                ret = errno;
                goto err_ret;
        }

        cargs->target.id.fd = fd;
        cargs->target.size = cargs->source.size;
        cargs->target.type = PATH_HOST;

        return 0;
err_ret:
        return ret;
}

static int __clone_path_init_uss_source(struct clone_args *cargs)
{
        int ret;
        fileid_t fileid;
        struct stat stbuf;

        cargs->source.path++; /* Skip ':' */

        ret = sdfs_lookup_recurive(cargs->source.path, &fileid);
        if (ret) {
                fprintf(stderr, "can't open uss file: %s, %s\n",
                        cargs->source.path, strerror(ret));
                goto err_ret;
        }

        ret = sdfs_getattr(&fileid, &stbuf);
        if (ret) {
                fprintf(stderr, "can't stat uss file: %s, %s\n",
                        cargs->source.path, strerror(ret));
                goto err_ret;
        }

        if (!S_ISREG(stbuf.st_mode)) {
                fprintf(stderr, "not a regular file\n");
                ret = EINVAL;
                goto err_ret;
        }

        cargs->source.id.fileid = fileid;
        cargs->source.size = stbuf.st_size;
        cargs->source.type = PATH_USS;

        return 0;
err_ret:
        return ret;
}

static int __clone_path_init_uss_target(struct clone_args *cargs)
{
        int ret;
        char name[MAX_PATH_LEN];
        fileid_t parent, fileid;

        cargs->target.path++; /* Skip ':' */

        ret = sdfs_splitpath(cargs->target.path, &parent, name);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_create(&parent, name, &fileid, 0644, 0, 0);
        if (ret) {
                fprintf(stderr, "can't create uss file: %s, %s\n",
                        cargs->target.path, strerror(ret));
                goto err_ret;
        }

        ret = sdfs_truncate(&fileid, cargs->source.size);
        if (ret) {
                fprintf(stderr, "can't truncate uss file: %s, %s\n",
                        cargs->target.path, strerror(ret));
                goto err_ret;
        }

        cargs->target.id.fileid = fileid;
        cargs->target.size = cargs->source.size;
        cargs->target.type = PATH_USS;

        return 0;
err_ret:
        return ret;
}

static int clone_path_init(struct clone_args *cargs)
{
        int ret;

        if (cargs->source.path[0] == ':')
                ret = __clone_path_init_uss_source(cargs);
        else
                ret = __clone_path_init_host_source(cargs);

        if (ret)
                goto err_ret;

        if (cargs->target.path[0] == ':') {
                ret = __clone_path_init_uss_target(cargs);
        } else {
                if (cargs->source.type == PATH_HOST) {
                        fprintf(stderr, "not support clone from host to host\n");

                        ret = EINVAL;
                        goto err_ret;
                }
                ret = __clone_path_init_host_target(cargs);
        }

        if (ret)
                goto err_ret;

        return 0;
err_ret:
        return ret;
}

/******************************************************************************/

struct ioctx {
        buffer_t buf;
        uint64_t off;
        int size;
        union {
                int fd;
                fileid_t fileid;
        } target_id;
};

#define MAX_JOB_WAIT    128
sem_t wait_sem;

#define CLONE_BUFSZ     (512*1024)
const char __zero_buf[CLONE_BUFSZ] = { 0 };

struct io_percent {
        pthread_spinlock_t lock;
        uint64_t total;
        uint64_t done;
};

struct io_percent io_percent;

/******************************************************************************/

static void percent_init(uint64_t total)
{
        if (verbose) {
                memset(&io_percent, 0x00, sizeof(struct io_percent));

                setbuf(stdout, NULL);
                setbuf(stderr, NULL);

                io_percent.total = total;
                pthread_spin_init(&io_percent.lock, PTHREAD_PROCESS_PRIVATE);
        }
}

static void percent_update(uint64_t done)
{
        char buf[8];

        if (verbose) {
                pthread_spin_lock(&io_percent.lock);
                io_percent.done += done;
                snprintf(buf, 8, "%6.2f%%",
                         (double)(io_percent.done * 1.0 / io_percent.total * 100));
                printf("\b\b\b\b\b\b\b\b%s", buf);
                pthread_spin_unlock(&io_percent.lock);
        }
}

static void percent_done()
{
        if (verbose) {
                printf("\n");
        }
}

static int __cb_host2uss(void *arg, int ret)
{
        struct ioctx *ctx = arg;

        if (ctx->size != ret) {
                fprintf(stderr, "callback return error: %d, %d\n", ret, ctx->size);
                exit(EIO);
        }

        mbuffer_free(&ctx->buf);
        yfree((void **)&ctx);

        sem_post(&wait_sem);

        percent_update(ret);

        return 0;
}

static int __clone_host2uss(struct clone_args *cargs)
{
        int ret, fd, len, cnt;
        char buf[CLONE_BUFSZ];
        fileid_t *fileid;
        uint64_t rest, off;
        struct ioctx *ctx;

        fd = cargs->source.id.fd;
        fileid = &cargs->target.id.fileid;

        off = 0;
        rest = cargs->source.size;

        while (rest > 0) {
                if (rest >= CLONE_BUFSZ)
                        cnt = CLONE_BUFSZ;
                else
                        cnt = (int)rest;

                len = pread(fd, buf, cnt, off);
                if (len != cnt) {
                        fprintf(stderr, "read error: (%d,%d)\n", len, cnt);
                        ret = EIO;
                        goto err_ret;
                }

                /*
                 * If this is a zero buffer, skip it
                 */
                if (!memcmp(__zero_buf, buf, cnt)) {
                        percent_update(cnt);
                        goto next;
                }

                ret = ymalloc((void **)&ctx, sizeof(struct ioctx));
                if (ret)
                        GOTO(err_ret, ret);

                ret = mbuffer_init(&ctx->buf, 0);
                if (ret)
                        GOTO(err_ret, ret);

                ret = mbuffer_copy(&ctx->buf, buf, cnt);
                if (ret)
                        GOTO(err_ret, ret);

                ctx->size = cnt;

                ret = sdfs_write_async(fileid, &ctx->buf, cnt, off, __cb_host2uss, ctx);
                if (ret)
                        GOTO(err_ret, ret);

                sem_wait(&wait_sem);
next:
                off += cnt;
                rest -= cnt;
        }

        while (1) {
                int val;

                sem_getvalue(&wait_sem, &val);

                if (val == MAX_JOB_WAIT)
                        break;

                sleep(1);
        }

        percent_done();

        return 0;
err_ret:
        return ret;
}

static int __cb_uss2host(void *arg, int ret)
{
        int fd, len;
        char buf[CLONE_BUFSZ];
        struct ioctx *ctx = arg;

        if (ctx->size != ret) {
                fprintf(stderr, "callback return error: %d, %d\n", ret, ctx->size);
                exit(EIO);
        }

        fd = ctx->target_id.fd;

        ret = mbuffer_get(&ctx->buf, buf, ctx->size);
        if (ret) {
                fprintf(stderr, "buffer_get error\n");
                exit(EIO);
        }

        /*
         * If this is a zero buffer, skip it
         */
        if (!memcmp(__zero_buf, buf, ctx->size)) {
                goto out;
        }

        len = pwrite(fd, buf, ctx->size, ctx->off);
        if (len != ctx->size) {
                fprintf(stderr, "write error: (%d,%d)\n", len, ctx->size);
                exit(EIO);
        }

out:
        percent_update(ctx->size);

        mbuffer_free(&ctx->buf);
        yfree((void **)&ctx);

        sem_post(&wait_sem);

        return 0;
}

static int __clone_uss2host(struct clone_args *cargs)
{
        int ret, cnt;
        fileid_t *fileid;
        uint64_t rest, off;
        struct ioctx *ctx;

        fileid = &cargs->source.id.fileid;

        off = 0;
        rest = cargs->source.size;

        while (rest > 0) {
                if (rest >= CLONE_BUFSZ)
                        cnt = CLONE_BUFSZ;
                else
                        cnt = (int)rest;

                ret = ymalloc((void **)&ctx, sizeof(struct ioctx));
                if (ret)
                        GOTO(err_ret, ret);

                ret = mbuffer_init(&ctx->buf, 0);
                if (ret)
                        GOTO(err_ret, ret);

                ctx->target_id.fd = cargs->target.id.fd;
                ctx->size = cnt;
                ctx->off = off;

                ret = sdfs_read_async(fileid, &ctx->buf, cnt, off, __cb_uss2host, ctx);
                if (ret)
                        GOTO(err_ret, ret);

                sem_wait(&wait_sem);

                off += cnt;
                rest -= cnt;
        }

        while (1) {
                int val;

                sem_getvalue(&wait_sem, &val);

                if (val == MAX_JOB_WAIT)
                        break;

                sleep(1);
        }

        percent_done();

        return 0;
err_ret:
        return ret;
}

static int __cb_uss2uss(void *arg, int ret)
{
        struct ioctx *ctx = arg;

        if (ctx->size != ret) {
                fprintf(stderr, "callback return error: %d, %d\n", ret, ctx->size);
                exit(EIO);
        }

        mbuffer_free(&ctx->buf);
        yfree((void **)&ctx);

        percent_update(ret);

        sem_post(&wait_sem);

        return 0;
}

static int __clone_uss2uss(struct clone_args *cargs)
{
        int ret, len, cnt, retry, retry_max;
        char buf[CLONE_BUFSZ];
        fileid_t *sfd, *tfd;
        uint64_t rest, off;
        struct ioctx *ctx;

        sfd = &cargs->source.id.fileid;
        tfd = &cargs->target.id.fileid;

        retry = 0;
        retry_max = 3;
        off = 0;
        rest = cargs->source.size;

        while (rest > 0) {
                if (rest >= CLONE_BUFSZ)
                        cnt = CLONE_BUFSZ;
                else
                        cnt = (int)rest;

                ret = ymalloc((void **)&ctx, sizeof(struct ioctx));
                if (ret)
                        GOTO(err_ret, ret);

                ret = mbuffer_init(&ctx->buf, 0);
                if (ret)
                        GOTO(err_ret, ret);

                len = sdfs_read_sync(sfd, &ctx->buf, cnt, off);
                if (len != cnt) {
                        fprintf(stderr, "read error: (%d,%d)\n", len, cnt);
                        ret = EIO;
                        goto err_ret;
                }

                ret = mbuffer_get(&ctx->buf, buf, cnt);
                if (ret)
                        GOTO(err_ret, ret);

                /*
                 * If this is a zero buffer, skip it
                 */
                if (!memcmp(__zero_buf, buf, cnt)) {
                        percent_update(cnt);

                        mbuffer_free(&ctx->buf);
                        yfree((void **)&ctx);
                        goto next;
                }

                ctx->size = cnt;

retry:
                ret = sdfs_write_async(tfd, &ctx->buf, cnt, off, __cb_uss2uss, ctx);
                if (ret) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, retry_max, 1000);
                }

                sem_wait(&wait_sem);
next:
                off += cnt;
                rest -= cnt;
        }

        while (1) {
                int val;

                sem_getvalue(&wait_sem, &val);

                if (val == MAX_JOB_WAIT)
                        break;

                sleep(1);
        }

        percent_done();

        return 0;
err_ret:
        return ret;

}

static int do_clone(struct clone_args *cargs)
{
        int ret = 0;

        YASSERT(cargs->source.size == cargs->target.size);

        percent_init(cargs->source.size);

        ret = sem_init(&wait_sem, 0, MAX_JOB_WAIT);
        if (ret)
                GOTO(err_ret, ret);

        if (verbose) {
                printf("copy size: %llu\n", (LLU)cargs->source.size);
        }

        switch (cargs->source.type) {
        case PATH_HOST:
                switch (cargs->target.type) {
                case PATH_HOST:
                        YASSERT(0);
                case PATH_USS:
                        ret = __clone_host2uss(cargs);
                        if (ret)
                                goto err_ret;
                        break;
                default:
                        YASSERT(0);
                }
                break;
        case PATH_USS:
                switch (cargs->target.type) {
                case PATH_HOST:
                        ret = __clone_uss2host(cargs);
                        if (ret)
                                goto err_ret;
                        break;
                case PATH_USS:
                        ret = __clone_uss2uss(cargs);
                        if (ret)
                                goto err_ret;
                        break;
                default:
                        YASSERT(0);
                }
                break;
        default:
                YASSERT(0);
        }

        return 0;
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret;
        char c_opt;
        struct clone_args cargs;

        dbg_info(0);

        while (srv_running) {
                int option_index = 0;

                static struct option long_options[] = {
                        { "verbose", 0, 0, 'v' },
                        { "help",    0, 0, 'h' },
                        { 0, 0, 0, 0 },
                };

                c_opt = getopt_long(argc, argv, "vh", long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 'v':
                        verbose = 1;
                        break;
                case 'h':
                        usage();
                        exit(0);
                default:
                        usage();
                        exit(EINVAL);
                }
        }

        if (argc - optind != 2) {
                usage();
                exit(1);
        }

        cargs.source.path = argv[optind++];
        cargs.target.path = argv[optind++];

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init_simple(PROG);
        if (ret)
                GOTO(err_ret, ret);

        ret = clone_path_init(&cargs);
        if (ret)
                goto err_ret;

        ret = do_clone(&cargs);
        if (ret)
                goto err_ret;

        return 0;
err_ret:
        exit(ret);
}

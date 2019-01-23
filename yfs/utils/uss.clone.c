

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <dirent.h>
#include <ctype.h>

#include "configure.h"
#include "ylib.h"
#include "sdfs_lib.h"

#define PROG    "uss.clone"

enum {
        PATH_HOST,
        PATH_USS,
        PATH_LUN,
};

struct clone_path {
        int type;
        char path[MAX_PATH_LEN];
        union {
                int fd;                         /* For HostSource/HostTarget */
                fileid_t fileid;                /* For UssSource/UssTarget/LunSource */
                struct {                        /* For LunTarget */
                        char temp[MAX_PATH_LEN];
                        char real[MAX_PATH_LEN];
                        fileid_t parent;
                        fileid_t fileid;
                } tmpfile;
        } id;
        uint64_t size;
};

struct clone_args {
        struct clone_path source;
        struct clone_path target;
};

#define ISCSI_LUN_MAX           254

/*
 * Global variable
 */
int verbose;

static void usage()
{
        fprintf(stderr, "\nusage: uss.clone [-vh] SOURCE DIST\n"
                "\t-v --verbose         Show verbose message\n"
                "\t-h --help            Show this help\n"
                "\n"
                "copy from:\n"
                "       a) host to lun\n"
                "       b) lun  to host\n"
                "       c) uss  to lun\n"
                "       d) lun  to uss\n"
                "       c) lun  to lun\n"
                "\n"
                "host format: `:/path/of/host/file'\n"
                " uss format: `/path/of/uss/file'\n"
                " lun format: `namespace.target/lun'\n"
                "\n"
               );
}

static int _is_valid_char(char ch)
{
        return ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') ||
                (ch == '.') || (ch == ':') || (ch == '-'));
}

static int _is_valid_name(char *name)
{
        size_t i;

        for (i = 0; i < strlen(name); ++i) {
                if (!_is_valid_char(name[i])) {
                        return 0;
                }
        }

        return 1;
}

static int check_lun_name(char *name)
{
        size_t i;
        uint32_t lun;

        for (i = 0; i < strlen(name); ++i) {
                if (!isdigit(name[i])) {
                        goto err_ret;
                }
        }

        lun = atoll(name);

        if (lun > ISCSI_LUN_MAX) {
                goto err_ret;
        }

        return 0;
err_ret:
        return EINVAL;
}

static int __target_name_conv(char *name, char *_target, char *_lun)
{
        int ret;
        char *ns, *sub, *lun;

        lun = strrchr(name, '/');
        if (!lun) {
                ret = EINVAL;
                goto err_ret;
        }

        *lun++ = 0;

        ret = check_lun_name(lun);
        if (ret) {
                fprintf(stderr, "invalid lun, only digit is allowed, "
                        "valid lun: 0~%u\n", ISCSI_LUN_MAX);
                goto err_ret;
        }

        ns = name;

        if (!_is_valid_name(name)) {
                fprintf(stderr, "invalid char found in name, only 'a-z', '0-9', "
                        "'.', '-', ':' is allowed\n");
                ret = EINVAL;
                goto err_ret;
        }

        sub = strchr(name, '.');
        if (!sub) {
                ret = EINVAL;
                goto err_ret;
        }

        if (sub != strrchr(name, '.')) {
                fprintf(stderr, "only one `.' is allowed in target name\n");
                ret = EINVAL;
                goto err_ret;
        }

        if (name[0] == '.' || name[strlen(name) - 1] == '.') {
                fprintf(stderr, "the `.' is only allowed in the middle of "
                        "the target name\n");
                ret = EINVAL;
                goto err_ret;
        }

        *sub++ = 0;

        snprintf(_target, MAX_PATH_LEN, "/%s/%s", ns, sub);
        snprintf(_lun, MAX_PATH_LEN, "%s", lun);

        return 0;
err_ret:
        fprintf(stderr, "the format of path of lun is: namespace.target/lun\n");
        return ret;
}

static int __clone_path_init_host_source(struct clone_args *cargs)
{
        int ret, fd;
        struct stat stbuf;
        char *path = cargs->source.path;

        path++; /* Skip ':' */

        fd = open(path, O_RDONLY);
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
        char *path = cargs->target.path;

        path++; /* Skip ':' */

        fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
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

        ret = sdfs_lookup_recurive(cargs->source.path, &fileid);
        if (ret) {
                fprintf(stderr, "can't open uss: %s, %s\n",
                        cargs->source.path, strerror(ret));
                goto err_ret;
        }

        ret = sdfs_getattr(&fileid, &stbuf);
        if (ret) {
                fprintf(stderr, "can't stat uss: %s, %s\n",
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

        ret = sdfs_splitpath(cargs->target.path, &parent, name);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_create(&parent, name, &fileid, 0644, 0 ,0);
        if (ret) {
                fprintf(stderr, "can't create uss: %s, %s\n",
                        cargs->target.path, strerror(ret));
                goto err_ret;
        }

        ret = sdfs_truncate(&fileid, cargs->source.size);
        if (ret) {
                fprintf(stderr, "can't truncate uss: %s, %s\n",
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

static int __clone_path_init_lun_source(struct clone_args *cargs)
{
        int ret;
        fileid_t fileid;
        struct stat stbuf;
        char target[MAX_PATH_LEN], lun[MAX_PATH_LEN], file[MAX_PATH_LEN];

        ret = __target_name_conv(cargs->source.path, target, lun);
        if (ret)
                goto err_ret;

        snprintf(file, MAX_PATH_LEN, "%s/%s", target, lun);

        ret = sdfs_lookup_recurive(file, &fileid);
        if (ret) {
                fprintf(stderr, "can't open lun: %s, %s\n",
                        cargs->source.path, strerror(ret));
                goto err_ret;
        }

        ret = sdfs_getattr(&fileid, &stbuf);
        if (ret) {
                fprintf(stderr, "can't stat lun: %s, %s\n",
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
        cargs->source.type = PATH_LUN;

        return 0;
err_ret:
        return ret;
}

static int __clone_path_init_lun_target(struct clone_args *cargs)
{
        int ret;
        char target[MAX_PATH_LEN], lun[MAX_PATH_LEN];
        char file[MAX_PATH_LEN], name[MAX_PATH_LEN];
        fileid_t parent, fileid;

        ret = __target_name_conv(cargs->target.path, target, lun);
        if (ret)
                goto err_ret;

        snprintf(file, MAX_PATH_LEN, "%s/%s", target, lun);

        ret = sdfs_lookup_recurive(file, &fileid);
        if (ret) {
                if (ret != ENOENT)
                        GOTO(err_ret, ret);
        } else {
                ret = EEXIST;
                GOTO(err_ret, ret);
        }

        /*
         * Create a hide file first, then rename it after truncate it to the
         * specify size
         */
retry:
        snprintf(file, MAX_PATH_LEN, "/%s/.__%s__.%u", target, lun, (uint32_t)random());

        ret = sdfs_splitpath(file, &parent, name);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_create(&parent, name, &fileid, 0755, 0, 0);
        if (ret) {
                if (ret == EEXIST) {
                        DERROR("temporary file exists, retry ...\n");
                        goto retry;
                } else
                        GOTO(err_ret, ret);
        }

        ret = sdfs_truncate(&fileid, cargs->source.size);
        if (ret)
                GOTO(err_unlink, ret);

        snprintf(cargs->target.id.tmpfile.temp, MAX_PATH_LEN, "%s", name);
        snprintf(cargs->target.id.tmpfile.real, MAX_PATH_LEN, "%s", lun);
        cargs->target.id.tmpfile.parent = parent;
        cargs->target.id.tmpfile.fileid = fileid;
        cargs->target.size = cargs->source.size;
        cargs->target.type = PATH_LUN;

        return 0;
err_unlink:
        sdfs_unlink(&parent, name);
err_ret:
        return ret;
}

static int clone_path_init(struct clone_args *cargs)
{
        int ret;

        switch (cargs->source.path[0]) {
        case ':':
                ret = __clone_path_init_host_source(cargs);
                break;
        case '/':
                ret = __clone_path_init_uss_source(cargs);
                break;
        default:
                ret = __clone_path_init_lun_source(cargs);
                break;
        }

        if (ret)
                goto err_ret;

        switch (cargs->target.path[0]) {
        case ':':
                if (cargs->source.type != PATH_LUN) {
                        fprintf(stderr, "Invalid clone arguments\n");
                        ret = EINVAL;
                        goto err_ret;
                }

                ret = __clone_path_init_host_target(cargs);
                break;
        case '/':
                if (cargs->source.type != PATH_LUN) {
                        fprintf(stderr, "Invalid clone arguments\n");
                        ret = EINVAL;
                        goto err_ret;
                }

                ret = __clone_path_init_uss_target(cargs);
                break;
        default:
                ret = __clone_path_init_lun_target(cargs);
                break;
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
        struct clone_args *cargs;
};

#define MAX_JOB_WAIT    128
sem_t wait_sem;

#define CLONE_BUFSZ     (512*1024)
const char __zero_buf[CLONE_BUFSZ] = { 0 };

struct io_percent {
        pthread_spinlock_t lock;
        uint64_t total;
        uint64_t done;
        int is_file;
};

struct io_percent io_percent;

/******************************************************************************/

static inline void percent_init(uint64_t total)
{
        struct stat stbuf;

        if (!verbose)
                return;

        memset(&io_percent, 0x00, sizeof(struct io_percent));

        fstat(STDOUT_FILENO, &stbuf);

        io_percent.is_file = S_ISREG(stbuf.st_mode);
        io_percent.total = total;
        pthread_spin_init(&io_percent.lock, PTHREAD_PROCESS_PRIVATE);
}

static void percent_update(uint64_t done)
{
        int ret;
        char buf[32];

        if (!verbose)
                return;

        pthread_spin_lock(&io_percent.lock);

        io_percent.done += done;

        if (io_percent.is_file) {
                snprintf(buf, sizeof(buf), "%6.2f%%",
                         (double)(io_percent.done * 1.0 / io_percent.total * 100));
                lseek(STDOUT_FILENO, 0, SEEK_SET);
        } else {
                snprintf(buf, sizeof(buf), "\b\b\b\b\b\b\b\b%6.2f%%",
                         (double)(io_percent.done * 1.0 / io_percent.total * 100));
        }

        pthread_spin_unlock(&io_percent.lock);

        ret = write(STDOUT_FILENO, buf, strlen(buf));
        (void) ret;
}

static inline void percent_done()
{
        int ret;

        if (!verbose)
                return;

        ret = write(STDOUT_FILENO, "\n", 1);
        (void) ret;
}

static int __cb_host2lun(void *arg, int ret)
{
        struct ioctx *ctx = arg;
        struct clone_args *cargs = ctx->cargs;

        if (ctx->size != ret) {
                fprintf(stderr, "callback return error: %d, %d\n", ret, ctx->size);
                sdfs_unlink(&cargs->target.id.tmpfile.parent,
                           cargs->target.id.tmpfile.temp);
                exit(EIO);
        }

        mbuffer_free(&ctx->buf);
        yfree((void **)&ctx);

        percent_update(ret);

        sem_post(&wait_sem);

        return 0;
}

static int __clone_host2lun(struct clone_args *cargs)
{
        int ret, fd, len, cnt;
        char buf[CLONE_BUFSZ];
        fileid_t *fileid;
        uint64_t rest, off;
        struct ioctx *ctx;

        fd = cargs->source.id.fd;
        fileid = &cargs->target.id.tmpfile.fileid;

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

                ctx->cargs = cargs;
                ctx->size = cnt;

                ret = sdfs_write_async(fileid, &ctx->buf, cnt, off, __cb_host2lun, ctx);
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

        ret = sdfs_rename(&cargs->target.id.tmpfile.parent, cargs->target.id.tmpfile.temp,
                         &cargs->target.id.tmpfile.parent, cargs->target.id.tmpfile.real);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        sdfs_unlink(&cargs->target.id.tmpfile.parent, cargs->target.id.tmpfile.temp);
        return ret;
}

static int __cb_lun2host(void *arg, int ret)
{
        int fd, len;
        char buf[CLONE_BUFSZ];
        struct ioctx *ctx = arg;
        struct clone_args *cargs = ctx->cargs;

        if (ctx->size != ret) {
                fprintf(stderr, "callback return error: %d, %d\n", ret, ctx->size);
                exit(EIO);
        }

        fd = cargs->target.id.fd;

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

static int __clone_lun2host(struct clone_args *cargs)
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

                ctx->cargs = cargs;
                ctx->size = cnt;
                ctx->off = off;

                ret = sdfs_read_async(fileid, &ctx->buf, cnt, off, __cb_lun2host, ctx);
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

static int __cb_uss2lun(void *arg, int ret)
{
        struct ioctx *ctx = arg;
        struct clone_args *cargs = ctx->cargs;

        if (ctx->size != ret) {
                fprintf(stderr, "callback return error: %d, %d\n", ret, ctx->size);
                sdfs_unlink(&cargs->target.id.tmpfile.parent,
                           cargs->target.id.tmpfile.temp);
                exit(EIO);
        }

        mbuffer_free(&ctx->buf);
        yfree((void **)&ctx);

        percent_update(ret);

        sem_post(&wait_sem);

        return 0;
}

static int __clone_uss2lun(struct clone_args *cargs)
{
        int ret, len, cnt;
        char buf[CLONE_BUFSZ];
        fileid_t *sfd, *tfd;
        uint64_t rest, off;
        struct ioctx *ctx;

        sfd = &cargs->source.id.fileid;
        tfd = &cargs->target.id.tmpfile.fileid;

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

                ctx->cargs = cargs;
                ctx->size = cnt;

                ret = sdfs_write_async(tfd, &ctx->buf, cnt, off, __cb_uss2lun, ctx);
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

        ret = sdfs_rename(&cargs->target.id.tmpfile.parent, cargs->target.id.tmpfile.temp,
                         &cargs->target.id.tmpfile.parent, cargs->target.id.tmpfile.real);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        sdfs_unlink(&cargs->target.id.tmpfile.parent, cargs->target.id.tmpfile.temp);
        return ret;
}

static int __cb_lun2uss(void *arg, int ret)
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

static int __clone_lun2uss(struct clone_args *cargs)
{
        int ret, len, cnt;
        char buf[CLONE_BUFSZ];
        fileid_t *sfd, *tfd;
        uint64_t rest, off;
        struct ioctx *ctx;

        sfd = &cargs->source.id.fileid;
        tfd = &cargs->target.id.fileid;

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

                ctx->cargs = cargs;
                ctx->size = cnt;

                ret = sdfs_write_async(tfd, &ctx->buf, cnt, off, __cb_lun2uss, ctx);
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

static int __clone_lun2lun(struct clone_args *cargs)
{
        return __clone_uss2lun(cargs);
}

static int do_clone(struct clone_args *cargs)
{
        int ret = 0;

        YASSERT(cargs->source.size == cargs->target.size);

        percent_init(cargs->source.size);

        ret = sem_init(&wait_sem, 0, MAX_JOB_WAIT);
        if (ret)
                GOTO(err_ret, ret);

        switch (cargs->source.type) {
        case PATH_HOST:
                switch (cargs->target.type) {
                case PATH_LUN:
                        ret = __clone_host2lun(cargs);
                        if (ret)
                                goto err_ret;
                        break;
                default:
                        YASSERT(0);
                }
                break;
        case PATH_USS:
                switch (cargs->target.type) {
                case PATH_LUN:
                        ret = __clone_uss2lun(cargs);
                        if (ret)
                                goto err_ret;
                        break;
                default:
                        YASSERT(0);
                }
                break;
        case PATH_LUN:
                switch (cargs->target.type) {
                case PATH_HOST:
                        ret = __clone_lun2host(cargs);
                        if (ret)
                                goto err_ret;
                        break;
                case PATH_USS:
                        ret = __clone_lun2uss(cargs);
                        if (ret)
                                goto err_ret;
                        break;
                case PATH_LUN:
                        ret = __clone_lun2lun(cargs);
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

        snprintf(cargs.source.path, MAX_PATH_LEN, "%s", argv[optind++]);
        snprintf(cargs.target.path, MAX_PATH_LEN, "%s", argv[optind++]);

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

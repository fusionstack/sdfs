

#include <sys/statvfs.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <semaphore.h>
#include <pthread.h>
#include <errno.h>

#include "sdfs_lib.h"
#include "dbg.h"

#define YFS_SANITY_TEST_DIR  "yfs_testdir"
#define YFS_SANITY_TEST_FILE "yfs_testfile"
#define YFS_SANITY_TEST_STR  "yfs_tetstr=oi3jfsdn23o2039jfsldknf"

#define YS_PATH_LEN 512
#define YS_BUF_LEN  1024

int verbose = 0;
sem_t th_sem;
sem_t fin_sem;
int good = 0;
int error = 0;
int dirs = 0;
int files = 0;

void *do_dir_sanity_test(void *arg)
{
        int ret, no;
        struct statvfs svbuf;
        struct stat    stbuf;
        void *de;
        unsigned int delen;
        char path[YS_PATH_LEN];

        no = *(int *)arg;
        dirs++;
        sem_post(&th_sem);

        snprintf(path, YS_PATH_LEN, "%s_%ld_%ld", YFS_SANITY_TEST_DIR,
                 time(NULL), random());
        if (verbose)
                printf("begin dir (%s) test\n", path);

        _memset(&svbuf, 0x0, sizeof(struct statvfs));
        ret = ly_statvfs("/", &svbuf);
        if (ret) {
                fprintf(stderr, "ERROR: ly_statvfs() ret (%d) %s\n", ret,
                        strerror(ret));
                goto err_ret;
        }
        if (verbose) {
                printf("ly_statvfs() got:\n\n");
                printf(" ID %lu Namelen: %lu Block size: %lu\n",
                       svbuf.f_fsid, svbuf.f_namemax, svbuf.f_bsize);
                printf(" Blocks: Total: %llu Free: %llu Available: %llu\n",
                       (unsigned long long)svbuf.f_blocks,
                       (unsigned long long)svbuf.f_bfree,
                       (unsigned long long)svbuf.f_bavail);
                printf(" Inodes: Total: %llu Free: %llu\n\n",
                       (unsigned long long)svbuf.f_files,
                       (unsigned long long)svbuf.f_ffree);
        }

        ret = ly_mkdir(path, 0755);
        if (ret) {
                fprintf(stderr, "ERROR: ly_mkdir(%s) ret (%d) %s\n", path, ret,
                        strerror(ret));
                goto err_ret;
        }
        if (verbose)
                printf("dir %s created\n", path);

        ret = ly_opendir(path);
        if (ret) {
                fprintf(stderr, "ERROR: ly_opendir(%s) ret (%d) %s\n", path,
                        ret, strerror(ret));
                goto err_ret;
        }
        if (verbose)
                printf("opendir %s good\n", path);

        _memset(&stbuf, 0x0, sizeof(struct stat));
        ret = ly_getattr(path, &stbuf);
        if (ret) {
                fprintf(stderr, "ERROR: ly_stat(%s) ret (%d) %s\n", path, ret,
                        strerror(ret));
                goto err_ret;
        }
        if (verbose)
                printf("stat %s good\n", path);

        de = NULL;
        delen = 0;
        ret = ly_readdir(path, 0, &de, &delen);
        if (ret) {
                fprintf(stderr, "ERROR: ly_readir(%s) ret (%d) %s\n", path, ret,
                        strerror(ret));
                goto err_ret;
        }
        if (verbose)
                printf("readdir %s good, len (%u)\n", path, delen);
        if (delen > 0)
                free(de);

        ret = ly_rmdir(path);
        if (ret) {
                fprintf(stderr, "ERROR: ly_rmdir(%s) ret (%d) %s\n", path, ret,
                        strerror(ret));
                goto err_ret;
        }
        if (verbose)
                printf("rmdir %s good\n", path);

        printf("dir test no. %d OK\n", no);
        good++;
        sem_post(&fin_sem);

        pthread_exit(NULL);
err_ret:
        printf("dir test no. %d ERROR\n", no);
        error++;
        sem_post(&fin_sem);
        pthread_exit((void *)&ret);
}

void *do_file_sanity_test(void *arg)
{
        int ret, no, fd;
        struct stat stbuf;
        char path[YS_PATH_LEN], buf[YS_BUF_LEN];

        no = *(int *)arg;
        files++;
        sem_post(&th_sem);

        snprintf(path, YS_PATH_LEN, "%s_%d_%ld_%ld", YFS_SANITY_TEST_FILE,
                 no, time(NULL), random());
        if (verbose)
                printf("begin file (%s) test\n", path);

        fd = ly_create(path, 0644);
        if (fd < 0) {
                ret = -fd;
                fprintf(stderr, "ERROR: ly_create(%s) ret (%d) %s\n", path, ret,
                        strerror(ret));
                goto err_ret;
        }
        if (verbose)
                printf("create %s good\n", path);

        _memset(&stbuf, 0x0, sizeof(struct stat));
        ret = ly_getattr(path, &stbuf);
        if (ret) {
                fprintf(stderr, "ERROR: ly_stat(%s) ret (%d) %s\n", path, ret,
                        strerror(ret));
                goto err_ret;
        }
        if (verbose)
                printf("stat %s good\n", path);

        ret = ly_pwrite(fd, YFS_SANITY_TEST_STR, sizeof(YFS_SANITY_TEST_STR),0);
        if (ret < 0) {
                ret = -ret;
                fprintf(stderr, "ERROR: ly_pwrite(%s) ret (%d) %s\n", path, ret,
                        strerror(ret));
                goto err_ret;
        }
        if (verbose)
                printf("pwrite %s good\n", path);

        ret = ly_release(fd);
        if (ret) {
                fprintf(stderr, "ERROR: ly_release(%s) ret (%d) %s\n", path,
                        ret, strerror(ret));
                goto err_ret;
        }
        if (verbose)
                printf("release %s good\n", path);

        sleep(7);

        fd = ly_open(path);
        if (fd < 0) {
                ret = -fd;
                fprintf(stderr, "ERROR: ly_open(%s) ret (%d) %s\n", path, ret,
                        strerror(ret));
                goto err_ret;
        }
        if (verbose)
                printf("open %s good\n", path);

        buf[0] = '\0';
        ret = ly_pread(fd, buf, YS_BUF_LEN, 0);
        if (ret < 0) {
                ret = -ret;
                fprintf(stderr, "ERROR: ly_pread(%s) ret (%d) %s\n", path, ret,
                        strerror(ret));
                goto err_ret;
        }
        if (verbose)
                printf("pread %s good\n", path);

        ret = strncmp(buf, YFS_SANITY_TEST_STR, sizeof(YFS_SANITY_TEST_STR));
        if (ret == 0) {
                if (verbose)
                        printf("readout data is same to writein data (%s)\n",
                               YFS_SANITY_TEST_STR);
        } else {
                fprintf(stderr,
                        "ERROR: data readout is not same to writein (%s)(%s)\n",
                        buf, YFS_SANITY_TEST_STR);
                goto err_ret;
        }

        ret = ly_release(fd);
        if (ret) {
                fprintf(stderr, "ERROR: ly_release(%s) ret (%d) %s\n", path,
                        ret, strerror(ret));
                goto err_ret;
        }
        if (verbose)
                printf("release %s good\n", path);

        printf("file test no. %d OK\n", no);
        good++;
        sem_post(&fin_sem);

        pthread_exit(NULL);
err_ret:
        printf("file test no. %d ERROR\n", no);
        error++;
        sem_post(&fin_sem);
        pthread_exit((void *)&ret);
}

int main(int argc, char *argv[])
{
        int ret, i, r, t = 1;
        pthread_t th;
        pthread_attr_t ta;

        if (argc == 2)
                t = atoi(argv[1]);
        if (t < 1)
                t = 1;

        ret = ly_init(LY_IO, NULL, "ytest", NULL, NULL);
        if (ret) {
                fprintf(stderr, "ERROR: ly_init() ret (%d) %s\n", ret,
                        strerror(ret));
                return 1;
        }
        printf("ly_init()'ed\n");

        (void) sem_init(&th_sem, 0, 0);
        (void) sem_init(&fin_sem, 0, 0);

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        srandom(getpid());

        for (i = 0; i < t; i++) {
                r = random() % 2;
                printf("creat %s testing thread\n", r ? "dir": "file");
                if (r)
                        ret = pthread_create(&th, &ta, do_dir_sanity_test,
                                             (void *)&i);
                else
                        ret = pthread_create(&th, &ta, do_file_sanity_test,
                                             (void *)&i);
                if (ret) {
                        fprintf(stderr, "pthread_create no. %d (%d) %s\n", ret,
                                i, strerror(ret));
                        return 1;
                }
                sem_wait(&th_sem);
                printf("testing thread no. %d created\n", i);
        }

        for (i = 0; i < t; i++)
                sem_wait(&fin_sem);

        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ERROR: ly_destroy() ret (%d) %s\n", ret,
                        strerror(ret));
                return 1;
        }
        printf("ly_destroy()'ed\n");

        printf("Testing Results: (total %d)\n", t);
        printf("\tdir %d file %d (total %d)\n\tgood %d error %d (total %d)\n",
               dirs, files, (dirs + files), good, error, (good + error));

        return 0;
}

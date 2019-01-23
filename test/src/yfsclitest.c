

#include <sys/statvfs.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <semaphore.h>
#include <pthread.h>
#include <errno.h>

#include "sdfs_lib.h"

#define YFS_SANITY_TEST_DIR  "yfs_testdir"
#define YFS_SANITY_TEST_FILE "yfs_testfile"
#define YFS_SANITY_TEST_STR  "yfs_tetstr=oi3jfsdn23o2039jfsldknf"

#define TEST_DIR "testdir"

#ifndef MAX_PATH_LEN
#define MAX_PATH_LEN 512
#endif

#ifndef MAX_BUF_LEN
#define MAX_BUF_LEN 1024*64
#endif

typedef struct {
        int usradd_total, usradd_is_ok;
        int mkdir_total, mkdir_is_ok;
        int write_total, write_is_ok;
        int read_total, read_is_ok;
} test_report;
test_report g_report;

void init_report(test_report* tr)
{
        tr->usradd_total = 0;
        tr->usradd_is_ok = 0;

        tr->mkdir_total = 0;
        tr->mkdir_is_ok = 0;

        tr->write_total = 0;
        tr->write_is_ok = 0;

        tr->read_total = 0;
        tr->read_is_ok = 0;
}

void creat_report(test_report* tr)
{
        printf("//----------------------------------------\n");
        printf("//%20s: %20d%20d%20d\n", "usradd", tr->usradd_total, tr->usradd_is_ok, tr->usradd_total-tr->usradd_is_ok);
        printf("//%20s: %20d%20d%20d\n", "mkdir",  tr->mkdir_total,  tr->mkdir_is_ok,  tr->mkdir_total-tr->mkdir_is_ok);
        printf("//%20s: %20d%20d%20d\n", "write",  tr->write_total,  tr->write_is_ok,  tr->write_total-tr->write_is_ok);
        printf("//%20s: %20d%20d%20d\n", "read",   tr->read_total,   tr->read_is_ok,   tr->read_total-tr->read_is_ok);
        printf("//----------------------------------------\n");
}

typedef struct {
        sem_t start_lock;
        sem_t end_lock;
        char srcpath[MAX_PATH_LEN];

} test_info;
test_info g_test_info;

void init_test_info(test_info* ti)
{
        sem_init(&ti->start_lock, 0, 0);
        sem_init(&ti->end_lock, 0, 0);
}

void release_test_info(test_info* ti)
{
        sem_destroy(&ti->start_lock);
        sem_destroy(&ti->end_lock);
}

// declarations
int test_addusr(int usrnum);
int do_addusr(int usrno);

int test_mkdir(int usrnum);
int do_mkdir(char* path);
int do_local_mkdir(char* path);

int test_upload(int usrnum);
void* do_upload(void* arg);
int _do_upload(char* path, int usrno);
void do_write_file(char *path, char* remotepath);

int test_download();
void* do_download(void* arg);
int _do_download(char* path, int usrno);
void do_read_file(char* remotepath, char* path);

int test_dele();
void*  do_dele(void* arg);

//
//
int test_addusr(int usrnum)
{
        int ret;
        int i;

        for(i = 0; i < usrnum; ++i) {
                ret = do_addusr(i);
                if(ret) {
                        printf("test_addusr_is_fail %d\n", i);
                }

        }
        return 0;
}

int do_addusr(int usrno)
{
        g_report.usradd_total++;

        printf("do_addusr %d\n", usrno);

        int ret;
        char uname[10];

        snprintf(uname, 10, "usr%d", usrno);
        ret = ly_useradd(uname, "one");
        if( ret) {

        }
        else {
                g_report.usradd_is_ok++;
        }
        return 0;
}

int test_mkdir(int usrnum)
{
        int ret;
        int i;
        char path[MAX_PATH_LEN];

        for(i = 0; i < usrnum; ++i) {
                snprintf(path, MAX_PATH_LEN, "/usr%d/%s", i, TEST_DIR);
                ret = do_mkdir(path);
                if(ret) {
                }
                else {
                }
        }
        return 0;
}

int do_mkdir(char* path)
{
        printf("do_mkdir %s\n", path);

        g_report.mkdir_total++;

        int ret;
        ret = ly_mkdir(path, 0755);
        if(ret) {

        }
        else {
                g_report.mkdir_is_ok++;
        }
        return ret;
}

int do_local_mkdir(char* path)
{
        int ret ;
        ret = mkdir(path, 0755);
        if(ret) {
        }
        else {
        }
        return ret;
}

int test_upload(int usrnum)
{
        int ret;
        int i;

        pthread_t th;
        pthread_attr_t ta;

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        int threads = 0;
        for(i = 0; i < usrnum; ++i) {
                ret = pthread_create(&th, &ta, do_upload, (void*)&i);
                if(ret) {

                } else {
                        threads++;
                }
                sem_wait(&g_test_info.start_lock);
        }

        for(i = 0; i < threads; ++i) {
                sem_wait(&g_test_info.end_lock);
        }

        return ret;
}

void * do_upload(void* arg)
{
        int usrno = *(int*)arg;
        sem_post(&g_test_info.start_lock);

        // printf("do_upload %s %d\n", g_test_info.srcpath, usrno);
        _do_upload(g_test_info.srcpath, usrno);

        sem_post(&g_test_info.end_lock);
        pthread_exit(NULL);
}

int _do_upload(char* path, int usrno)
{
        int ret;
        DIR* dp;
        struct dirent* dirp;
        struct stat stbuf;
        char remotepath[MAX_PATH_LEN];
        char fullpath[MAX_PATH_LEN];
        char subpath[MAX_PATH_LEN];

        lstat(path, &stbuf);

        if( S_ISREG(stbuf.st_mode) ) {
                snprintf(remotepath, MAX_PATH_LEN, "/usr%d/%s", usrno, path);
                do_write_file(path, remotepath);
        }
        else if( S_ISDIR(stbuf.st_mode) ) {
                snprintf(fullpath, MAX_PATH_LEN, "/usr%d/%s", usrno, path);
                do_mkdir(fullpath);

                dp = opendir(path) ;
                while((dirp = readdir(dp)) != NULL) {
                        if( __strcmp(dirp->d_name, ".") == 0 || __strcmp(dirp->d_name, "..") == 0)
                                continue;
                        snprintf(subpath, MAX_PATH_LEN, "%s/%s", path, dirp->d_name);
                        _do_upload(subpath, usrno);
                }
                closedir(dp);
        }

        return ret;
}

void do_write_file(char *path, char* remotepath)
{
        // printf("do_write_file %s %s\n", path, remotepath);
        g_report.write_total++;

        int ret;
        int lfd;
        int yfd;
        char buf[MAX_BUF_LEN];
        ssize_t buflen;
        size_t size;
        off_t offset;

        lfd = open(path, O_RDONLY);
        if( lfd <= 0 ) {
                goto err_ret;
        }

        yfd = ly_create(remotepath, 0644);
        if( yfd < 0 ) {
                printf("ly_create_is_failed %s.\n", remotepath);
                goto err_ly_create;
        }

        offset = 0;
        while (srv_running) {
                size = MAX_BUF_LEN;
                buflen = _read(lfd, buf, size);
                if(buflen == 0)
                        break;

                // printf("%d %d\n", buflen, offset);
                ret = ly_pwrite(yfd, buf, buflen, offset);
                if(ret<0) {
                        printf("do_write_file_is_failed %s -> %s\n", path, remotepath);
                }
                else {
                        offset += buflen;
                }
        }

        ret = ly_fsync(yfd);
        if( ret ) {

        }
        else {
                g_report.write_is_ok++;
        }

        ly_release(yfd);

err_ly_create:
        close(lfd);
err_ret:
        return;
}


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
        char path[MAX_PATH_LEN];

        no = *(int *)arg;
        dirs++;
        sem_post(&th_sem);

        snprintf(path, MAX_PATH_LEN, "%s_%ld_%ld", YFS_SANITY_TEST_DIR,
                 time(NULL), random());
        if (verbose)
                printf("begin dir (%s) test\n", path);

        memset(&svbuf, 0x0, sizeof(struct statvfs));
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
                       svbuf.f_blocks, svbuf.f_bfree, svbuf.f_bavail);
                printf(" Inodes: Total: %llu Free: %llu\n\n",
                       svbuf.f_files, svbuf.f_ffree);
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

        memset(&stbuf, 0x0, sizeof(struct stat));
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
        char path[MAX_PATH_LEN], buf[MAX_BUF_LEN];

        no = *(int *)arg;
        files++;
        sem_post(&th_sem);

        snprintf(path, MAX_PATH_LEN, "%s_%d_%ld_%ld", YFS_SANITY_TEST_FILE,
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

        memset(&stbuf, 0x0, sizeof(struct stat));
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

        ret = ly_fsync(fd);
        if (ret) {
                fprintf(stderr, "ERROR: ly_fsync(%s) ret (%d) %s\n", path, ret,
                        strerror(ret));
                goto err_ret;
        }
        if (verbose)
                printf("fsync %s good\n", path);

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
        ret = ly_pread(fd, buf, MAX_BUF_LEN, 0);
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


int test_download(int usrnum)
{
        int ret;
        int i;

        pthread_t th;
        pthread_attr_t ta;

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        int threads = 0;
        for(i = 0; i < usrnum; ++i) {
                ret = pthread_create(&th, &ta, do_download, (void*)&i);
                if(ret) {

                } else {
                        threads++;
                }
                sem_wait(&g_test_info.start_lock);
        }

        for(i = 0; i < threads; ++i) {
                sem_wait(&g_test_info.end_lock);
        }

        return ret;
}

void * do_download(void* arg)
{
        int usrno = *(int*)arg;
        sem_post(&g_test_info.start_lock);

        printf("do_download %s %d\n", g_test_info.srcpath, usrno);
        _do_download(g_test_info.srcpath, usrno);

        sem_post(&g_test_info.end_lock);
        pthread_exit(NULL);
}

int _do_download(char* path, int usrno)
{
        int ret;
        struct stat stbuf;
        void *de0, *ptr;
        struct dirent *dirp;
        unsigned int delen = 0;
        char remotepath[MAX_PATH_LEN];
        char subpath[MAX_PATH_LEN];

        snprintf(remotepath, MAX_PATH_LEN, "/usr%d/%s", usrno, path);
        ret = ly_getattr(remotepath, &stbuf);
        if(ret) {

        }

        printf("_do_download %s -> %s\n", remotepath, path);

        if( S_ISREG(stbuf.st_mode) ) {
                do_read_file(remotepath, path);
        }
        else if( S_ISDIR(stbuf.st_mode) ) {
                printf("do_local_mkdir %s\n", path);
                do_local_mkdir(path);

                ret = ly_readdir(remotepath, 0, &de0, &delen);
                if(ret) {

                }

                ptr = de0;
                while(delen > 0) {
                        dirp = (struct dirent*)ptr;
                        if( __strcmp(dirp->d_name, ".") != 0 && __strcmp(dirp->d_name, "..") != 0) {
                                if(dirp->d_reclen > delen) {

                                }

                                snprintf(subpath, MAX_PATH_LEN, "%s/%s", path, dirp->d_name);
                                _do_download(subpath, usrno);
                        }

                        ptr   += dirp->d_reclen;
                        delen -= dirp->d_reclen;
                }

                yfree((void **)&de0);
                delen = 0;
        }

        return ret;
}

void do_read_file(char *remotepath, char* path)
{
        // printf("do_read_file %s -> %s\n", remotepath, path);

        g_report.read_total++;

        int ret;
        int lfd;
        int yfd;
        struct stat stbuf;
        yfs_off_t offset;
        yfs_size_t size;

        ret = ly_getattr(remotepath, &stbuf);
        if(ret) {
                goto err_ret;
        }

        yfd = ly_open(remotepath);
        if(yfd < 0) {
                goto err_ret;
        }

        lfd = open(path, O_WRONLY|O_CREAT|O_TRUNC);
        if( lfd <= 0 ) {
                goto err_open;
        }

#if 1
#define YFTP_USE_SPLICE

        offset = 0;
        size = stbuf.st_size;

#ifdef YFTP_USE_SPLICE
        ret = ly_splice(yfd, &offset, lfd, NULL, &size);
#else
        ret = ly_fetchfile(yfd, lfd, offset, size, 1);
#endif
#endif
        if(ret) {
        }
        else {
                fchmod(lfd, stbuf.st_mode);
                g_report.read_is_ok++;
        }

        close(lfd);

err_open:
        ly_release(yfd);
err_ret:
        return;
}

int test_dele(int usrnum)
{
        int ret;
        int i;

        pthread_t th;
        pthread_attr_t ta;

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        int threads = 0;
        for(i = 0; i < usrnum; ++i) {
                ret = pthread_create(&th, &ta, do_dele, (void*)&i);
                if(ret) {

                } else {
                        threads++;
                }
                sem_wait(&g_test_info.start_lock);
        }

        for(i = 0; i < threads; ++i) {
                sem_wait(&g_test_info.end_lock);
        }

        return ret;
}

void _do_dele(char* path, int usrno)
{
        (void) path;
        (void) usrno;
}

void *do_dele(void* arg)
{
        int usrno = *(int*)arg;
        sem_post(&g_test_info.start_lock);

        // printf("do_dele %s %d\n", g_test_info.srcpath, usrno);
        _do_dele(g_test_info.srcpath, usrno);

        sem_post(&g_test_info.end_lock);
        pthread_exit(NULL);
}


int main(int argc, char *argv[])
{
        int ret;

        char c_opt;

        int isaddusr = 0;
        int ismkdir = 0;
        int isupload = 0;
        int isdownload = 0;

        int usrnum = 1;

        init_report(&g_report);
        init_test_info(&g_test_info);

        while ((c_opt = getopt(argc, argv, "adp:g:cfu:")) > 0) {
                switch (c_opt) {
                        case 'a':
                                isaddusr = 1;
                                break;
                        case 'd':
                                ismkdir = 1;
                                break;
                        case 'p':
                                isupload = 1;
                                snprintf(g_test_info.srcpath, MAX_PATH_LEN, "%s", optarg);
                                break;
                        case 'g':
                                isdownload = 1;
                                snprintf(g_test_info.srcpath, MAX_PATH_LEN, "%s", optarg);
                                break;
                        case 'u':
                                usrnum = atoi(optarg);
                                break;
                        default:
                                fprintf(stderr, "Hoops, wrong op got!\n");
                                break;
                }
        }

        ret = ly_init(LY_IO);
        if (ret) {
                fprintf(stderr, "ERROR: ly_init() ret (%d) %s\n", ret,
                        strerror(ret));
                return 1;
        }
        printf("ly_init()'ed\n");

        if( isaddusr ) {
                test_addusr(usrnum);
        }

        if( ismkdir ) {
                test_mkdir(usrnum);
        }

        if( isupload ) {
                test_upload(usrnum);
        }

        if( isdownload ) {
                test_download(usrnum);
        }

        creat_report(&g_report);
        release_test_info(&g_test_info);

        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ERROR: ly_destroy() ret (%d) %s\n", ret,
                        strerror(ret));
                return 1;
        }
        printf("ly_destroy()'ed\n");

        return 0;
}

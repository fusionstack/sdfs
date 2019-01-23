

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
#include <stdint.h>
#include <errno.h>

#ifndef MAX_PATH_LEN
#define MAX_PATH_LEN 512
#endif

#ifndef MAX_BUF_LEN
#define MAX_BUF_LEN 1024*64
#endif

sem_t start_lock;
sem_t end_lock;

static char *s_filename;

void *do_cpto(void *arg)
{
        int i = *(int *)arg;

        char src_path[MAX_PATH_LEN];
        char dst_path[MAX_PATH_LEN];
        int src_fd;
        int dst_fd;
        char buf[MAX_BUF_LEN];
        size_t count;
        uint64_t filelen;

        sem_post(&start_lock);

        bzero(src_path, MAX_PATH_LEN);
        bzero(dst_path, MAX_PATH_LEN);

        snprintf(src_path, MAX_PATH_LEN, "%s", s_filename);
        snprintf(dst_path, MAX_PATH_LEN, "/mnt/yfs/mds/data/%s_%d_%ld", s_filename, i, random());

        src_fd = open(s_filename, O_RDONLY);
        if (src_fd < 0) {
                fprintf(stderr, "open %s failed: %s.\n", s_filename, strerror(errno));
                goto out;
        }

        dst_fd = open(dst_path, O_CREAT|O_WRONLY, 0644);
        if (dst_fd < 0) {
                fprintf(stderr, "open %s failed: %s.\n", dst_path, strerror(errno));
                goto out;
        }

        filelen = 0;
        while (srv_running) {
                count = _read(src_fd, buf, MAX_BUF_LEN);
                if (count <= 0)
                        break;

                buf[count] = '\0';
                _write(dst_fd, buf, count);
                filelen += count;
        }
        printf("copy %s to %s: %llu\n", src_path, dst_path, (LLU)filelen);

out:
        close(src_fd);
        close(dst_fd);
        sem_post(&end_lock);
        pthread_exit(NULL);
}

void* do_cpfrom(void *arg)
{
        int i = *(int *)arg;

        char src_path[MAX_PATH_LEN];
        char dst_path[MAX_PATH_LEN];
        int src_fd;
        int dst_fd;
        char buf[MAX_BUF_LEN];
        size_t count;
        uint64_t filelen;

        sem_post(&start_lock);

        bzero(src_path, MAX_PATH_LEN);
        bzero(dst_path, MAX_PATH_LEN);
        snprintf(src_path, MAX_PATH_LEN, "/mnt/yfs/mds/data/%s", s_filename);
        snprintf(dst_path, MAX_PATH_LEN, "%s_%d_%ld", s_filename, i, random());

        src_fd = open(s_filename, O_RDONLY);
        if (src_fd < 0) {
                fprintf(stderr, "open %s failed: %s.\n", s_filename, strerror(errno));
                goto out;
        }

        dst_fd = open(dst_path, O_CREAT|O_WRONLY, 0644);
        if (dst_fd < 0) {
                fprintf(stderr, "open %s failed: %s.\n", dst_path, strerror(errno));
                goto out;
        }

        filelen = 0;
        while (srv_running) {
                count = _read(src_fd, buf, MAX_BUF_LEN);
                if (count <= 0)
                        break;

                buf[count] = '\0';
                _write(dst_fd, buf, count);
                filelen += count;
        }
        printf("copy %s to %s: %llu\n", src_path, dst_path, (LLU)filelen);

out:
        close(src_fd);
        close(dst_fd);

        sem_post(&end_lock);
        pthread_exit(NULL);
}

void test_put(int threads)
{
        int ret;
        int i;
        int nr_ok;

        time_t t;
        srandom(time(&t));

        pthread_t th;
        pthread_attr_t ta;

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        nr_ok = 0;

        for(i = 0; i < threads; ++i) {
                printf("pthread %d started ...\n", i);
                if (i % 2)
                        ret = pthread_create(&th, &ta, do_cpto, (void*)&i);
                else 
                        ret = pthread_create(&th, &ta, do_cpfrom, (void*)&i);
                if(ret) {

                } else {
                        nr_ok++;
                }
                sem_wait(&start_lock);
        }

        printf("%d ok.\n", nr_ok);
        for(i = 0; i < nr_ok; ++i) {
                sem_wait(&end_lock);
        }
}

int main(int argc, char *argv[])
{
        char c_opt;
        int threads;

        while ((c_opt = getopt(argc, argv, "f:t:")) > 0) {
                switch (c_opt) {
                        case 'f':
                                s_filename = optarg;
                                break;
                        case 't':
                                threads = atoi(optarg);
                                break;
                        default:
                                fprintf(stderr, "Hoops, wrong op got!\n");
                                break;
                }
        }

        sem_init(&start_lock, 0, 0);
        sem_init(&end_lock, 0, 0);

        test_put(threads);

        sem_destroy(&start_lock);
        sem_destroy(&end_lock);

        return 0;
}

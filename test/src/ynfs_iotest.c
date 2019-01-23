#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <dirent.h>
#include <limits.h>
#include <pthread.h>
#include <assert.h>
#include <errno.h>
#include "md5.h"

#define GOTO(label, ret) \
        do { \
                fprintf(stderr, "%s:%s():%u Process halting via %s (%d)%s\n", \
                        __FILE__, __FUNCTION__, __LINE__, #label, ret, strerror(ret)); \
                goto label; \
        } while (0)

#define BUFSIZE 4096

#define RED             "\033[31m"
#define GREEN           "\033[32m"
#define NORMAL          "\033[0m"

#define LOG_FILE        "/var/log/ynfs_iotest.log"
int log_fd = -1;

/* Per-thread structure */
struct thread_param {
        char *dir;
        int number;
        int size;
};

#define TEST_FILE_COUNT         100
#define TEST_DIR_COUNT          1000

/*
 * prepare_env -
 *
 * @path: root directory to change to
 * @create_dir: whether create a new dir when not exist
 *
 * @return zero on success, otherwise errno is returned.
 */
static int prepare_env(char *path, int create_dir)
{
        int ret;
        struct  stat buf;

        memset(&buf, 0, sizeof(struct stat));

        stat(path, &buf);

        if (!S_ISDIR(buf.st_mode)) {
                if (create_dir) {
                        ret = mkdir(path, 0755);
                        if (ret < 0) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }
                } else {
                        ret = ENOENT;
                        GOTO(err_ret, ret);
                }
        }

        ret = chdir(path);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

/*
 * write_log -
 *
 * @buf: buf to record
 */
static void write_log(char *buf)
{
        int ret;
        time_t timep;
        char str_time[1024];

        if (log_fd == -1) {
                log_fd = open(LOG_FILE, O_CREAT | O_WRONLY | O_APPEND, 0644);
                if (log_fd < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        time(&timep);
        strncpy(str_time, ctime(&timep), sizeof(str_time) - 1);
        str_time[sizeof(str_time) - 1] = '\0';
        str_time[strlen(str_time) - 1] = '\t';

        fprintf(stderr, RED"----------%s%s"NORMAL, str_time, buf);

        ret = write(log_fd, str_time, strlen(str_time));
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = write(log_fd, buf, strlen(buf));
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return;
err_ret:
        (void) ret;
}

/**
 * Function tester units
 */

/*
 * __copy - cp
 *
 * @src: source file
 * @dst: destination file
 * @append: whether append to destination file
 *
 * @return zero on success, otherwize errno is returned.
 */
static int __copy(char *src, char *dst, int append)
{
        int ret, n, mode;
        int fd_src, fd_dst;
        char buf[BUFSIZE];

        fd_src = open(src, O_RDONLY);
        if (fd_src < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        mode = O_CREAT | O_WRONLY;
        if (append)
                mode |= O_APPEND;

        fd_dst = open(dst, mode, 0755);
        if (fd_dst < 0) {
                ret = errno;
                GOTO(err_close, ret);
        }

        while ((n = read(fd_src, buf, sizeof(buf))) > 0) {
                ret = write(fd_dst, buf, n);
                if (ret != n) {
                        ret = EIO;
                        GOTO(err_close2, ret);
                }
        }

        ret = 0;
err_close2:
        close(fd_dst);
err_close:
        close(fd_src);
err_ret:
        return ret;
}

/*
 * __move - mv
 *
 * @src: source file
 * @dst: destination file
 * @append: whether append to destination file
 *
 * @return zero on success, otherwize errno is returned.
 */
static int __move(char *src, char *dst, int append)
{
        int ret;

        ret = __copy(src, dst, append);
        if (ret)
                GOTO(err_ret, ret);

        ret = remove(src);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

/*
 * __split - split
 *
 * @src: source file
 * @dir: directory to put the sub file
 * @basename: prefix of sub file
 * @byte: bytes per output file
 *
 * @return zero on success, otherwize errno is returned.
 */
static int __split(char *src, char *dir, char *basename, uint32_t bytes)
{
        int ret, n, i, j, k;
        char alph[] = "abcdefghijklmnopqrstuvwxyz";
        char split[PATH_MAX], buf[bytes];
        int fd_src, fd_dst;

        fd_src = open(src, O_RDONLY);
        if (fd_src < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        i = j = k = 0;

        while ((n = read(fd_src, buf, bytes)) > 0) {
                if (i >= 26) {
                        i = 0;
                        if (++j >= 26) {
                                j = 0;
                                if (++k >= 26) {
                                        fprintf(stderr, "Split count over range!\n");
                                        ret = EINVAL;
                                        GOTO(err_close, ret);
                                }
                        }
                }
                if (dir)
                        snprintf(split, sizeof(split), "%s/%s%c%c%c",
                                 dir, basename, alph[k], alph[j], alph[i]);
                else
                        snprintf(split, sizeof(split), "%s%c%c%c",
                                 basename, alph[k], alph[j], alph[i]);

                fd_dst = open(split, O_CREAT | O_WRONLY | O_TRUNC, 0755);
                if (fd_dst < 0) {
                        ret = errno;
                        GOTO(err_close, ret);
                }

                ret = write(fd_dst, buf, n);
                if (ret != n) {
                        ret = EIO;
                        GOTO(err_close, ret);
                }

                ++i;
                close(fd_dst);
        }

        ret = 0;
err_close:
        close(fd_src);
        close(fd_dst);
err_ret:
        return ret;
}

/*
 * create_test_file -
 *
 * @bytes: bytes of file to create
 * @file: path of file
 *
 * Generate a file with spefic size.
 *
 * @return zero on success, otherwise errno is returned.
 */
static int create_test_file(uint32_t bytes, char *file)
{
        int ret, fd, i;
        uint32_t p, q;
        char alph[] = "abcdefghijklmnopqrstuvwxyz0123456789\n";

        p = bytes / strlen(alph);
        q = bytes % strlen(alph);

        fd = open(file, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0755);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        for (i = 0; i < p; ++i) {
                ret = write(fd, alph, strlen(alph));
                if (ret != strlen(alph)) {
                        ret = EIO;
                        GOTO(err_close, ret);
                }
        }

        ret = write(fd, alph, q);
        if (ret != q) {
                ret = EIO;
                GOTO(err_close, ret);
        }

        ret = 0;
err_close:
        close(fd);
err_ret:
        return ret;
}

/*
 * test_dir - directory tester
 *
 * @path: directory to test in
 */
static void test_dir(char *path)
{
        int ret, i;
        char buf[PATH_MAX];

        ret = prepare_env(path, 1);
        if (ret)
                GOTO(err_ret, ret);

        printf("[ test mkdir ... ]\n");

        for (i = 0; i < TEST_DIR_COUNT; ++i) {
               snprintf(buf, sizeof(buf), "__test_dir_%d", i);

               ret = mkdir(buf, 0755);
               if (ret < 0) {
                       ret = errno;
                       GOTO(err_ret, ret);
               }
        }

        sleep (1);

        printf("[ test rmdir ... ]\n");

        for (i = 0; i < TEST_DIR_COUNT; ++i) {
                snprintf(buf, sizeof(buf), "__test_dir_%d", i);

                ret = rmdir(buf);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        return;
err_ret:
        exit(-1);
}

/*
 * test_mv - move file tester
 *
 * @path: directory to test in
 */
static void test_mv(char *path)
{
        int ret, i;
        char src_file[PATH_MAX], dst_file[PATH_MAX];
        char src_dir[] = "__move_src_dir__", dst_dir[] = "__move_dst_dir__";
        char md5[TEST_FILE_COUNT][33];
        uint32_t unitsize = 1000 * 100;

        ret = prepare_env(path, 1);
        if (ret)
                GOTO(err_ret, ret);

        printf("[ test mv ... ]\n");

        if (mkdir(src_dir, 0755) == -1 || mkdir(dst_dir, 0755) == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        /*
         * Create source files and save their md5 for check. then move the file
         * to destination directory.
         */
        for (i = 1; i <= TEST_FILE_COUNT; ++i) {
                snprintf(src_file, sizeof(src_file), "%s/%d", src_dir, unitsize * i);
                snprintf(dst_file, sizeof(dst_file), "%s/%d", dst_dir, unitsize * i);
                ret = create_test_file(unitsize * i, src_file);
                if (ret)
                        GOTO(err_ret, ret);
                md5_file(src_file, md5[i - 1], 33);
                ret = __move(src_file, dst_file, 0);
                if (ret)
                        GOTO(err_ret, ret);
        }

        printf("[ check and rm test file ... ]\n");

        /*
         * Check MD5, wirte log if not equal.
         */
        for (i = 1; i <= TEST_FILE_COUNT; ++i) {
                char md5_now[33];
                snprintf(dst_file, sizeof(dst_file), "%s/%d", dst_dir, unitsize * i);

                md5_file(dst_file, md5_now, 33);
                if (memcmp(md5[i - 1], md5_now, 32))
                        write_log("Warning: wrong md5 is found in function test_mv.\n");

                ret = remove(dst_file);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        if (rmdir(src_dir) == -1 || rmdir(dst_dir) == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return;
err_ret:
        exit(-1);
}

/*
 * test_cp - copy file tester
 *
 * @path: directory to test in
 */
static void test_cp(char *path)
{
        int ret, i;
        char src_file[PATH_MAX], dst_file[PATH_MAX];
        char src_dir[] = "__copy_src_dir__", dst_dir[] = "__copy_dst_dir__";
        char md5[TEST_FILE_COUNT][33];
        uint32_t unitsize = 1000 * 100;

        ret = prepare_env(path, 1);
        if (ret)
                GOTO(err_ret, ret);

        if (mkdir(src_dir, 0755) == -1 || mkdir(dst_dir, 0755) == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        printf("[ test cp ... ]\n");

        for (i = 1; i <= TEST_FILE_COUNT; ++i) {
                snprintf(src_file, sizeof(src_file), "%s/%d", src_dir, unitsize * i);
                snprintf(dst_file, sizeof(dst_file), "%s/%d", dst_dir, unitsize * i);
                ret = create_test_file(unitsize * i, src_file);
                if (ret)
                        GOTO(err_ret, ret);
                md5_file(src_file, md5[i - 1], 33);
                ret = __copy(src_file, dst_file, 0);
                if (ret)
                        GOTO(err_ret, ret);
        }

        printf("[ check and rm test file ... ]\n");

        for (i = 1; i <= TEST_FILE_COUNT; ++i) {
                char md5_now[33];
                snprintf(src_file, sizeof(src_file), "%s/%d", src_dir, unitsize * i);
                snprintf(dst_file, sizeof(dst_file), "%s/%d", dst_dir, unitsize * i);

                md5_file(dst_file, md5_now, 33);
                if (memcmp(md5[i - 1], md5_now, 32))
                        write_log("Warning: wrong md5 is found in function test_cp.\n");

                if (remove(src_file) == -1 || remove(dst_file) == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        if (rmdir(src_dir) == -1 || rmdir(dst_dir) == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return;
err_ret:
        exit(-1);
}

/*
 * test_split - split file tester
 *
 * @path: directory to test in
 */
static void test_split(char *path)
{
        int ret, n, i;
        DIR *dp;
        struct dirent *dirp, **namelist;
        char cmd[1024], md5_src[33], md5_dst[33];
        char file_src[] = "__64m_src", file_dst[] = "__64m_merge";
        uint32_t size = 1024 * 1024 * 64;

        ret = prepare_env(path, 1);
        if (ret)
                GOTO(err_ret, ret);

        printf("[ test split ... ]\n");

        ret = create_test_file(size, file_src);
        if (ret)
                GOTO(err_ret, ret);

        md5_file(file_src, md5_src, 33);

        ret = __split(file_src, NULL, "_split_", 102400);
        if (ret)
                GOTO(err_ret, ret);

        n = scandir(path, &namelist, 0, alphasort);
        if (n < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        } else {
                for (i = 0; i < n; ++i) {
                        if (strstr(namelist[i]->d_name, "_split_")) {
                                ret = __copy(namelist[i]->d_name, file_dst, 1);
                                if (ret)
                                        GOTO(err_ret, ret);
                        }
                        free(namelist[i]);
                }
                free(namelist);
        }

        printf("[ check and rm test file ... ]\n");

        md5_file(file_dst, md5_dst, 33);

        if (memcmp(md5_src, md5_dst, 32))
                write_log("Warning: wrong md5 is found in function test_split.\n");

        dp = opendir(path);
        if (!dp) {
                ret = errno;
                GOTO(err_ret, ret);
        }
        while ((dirp = readdir(dp)) != NULL) {
                if (strstr(dirp->d_name, "_split_") != NULL) {
                        if (remove(dirp->d_name) == -1) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }
                }
        }
        closedir(dp);

        if (remove(file_src) == -1 || remove(file_dst) == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return;
err_ret:
        exit(-1);
}

/*
 * test_file - other file tester
 *
 * @path: directory to test in
 */
static void test_file(char *path)
{
        int ret, fd, i;
        unsigned long head, tail;
        struct stat statbuf;
        char buf[10];
        char *file = "file_to_read";

        ret = prepare_env(path, 1);
        if (ret)
                GOTO(err_ret, ret);

        printf("[ test file write and read ...]\n");

        printf("[ ----- read file not existed ...]\n");
        for (i = 0; i < 5; ++i) {
                ret = open("file_not_exist", O_RDONLY);
                if (ret < 0)
                        /* Is this necessary ? */
                        perror("This is _CORRENT_");
        }

        ret = create_test_file((rand() % 5 + 1) * 1024 * 1024, file);
        if (ret)
                GOTO(err_ret, ret);

        ret = stat(file, &statbuf);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }
        tail = statbuf.st_size;

	fd = open(file, O_RDONLY);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
	}

	printf("----- read part of file -----\n");
        for (i = 0; i < 10; ++i) {
                head = rand() % tail;
                if (head > tail - BUFSIZE)
                        head = tail - BUFSIZE;

                ret = lseek(fd, head, SEEK_SET);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
		}

                memset(buf, 0, sizeof(buf));
                ret = read(fd, buf, sizeof(buf));
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        printf("----- read file out of range -----\n");
        for (i = 0; i < 10; ++i) {
		ret = lseek(fd, 0, SEEK_SET);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
		}

		while (read(fd, buf, sizeof(buf)) != 0)
			continue;

		ret = read(fd, buf, sizeof(buf));
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
		}

		head = rand() % tail;
		if (head > tail - BUFSIZE)
			head = tail - BUFSIZE;

		ret = lseek(fd, head, SEEK_SET);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
		}

		while (read(fd, buf, sizeof(buf)) != 0)
			continue;

		ret = read(fd, buf, sizeof(buf));
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
		}
	}

	close(fd);

        ret = remove(file);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

	return;
err_ret:
	exit(-1);
}

/*
 * __write_worker - write worker thread
 */
static void *__write_worker(void *arg)
{
        int ret, flist_fd;
        struct stat stat_buf;
        char alph[]="abcdefghijklmnopqrstuvwxyz0123456789";
        struct thread_param *param = (struct thread_param *)arg;
        char dir[PATH_MAX], file_list[PATH_MAX];

        snprintf(dir, sizeof(dir), "%s/%d", param->dir, param->number);

        stat(dir, &stat_buf);

        if (!S_ISDIR(stat_buf.st_mode)) {
                ret = mkdir(dir, 0755);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        /*
         * Open the file to contain file list
         */
        snprintf(file_list, sizeof(file_list), "filelist_%d", param->number);

        flist_fd = open(file_list, O_CREAT | O_APPEND | O_WRONLY, 0644);
        if (flist_fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        while (1) {
                struct timeval tv;
                int fd, i, buflen, size, count = 0;
                char file_name[PATH_MAX], file_md5[PATH_MAX];
                char buf[4096], md5[33], ch;

                /*
                 * Calculate random size, minimum is 1k
                 */
                size = rand() % (param->size + 1);
                if (size < 1024)
                        size = 1024;

		(void) gettimeofday(&tv, NULL);

                snprintf(file_name, sizeof(file_name), "%s/testfile_sz%d_sec%ld_usec%ld_rand%d",
                         dir, size, tv.tv_sec, tv.tv_usec, rand() % 10000);

                printf("Thread %d\t==>\t%s\n", param->number, file_name);

                buflen = 4096;
                ch = alph[rand() % strlen(alph)];
                memset(buf, ch, buflen);

                /*
                 * Create a file with unique name and random size
                 */
                fd = open(file_name, O_CREAT | O_TRUNC | O_WRONLY, 0644);
                if (fd < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                while (count < size) {
                        if (buflen > size - count)
                                buflen = size - count;

                        ret = write(fd, buf, buflen);
                        if (ret != buflen) {
                                ret = EIO;
                                GOTO(err_ret, ret);
                        }

                        count += buflen;
                }
                close(fd);

                /*
                 * Create a file to record the md5 value of test_file
                 */
                snprintf(file_md5, sizeof(file_md5), "%s.md5", file_name);

                fd = open(file_md5, O_CREAT | O_TRUNC | O_WRONLY, 0644);
                if (fd < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                md5_file(file_name, md5, 33);

                ret = write(fd, md5, 32);
                if (ret != 32) {
                        ret = EIO;
                        GOTO(err_ret, ret);
                }
                close(fd);

                /*
                 * Write file name to file list
                 */
                if (write(flist_fd, file_name, strlen(file_name)) != strlen(file_name) ||
                    write(flist_fd, "\n", 1) != 1) {
                        ret = EIO;
                        GOTO(err_ret, ret);
                }
        }

        return;
err_ret:
        exit(-1);
}

/*
 * write_test - write file
 *
 * @dir: directory to write file
 * @concurrent: thread count
 * @size: maximum size
 */
static void write_test(char *dir, int concurrent, int size)
{
        int ret, i;
        struct thread_param *params;
        pthread_t th;
        pthread_attr_t ta;
        void *pret;

        ret = prepare_env(dir, 1);
        if (ret)
                GOTO(err_ret, ret);

        /*
         * Allocate per-thread structures
         */
        params = malloc(concurrent * sizeof(struct thread_param));
        if (!params) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        for (i = 0; i < concurrent; ++i) {
                (params + i)->dir = dir;
                (params + i)->number = i;
                (params + i)->size = size;

                (void) pthread_create(&th, &ta, &__write_worker, (void *)(params + i));
        }

        (void) pause();

        free(params);
err_ret:
        exit(ret);
}

static void __md5_cmp(FILE *pfile_list, char *file_name)
{
        int ret;
        FILE *pfile_md5;
        char line[LINE_MAX], file_md5[PATH_MAX], md5_old[33], md5_now[33], err_log[LINE_MAX];

        while (fgets(line, LINE_MAX, pfile_list) != NULL) {
                if (line[strlen(line) - 1] == '\n')
                        line[strlen(line) - 1] = '\0';
                snprintf(file_md5, sizeof(file_md5), "%s.md5", line);

                printf("Check\t%s\n", line);

                pfile_md5 = fopen(file_md5, "r");
                if (!pfile_md5) {
                        snprintf(err_log, LINE_MAX, "Warning: fopen file %s failed fileList_file %s\n", file_md5, file_name);
                        write_log(err_log);
                        continue;
                }
                if (fgets(md5_old, 33, pfile_md5) == NULL) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
                fclose(pfile_md5);

                md5_file(line, md5_now, 33);

                if (memcmp(md5_old, md5_now, 32)) {
                        snprintf(err_log, sizeof(err_log), "Warning: wrong md5 is found in function read_test.\t%s\n", line);
                        write_log(err_log);
                }
        }

        return;
err_ret:
        exit(-1);
}

void *__read_worker(void *arg)
{
        int ret;
        FILE *fp = NULL;

        fp = fopen((char *)arg, "r");
        if (!fp) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        __md5_cmp(fp, (char *)arg);
        fclose(fp);

        return;
err_ret:
        exit(-1);
}

void read_test(char *dir)
{
        int ret, count, i = 0;
        pthread_t *ptid = NULL;
        DIR *dp = NULL;
        struct dirent *dirp = NULL;
        char **file;
        void *pret;

        ret = prepare_env(dir, 0);
        if (ret)
                GOTO(err_ret, ret);

        dp = opendir(dir);
        if (!dp) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        while ((dirp = readdir(dp)) != NULL) {
                if (strstr(dirp->d_name, "filelist_") != NULL)
                        ++i;
        }
        closedir(dp);
        count = i;

        if (!(ptid = (pthread_t *)malloc(count * sizeof(pthread_t))) ||
            !(file = (char **)malloc(count * sizeof(char *)))) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        for (i = 0; i < count; ++i) {
                file[i] = (char *)malloc(PATH_MAX * sizeof(char));

                snprintf(file[i], PATH_MAX, "filelist_%d", i);

                (void) pthread_create(ptid + i, NULL, &__read_worker, (void *)(file[i]));
        }

        for (i = 0; i < count; ++i) {
                ret = pthread_join(ptid[i], &pret);
                if (ret)
                        GOTO(err_ret, ret);
                free(file[i]);
        }

        free(file);

        return;
err_ret:
        exit(-1);
}

void usage(char *prog)
{
        printf("Usage: %s [options] --args\n"
               "Options:\n "
               "\t-c CONCURRENT \tSet concurrent of write, valid only when -t is set to \"write\".\n"
               "\t\t\tDefault value: 1\tMinimum: 1\tMaximun: 2147483647.\n"
               "\t-d DIR \t\tSet the directory to test.\n"
               "\t-h \t\tShow this help message and exit.\n"
               "\t-s SIZE \tSet maximal size of write, valid only when -t is set to \"write\".\n"
               "\t\t\tExamples: 512b 512B 1k 1K 1m 1M.\n"
               "\t\t\tDefault value: 1k\tMinimum: 1k\tMaximum: 2147483647b\n"
               "\t-t TEST \tSet test mode:\"func\", \"read\", \"write\".\n"
               , prog);

        printf("Describe:\n"
               "Program will check the md5 value of test files, if"
               " an error occurred, a warning\nwill be written into"
               " the file \"/var/log/ynfs_functest.log\".\n");
        exit(1);
}

#define ARRARY_SIZE(array) (sizeof(array) / sizeof(array[0]))
#define BYTES_PER_KB    (1024)
#define BYTES_PER_MB    (1024 * 1024)

void (*func_testers[])(char *) =  {
        test_dir,
        test_mv,
        test_cp,
        test_split,
        test_file,
};

enum tester_mode {
    tm_func,
    tm_read,
    tm_write,
};

int main(int argc, char *argv[])
{
        int c_opt;
        int concurrent = 1, size = 1024;
        char unit, *prog, root[PATH_MAX];
        enum tester_mode mode = -1;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        if (getuid() != 0) {
                fprintf(stderr, "Please run this program with root.\n");
                exit(1);
        }

        while ((c_opt = getopt(argc, argv, "c:d:hs:t:")) != -1) {
                switch (c_opt) {
                case 'c':
                        concurrent = atoi(optarg);
                        if (concurrent < 1)
                                goto opt_err;
                        break;

                case 'd':
                        snprintf(root, sizeof(root), "%s", optarg);
                        break;

                case 'h':
                        usage(prog);
                        break;

                case 's':
                        unit = optarg[strlen(optarg) - 1];

                        switch (unit) {
                        case 'b':
                        case 'B':
                                size = atoi(optarg);
                                if (size < 1024)
                                        goto opt_err;
                                break;
                        case 'k':
                        case 'K':
                                size = atoi(optarg);
                                if (size < 1 || size > (INT_MAX / BYTES_PER_KB))
                                        goto opt_err;
                                else
                                        size *= BYTES_PER_KB;
                                break;
                        case 'm':
                        case 'M':
                                size = atoi(optarg);
                                if (size < 1 || size > (INT_MAX / BYTES_PER_MB))
                                        goto opt_err;
                                else
                                        size *= BYTES_PER_MB;
                                break;
                        default:
                                goto opt_err;
                        }
                        break;

                case 't':
                        if (!strcmp("func", optarg))
                                mode = tm_func;
                        else if (!strcmp("read", optarg))
                                mode = tm_read;
                        else if (!strcmp("write", optarg))
                                mode = tm_write;
                        else
                                goto opt_err;
                        break;

opt_err:
                default:
                        usage(prog);
                }
        }

        /*
         * Option of -d and -t must be set
         */
        if (!strlen(root) || mode == -1)
                usage(prog);

        srand((unsigned int)time(NULL));

        printf("[ concurrent: %d    root: %s    size: %d    mode: %d\n",
               concurrent, root, size, mode);

        switch (mode) {
        case tm_func:
                while (1) {
                        /*
                         * In function test mode, choose one to execute at random.
                         */
                        func_testers[rand() % ARRARY_SIZE(func_testers)](root);
                }
                break;

        case tm_read:
                read_test(root);
                break;

        case tm_write:
                write_test(root, concurrent, size);
                break;
        }

        return 0;
}

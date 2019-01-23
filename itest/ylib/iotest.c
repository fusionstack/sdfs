#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/times.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <limits.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#include "iotest.h"
#include "functest.h"
#include "md5.h"

void *__write_thr_fn(void *arg)
{
        int file_size, i, count, buflen, fd;
        MD5_CTX context;
	struct timeval  tv, tv1;
	struct timezone tz, tz1;
        struct stat stat_buf;
        char s[]="abcdefghijklmnopqrstuvwxyz0123456789";
        char buf[4096], md5[33], md5_tmp[33], digest[16], ch;
        char dir[PATH_MAX], file_name[PATH_MAX], file_md5[PATH_MAX], file_link[PATH_MAX], file_list[PATH_MAX];

        snprintf(dir, sizeof(dir), "%s/%d", ((pthread_arg *)arg)->dir, ((pthread_arg *)arg)->number);

        stat(dir, &stat_buf);

        if (S_ISDIR(stat_buf.st_mode) == 0) {
                if (mkdir(dir, 0755) == -1) {
                        perror("");
                        goto __func__err_exit;
                }
        }

        srand((unsigned)time(0));

        while (1) {
                MD5Init(&context);

                // random size in range 1k to size
                file_size = rand() % (((pthread_arg *)arg)->size + 1);
                if (file_size < 1024)
                        file_size = 1024;

                // make the test_file with the file_size
		if (gettimeofday(&tv, &tz) == -1) {
			perror("");
			goto __func__err_exit;
		}
                snprintf(file_name, sizeof(file_name), "%s/testfile_sz%d_sec%ld_usec%ld_rand%d",
                                dir, file_size, tv.tv_sec, tv.tv_usec, rand() % 10000);

                //printf("tv.tv_sec Thread %d ==> %s\n", ((pthread_arg *)arg)->number, file_name);

                buflen = 4096;
                ch = s[rand() % strlen(s)];
                memset(buf, ch, buflen - 1);
                buf[buflen - 1] = '\0';

                if ((fd = open(file_name, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1) {
                        perror("");
                        goto __func__err_exit;
                }

                count = 0;
                while (count < file_size) {
                        if (buflen > file_size - count)
                                buflen = file_size - count;

                        if (write(fd, buf, buflen) == -1) {
                                perror("");
                                goto __func__err_exit;
                        }

		if (gettimeofday(&tv1, &tz1) == -1) {
			perror("");
			goto __func__err_exit;
		}
                printf("%ld %ld Thread %d ==> %s\n",tv.tv_sec, (tv1.tv_sec - tv.tv_sec), ((pthread_arg *)arg)->number, file_name);

                        MD5Update(&context, buf, buflen);

                        count += buflen;
                }
                close(fd);

                MD5Final(digest, &context);


                snprintf(file_link, sizeof(file_link), "%s.link", file_name);
                if (symlink(file_name, file_link) == -1) {
                        perror("");
                        goto __func__err_exit;
                }

                // make the file_md5 to record the md5 value of test_file
                for (i = 0; i < 16; i++) {
                        snprintf(&(md5_tmp[2 * i]), 3, "%02x", (unsigned char)digest[i]);
                        snprintf(&(md5_tmp[2 * i + 1]), 3, "%02x", (unsigned char)(digest[i] << 4));
                }

                memset(md5, 0x0, sizeof(md5));

                for (i = 0; i < 32; i++)
                        md5[i] = md5_tmp[i];

                snprintf(file_md5, sizeof(file_md5), "%s.md5", file_link);

                if ((fd = open(file_md5, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1) {
                        perror("");
                        goto __func__err_exit;
                }

                if (write(fd, md5, 32) == -1) {
                        perror("");
                        goto __func__err_exit;
                }
                close(fd);

                // write the file path to the file_list
                snprintf(file_list, sizeof(file_list), "filelist_%d", ((pthread_arg *)arg)->number);

                if ((fd = open(file_list, O_CREAT | O_APPEND | O_WRONLY, 0644)) == -1) {
                        perror("");
                        goto __func__err_exit;
                }

                if (write(fd, file_link, strlen(file_link)) == -1 ||
                                write(fd, "\n", 1) == -1) {
                        perror("");
                        goto __func__err_exit;
                }
                close(fd);
        }
        return;

__func__err_exit:
        EXIT(-1);
}


void write_test(char *dir, int concurrent, int size)
{
        int i;
        pthread_arg *parg = NULL;
        pthread_t *ptid = NULL;
        void *pret;

        if ((ptid = (pthread_t *)malloc(concurrent * sizeof(pthread_t))) == NULL ||
                        (parg = (pthread_arg *)malloc(concurrent * sizeof(pthread_arg))) == NULL) {
                perror("");
                goto __func__err_exit;
        }

        prepare_env(dir, CREATE_DIR_TRUE);

        for (i = 0; i < concurrent; ++i) {
                (parg + i)->dir = dir;
                (parg + i)->number = i;
                (parg + i)->size = size;

                if (pthread_create(ptid + i, NULL, &__write_thr_fn, (void *)(parg + i)) != 0) {
                        perror("");
                        goto __func__err_exit;
                }
        }

        for (i = 0; i < concurrent; ++i) {
                if (pthread_join(ptid[i], &pret) != 0) {
                        perror("");
                        continue;
                }
        }

        free((void *)ptid);
        free((void *)parg);
        return;

__func__err_exit:
        free(ptid);
        free(parg);
        EXIT(-1);
}


static void __md5_cmp(FILE *pfile_list)
{
        char            line[LINE_MAX], file_md5[PATH_MAX], md5_old[33], md5_now[33], err_log[LINE_MAX];
        FILE            *pfile_md5;
        static pthread_mutex_t mutex_md5 = PTHREAD_MUTEX_INITIALIZER;


        while (fgets(line, LINE_MAX, pfile_list) != NULL) {
                if (line[strlen(line) - 1] == '\n')
                        line[strlen(line) - 1] = '\0';
                snprintf(file_md5, sizeof(file_md5), "%s.md5", line);

                printf("Check\t%s\n", line);

                if ((pfile_md5 = fopen(file_md5, "r")) == NULL) {
                        snprintf(err_log, LINE_MAX, "Warning: fopen file %s failed\n", file_md5);
                        write_log(err_log);
                        continue;
#if 0
                        perror("");
                        goto __func__err_exit;
#endif
                }
                if (fgets(md5_old, 33, pfile_md5) == NULL) {
                        perror("");
                        goto __func__err_exit;
                }
                fclose(pfile_md5);

                pthread_mutex_lock(&mutex_md5);
                strncpy(md5_now, MDFile(line), 32);
                md5_now[32] = '\0';
                pthread_mutex_unlock(&mutex_md5);

                if (strcmp(md5_old, md5_now) != 0) {
                        snprintf(err_log, sizeof(err_log), "Warning: wrong md5 is found in function read_test.\t%s\n", line);
                        write_log(err_log);
                }
        }
        return;

__func__err_exit:
        EXIT(-1);
}


void *__read_thr_fn(void *arg)
{
        FILE *fp = NULL;

        if ((fp = fopen((char *)arg, "r")) == NULL) {
                perror("");
                goto __func__err_exit;
        }
        __md5_cmp(fp);
        fclose(fp);

        return;

__func__err_exit:
        EXIT(-1);
}


void read_test(char *dir)
{
        int             count, i = 0;
        pthread_t       *ptid = NULL;
        DIR             *dp = NULL;
        struct dirent   *dirp = NULL;
        char            **file;
        void            *pret;

        prepare_env(dir, CREATE_DIR_FALSE);

        if ((dp = opendir(dir)) == NULL) {
                perror("");
                goto __func__err_exit;
        }

        while ((dirp = readdir(dp)) != NULL) {
                if (strstr(dirp->d_name, "filelist_") != NULL)
                        ++i;
        }
        closedir(dp);
        count = i;

        if ((ptid = (pthread_t *)malloc(count * sizeof(pthread_t))) == NULL || (file = (char **)malloc(count * sizeof(char *))) == NULL) {
                perror("");
                goto __func__err_exit;
        }

        for (i = 0; i < count; ++i) {
                file[i] = (char *)malloc(PATH_MAX * sizeof(char));

                snprintf(file[i], PATH_MAX, "filelist_%d", i);

                if (pthread_create(ptid + i, NULL, &__read_thr_fn, (void *)(file[i])) != 0) {
                        perror("");
                        goto __func__err_exit;
                }
        }

        for (i = 0; i < count; ++i) {
                if (pthread_join(ptid[i], &pret) != 0) {
                        perror("");
                        goto __func__err_exit;
                }
                free(file[i]);
        }

        free(file);

        return;

__func__err_exit:
        EXIT(-1);
}

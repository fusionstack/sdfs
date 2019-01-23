#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <limits.h>
#include <errno.h>
#include <string.h>
#include <time.h>

#include "functest.h"
#include "md5.h"

#define BUFSIZE 4096


void prepare_env(const char *path, int create_dir)
{
        struct  stat buf;

        memset(&buf, 0, sizeof(struct stat));

        stat(path, &buf);

        if (S_ISDIR(buf.st_mode) == 0) {
                if (create_dir) {
                        if (mkdir(path, 0755) == -1) {
                                perror("");
                                goto __func__err_exit;
                        }
                } else {
                        fprintf(stderr, "Directory not exists\n");
                        goto __func__err_exit;
                }
        }

        if (chdir(path) == -1) {
                perror("");
                goto __func__err_exit;
        }
        return;

__func__err_exit:
        EXIT(-1);
}

void write_log(const char *data)
{
        int fd;
        time_t timep;
        char str_time[1024];
        const char log_path[] = "/var/log/ynfs_iotest.log";

        if ((fd = open(log_path, O_CREAT | O_WRONLY | O_APPEND, 0644)) == -1) {
                perror("");
                goto __func__err_exit;
        }

        time(&timep);
        strncpy(str_time, ctime(&timep), sizeof(str_time) - 1);
        str_time[sizeof(str_time) - 1] = '\0';
        str_time[strlen(str_time) - 1] = '\t';

        fprintf(stderr, RED"----------%s%s"NORMAL, str_time, data);

        if (write(fd, str_time, strlen(str_time)) == -1 || write(fd, data, strlen(data)) == -1) {
                perror("");
                goto __func__err_exit;
        }
        close(fd);
        return;

__func__err_exit:
        EXIT(-1);
}


static void __copy(const char *src, const char *tag, const char *mode)
{
        int     n;
        int     fd_src, fd_tag;
        char    buf[BUFSIZE];

        if ((fd_src = open(src, O_RDONLY)) == -1) {
                perror("");
                goto __func__err_exit;
        }

        if((fd_tag = open(tag, O_CREAT | O_WRONLY, 0755)) == -1) {
                perror("");
                goto __func__err_exit;

        }

        while ((n = read(fd_src, buf, BUFSIZE)) > 0) {
                if (mode && strcmp(mode, "append") == 0)
                        if (lseek(fd_tag, 0, SEEK_END) == -1) {
                                perror("");
                                goto __func__err_exit;
                        }

                if (write(fd_tag, buf, n) != n) {
                        perror("");
                        goto __func__err_exit;
                }
        }

        close(fd_src);
        close(fd_tag);
        return;

__func__err_exit:
        EXIT(-1);

}


static void __move(const char *src, const char *tag, const char *mode)
{
        __copy(src, tag, mode);

        if (remove(src) == -1) {
                perror("");
                goto __func__err_exit;
        }
        return;

__func__err_exit:
        EXIT(-1);
}


static void __split(const char *src, const char *path, const char *basename, unsigned byte)
{
        const   char s[] = "abcdefghijklmnopqrstuvwxyz";
        char    temp[PATH_MAX], buf[byte];
        int     fd_read, fd_write, n, i = 0, j = 0, k = 0;

        if ((fd_read = open(src, O_RDONLY)) == -1) {
                perror("");
                goto __func__err_exit;
        }

        while ((n = read(fd_read, buf, byte)) > 0) {
                if (i >= 26) {
                        i = 0;
                        if (++j >= 26) {
                                j = 0;
                                if (++k >= 26) {
                                        fprintf(stderr, "[-] Split number over range!\n");
                                        goto __func__err_exit;
                                }
                        }
                }
                if (path) {
                        snprintf(temp, PATH_MAX, "%s/%s%c%c%c", path, basename, s[k], s[j],s[i]);
                } else {
                        snprintf(temp, PATH_MAX, "%s%c%c%c",  basename, s[k], s[j], s[i]);
                }

                if ((fd_write = open(temp, O_CREAT | O_WRONLY | O_TRUNC, 0755)) == -1) {
                        perror("");
                        goto __func__err_exit;
                }

                if (write(fd_write, buf, n) != n) {
                        perror("");
                        goto __func__err_exit;
                }

                ++i;
                close(fd_write);
        }

        close(fd_read);
        return;

__func__err_exit:
        EXIT(-1);
}


void make_test_file(unsigned byte, const char *file)
{
        int             fd, i;
        unsigned        p, q;
        char s[] = "abcdefghijklmnopqrstuvwxyz0123456789\n";
        char buf[strlen(s)];

        p = byte / strlen(s);
        q = byte % strlen(s);

        if ((fd = open(file, O_WRONLY | O_CREAT | O_TRUNC, 0755)) == -1) {
                perror("");
                goto __func__err_exit;
        }

        for (i = 0; i < p; ++i) {
                if (lseek(fd, 0, SEEK_END) == -1) {
                        perror("");
                        goto __func__err_exit;
                }

                if (write(fd, s, strlen(s)) == -1) {
                        perror("");
                        goto __func__err_exit;
                }
        }

        strncpy(buf, s, q);
        buf[q] = '\0';

        if (write(fd, s, strlen(buf)) == -1) {
                perror("");
                goto __func__err_exit;
        }

        close(fd);
        return;

 __func__err_exit:
        EXIT(-1);
}


void test_dir(const char *path)
{
        int     i;
        char    buffer[PATH_MAX];

        prepare_env(path, CREATE_DIR_TRUE);

        printf("[ test mkdir ... ]\n");

        for (i = 0; i < 1000; ++i) {
               snprintf(buffer, sizeof(buffer), "_test_dir_%d", i);

               if (mkdir(buffer, 0755) == -1) {
                        perror("");
                        goto __func__err_exit;
               }
        }

        sleep (2);

        printf("[ test rmdir ... ]\n");

        for (i = 0; i < 1000; ++i) {
                snprintf(buffer, sizeof(buffer), "_test_dir_%d", i);

                if (rmdir(buffer) == -1) {
                        perror("");
                        goto __func__err_exit;
                }
        }
        return;

__func__err_exit:
        EXIT(-1);
}


void test_mv(const char *path)
{
        char    src_file[PATH_MAX], tag_file[PATH_MAX];
        char    src_dir[] = "_move_src_dir_", tag_dir[] = "_move_tag_dir_";
        char    md5[100][33];
        int     i;

        prepare_env(path, CREATE_DIR_TRUE);

        printf("[ test mv ... ]\n");

        if (mkdir(src_dir, 0755) == -1) {
                perror("");
                goto __func__err_exit;
        }

        if (mkdir(tag_dir, 0755) == -1) {
                perror("");
                goto __func__err_exit;
        }

        for (i = 1; i <= 100; ++i) {
                snprintf(src_file, PATH_MAX, "%s/%d", src_dir, 1000 * 100 * i);
                snprintf(tag_file, PATH_MAX, "%s/%d", tag_dir, 1000 * 100 * i);
                make_test_file(1000 * 100 * i, src_file);
                strncpy(md5[i - 1], MDFile(src_file), 32);
                md5[i - 1][32] = '\0';
                __move(src_file, tag_file, NULL);
        }

        printf("[ check and rm test file ... ]\n");

        for (i = 1; i <= 100; ++i) {
                snprintf(tag_file, PATH_MAX, "%s/%d", tag_dir, 1000 * 100 * i);

                if (strcmp(md5[i - 1], MDFile(tag_file)) != 0)
                        write_log("Warning: wrong md5 is found in function test_mv.\n");

                if (remove(tag_file) == -1) {
                        perror("");
                        goto __func__err_exit;
                }
        }

        if (rmdir(src_dir) == -1 || rmdir(tag_dir) == -1) {
                perror("");
                goto __func__err_exit;
        }
        return;

__func__err_exit:
        EXIT(-1);
}


void test_cp(const char *path)
{
        char    src_file[PATH_MAX], tag_file[PATH_MAX];
        char    src_dir[] = "_copy_src_dir_", tag_dir[] = "_copy_tag_dir_";
        char    md5[100][33];
        int     i;

        prepare_env(path, CREATE_DIR_TRUE);

        printf("[ test cp ... ]\n");

        if (mkdir(src_dir, 0755) == -1) {
                perror("");
                goto __func__err_exit;
        }

        if (mkdir(tag_dir, 0755) == -1) {
                perror("");
                goto __func__err_exit;
        }

        for (i = 1; i <= 100; ++i) {
                snprintf(src_file, PATH_MAX, "%s/%d", src_dir, 1000 * 100 * i);
                snprintf(tag_file, PATH_MAX, "%s/%d", tag_dir, 1000 * 100 * i);
                make_test_file(1000 * 100 * i, src_file);
                strncpy(md5[i - 1], MDFile(src_file), 32);
                md5[i - 1][32] = '\0';
                __copy(src_file, tag_file, NULL);
        }

        printf("[ check and rm test file ... ]\n");

        for (i = 1; i <= 100; ++i) {
                snprintf(src_file, PATH_MAX, "%s/%d", src_dir, 1000 * 100 * i);
                snprintf(tag_file, PATH_MAX, "%s/%d", tag_dir, 1000 * 100 * i);

                if (strcmp(md5[i - 1], MDFile(tag_file)) != 0)
                        write_log("Warning: wrong md5 is found in function test_cp.\n");

                if (remove(src_file) == -1 || remove(tag_file) == -1) {
                        perror("");
                        goto __func__err_exit;
                }
        }

        if (rmdir(src_dir) == -1 || rmdir(tag_dir) == -1) {
                perror("");
                goto __func__err_exit;
        }
        return;

__func__err_exit:
        EXIT(-1);
}


void test_split(const char *path)
{
        DIR             *dp;
        struct dirent   *dirp, **namelist;
        char            cmd[1024], md5[33];
        char            file_src[] = "_64m_src", file_tag[] = "_64m_merge";
        unsigned        size = 1024 * 1024 * 64;
        int             n, i;

        prepare_env(path, CREATE_DIR_TRUE);

        printf("[ test split ... ]\n");

        make_test_file(size, file_src);

        strncpy(md5, MDFile(file_src), sizeof(md5) - 1);
        md5[sizeof(md5) - 1] = '\0';

        __split(file_src, NULL, "_split_", 102400);

        n = scandir(path, &namelist, 0, alphasort);
        if (n < 0) {
                perror("");
                goto __func__err_exit;
        } else {
                for (i = 0; i < n; ++i) {
                        if (strstr(namelist[i]->d_name, "_split_") != NULL)
                                __copy(namelist[i]->d_name, file_tag, "append");
                        free(namelist[i]);
                }
                free(namelist);
        }

        printf("[ check and rm test file ... ]\n");

        if (strcmp(md5, MDFile(file_tag)) != 0)
                write_log("Warning: wrong md5 is found in function test_split.\n");

        if ((dp = opendir(path)) == NULL) {
                perror("");
                goto __func__err_exit;
        }
        while ((dirp = readdir(dp)) != NULL)
                if (strstr(dirp->d_name, "_split_") != NULL)
                        if (remove(dirp->d_name) == -1) {
                                perror("");
                                goto __func__err_exit;
                        }
        closedir(dp);

        if (remove(file_src) == -1 || remove(file_tag) == -1) {
                perror("");
                goto __func__err_exit;
        }
        return;

__func__err_exit:
        EXIT(-1);
}


void test_longname(const char *path)
{
        const int       name_length = 256;
        char            fname[name_length], s[]="abcdefghijklmnopqrstuvwxyz0123456789.-_";
        char            err_log[PATH_MAX];
        int             i, j, fd;

        prepare_env(path, CREATE_DIR_TRUE);
        srand((unsigned)time(0));

        printf("[ test long name file ... ]\n");

        for (j = 0; j < 10; ++j) {
                for (i = 0; i < name_length - 1; ++i)
                        fname[i] = s[rand() % strlen(s)];
                fname[i] = '\0';

                if ((fd = open(fname, O_CREAT | O_WRONLY | O_TRUNC, 0644)) == -1) {
                        perror("");
                        goto __func__err_exit;
                }
                if (write(fd, fname, strlen(fname)) == -1) {
                        perror("");
                        goto __func__err_exit;
                }
                close(fd);

                sleep(1);
                if ((fd = open(fname, O_RDONLY)) == -1) {
                        perror("");
                        snprintf(err_log, sizeof(err_log), "Warning: file %s does not exist.\n", fname);
                        write_log(err_log);
                        continue;
                }
                close(fd);

                if (remove(fname) == -1) {
                        perror("");
                        goto __func__err_exit;
                }
        }
        return;

__func__err_exit:
        EXIT(-1);
}

void test_file(const char *path)
{
	int             fd, i;
        unsigned long   head, tail;
        struct stat     statbuf;
        char            buf[10];
        const char      *file = "file_to_read";

        prepare_env(path, CREATE_DIR_TRUE);
        srand((unsigned)time(0));

        printf("[ test file write and read ...]\n");

        printf("[ ----- read file not existed ...]\n");
        for (i = 0; i < 5; ++i)
                if (open("file_not_exist", O_RDONLY) == -1) {
                        perror("");
                }

        make_test_file((rand() % 5 + 1) * 1024 * 1024, file);

        if (stat(file, &statbuf) == -1) {
                perror("");
                goto __func__err_exit;
        }
        tail = statbuf.st_size;

	if ((fd = open(file, O_RDONLY)) == -1) {
		perror("");
		goto __func__err_exit;
	}

	printf("----- read part of file -----\n");
        for (i = 0; i < 10; ++i) {
                head = rand() % tail;
                if (head > tail - BUFSIZE)
                        head = tail - BUFSIZE;

                if (lseek(fd, head, SEEK_SET) == -1) {
			perror("");
			goto __func__err_exit;
		}

                memset(buf, 0, sizeof(buf));
                if (read(fd, buf, sizeof(buf)) == -1) {
                        perror("");
                        goto __func__err_exit;
                }
        }

        printf("----- read file out of range -----\n");
        for (i = 0; i < 10; ++i) {
		if (lseek(fd, 0, SEEK_SET) == -1) {
			perror("");
			goto __func__err_exit;
		}

		while (read(fd, buf, sizeof(buf)) != 0)
			continue;
                memset(buf, 0, sizeof(buf));
		if (read(fd, buf, sizeof(buf)) != 0) {
			perror("");
			goto __func__err_exit;
		}

		head = rand() % tail;
		if (head > tail - BUFSIZE)
			head = tail - BUFSIZE;

		if (lseek(fd, head, SEEK_SET) == -1) {
			perror("");
			goto __func__err_exit;
		}

		while (read(fd, buf, sizeof(buf)) != 0)
			continue;
                memset(buf, 0, sizeof(buf));
		if (read(fd, buf, sizeof(buf)) != 0) {
			perror("");
			goto __func__err_exit;
		}
	}
	close(fd);
        if (remove(file) == -1) {
                perror("");
                goto __func__err_exit;
        }
	return;

__func__err_exit:
	exit(-1);
}

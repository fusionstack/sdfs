#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <string.h>

#define S_OFFSET (64*1024*1024UL)
#define BUF_LEN  256

int main()
{
        int ret;
        int fd;
        const char *path = "data";
        const char *s = "ABCDE";
        char buf[BUF_LEN];
        
        printf("write ----------------------------\n");
        fd = open(path, O_CREAT|O_TRUNC|O_WRONLY|O_DIRECT, 0644);
        if (fd == -1) {
                printf("%s\n", strerror(errno));
                EXIT(1);
        }

        _write(fd, s, _strlen(s));
        // _write(fd, "abcde", 5);
        ret = lseek(fd, S_OFFSET, SEEK_SET);
        if (ret == -1) {
                printf("%s\n", strerror(errno));
                EXIT(1);
        }
        _write(fd, s, _strlen(s));


        close(fd);

        printf("read ----------------------------\n");
        fd = open(path, O_RDONLY);
        if (fd == -1) {
                printf("%s\n", strerror(errno));
                EXIT(1);
        }

        printf("1: read before\n");
        ret = _read(fd, buf, BUF_LEN);
        if (ret == -1) {
                printf("%s\n", strerror(errno));
                EXIT(1);
        }
        printf("ret %d %s\n", ret, buf);

        printf("2: read before 2\n");
        ret = _read(fd, buf, BUF_LEN);
        if (ret == -1) {
                printf("%s\n", strerror(errno));
                EXIT(1);
        }
        printf("ret %d %s\n", ret, buf);

        ret = lseek(fd, S_OFFSET, SEEK_SET);
        if (ret == -1) {
                printf("%s\n", strerror(errno));
                EXIT(1);
        }

        printf("3: read at offset %d\n", S_OFFSET);
        ret = _read(fd, buf, BUF_LEN);
        if (ret == -1) {
                printf("%s\n", strerror(errno));
                EXIT(1);
        }
        printf("ret %d %s\n", ret, buf);

        printf("4: read at offset %d\n", S_OFFSET+10);
        ret = lseek(fd, S_OFFSET+10, SEEK_SET);
        if (ret == -1) {
                printf("%s\n", strerror(errno));
                EXIT(1);
        }

        printf("5: read at offset %d\n", S_OFFSET / 2);
        ret = lseek(fd, S_OFFSET / 2, SEEK_SET);
        if (ret == -1) {
                printf("%s\n", strerror(errno));
                EXIT(1);
        }

        ret = _read(fd, buf, BUF_LEN);
        if (ret == -1) {
                printf("%s\n", strerror(errno));
                EXIT(1);
        }
        printf("ret %d %s\n", ret, buf);

        close(fd);
        printf("OK\n");

        return 0;
}

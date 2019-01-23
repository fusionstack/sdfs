

#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#include "ylib.h"

int main(int argc, char *argv[])
{
        int ret, fd_in, fd_out;
        char *prog;
        struct stat stbuf;
        uint64_t off_in, off_out;
        uint32_t left, count;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        if (argc != 3) {
                fprintf(stderr, "%s <filefrom> <fileto>\n", prog);
                EXIT(1);
        }

        fd_in = open(argv[1], O_RDONLY);
        if (fd_in == -1) {
                ret = errno;
                fprintf(stderr, "ERROR: open(%s, ...) ret (%d) %s\n", argv[1],
                        ret, strerror(ret));
                EXIT(1);
        }

        fd_out = open(argv[2], O_CREAT | O_EXCL | O_WRONLY, 0644);
        if (fd_out == -1) {
                ret = errno;
                fprintf(stderr, "ERROR: creat(%s, ...) ret (%d) %s\n", argv[2],
                        ret, strerror(ret));
                EXIT(1);
        }


        ret = fstat(fd_in, &stbuf);
        if (ret == -1) {
                ret = errno;
                fprintf(stderr, "ERROR: fstat(%s, ...) ret (%d) %s\n", argv[1],
                        ret, strerror(ret));
                EXIT(1);
        }

        off_in = 0;
        off_out = 0;
        left = stbuf.st_size;

        while (left > 0) {
                count = left;

                ret = sy_splice(fd_in, &off_in, fd_out, &off_out, &count, 0, NULL, 0);
                if (ret) {
                        fprintf(stderr, "ERROR: sy_splice() ret (%d) %s\n", ret,
                                strerror(ret));
                        EXIT(1);
                }

                left -= count;
        }

        (void) sy_close(fd_in);
        (void) sy_close(fd_out);

        return 0;
}

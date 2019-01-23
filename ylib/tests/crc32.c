

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#include "sysutil.h"
#include "ylib.h"

#define YFS_CDS_CHK_OFF (4096 * 10)

int main(int argc, char *argv[])
{
        int ret, fd;
        char *prog = NULL, *buf = NULL, *file = NULL;
        struct stat stbuf;
        uint32_t crcode, off = 0, size = 0;
        int c_opt;

        (void) argc;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        DINFO("count %u\n", argc);

        if (argc != 7) {
                fprintf(stderr, "%s -c <chunk> -o <offset> -s <size>\n", prog);
                EXIT(1);
        }

        while ((c_opt = getopt(argc, argv, "c:o:s:")) > 0)
                switch (c_opt) {
                case 'o':
                        off = atoi(optarg);
                        break;
                case 's':
                        size = atoi(optarg);
                        break;
                case 'c':
                        file = optarg;
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        EXIT(1);
                }


        fd = open(file, O_RDONLY);
        if (fd == -1) {
                ret = errno;
                fprintf(stderr, "ERROR: open(%s, ...) ret (%d) %s\n", argv[1],
                        ret, strerror(ret));
                EXIT(1);
        }

        ret = fstat(fd, &stbuf);
        if (ret == -1) {
                ret = errno;
                fprintf(stderr, "ERROR: fstat(%s, ...) ret (%d) %s\n", argv[1],
                        ret, strerror(ret));
                EXIT(1);
        }

        ret = ymalloc((void **)&buf, size);
        if (ret)
                GOTO(err_ret, ret);

        ret = _pread(fd, buf, size, off + YFS_CDS_CHK_OFF);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        crc32_init(crcode);
        crc32_stream(&crcode, buf, size);
        printf("offset %u size %u crc %x\n", off, size, crc32_stream_finish(crcode));


        (void) sy_close(fd);

        return 0;
err_ret:
        return ret;
}

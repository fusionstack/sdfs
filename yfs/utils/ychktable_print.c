#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "chktable.h"
#include "dbg.h"

/* Default mds journal directry */
#define DEFAULT_CHUNK_DIR "/sysy/yfs/mds/1/chunk/"

/* functions */
int __chktable_print(const char *path)
{
        int ret, i, j, chks, fd, found;
        uint64_t off;
        uint32_t buflen;
        char buf[CHKTABLE_RECORDLEN];
        char *ptr;
        struct stat stbuf;
        chktable_disk_t *disk;
        int chknum = 0;

        printf("%s\n", path);

        fd = open(path, O_RDONLY);
        if (fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = fstat(fd, &stbuf);
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        chks = (stbuf.st_size - CHKTABLE_OFF_BASE) / CHKTABLE_RECORDLEN;
        off = CHKTABLE_OFF_BASE;
        for (i = 0; i < chks; ++i) {
                buflen = CHKTABLE_RECORDLEN;
                ret = sy_pread(fd, buf, &buflen, off);
                if (ret || buflen != CHKTABLE_RECORDLEN)
                        continue;

                found = 0;
                ptr = buf;
                for (j = 0; j < YFS_CHK_REP_MAX; ++j) {
                        disk = (chktable_disk_t *)ptr;
                        if (disk->id == 0)
                                break;
                        printf("%8d: %llu -- %-4d: %llu_v%u %u\n", i+1, (LLU)off, j, (LLU)disk->id, disk->version, disk->chkver);
                        ptr += sizeof(chktable_disk_t);
                        found = 1;
                }

                if (found) {
                        printf("\n");
                        chknum++;
                }
                off += CHKTABLE_RECORDLEN;
        }

        printf("** summary ---------------------------------------------\n");
        printf("chknum %u\n", chknum);

        return 0;
err_ret:
        return ret;
}


int main(int argc, char **argv)
{
        char c_opt;
        char *prog;
        char path[MAX_PATH_LEN];

        prog = strrchr(argv[0], '/');
        prog?++prog:argv[0];

        if (argc != 2) {
                exit(1);
        }

        while ((c_opt = getopt(argc, argv, "d")) > 0) {
                switch (c_opt) {
                case 'd':
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        exit(1);
                }
        }

        snprintf(path, MAX_PATH_LEN, "%s/%s", DEFAULT_CHUNK_DIR, argv[1]);
        __chktable_print(path);

        return 0;
}

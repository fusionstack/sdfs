

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>

#include "configure.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "sdfs_buffer.h"

int main(int argc, char *argv[])
{
        int ret, args, verbose = 0;
        char c_opt, *prog, *path, depath[MAX_PATH_LEN], perms[11], date[64];
        off_t offset;
        void *de0, *ptr;
        unsigned int delen, len;
        struct dirent *de;
        struct stat stbuf;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;

        if (argc < 2) {
                fprintf(stderr, "%s [-v] <dirpath>\n", prog);
                exit(1);
        }

        while ((c_opt = getopt(argc, argv, "v")) > 0)
                switch (c_opt) {
                case 'v':
                        verbose = 1;
                        args++;
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        exit(1);
                }

        if (argc - args != 1) {
                fprintf(stderr, "%s [-v] <dirpath>\n", prog);
                exit(1);
        } else
                path = argv[args++];

        if (verbose)
                printf("%s %s\n", prog, path);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init(0, NULL, "yls", NULL);
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_init()'ed\n");

        ret = network_connect_mond(0);
        if (ret)
                GOTO(err_ret, ret);

        offset = 0;
        de0 = NULL;
        delen = 0;
        while (srv_running) {
                ret = ly_readdir(path, offset, &de0, &delen);
                if (ret) {
                        fprintf(stderr, "ly_readdir(%s, ...) %s\n", path,
                                strerror(ret));
                        exit(1);
                } else if (delen == 0) {
//                        printf("delen 0\n");
                        break;
                }

                ptr = de0;
                while (delen > 0) {
                        de = (struct dirent *)ptr;

                        if (de->d_reclen > delen) {
                                fprintf(stderr, "ERROR:reclen %u > delen %lu\n",
                                        de->d_reclen, (unsigned long)delen);
                                exit(1);
                        }

                        len = _strlen(de->d_name);
                        if ((len == 1 && de->d_name[0] == '.')
                            || (len == 2 && de->d_name[0] == '.'
                                && de->d_name[1] == '.'))
                                goto next;

                        snprintf(depath, MAX_PATH_LEN, "%s/%s", path,
                                 de->d_name);

                        _memset(&stbuf, 0x0, sizeof(struct stat));

                        /* stat() the file. Of course there's a race condition -
                         * the directory entry may have gone away while we
                         * read it, so ignore failure to stat
                         */
                        ret = ly_getattr(depath, &stbuf);
                        if (ret)
                                goto next;

                        mode_to_permstr((uint32_t)stbuf.st_mode, perms);
                        stat_to_datestr(&stbuf, date);

                        /* lrwxrwxrwx 1 500 500 12 Jun 30 04:31 sy -> wk/sy
                         * drwxrwxr-x 2 500 500 4096 Jun 28 10:59 good
                         * -rw-rw-r-- 1 500 500 677859 Feb 27  2006 lec01.pdf
                        */
                        printf("%s %lu yfs yfs %llu %s %s\n", perms,
                               (unsigned long)stbuf.st_nlink,
                               (unsigned long long)stbuf.st_size, date,
                               de->d_name);

next:
                        offset = de->d_off;
                        ptr += de->d_reclen;
                        delen -= de->d_reclen;
                }

                yfree((void **)&de0);
                delen = 0;
        }

        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ly_destroy() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_destroy()'ed\n");

        return 0;
}

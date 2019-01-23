

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "ylib.h"
#include "yfscli_conf.h"

int recursion_rmdir(char *path, int verbose, int del_all, int month);

void usage(char *prog)
{
        printf("Usage: %s [options...]\n", prog);
        printf("       -a        delete all\n"); 
        printf("       -m=month  delete this month\n");
        printf("       -v        verbose\n");
        printf("       -h        show this help\n\n");

        exit(0);
}

#define BEGIN_PATH "/"

int main(int argc, char *argv[])
{
        int ret, delete_all = 0, month = -1, verbose = 0, del_month = 0;
        char c_opt, *prog;

        (void) delete_all;
        (void) month;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        while ((c_opt = getopt(argc, argv, "am:vh")) > 0)
                switch (c_opt) {
                case 'a':
                        delete_all = 1;
                        break;
                case 'm':
                        del_month = 1;
                        month = atoi(optarg);
                case 'v':
                        verbose = 1;
                        break;
                case 'h':
                        usage(prog);
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n\n");
                        usage(prog);
                }

        if (delete_all == 1 && del_month == 1) {
                printf("You can't have your cake and eat it too. \n");
                exit(1);
        }

        if ((month < 1 || month > 12) && del_month == 1) {
                printf("Error: wrong month...\n");
                usage(prog);
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("ycompact");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_init()'ed\n");

       ret = recursion_rmdir(BEGIN_PATH, verbose, delete_all, month); 
        if (ret) {
                fprintf(stderr, "recursion rmdir(%s) %s\n", BEGIN_PATH, strerror(ret));
                exit(1);
        } 

        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ly_destroy() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_destroy()'ed\n");

        return 0;
}

int recursion_rmdir(char *path, int verbose, int del_all, int month)
{
        int ret;
        char depath[MAX_PATH_LEN], daynow[MAX_PATH_LEN];
        off_t offset;
        void *de0, *ptr;
        unsigned int delen, len;
        struct dirent *de;
        struct stat stbuf;
        time_t _t;
        struct tm t;

        offset  = 0;
        de0 = NULL;
        delen = 0;

        if (verbose)
                printf("Enter %s\n", path);

        while (srv_running) {
                ret = ly_readdir(path, offset, &de0, &delen);
                if (ret) {
                        fprintf(stderr, "ly_readdir(%s, ...) %s\n", path, strerror(ret));
                        return ret;
                } else if (delen == 0) {
                        break;
                }

                ptr = de0;
                while (delen > 0) {
                        de = (struct dirent *)ptr;

                        if (de->d_reclen > delen) {
                                fprintf(stderr, "Error: reclen %u > delen %lu\n",
                                                de->d_reclen,
                                                (unsigned long)delen);
                                return 1;
                        }

                        len = _strlen(de->d_name);
                        if ((len == 1 && de->d_name[0] == '.') 
                           || (len == 2 && de->d_name[0] == '.' 
                           && de->d_name[1] == '.'))
                                goto next;

                        snprintf(depath, MAX_PATH_LEN, "%s/%s", path, de->d_name);

                        _memset(&stbuf, 0x0, sizeof(struct stat));

                        ret = ly_getattr(depath, &stbuf);
                        if (ret) {
                                fprintf(stderr, "Error: get %s attr: %s\n",
                                                depath, strerror(ret));
                                goto next;
                        }

                        if (S_ISDIR((stbuf).st_mode)) {

                                _t = time(NULL);
                                gmtime_r(&_t, &t);

                                if (del_all)
                                        sprintf(daynow, 
                                                "%d-%d-%d", 
                                                1900 + t.tm_year,
                                                1 + t.tm_mon,
                                                t.tm_mday);
                                else
                                        sprintf(daynow,
                                                "%d-%d",
                                                1900+t.tm_year,
                                                month);

                                if (del_all || (strncmp(daynow, de->d_name, _strlen(daynow))== 0 && !del_all)) {
                                        ret = recursion_rmdir(depath, verbose, del_all, month);
                                        if (ret) {
                                                fprintf(stderr, "Error: recursion_rmdir %s error: %s", depath, strerror(ret));
                                                goto next;
                                        }

#if 0
                                        if (verbose) 
                                                printf("** delete dir: %s\n", depath);

                                        ret = ly_rmdir(depath);
                                        if (ret)
                                                fprintf(stderr, "ly_rmdir(%s) %s\n",
                                                                depath,
                                                                strerror(ret));
#endif

                                } else {
                                        printf("Ignore path %s\n", depath);
                                }

                        } else if (S_ISREG((stbuf).st_mode))  {
                                if (verbose)
                                        printf("** delete file: %s\n", depath);

#if 0
                                ret = ly_unlink(depath, 1);
                                if (ret) 
                                        fprintf(stderr, "ly_unlink(%s,...) %s\n", path, strerror(ret));
                        }
#endif

next:
                        offset = de->d_off;
                        ptr += de->d_reclen;
                        delen -= de->d_reclen;
                }

                yfree((void **) &de0);
                delen = 0;
        }
                
        return 0;
}



#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "sdfs_conf.h"

int touch_datadir(char *user)
{
        int ret;
        char path[MAX_PATH_LEN];
        struct stat stbuf;
        
        snprintf(path, MAX_PATH_LEN, "/%s/data", user);

        ret = ly_getattr(path, &stbuf);
        if (ret) {
                ret = ly_mkdir(path, 0755);
                if (ret) {
                        fprintf(stderr, "create %s failed.\n", path);
                } else
                        printf("%s created successfully.\n", path);
        } else
                printf("%s existed.\n", path);

        return ret;
}

int main(int argc, char *argv[])
{
        int ret, args, useradd, userdel, verbose = 0;
        char c_opt, *prog, *user, *passwd;
        char path[MAX_NAME_LEN];

        fileid_t fileid;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;

        if (argc < 4) {
                fprintf(stderr, "%s [-v] <-a|-d> USERNAME PASSWD\n", prog);
                exit(1);
        }

        useradd = 0;
        userdel = 0;
        memset(path, 0x00, MAX_NAME_LEN);

        while ((c_opt = getopt(argc, argv, "adv")) > 0)
                switch (c_opt) {
                case 'a':
                        useradd = 1;
                        args++;
                        break;
                case 'd':
                        userdel = 1;
                        args++;
                        break;
                case 'v':
                        verbose = 1;
                        args++;
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        exit(1);
                }

        if (useradd == userdel) {
                fprintf(stderr, "Err, useradd, or userdel ?\n");
                exit(1);
        }

        if (argc - args != 2) {
                fprintf(stderr, "%s [-v] <-a|-d> USERNAME PASSWD\n", prog);
                exit(1);
        }

        user = argv[args++];
        passwd = argv[args++];

        if (verbose)
                printf("%s %s %s %s\n", prog, useradd ? "USERADD" : "USERDEL",
                       user, passwd);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("yuser");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_init()'ed\n");

        if (useradd) {
                sprintf(path, "/%s", user);
                ret = sdfs_lookup_recurive(path, &fileid);
                if (ret) {
                        fprintf(stderr, "Err: add user(%s) passwd(%s) %s\n",
                                user, passwd, strerror(ret));
                        exit(1);
                }

                ret = raw_lvs_setattr(&fileid, RAW_LVS_SET_PWD,
                                      user, passwd);
#if 0
                ret = ly_useradd(user, passwd);
#endif
                if (ret) {
                        fprintf(stderr, "Err: add user(%s) passwd(%s) %s\n",
                                user, passwd, strerror(ret));
                        exit(1);
                } else if (verbose)
                        printf("user(%s) created\n", user);

                /* add data directory 
                ret = touch_datadir(user);
                if (ret)
                        exit(1);
                else if (verbose)
                        printf("data dir created\n");
                */

        } else {
                fprintf(stderr, "Oops: userdel(%s, %s) Not supported yet\n",
                        user, passwd);
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

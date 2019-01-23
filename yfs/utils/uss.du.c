

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>

#include "configure.h"
#include "adt.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "yfs_md.h"
#include "sdfs_buffer.h"

int main(int argc, char *argv[])
{
        int ret, args, verbose = 0, stop;
        char c_opt, *prog, *path, perms[11], date[64];
	    char name[MAX_NAME_LEN],depath[MAX_PATH_LEN];
        off_t offset;
        void *de0;
        int delen;//, len;
        struct dirent *de;
        struct stat stbuf;
	    fileid_t fileid, parent;
        fileinfo_t *md;

        dbg_info(0);

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;

        if (argc < 2) {
                fprintf(stderr, "%s [-v] <path_from_yfs_root>\n", prog);
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
                fprintf(stderr, "%s [-v] <path from yfs root>\n", prog);
                exit(1);
        } else
                path = argv[args++];

        if (verbose)
                printf("%s %s\n", prog, path);

/*
        if (path[0] != ':') {
                DERROR("try add ':' before path\n");

                UNIMPLEMENTED(__DUMP__);
        }
*/
        memmove(path, path, strlen(path));

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("yls");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_init()'ed\n");

        offset = 0;
        de0 = NULL;
        delen = 0;

//XXX
        if (path[0] == '/') {
                ret = sdfs_lookup_recurive(path, &fileid);
                if (ret){
			fprintf(stderr,"uss.ls: %s: No such file or directory\n",path);
                        exit (ret);
		}
        } else {

                if (ret != 2) {
                        ret = EINVAL;
                        exit (ret);
                }
        }

        ret = sdfs_getattr(&fileid, &stbuf);
        if (ret)
                exit (ret);

        if (S_ISREG((stbuf).st_mode)) {
		ret = sdfs_splitpath(path, &parent, name);
		if (ret){
			exit(ret);
			fprintf(stderr,"sdfs_splitpath() faild ret %d\n", ret);
		}
                fprintf(stdout,"%s\n", name);
		exit (0);

        }

        stop = 0;
        while (srv_running) {
                ret = ly_readdirplus(path, offset, &de0, &delen, 0);
                if (ret) {
                        fprintf(stderr, "ly_readdir(%s, ...) %s\n", path,
                                strerror(ret));
                        exit(1);
                } else if (delen == 0) {
//                        printf("delen 0\n");
                        break;
                }

                if (delen > 0) {
                        dir_for_each(de0, delen, de, offset) {
                                DBUG("name %s d_off %llu\n", de->d_name, (LLU)de->d_off);

                                if (de->d_off == 0)
                                        stop = 1;

                                YASSERT(de->d_reclen <= delen);
                                if (strcmp(de->d_name, ".") == 0
                                    || strcmp(de->d_name, "..") == 0) {
                                        //offset = de->d_off;
                                        continue;
                                }

                                if (strcmp(path, "/") == 0)
                                {
                                        sprintf(depath, "/%s", de->d_name);
                                }
                                else
                                        snprintf(depath, MAX_PATH_LEN, "%s/%s", path,
                                                 de->d_name);

                                md = (void *)de + de->d_reclen - sizeof(md_proto_t);
                                MD2STAT(md, &stbuf);

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

                                //offset = de->d_off;
                        }

                        if (stop)
                                break;
                } else
                        break;

                yfree((void **)&de0);
        }
        return 0;
}

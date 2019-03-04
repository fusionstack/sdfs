#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <getopt.h>
#include <pwd.h>
#include <grp.h>
#include <unistd.h>
#include <sys/types.h>

#include "configure.h"
#include "adt.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "yfs_md.h"
#include "redis_util.h"
#include "sdfs_buffer.h"

#define RANGE_MAX 2

static int __ls_line_plus(const char *path, struct dirent *de, int verbose, int row)
{
        int ret;
        fileinfo_t *md;
        fileid_t fileid;
        char depath[MAX_PATH_LEN] = "";
        char perms[11], mtime[64], atime[64], ctime[64];
        struct stat stbuf;
        struct passwd *user_info = NULL;
        struct group *group_info = NULL;
        char user_name[MAX_NAME_LEN] = "";
        char group_name[MAX_NAME_LEN] = "";

        if (strcmp(de->d_name, ".") == 0
            || strcmp(de->d_name, "..") == 0) {
                //offset = de->d_off;
                return 0;
        }

        if (strcmp(path, "/") == 0)  {
                sprintf(depath, "/%s", de->d_name);
        } else
                snprintf(depath, MAX_PATH_LEN, "%s/%s", path,
                         de->d_name);

        md = (void *)de + de->d_reclen - sizeof(md_proto_t);
        MD2STAT(md, &stbuf);

        mode_to_permstr((uint32_t)stbuf.st_mode, perms);
        stat_to_datestr(&stbuf, mtime);
        user_info = getpwuid(stbuf.st_uid);
        if (NULL != user_info)
                strncpy(user_name, user_info->pw_name, sizeof(user_name));
        else
                snprintf(user_name, sizeof(user_name), "%d", stbuf.st_uid);

        group_info = getgrgid(stbuf.st_gid);
        if (NULL != group_info)
                strncpy(group_name, group_info->gr_name, sizeof(group_name));

        snprintf(group_name, sizeof(group_name), "%d", stbuf.st_gid);
        
        if (verbose) {
                ret = sdfs_lookup_recurive(depath, &fileid);
                if (ret){
                        DWARN("uss.ls: %s: No such file or directory\n",path);
                }

                stat_to_datestr(&stbuf, atime);
                stat_to_datestr(&stbuf, ctime);

                if (row) {
                        printf("permission:%s\nlink:%lu\nuser name:%s\ngroup name:%s\nsize:%llu\naccess time:%s\nmodified time:%s\ncreate time:%s\nfileid:"FID_FORMAT"\nname:%s\n\n",
                               perms,
                               (unsigned long)stbuf.st_nlink,
                               user_name,
                               group_name,
                               (unsigned long long)stbuf.st_size,
                               atime,
                               mtime,
                               ctime,
                               FID_ARG(&fileid),
                               de->d_name);
                } else {
                        printf("%s %lu %s %s %llu %s %s %s "FID_FORMAT" %s\n",
                               perms,
                               (unsigned long)stbuf.st_nlink,
                               user_name,
                               group_name,
                               (unsigned long long)stbuf.st_size,
                               atime,
                               mtime,
                               ctime,
                               FID_ARG(&fileid),
                               de->d_name);
                }
        } else { 
                if (row) {
                        printf("permission:%s\nlink:%lu\nuser name:%s\ngroup name:%s\nsize:%llu\nmodified time:%s\nname:%s\n\n",
                               perms,
                               (unsigned long)stbuf.st_nlink,
                               user_name,
                               group_name,
                               (unsigned long long)stbuf.st_size,
                               mtime,
                               de->d_name);
                } else {
                        printf("%s %lu %s %s %llu %s %s\n",
                               perms,
                               (unsigned long)stbuf.st_nlink,
                               user_name,
                               group_name,
                               (unsigned long long)stbuf.st_size,
                               mtime,
                               de->d_name);
                }
        }
       

        return 0;
}

static int __ls_line(const char *path, const fileid_t *fileid, struct dirent *de, int verbose, int row)
{
        int ret;
        char depath[MAX_PATH_LEN] = "";
        char perms[11], mtime[64], atime[64], ctime[64];
        struct stat stbuf;
        struct passwd *user_info = NULL;
        struct group *group_info = NULL;
        char user_name[MAX_NAME_LEN] = "";
        char group_name[MAX_NAME_LEN] = "";

        if (strcmp(de->d_name, ".") == 0
            || strcmp(de->d_name, "..") == 0) {
                //offset = de->d_off;
                return 0;
        }

        if (strcmp(path, "/") == 0)  {
                sprintf(depath, "/%s", de->d_name);
        } else
                snprintf(depath, MAX_PATH_LEN, "%s/%s", path,
                         de->d_name);

        ret = sdfs_getattr(NULL, fileid, &stbuf);
        if (ret) {
                if (ret == ENOENT) {
                        memset(&stbuf, 0x0, sizeof(stbuf));
                } else
                        GOTO(err_ret, ret);
        }

        mode_to_permstr((uint32_t)stbuf.st_mode, perms);
        stat_to_datestr(&stbuf, mtime);
        user_info = getpwuid(stbuf.st_uid);
        if (NULL != user_info)
                strncpy(user_name, user_info->pw_name, sizeof(user_name));
        else
                snprintf(user_name, sizeof(user_name), "%d", stbuf.st_uid);

        group_info = getgrgid(stbuf.st_gid);
        if (NULL != group_info)
                strncpy(group_name, group_info->gr_name, sizeof(group_name));

        snprintf(group_name, sizeof(group_name), "%d", stbuf.st_gid);
        
        if (verbose) {
                stat_to_datestr(&stbuf, atime);
                stat_to_datestr(&stbuf, ctime);

                if (row) {
                        printf("permission:%s\nlink:%lu\nuser name:%s\ngroup"
                               " name:%s\nsize:%llu\naccess time:%s\nmodified"
                               " time:%s\ncreate time:%s\nfileid:"FID_FORMAT"\nname:%s\n\n",
                               perms,
                               (unsigned long)stbuf.st_nlink,
                               user_name,
                               group_name,
                               (unsigned long long)stbuf.st_size,
                               atime,
                               mtime,
                               ctime,
                               FID_ARG(fileid),
                               de->d_name);
                } else {
                        printf("%s %lu %s %s %llu %s %s %s "FID_FORMAT" %s\n",
                               perms,
                               (unsigned long)stbuf.st_nlink,
                               user_name,
                               group_name,
                               (unsigned long long)stbuf.st_size,
                               atime,
                               mtime,
                               ctime,
                               FID_ARG(fileid),
                               de->d_name);
                }
        } else { 
                if (row) {
                        printf("permission:%s\nlink:%lu\nuser name:%s\ngroup"
                               " name:%s\nsize:%llu\nmodified time:%s\nname:%s\n\n",
                               perms,
                               (unsigned long)stbuf.st_nlink,
                               user_name,
                               group_name,
                               (unsigned long long)stbuf.st_size,
                               mtime,
                               de->d_name);
                } else {
                        printf("%s %lu %s %s %llu %s %s\n",
                               perms,
                               (unsigned long)stbuf.st_nlink,
                               user_name,
                               group_name,
                               (unsigned long long)stbuf.st_size,
                               mtime,
                               de->d_name);
                }
        }
       

        return 0;
err_ret:
        return ret;
}

static int __number_check(const char *name, off_t *offset, size_t *size)
{
        int ret, count = 0;
        char *list[2], tmp[MAX_NAME_LEN];

        strcpy(tmp, name);

        count = 2;
        _str_split(tmp, ',', list, &count);
        if (count != 2) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        *offset = atol(list[0]);
        *size = atol(list[1]);

        return 0;
err_ret:
        return ret;
}

static void __init_filter(filter_t *filter, uint64_t from_time, uint64_t to_time,
                                 off_t off, size_t size)
{
        memset(filter->pattern, 0, sizeof(filter->pattern));

        filter->from_time = from_time;
        filter->to_time = to_time;
        filter->offset = off;
        filter->count = size;
}

static int __ls_filter(const char *path, const filter_t *filter, int verbose, int statis, int row)
{
        int ret, stop;
        off_t offset = 0;
        void *de0 = NULL;
        int delen = 0;
        struct dirent *de;
        fileid_t fileid;
        uint64_t count;

        (void) statis;
        
        //XXX
        if (path[0] != '/') {
                ret = EINVAL;
                exit (ret);
        }

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret){
                fprintf(stderr,"raw_lookup1 uss.ls: %s: No such file or directory\n",path);
                exit (ret);
        }

        DBUG("path %s "CHKID_FORMAT"\n",  path, CHKID_ARG(&fileid));
        
#if 0
        struct stat stbuf;
        ret = sdfs_getattr(NULL, &fileid, &stbuf);
        if (ret)
                exit (ret);

        fileid_t parent;
        char name[MAX_NAME_LEN];
        if (S_ISREG((stbuf).st_mode)) {
                ret = sdfs_splitpath(path, &parent, name);
                if (ret){
                        exit(ret);
                        fprintf(stderr,"sdfs_splitpath() faild ret %d\n", ret);
                }
                fprintf(stdout,"%s\n", name);
                exit(0);
        }

        if (statis) {
                ret = __ls_count(path, &file_statis, filter);
                if (ret)
                        GOTO(err_ret, ret);
                else
                        return 0;
        }
#endif
        
        stop = 0;
        count = 0;
        while (srv_running) {
                DBUG("read %s offset %ju\n", path, offset);

                if (filter) {
                        ret = sdfs_readdirplus_with_filter(NULL, &fileid, offset, &de0, &delen, filter);
                } else {
                        ret = sdfs_readdirplus(NULL, &fileid, offset, &de0, &delen);
                }
                if (ret) {
                        fprintf(stderr, "ly_readdir(%s, ...) %s\n", path,
                                        strerror(ret));
                        exit(1);
                } else if (delen == 0) {
                        stop = 1;
                        break;
                }

                if (delen > 0) {
                        dir_for_each(de0, delen, de, offset) {
                                __ls_line_plus(path, de, verbose, row);
                                offset = de->d_off;
                                count ++;

                                DBUG("%s offset %ju %p\n", de->d_name, offset, de);

                                if (filter) {
                                        if (count >= filter->count) {
                                                DBUG("break\n");
                                                stop = 1;
                                                break;
                                        }
                                }
                        }

                        if (offset == 0) {
                                DBUG("break\n");
                                stop = 1;
                        }

                        if (stop)
                                break;
                } else
                        break;

                yfree((void **)&de0);
        }

        if (filter) {
                printf("#cursor:%ju,total %ju\n", offset, count);
        }
        
        return 0;
}

inline static int __ls(const char *path, int verbose, int statis, int row)
{
        int ret;
        off_t offset = 0;
        struct dirent *de;
        dirid_t dirid, fileid;
        dirhandler_t *dirhandler;
        
        (void) statis;
        
        //XXX
        if (path[0] != '/') {
                ret = EINVAL;
                exit (ret);
        }

        ret = sdfs_lookup_recurive(path, &dirid);
        if (ret){
                fprintf(stderr,"raw_lookup1 uss.ls: %s: No such file or directory\n",path);
                GOTO(err_ret, ret);
        }

        ret = sdfs_opendir(NULL, &dirid, &dirhandler);
        if (ret)
                GOTO(err_ret, ret);

        DBUG("path %s "CHKID_FORMAT"\n",  path, CHKID_ARG(&dirid));
        
        while (srv_running) {
                DBUG("read %s offset %ju\n", path, offset);

                ret = sdfs_readdir(NULL, dirhandler, &de, &fileid);
                if (ret) {
                        fprintf(stderr, "ly_readdir(%s, ...) %s\n", path,
                                strerror(ret));
                        GOTO(err_close, ret);
                }

                if (de == NULL) {
                        break;
                }

                __ls_line(path, &fileid, de, verbose, row);
        }

        sdfs_closedir(NULL, dirhandler);

        return 0;
err_close:
        sdfs_closedir(NULL, dirhandler);
err_ret:
        return ret;
}

void usage()
{
        fprintf(stderr, "\nusage:\n"
                "uss.ls <path> [--from_time] [--to_time] [--num] [-v][-a][--row|-r]\n"
                "    <path>             Could be a directory, a file, or a fuzzy matching filter\n"
                "    --from_time, -f    The beginning of time to query, format likes '2017-04-30 12:00:00'\n"
                "    --to_time, -t      The end of time to query, format likes '2017-05-01 12:00:00'\n"
                "    --seek_num, -n     The number of files to travel, format likes '50 forward lookup' '-50 backward lookup' '30,50 lookup from 30 to 50'\n"
                "    --total, -a        The total number of files or dirs or all.\n"
               );
}

int main(int argc, char *argv[])
{
        int ret, verbose = 0, row = 0, statis = 0;
        char c_opt, *prog, *path, *f_time = NULL, *t_time = NULL;
        uint32_t from_time = 0, to_time = 0;
        filter_t *filter = NULL, _filter;
        off_t off = 0;
        size_t size = 0;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        if (argc < 2) {
                usage();
                exit(1);
        }

        while (srv_running) {
                int option_index = 0;

                static struct option long_options[] = {
                        { "from_time", required_argument, 0, 'f' },
                        { "to_time", required_argument, 0, 't' },
                        { "seek_num", required_argument, 0, 'n' },
                        { "total", 0, 0, 'a' },
                        { "row", 0, 0, 'r' },
                        { "verbose", 0, 0, 'v' },
                        { 0, 0, 0, 0 },
                };

                c_opt = getopt_long(argc, argv, "varf:t:n:", long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 'v':
                        verbose = 1;
                        break;
                case 'a':
                        statis = 1;
                        break;
                case 'r':
                        row = 1;
                        break;
                case 'f':
                        ret = date_check(optarg);
                        if (ret) {
                                fprintf(stderr, "from_time %s invalid, please check it.\n", optarg);
                                GOTO(err_ret, ret);
                        }

                        f_time = optarg;
                        filter = &_filter;
                        break;
                case 't':
                        ret = date_check(optarg);
                        if (ret) {
                                fprintf(stderr, "to_time %s invalid, please check it.\n", optarg);
                                GOTO(err_ret, ret);
                        }

                        t_time = optarg;
                        filter = &_filter;
                        break;
                case 'n':
                        ret = __number_check(optarg, &off, &size);
                        if (ret) {
                                fprintf(stderr, "num invalid, please check it.\n");
                                GOTO(err_ret, ret);
                        }

                        filter = &_filter;
                        break;
                default:
                        usage(prog);
                        exit(1);
                }
        }

        if (optind >= argc) {
                usage(prog);
                exit(1);
        }

        path = argv[optind];

#if 0
        if (verbose)
                printf("%s %s\n", prog, path);
#endif

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        dbg_info(0);

        ret = ly_init_simple("uss.ls");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                GOTO(err_ret, ret);
        }

        if (f_time != NULL) {
                str2time(f_time, &from_time);
                if (to_time > 0 && from_time > to_time) {
                        ret = EINVAL;
                        fprintf(stderr, "from_time must be less than to_time, please check it.\n");
                        GOTO(err_ret, ret);
                }
        }

        if (t_time != NULL) {
                str2time(t_time, &to_time);
                if (from_time > 0 && from_time > to_time) {
                        ret = EINVAL;
                        fprintf(stderr, "from_time must be less than to_time, please check it.\n");
                        GOTO(err_ret, ret);
                }
        }


        if (filter) {
                __init_filter(filter, from_time, to_time, off, size);
                ret = __ls_filter(path, filter, verbose, statis, row);
                if (ret) {
                        exit(ret);
                }
        } else {
                //ret = __ls_filter(path, NULL, verbose, statis, row);
                ret = __ls(path, verbose, statis, row);
                if (ret) {
                        exit(ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

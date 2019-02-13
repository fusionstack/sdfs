#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <sys/types.h>

#include "configure.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "sdfs_ec.h"
#include "sdfs_worm.h"

static void usage(char *prog)
{
        prog = "mkdir";
        
        fprintf(stderr, "\nusage:\n");
        fprintf(stderr, "%s [-vehg]\n", prog);
        fprintf(stderr, "%s [--help|-h]\n", prog);
        fprintf(stderr, "%s [--verbose|-v]\n", prog);
        fprintf(stderr, "%s [--erasure|-e k+r] <dirpath>\n", prog);
        fprintf(stderr, "%s [--engine|-g] <localfs|leveldb> <dirpath>\n", prog);
}

static int __sdfs_mkdir_recurive(const char *path, const ec_t *ec, mode_t mode, fileid_t *_fileid)
{
        int ret;
        fileid_t parent, fileid;
        char dirname[MAX_NAME_LEN], basename[MAX_NAME_LEN];

        _path_split2(path, dirname, basename);
        
        if (strcmp(dirname, ROOT_NAME) == 0) {
                DINFO("dirname (%s), basename (%s)\n", dirname, basename);

                ret = sdfs_mkvol(basename, ec, mode, &fileid);
                if (ret) {
                        if (ret == EEXIST) {
                                //pass
                        } else 
                                GOTO(err_ret, ret);
                }
        } else {
                DINFO("dirname (%s) basename (%s)\n", dirname, basename);

        retry:
                ret = sdfs_lookup_recurive(dirname, &parent);
                if (ret) {
                        if (ret == ENOENT) {
                                ret = __sdfs_mkdir_recurive(dirname, ec, mode, &parent);
                                if (ret) {
                                        if (ret == EEXIST) {
                                                goto retry;
                                        } else
                                                GOTO(err_ret, ret);
                                }
                        } else
                                GOTO(err_ret, ret);
                }

                ret = sdfs_mkdir(NULL, &parent, basename, ec, &fileid, mode, geteuid(), getgid());
                if (ret)
                        GOTO(err_ret, ret);
        }
        
        DBUG("dir (%s) created\n", path);

        if (_fileid)
                *_fileid = fileid;
        
        return 0;
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret, args, verbose = 0, c_opt;
        uint32_t k = 0, r = 0;
        char *prog, *path_tmp = NULL, path[MAX_PATH_LEN], buf[MAX_PATH_LEN];
        ec_t ec;
        //uid_t uid = 0;
        //gid_t gid = 0;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;

        if (argc < 2) {
                usage(prog);
                exit(1);
        }

        while (srv_running) {
                int option_index = 0;

                static struct option long_options[] = {
                        { "engine", required_argument, 0, 'g' },
                        { "erasure", required_argument, 0, 'e' },
                        { "verbose", no_argument, 0, 'v' },
                        { "help", no_argument, 0, 'h' },
                        { 0, 0, 0, 0 },
                };

                c_opt = getopt_long(argc, argv, "hve:g:", long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                        case 'v':
                                verbose = 1;
                                args++;
                                break;
                        case 'e':
                                ret = sscanf(optarg, "%u+%u", &k, &r);
                                args += 2;
                                if (k < 1 || r < 1 || k + r > EC_MMAX) {
                                        usage(prog);
                                        exit(1);
                                }
                                break;
                        case 'g':
                                ret = sscanf(optarg, "%s", buf);
                                args += 2;
                                if (strcmp(optarg, "localfs") == 0) {
                                } else if (strcmp(optarg, "leveldb") == 0) {
                                } else {
                                        usage(prog);
                                        exit(1);
                                }
                                break;
                        case 'h':
                                usage(prog);
                                exit(0);
                        default:
                                fprintf(stderr, "Hoops, wrong op got!\n");
                                exit(1);
                }
        }

        if (argc - args != 1) {
                usage(prog);
                exit(1);
        } else {
                path_tmp = argv[args++];
                ret = path_normalize(path_tmp, path, sizeof(path));
                if (ret) {
                        fprintf(stderr, "path_normalize fail : %s.\n", strerror(ret));
                        exit(ret);
                }
        }
        if (verbose)
                printf("%s %s\n", prog, path);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        dbg_info(0);
        
        ret = ly_init_simple("ymkdir");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_init()'ed\n");

        ec.m = k + r;
        ec.k = k;
        ec.plugin = (!k || !r) ? PLUGIN_NULL:PLUGIN_EC_ISA;

        ret = __sdfs_mkdir_recurive(path, &ec, 0755, NULL);
        if (ret) {
                fprintf(stderr, "ly_mkdir(%s,...) %s\n", path, strerror(ret));
                exit(ret);
        }

        /*        uid = getuid();
                  gid = getgid();
                  ret = ly_chown(path, uid, gid);
                  if (ret){
                  fprintf(stderr, "ly_chown(%s,...) %s\n", path, strerror(ret));
                  exit(ret);
                  */


        UNIMPLEMENTED(__NULL__);
#if 0
        if (is_lvm(path)) {
                if (engine == ENGINE_NULL)
                        engine = ENGINE_LOCALFS;

                ret = lvm_set_engine(path, engine);
                if (ret) {
                        fprintf(stderr, "set lvm engine of %s failed, error: %s\n",
                                        path, strerror(ret));
                        exit(ret);
                }
        } else {
                if (engine != ENGINE_NULL)
                        DWARN("only lvm can set engine. [%s]\n", path);
        }
#endif

        return 0;
}

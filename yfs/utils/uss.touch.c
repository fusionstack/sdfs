#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <dirent.h>
//#include <atomic.h>
#include <errno.h>
#include <sys/types.h>

#include "configure.h"
#include "ylib.h"
#include "sdfs_lib.h"

#define THREAD_MAX 200
#define BUFF_LEN 1024
static int g_file_size = 1024;
static int g_thread_file_num = 1000;
char *g_path = NULL;

static void usage(const char *prog)
{
        fprintf(stderr, "usage: %s [-d] [-t] [-v] [-s] [-n] [-h]PATH\n"
                "\t-d --delete          delete file flag.\n"
                "\t-t --thread          thread number.\n"
                "\t-v --verbose         Show verbose message.\n"
                "\t-s --file_size       the size of create file(KB).\n"
                "\t-n --file_num        the file number for each thread create.\n"
                "\t-h --help            Show this help.\n",
                prog);
}

typedef void* (*pfile_op) (void *);
typedef enum {
        WRITE_FILE,
        READ_FILE,
        DELETE_FILE,
        INVALID_FILE_OPER,
} file_oper_type_t;

typedef struct thread_arg {
        uint64_t timeUsed;
        int threadNum;
}thread_arg_t;

static void *write_file(void *arg)
{
        int ret = 0;
        int len = 0;
        fileid_t fid;
        fileid_t parent;
        char name[MAX_NAME_LEN];
        name[0] = '\0';
        int i;
        char *buf = NULL;
        buffer_t stbuf;
        struct timeval startTime;
        struct timeval endTime;
        uint64_t timeUsed = 0;
        thread_arg_t *thread_arg = (thread_arg_t *)arg;

        if (g_file_size > 0) {
                ret = ymalloc((void **)&buf, g_file_size);
                if (ret) {
                        printf("yamlloc failed.ret = %d\n", ret);
                        exit(ret);
                } else
                        memset(buf, 'a', g_file_size);
        }

#if 0
        char *nametmp = NULL;
        nametmp = strrchr(g_path,'/');
        nametmp++;
#else

        char nametmp[MAX_NAME_LEN];
        nametmp[0] = '\0';
        ret = sdfs_splitpath(g_path, &parent, nametmp);
#endif
        _gettimeofday(&startTime, NULL);
        if (0 == ret) {
                for (i = 0; i < g_thread_file_num; i++) {
                        snprintf(name, MAX_NAME_LEN, "%s_%d_%d", nametmp, thread_arg->threadNum, i);
                        ret = sdfs_create(&parent, name, &fid, 0777, 0, 0);
                        if (ret) {
                                printf("creat file %s failed, errno %d\n", name, ret);
                                continue;
                        }

                        if ( g_file_size > 0){
                                 mbuffer_init(&stbuf, 0);
                                 mbuffer_copy(&stbuf, buf, g_file_size);
                                 len = sdfs_write_sync(&fid, &stbuf, g_file_size, 0);
                                 if (len != g_file_size) {
                                        printf("write failed len = %d\n", len);
                                 }
                        }
                }
        }
        _gettimeofday(&endTime, NULL);
        timeUsed = _time_used(&startTime, &endTime);
        thread_arg->timeUsed = timeUsed;

        return 0;

}


static void *read_file(void *arg)
{
        int ret = 0;
        fileid_t fid;
        fileid_t parent;
        char nametmp[MAX_NAME_LEN];
        char name[MAX_NAME_LEN];
        nametmp[0] = '\0';
        name[0] = '\0';
        int i;
        struct timeval startTime;
        struct timeval endTime;
        uint64_t timeUsed = 0;
        buffer_t stbuf;
        thread_arg_t *thread_arg = (thread_arg_t *)arg;
        (void)arg;

        _gettimeofday(&startTime, NULL);
        ret = sdfs_splitpath(g_path, &parent, nametmp);
        if (0 == ret) {
                for (i = 0; i < g_thread_file_num; i++) {
                        snprintf(name, MAX_NAME_LEN, "%s_%d_%d", nametmp, thread_arg->threadNum, i);
                        ret = sdfs_lookup(&parent, name, &fid);
                        if (ret) {
                                printf("lookup %s failed, ret = %d.\n", name, ret);
                                continue;
                        }

                        if (g_file_size > 0) {
                                mbuffer_init(&stbuf, g_file_size);
                                ret = sdfs_read_sync(&fid, &stbuf, g_file_size, 0);
                                if (ret != g_file_size)
                                        printf("read failed ret = %d\n", ret);
                        }
                }

        }
        _gettimeofday(&endTime, NULL);
        timeUsed = _time_used(&startTime, &endTime);
        thread_arg->timeUsed = timeUsed;

        return 0;

}



static void *delete_file(void *arg)
{
        int ret = 0;
        fileid_t parent;
        char nametmp[MAX_NAME_LEN];
        char name[MAX_NAME_LEN];
        nametmp[0] = '\0';
        name[0] = '\0';
        int i;
        struct timeval startTime;
        struct timeval endTime;
        uint64_t timeUsed = 0;
        thread_arg_t *thread_arg = (thread_arg_t *)arg;
        (void)arg;

        _gettimeofday(&startTime, NULL);
        ret = sdfs_splitpath(g_path, &parent, nametmp);
        if (0 == ret) {
                for (i = 0; i < g_thread_file_num; i++) {
                        snprintf(name, MAX_NAME_LEN, "%s_%d_%d", nametmp, thread_arg->threadNum, i);
                        ret = sdfs_unlink(&parent, name);
                        if (ret && ENOENT != ret)
                                printf("delete file %s failed,errno %d\n", name, ret);
                }

        }
        _gettimeofday(&endTime, NULL);
        timeUsed = _time_used(&startTime, &endTime);
        thread_arg->timeUsed = timeUsed;

        return 0;

}



int main(int argc, char *argv[])
{
        int i = 0;
        int ret, verbose;
        char c_opt, *prog;
        fileid_t parent, fid;
        char name[MAX_NAME_LEN];
        char file_oper[BUFF_LEN];
        int iThreadNum = 0;
        pthread_t pthreads[THREAD_MAX];
        thread_arg_t thread_args[THREAD_MAX];
        uint64_t timeUsed = 0;
        uint64_t latency = 0;
        pfile_op pfile_operate = write_file;
        uid_t uid = 0;
        gid_t gid = 0;
        uint64_t totalFileNum = 0;
        uint64_t filePerSec = 0;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        verbose = 0;

        strncpy(file_oper, "create", BUFF_LEN);

        while (srv_running) {
                int option_index = 0;

                static struct option long_options[] = {
                        {"verbose", 0, NULL, 'v' },
                        {"help",    0, NULL, 'h' },
                        {"thread", required_argument, NULL, 't'},
                        {"file_size", required_argument, NULL, 's'},
                        {"file_num", required_argument, NULL, 'n'},
                        {"delete", 0, NULL, 'd'},
                        {"read", 0, NULL, 'r'},
                        { 0, 0, 0, 0 },
                };

                c_opt = getopt_long(argc, argv, "vht:s:n:dr", long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 't':
                        iThreadNum = atoi(optarg);
                        break;
                case 'v':
                        verbose = 1;
                        break;
                case 's':
                        g_file_size = atoi(optarg) * 1024;
                        break;
                case 'n':
                        g_thread_file_num = atoi(optarg);
                        break;
                case 'd':
                        pfile_operate = delete_file;
                        strncpy(file_oper, "delete", BUFF_LEN);
                        break;
                case 'r':
                        pfile_operate = read_file;
                        strncpy(file_oper, "read", BUFF_LEN);
                        break;
                case 'h':
                        usage(prog);
                        exit(0);
                default:
                        usage(prog);
                        exit(1);
                }
        }

        if (optind >= argc) {
                usage(prog);
                exit(1);
        }

        g_path = argv[optind];

        if (verbose)
                printf("%s\n", g_path);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        dbg_info(0);
        
        ret = ly_init_simple(prog);
        if (ret)
                GOTO(err_ret, ret);

        if (iThreadNum > THREAD_MAX) {
                iThreadNum = THREAD_MAX;
        }

        if (iThreadNum > 0){
                for (i = 0; i < iThreadNum; i ++) {
                        thread_args[i].threadNum = i;
                        pthread_create(&pthreads[i], NULL, pfile_operate, &thread_args[i]);
                }
                for (i = 0; i < iThreadNum; i ++) {
                        pthread_join(pthreads[i], NULL);
                }
                for (i = 0; i < iThreadNum; i ++) {
                        timeUsed += thread_args[i].timeUsed;
                }
                totalFileNum = g_thread_file_num * iThreadNum;
                latency = timeUsed / totalFileNum;
                timeUsed = timeUsed / iThreadNum ;
                printf("thread_nums = %d, %s %llu files, time used %llu usec.\n",
                       iThreadNum, file_oper, (LLU)totalFileNum, (LLU)timeUsed);
                filePerSec = totalFileNum *1000 * 1000 / timeUsed;
                printf("latency is %llu usec, %s %llu files per second.\n",
                       (LLU)latency, file_oper, (LLU)filePerSec);
        }
        else if (iThreadNum ==0 ) {
                ret = sdfs_splitpath(g_path, &parent, name);
                if (ret)
                        GOTO(err_ret, ret);

                if(_strlen(name) >= MAX_NAME_LEN) {
                        ret = ENAMETOOLONG;
                        GOTO(err_ret, ret);
                }
		/*add by wangyingjie for uid and gid*/
		uid = getuid();
		gid = getgid();

                ret = sdfs_create(&parent, name, &fid, 0777, uid, gid);
                if (ret)
                        GOTO(err_ret, ret);

        }
        //analysis_dump();


        return 0;
err_ret:
        return ret;
}

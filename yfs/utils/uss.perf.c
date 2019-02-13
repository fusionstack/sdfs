#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <uuid/uuid.h>

#include "configure.h"
#include "yatomic.h"
#include "dbg.h"
#include "sdfs_lib.h"

#define INVALID_CMD (-1)
#define CREATE 1
#define LOOKUP 2
#define REMOVE 3
#define WRITE 4
#define READ 5
#define THREAD_MAX 100
#define THREAD_FOR_DEL 20
#undef BIG_BUF_LEN
#define BIG_BUF_LEN (1024*1024)

extern int normalize_path(const char *path, char *path2);
extern uint32_t crc32_sum(const void *ptr, uint32_t len);

struct perf_args {
        int cmd;
        int should_remove;
        int should_random;
        int file_num;
        size_t f_size;
        size_t f_bsize;
        int thread_num;
        char path[MAX_PATH_LEN];
};

struct thread_args {
        struct perf_args *perf;
        int thread_no;
        int do_file;
        int file_per_thread;
};

static void usage() {
        fprintf(stderr,
                        "uss.perf {create|lookup|...} \n"
                        "--create [--file nums] [--thread nums] [--remove] --dir /path 创建文件\n"
                        "--lookup [--file nums] [--thread nums] [--remove] [--random] --dir /path 查找文件\n"
                        "--write  [--size GB] [--block KB] [--thread nums] [--remove] --dir /path 写文件\n"
                        "-h|-?|--help                                               print usage\n"
               );
}

//打印测试报告
int print_result(int cmd_type, double interval, double file_per_sec)
{
        fprintf(stdout, "------------------------------\n");
        if(cmd_type == CREATE) {
                fprintf(stdout, "Create Test Report:\n");
        } else if(cmd_type == LOOKUP) {
                fprintf(stdout, "Lookup Test Report:\n");
        } else if(cmd_type == REMOVE) {
                fprintf(stdout, "Remove Test Report:\n");
        } else if(cmd_type == WRITE) {
                fprintf(stdout, "Write Test Report:\n");
                fprintf(stdout, "used_time:%f (s)\n", interval);
                fprintf(stdout, "%f MB/s\n\n", file_per_sec / (1024*1024*1.0));
                return 0;
        }
        fprintf(stdout, "used_time:%f (s)\n", interval);
        fprintf(stdout, "files/s:%d\n\n", (int)file_per_sec);

        return 0;
}

int get_random_by_uuid(long range)
{
        uuid_t u_no;
        char out_str[40] = {0};

        uuid_generate(u_no);
        uuid_unparse(u_no, out_str);

        return crc32_sum(out_str, sizeof(out_str)) % range;
}

void get_filename_by_uuid(char *out_str)
{
        uuid_t u_no;

        uuid_generate(u_no);
        uuid_unparse(u_no, out_str);
}

int create_file(struct perf_args *perf, int file_per_thread, int do_file, int thread_no)
{
        int i, ret, file_num, start_no;
        char filename[MAX_NAME_LEN], tmp_filename[MAX_NAME_LEN];
        fileid_t parent, fileid;

        start_no = thread_no * file_per_thread;
        file_num = thread_no * file_per_thread + do_file;

        printf("thread[%4d] : create from %4d to %4d\n",
                        thread_no, start_no, file_num);

        ret = sdfs_splitpath(perf->path, &parent, tmp_filename);
        if(ret == 0) {
                for(i=start_no; i<file_num; ++i) {
                        snprintf(filename, MAX_NAME_LEN, "%s_%d", tmp_filename, i);
                        ret = sdfs_create(NULL, &parent, filename, &fileid, 0644, 0, 0);
                        if(ret) {
                                fprintf(stderr, "create file %s, failed:%d\n", filename, ret);
                        }
                }
        }

        return 0;
}

int lookup_file(struct perf_args *perf, int file_per_thread, int do_file, int thread_no)
{
        int i, ret, file_num, start_no, should_random, total_num;
        char path[MAX_PATH_LEN];
        fileid_t fileid;

        should_random = perf->should_random;
        total_num = perf->file_num;
        //0 * 50 + 50
        start_no = thread_no * file_per_thread;
        file_num = thread_no * file_per_thread + do_file;

        printf("thread[%4d] : lookup from %4d to %4d\n",
                        thread_no, start_no, file_num);

        for(i=start_no; i<file_num; ++i) {
                if(should_random) {
                        snprintf(path, MAX_NAME_LEN, "%s_%d", perf->path, get_random_by_uuid(total_num + 1));
                } else {
                        snprintf(path, MAX_NAME_LEN, "%s_%d", perf->path, i);
                }
                printf("thread[%4d] : lookup file %s\n", thread_no, path);

                ret = sdfs_lookup_recurive(path, &fileid);
                if(ret) {
                        fprintf(stderr, "lookup_file %s, failed:%d\n", path, ret);
                }
        }

        return 0;
}

int delete_file(struct perf_args *perf, int file_per_thread, int do_file, int thread_no)
{
        int i, ret, file_num, start_no;
        char filename[MAX_NAME_LEN], tmp_filename[MAX_NAME_LEN];
        fileid_t parent;

        //0 * 50 + 50
        start_no = thread_no * file_per_thread;
        file_num = thread_no * file_per_thread + do_file;

        printf("thread[%4d] : delete from %4d to %4d\n",
                        thread_no, start_no, file_num);

        ret = sdfs_splitpath(perf->path, &parent, tmp_filename);
        if(ret == 0) {
                for(i=start_no; i<file_num; ++i) {
                        snprintf(filename, MAX_NAME_LEN, "%s_%d", tmp_filename, i);
                        ret = sdfs_unlink(NULL, &parent, filename);
                        if(ret) {
                                fprintf(stderr, "delete file %s, failed:%d\n", filename, ret);
                        }
                }
        }

        return 0;
}

int write_file(struct perf_args *perf, int thread_no)
{
        int ret, len, left;
        double file_size;
        char filename[MAX_NAME_LEN], tmp_filename[MAX_NAME_LEN], abs_path[MAX_PATH_LEN];
        fileid_t parent, fileid;
        char w_buf[BIG_BUF_LEN];
        yfs_off_t offset;
        buffer_t pack;

        //G->B
        file_size = perf->f_size * 1024 * 1024 * 1024;
        //首先创建文件
        ret = sdfs_splitpath(perf->path, &parent, tmp_filename);
        if(ret == 0) {
                snprintf(filename, MAX_NAME_LEN, "%s_%d", tmp_filename, thread_no);
                ret = sdfs_create(NULL, &parent, filename, &fileid, 0644, 0, 0);
                if(ret) {
                        fprintf(stderr, "create file %s, failed:%d\n", filename, ret);
                        pthread_exit("sdfs_create");
                } else {
                        //create ok
                }
        } else {
                fprintf(stderr, "raw_splitpath failed\n");
                pthread_exit("raw_splitpath");
        }

        offset = 0;
        len = perf->f_bsize * 1024;
        if(len > 1024*1024) {
                fprintf(stderr, "max block size is 1m\n");
                pthread_exit("len>1m");
        }

        left = file_size;
        mbuffer_init(&pack, 0);

        snprintf(abs_path, MAX_PATH_LEN, "%s_%d", perf->path, thread_no);
        printf("thread[%4d] : write file_name:%s\tfile_size:%f\tblock_size:%d\n",
                        thread_no, abs_path, file_size, len);

        while(left > 0) {
                if(len > left) {
                        len = left;
                }
                ret = mbuffer_copy(&pack, w_buf, len);
                if (ret) {
                        fprintf(stderr, "buffer_copy failed\n");
                        mbuffer_free(&pack);
                        pthread_exit("buffer_copy");
                }

                /* fprintf(stdout, "offset:%llu, size:%d left:%d\n", (LLU)offset, len, left); */

                ret = sdfs_write_sync(NULL, &fileid, &pack, len, offset);
                if (ret < 0) {
                        fprintf(stderr, "sdfs_write_sync failed\n");
                        mbuffer_free(&pack);
                        pthread_exit("sdfs_write_sync");
                }

                left -= ret;
                offset += ret;

                mbuffer_free(&pack);
        }

        if(perf->should_remove) {
                ret = sdfs_unlink(NULL, &parent, filename);
                if(ret) {
                        fprintf(stderr, "raw_unlink failed\n");
                        pthread_exit("raw_unlink");
                }
        }

        return 0;
}

void *create_test(void *arg)
{
        struct perf_args *perf;
        struct thread_args *thr;
        int file_per_thread, do_file, thread_no;

        thr = (struct thread_args *)arg;
        perf = thr->perf;
        file_per_thread = thr->file_per_thread;
        do_file = thr->do_file;
        thread_no = thr->thread_no;

        create_file(perf, file_per_thread, do_file, thread_no);

        return 0;
}

void *lookup_test(void *arg)
{
        struct perf_args *perf;
        struct thread_args *thr;
        int file_per_thread, do_file, thread_no;

        thr = (struct thread_args *)arg;
        perf = thr->perf;
        file_per_thread = thr->file_per_thread;
        do_file = thr->do_file;
        thread_no = thr->thread_no;

        lookup_file(perf, file_per_thread, do_file, thread_no);

        return 0;
}

void *delete_test(void *arg)
{
        struct perf_args *perf;
        struct thread_args *thr;
        int file_per_thread, do_file, thread_no;

        thr = (struct thread_args *)arg;
        perf = thr->perf;
        file_per_thread = thr->file_per_thread;
        do_file = thr->do_file;
        thread_no = thr->thread_no;

        delete_file(perf, file_per_thread, do_file, thread_no);

        return 0;
}

void *write_test(void *arg)
{
        struct perf_args *perf;
        struct thread_args *thr;
        int thread_no;

        thr = (struct thread_args *)arg;
        perf = thr->perf;
        thread_no = thr->thread_no;

        write_file(perf, thread_no);

        return 0;
}


void __create(struct perf_args *perf)
{
        int file_per_thread, left_file;
        double interval, file_per_sec;
        time_t start_time, end_time;
        int i, _create_num, thread_num;
        pthread_t tid[THREAD_MAX];
        struct thread_args thr[THREAD_MAX];

        _create_num = perf->file_num;
        thread_num = perf->thread_num;

        //file start from 0
        file_per_thread = _create_num / thread_num;
        left_file = _create_num % thread_num;

        start_time = time(NULL);
        if(file_per_thread) {
                for(i=0; i<thread_num; ++i) {
                        thr[i].perf = perf;
                        thr[i].file_per_thread = file_per_thread;
                        thr[i].do_file = file_per_thread;
                        thr[i].thread_no = i;
                }
                thr[thread_num-1].do_file = file_per_thread + left_file;

                for(i=0; i<thread_num; ++i) {
                        pthread_create(&tid[i], NULL, create_test, &thr[i]);
                }
                for(i=0; i<thread_num; ++i) {
                        pthread_join(tid[i], NULL);
                }

        } else {
                int ret;
                thr[0].perf = perf;
                thr[0].file_per_thread = file_per_thread;
                thr[0].do_file = left_file;
                thr[0].thread_no = 0;
                ret = pthread_create(&tid[0], NULL, create_test, &thr[0]);
                if(ret) {
                        perror("pthread");
                }
                pthread_join(tid[0], NULL);
        }

        end_time = time(NULL);

        interval = difftime(end_time, start_time);
        if(interval)
                file_per_sec = (_create_num * 1.0) / interval; //每秒创建文件个数
        else {
                fprintf(stderr, "it is too soon to create file, please try more files\n");
                exit(-1);
        }

        print_result(CREATE, interval, file_per_sec);

        return;
}

void __lookup(struct perf_args *perf)
{
        int file_per_thread, left_file;
        double interval, file_per_sec;
        time_t start_time, end_time;
        int i, _create_num, thread_num;
        pthread_t tid[THREAD_MAX];
        struct thread_args thr[THREAD_MAX];

        _create_num = perf->file_num;
        thread_num = perf->thread_num;

        //file start from 0
        file_per_thread = _create_num / thread_num;
        left_file = _create_num % thread_num;

        __create(perf);

        start_time = time(NULL);
        if(file_per_thread) {
                for(i=0; i<thread_num; ++i) {
                        thr[i].perf = perf;
                        thr[i].file_per_thread = file_per_thread;
                        thr[i].do_file = file_per_thread;
                        thr[i].thread_no = i;
                }
                thr[thread_num-1].do_file = file_per_thread + left_file;

                for(i=0; i<thread_num; ++i) {
                        pthread_create(&tid[i], NULL, lookup_test, &thr[i]);
                }
                for(i=0; i<thread_num; ++i) {
                        pthread_join(tid[i], NULL);
                }

        } else {
                int ret;
                thr[0].perf = perf;
                thr[0].file_per_thread = file_per_thread;
                thr[0].do_file = left_file;
                thr[0].thread_no = 0;
                ret = pthread_create(&tid[0], NULL, lookup_test, &thr[0]);
                if(ret) {
                        perror("pthread");
                }
                pthread_join(tid[0], NULL);
        }
        end_time = time(NULL);
        interval = difftime(end_time, start_time);
        if(interval)
                file_per_sec = (_create_num * 1.0) / interval; //每秒创建文件个数
        else {
                fprintf(stderr, "it is too soon to lookup file, please try more files\n");
                exit(-1);
        }

        print_result(LOOKUP, interval, file_per_sec);

        return;
}

void __remove(struct perf_args *perf)
{
        int file_per_thread, left_file;
        double interval, file_per_sec;
        time_t start_time, end_time;
        int i, _create_num, thread_num;
        pthread_t tid[THREAD_MAX];
        struct thread_args thr[THREAD_MAX];

        _create_num = perf->file_num;
        thread_num = perf->thread_num;

        //file start from 0
        file_per_thread = _create_num / thread_num;
        left_file = _create_num % thread_num;

        start_time = time(NULL);
        if(file_per_thread) {
                for(i=0; i<thread_num; ++i) {
                        thr[i].perf = perf;
                        thr[i].file_per_thread = file_per_thread;
                        thr[i].do_file = file_per_thread;
                        thr[i].thread_no = i;
                }
                thr[thread_num-1].do_file = file_per_thread + left_file;

                for(i=0; i<thread_num; ++i) {
                        pthread_create(&tid[i], NULL, delete_test, &thr[i]);
                }
                for(i=0; i<thread_num; ++i) {
                        pthread_join(tid[i], NULL);
                }

        } else {
                int ret;
                thr[0].perf = perf;
                thr[0].file_per_thread = file_per_thread;
                thr[0].do_file = left_file;
                thr[0].thread_no = 0;
                ret = pthread_create(&tid[0], NULL, delete_test, &thr[0]);
                if(ret) {
                        perror("pthread");
                }
                pthread_join(tid[0], NULL);
        }
        end_time = time(NULL);
        interval = difftime(end_time, start_time);
        if(interval)
                file_per_sec = (_create_num * 1.0) / interval; //每秒创建文件个数
        else {
                fprintf(stderr, "it is too soon to delete file, please try more files\n");
                exit(-1);
        }

        print_result(REMOVE, interval, file_per_sec);

        return;
}

void __write(struct perf_args *perf)
{
        int i, ret, thread_num;
        pthread_t tid[THREAD_MAX];
        struct thread_args thr[THREAD_MAX];
        time_t start_time, end_time;
        double interval, file_size, size_per_sec;

        file_size = perf->f_size * 1024 * 1024 * 1024;
        thread_num = perf->thread_num;

        start_time = time(NULL);
        for(i=0; i<thread_num; ++i) {
                thr[i].perf = perf;
                thr[i].thread_no = i;
                ret = pthread_create(&tid[i], NULL, write_test, &thr[i]);
                if(ret) {
                        perror("pthread");
                }
        }
        for(i=0; i<thread_num; ++i) {
                pthread_join(tid[i], NULL);
        }
        end_time = time(NULL);
        interval = difftime(end_time, start_time);

        if(interval)
                size_per_sec = file_size * thread_num / interval;
        else {
                fprintf(stderr, "it is too soon to write file, please try large file\n");
                exit(-1);
        }

        print_result(WRITE, interval, size_per_sec);
}


void __test(struct perf_args *perf)
{
        int cmd_type, should_remove;

        cmd_type = perf->cmd;
        should_remove = perf->should_remove;


        if(cmd_type == CREATE) {
                __create(perf);
                if(should_remove) {
                        __remove(perf);
                } else {
                        //do_nothing
                }
        } else if(cmd_type == LOOKUP) {
                __lookup(perf);
                if(should_remove) {
                        __remove(perf);
                } else {
                        //do_nothing
                }
        } else if(cmd_type == WRITE) {
                __write(perf);
        } else if(cmd_type == READ) {
                //__read(perf);
        } else {
                fprintf(stderr, "INVALID_CMD...\n");
                exit(EINVAL);
        }

        return;
}

void start_test(struct perf_args *perf)
{
        if(perf == NULL) {
                fprintf(stderr, "invalid argument\n");
                exit(EINVAL);
        }

        if(perf->thread_num > THREAD_MAX) {
                perf->thread_num = THREAD_MAX;
        }

        __test(perf);

        return;

}

int is_exist_dir(const char *path)
{
        int ret;
        fileid_t fileid;

        if(path == NULL) {
                fprintf(stderr, "invalid path\n");
                exit(EINVAL);
        }

        ret = sdfs_lookup_recurive(path, &fileid);
        if(ret == 0) {//存在
                return 1;
        }

        return 0;//不存在
}

int main(int argc, char *argv[])
{
        int opt, ret, given_dir = 0;
        int options_index;
        char *prog;
        char path[MAX_PATH_LEN];
        //cmd | should_remove | should_random | file_nums | size| block_size | thread_nums | directory
        struct perf_args perf = {INVALID_CMD, 0, 0, 1, 1, 4, 1, "/"};

        static const struct option long_options[] = {
                {"create",no_argument,NULL,'c'},
                {"lookup",no_argument,NULL,'l'},
                {"write",no_argument,NULL,'w'},
                {"size",required_argument,NULL,'q'},
                {"block",required_argument,NULL,'b'},
                {"file",required_argument,NULL,'f'},
                {"thread",required_argument,NULL,'t'},
                {"dir",required_argument,NULL,'d'},
                {"remove",no_argument,NULL,'r'},
                {"random",no_argument,NULL,'s'},
                {"help",no_argument,NULL,'h'},
                {0,0,0,0}
        };

        if(argc == 1) {
                usage();
                return (-1);
        }

        dbg_info(0);

        //获取程序名字
        prog = strrchr(argv[0], '/');
        if(prog)
                prog++;
        else
                prog = argv[0];

        while((opt = getopt_long(argc,argv,"h?",long_options,&options_index)) != -1) {

                switch(opt) {
                        case 'c':
                                perf.cmd = CREATE;
                                break;
                        case 'l':
                                perf.cmd = LOOKUP;
                                break;
                        case 'w':
                                perf.cmd = WRITE;
                                break;
                        case 'q':
                                perf.f_size = atoi(optarg);
                                if(perf.f_size == 0) perf.f_size = 1;
                                break;
                        case 'b':
                                perf.f_bsize = atoi(optarg);
                                if(perf.f_bsize == 0) perf.f_bsize = 4;//4k
                                break;
                        case 'f':
                                perf.file_num = atoi(optarg);
                                if(perf.file_num == 0) perf.file_num = 1;
                                break;
                        case 't':
                                perf.thread_num = atoi(optarg);
                                if(perf.thread_num == 0) perf.thread_num = 1;
                                break;
                        case 'd':
                                if(strcmp(optarg, "/") == 0) {
                                        fprintf(stderr, "given directory can not be root\n");
                                        return (-1);
                                }
                                normalize_path(optarg, path);//标准化path格式
                                snprintf(perf.path, MAX_PATH_LEN, "%s/uss_perf", path);
                                given_dir = 1;
                                break;
                        case 'r':
                                perf.should_remove = 1;
                                break;
                        case 's':
                                perf.should_random = 1;
                                break;
                        case 'h':
                        case '?':
                                usage();return -1;
                                break;
                        default:
                                printf("wrong argument\n");
                                break;
                }

        }

        if (optind < argc) {
                printf("non-option ARGV-elements: ");
                while (optind < argc)
                        printf("%s ", argv[optind++]);
                printf("\n");
        }

        if(perf.cmd == INVALID_CMD) {
                usage();
                fprintf(stderr, "\n\nmust specify cmd\n");
                return (-1);
        }
        //必须指定目录
        if(given_dir == 0) {
                usage();
                fprintf(stderr, "\n\nmust specify directory\n");
                return (-1);
        }

        printf("cmd:%d\tfile_num:%d\tthread_num:%d\tdir:%s\n",
                        perf.cmd, perf.file_num, perf.thread_num, perf.path);

        ret = ly_init_simple(prog);
        if(ret)
                GOTO(err_ret, ret);

        //判断指定目录是否已存在
        if(is_exist_dir(path) == 0) {
                fprintf(stderr, "No such file or directory\n");
                return (-1);
        }

        //开始测试
        start_test(&perf);

        return 0;
err_ret:
        return ret;
}

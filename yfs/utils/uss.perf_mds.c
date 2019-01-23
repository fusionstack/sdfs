#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <uuid/uuid.h>
#include <signal.h>

#include "configure.h"
#include "yatomic.h"
#include "dbg.h"
#include "sdfs_lib.h"
#include "job_dock.h"
#include "mds_lib.h"
#include "inode_proto.h"
#include "namei.h"
#include "mds_main.h"
#include "schedule.h"
#include "cpuset.h"
#include "main_loop.h"
#include "leveldb_queue.h"
#include "variable.h"
#include "worm_srv_lib.h"

#define INVALID_CMD (-1)

typedef enum {
        UNKNOWN = 0,
        CREATE,
        LOOKUP,
        REMOVE,
        WRITE,
        READ,
        PERF,
        CREATE_SCHEDULE,
        LOOKUP_SCHEDULE,
        PERF_THREAD_POOL_SCHEDULE,
} test_op_t;

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

typedef struct {
        fileid_t parent;
        yatomic_t perf_atom;
        uint64_t range;
        uint64_t time_used;
        int thread_id;
        sem_t sem;
} schedule2_arg_t;

void __remove(struct perf_args *perf);
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

int __create_file_mds(const fileid_t *parent, const char *name, uint32_t mode,
                fileid_t *fileid, uint32_t uid, uint32_t gid)
{
        int ret;
        char _buf[MAX_BUF_LEN];
        mdp_create_req_t *req = (void *)_buf;
        job_t *job;
        md_proto_t md;

        req->parent = *parent;
        req->name.len = strlen(name) + 1;
        strcpy(req->name.buf, name);
        req->mode = mode;
        req->uid = uid;
        req->gid = gid;
        req->atime = time(NULL);
        req->mtime = time(NULL);

        ret = job_create(&job, jobtracker, "create_file_mds_wait");
        if (ret)
                GOTO(err_ret, ret);

        job_wait_init(job);

#if 0
        ret = mds_queue_create(req, sizeof(*req) + strlen(name) + 1, job);
        if (ret) {
                GOTO(err_job, ret);
        }
#endif
        (void)req;
        (void)name;
        
        ret = job_timedwait(job, 10);
        if (ret) {
                exit(EPERM);
                GOTO(err_job, ret);
        }

        mbuffer_get(&job->reply, &md, sizeof(md_proto_t));
        printf("create %s ok\n", name);

        job_destroy(job);

        DBUG(""FID_FORMAT"\n", FID_ARG(fileid));

        return 0;
err_job:
        job_destroy(job);
err_ret:
        return ret;
}

int __lookup_file_mds_direct(uss_leveldb_t * db, const fileid_t *parent, const char *name, fileid_t *fileid)
{
        int ret, klen, vlen;
        char key[USS_DB_MAX_KEY_LEN], buf[USS_DB_MAX_KEY_LEN];
        dir_entry_t *ent = (void *)buf;

        klen = USS_DB_MAX_KEY_LEN;
        uss_leveldb_encodekey(parent->id, name, key, &klen);
        vlen = USS_DB_MAX_KEY_LEN;
        ret = uss_leveldb_get3(db, key, klen,
                        (void *)ent, &vlen, VTYPE_DENT);
        if (ret)
                GOTO(err_ret, ret);

        *fileid = ent->fileid;

        return 0;
err_ret:
        return ret;
}

#if 0
int __lookup_file_mds(const fileid_t *parent, const char *name, fileid_t *fileid)
{
        int ret;
        char _buf[MAX_BUF_LEN];
        mdp_lookup_req_t *req = (void *)_buf;
        job_t *job;

        req->parent = *parent;
        req->name.len = strlen(name) + 1;
        strcpy(req->name.buf, name);

        ret = job_create(&job, jobtracker, "lookup_file_mds");
        if (ret)
                GOTO(err_ret, ret);

        job_wait_init(job);

        ret = mds_queue_lookup(req, sizeof(*req) + strlen(name) + 1, job);
        if (ret)
                GOTO(err_job, ret);

        /*ret = job_timedwait(job, 10);*/
        /*if (ret) {*/
                /*exit(EPERM);*/
                /*GOTO(err_job, ret);*/
        /*}*/

        mbuffer_get(&job->reply, fileid, sizeof(fileid_t));
        DBUG(""FID_FORMAT"\n", FID_ARG(fileid));

        job_destroy(job);
        return 0;
err_job:
        job_destroy(job);
err_ret:
        return ret;
}
#endif

int __lookup1_file_mds(const char *path, fileid_t *fileid)
{
        return namei_lookup(path, fileid);
}

int __split_path_mds(const char *_path, fileid_t *parent, char *name)
{
        int ret;
        char path[MAX_PATH_LEN], *nameidx;

        strcpy(path, _path);
        nameidx = rindex(path, '/');
        strcpy(name, nameidx + 1);
        if (nameidx != path) {
                *nameidx = '\0';
        } else {
                path[1] = '\0';
        }

        ret = __lookup1_file_mds(path, parent);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int __delete_file_mds(const fileid_t *parent, const char *name)
{
        int ret;
        fileinfo_t md;

        ret = inode_unlink(&md, parent, name);
        if (ret) {
                if (ret == ENOENT) {
                        goto err_ret;
                } else {
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

int create_file(struct perf_args *perf, int file_per_thread, int do_file, int thread_no)
{
        int i, ret, file_num, start_no;
        char filename[MAX_NAME_LEN], tmp_filename[MAX_NAME_LEN];
        fileid_t parent;
        fileinfo_t md;

        start_no = thread_no * file_per_thread;
        file_num = thread_no * file_per_thread + do_file;

        printf("thread[%4d] : create from %4d to %4d\n",
                        thread_no, start_no, file_num);

        ret = __split_path_mds(perf->path, &parent, tmp_filename);
        if (ret) {
                fprintf(stderr, "splitepath file path %s, ret %d\n", perf->path, ret);
                exit(EPERM);
        }

        for(i=start_no; i<file_num; ++i) {
                snprintf(filename, MAX_NAME_LEN, "u_%d", i);
                ret = inode_create(&md, &parent, filename, 0644, 0, 0, time(NULL), time(NULL));
                if(ret) {
                        fprintf(stderr, "create file %s, failed:%d\n", filename, ret);
                }
        }

        return 0;
}

int lookup_file(struct perf_args *perf, int file_per_thread, int do_file, int thread_no)
{
        int i, ret, file_num, start_no, should_random, total_num;
        char path[MAX_PATH_LEN], name[MAX_NAME_LEN], tmp_filename[MAX_NAME_LEN];
        fileid_t fileid, parent;
        uss_leveldb_t *db;

        should_random = perf->should_random;
        total_num = perf->file_num;
        //0 * 50 + 50
        start_no = thread_no * file_per_thread;
        file_num = thread_no * file_per_thread + do_file;

        printf("thread[%4d] : lookup from %4d to %4d\n",
                        thread_no, start_no, file_num);

        ret = __split_path_mds(perf->path, &parent, tmp_filename);
        if (ret) {
                exit(EPERM);
        }

        db = uss_get_leveldb(parent.id);

        for(i=start_no; i<file_num; ++i) {
                if(should_random) {
                        snprintf(name, MAX_NAME_LEN, "%s_%d", tmp_filename, (int)_random_range(0, total_num));
                } else {
                        snprintf(name, MAX_NAME_LEN, "%s_%d", tmp_filename, i);
                }
                /*printf("thread[%4d] : lookup file path %s name %s\n", thread_no, perf->path, name);*/

                ret = __lookup_file_mds_direct(db, &parent, name, &fileid);
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

        ret = __split_path_mds(perf->path, &parent, tmp_filename);
        if(ret == 0) {
                for(i=start_no; i<file_num; ++i) {
                        snprintf(filename, MAX_NAME_LEN, "u_%d", i);
                        /*ret = __delete_file_mds(&parent, filename);*/
                        /*if(ret) {*/
                                /*if (ret == ENOENT) {*/
                                /*} else {*/
                                        /*[>fprintf(stderr, "delete parent "FID_FORMAT", file %s, failed:%d\n",<]*/
                                                        /*FID_ARG(&parent), filename, ret);*/
                                /*}*/
                        /*}*/

                        int klen, exist;
                        char key[USS_DB_MAX_KEY_LEN];

                        klen = USS_DB_MAX_KEY_LEN;
                        snprintf(filename, MAX_NAME_LEN, "u_%d", i);
                        uss_leveldb_encodekey(parent.id, filename, key, &klen);
                        uss_leveldb_exist(parent.volid, key, klen, &exist);
                        if (exist) {
                                uss_leveldb_delete(parent.volid, key, klen);
                        }

                        klen = USS_DB_MAX_KEY_LEN;
                        snprintf(filename, MAX_NAME_LEN, "u_0_%d", i);
                        uss_leveldb_encodekey(parent.id, filename, key, &klen);
                        uss_leveldb_exist(parent.volid, key, klen, &exist);
                        if (exist) {
                                uss_leveldb_delete(parent.volid, key, klen);
                        }

                        klen = USS_DB_MAX_KEY_LEN;
                        snprintf(filename, MAX_NAME_LEN, "u_1_%d", i);
                        uss_leveldb_encodekey(parent.id, filename, key, &klen);
                        uss_leveldb_exist(parent.volid, key, klen, &exist);
                        if (exist) {
                                uss_leveldb_delete(parent.volid, key, klen);
                        }

                        klen = USS_DB_MAX_KEY_LEN;
                        snprintf(filename, MAX_NAME_LEN, "u_2_%d", i);
                        uss_leveldb_encodekey(parent.id, filename, key, &klen);
                        uss_leveldb_exist(parent.volid, key, klen, &exist);
                        if (exist) {
                                uss_leveldb_delete(parent.volid, key, klen);
                        }
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

#if 1
        create_file(perf, file_per_thread, do_file, thread_no);
#else
        create_file_schedule(perf, file_per_thread, do_file, thread_no);
#endif

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

void __create(struct perf_args *perf)
{
        int file_per_thread, left_file;
        double interval, file_per_sec;
        time_t start_time, end_time;
        int i, _create_num, thread_num;
        pthread_t tid[THREAD_MAX];
        struct thread_args thr[THREAD_MAX];

        __remove(perf);

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
                /*exit(-1);*/
        }

        print_result(REMOVE, interval, file_per_sec);

        return;
}

void *__remove2__(void *_arg)
{
        int klen, ret;
        char name[MAX_NAME_LEN];
        char key[USS_DB_MAX_KEY_LEN];
        schedule2_arg_t *arg = _arg;
        const fileid_t *parent = &arg->parent;
        uint64_t idx;
        struct timeval last, now;
        fileinfo_t md;

        _gettimeofday(&last, NULL);
        idx = 0;
        while (idx < arg->range) {
                snprintf(name, MAX_NAME_LEN, "u_%d_%lu", arg->thread_id, idx++);
                klen = USS_DB_MAX_KEY_LEN;
                uss_leveldb_encodekey(parent->id, name, key, &klen);

                ret = inode_unlink(&md, parent, name);
                if (ret) {
                        DBUG("unlink parent "FID_FORMAT", name %s, ret %d\n", FID_ARG(parent), name, ret);
                } else {
                }
                yatomic_get_and_inc(&arg->perf_atom, NULL);
        }

        _gettimeofday(&now, NULL);
        arg->time_used = _time_used(&last, &now);

        return NULL;
}

int __remove2(const fileid_t *parent, uint64_t range, int thread)
{
        int i;
        uint64_t perf, perf_total, used;
        pthread_t tid[THREAD_MAX];
        schedule2_arg_t arg[THREAD_MAX];
        struct timeval last, now;

        _gettimeofday(&last, NULL);

        memset(arg, 0, sizeof(arg));
        for (i = 0; i < thread; i++) {
                arg[i].parent = *parent;
                arg[i].range = range;
                arg[i].time_used = 0;
                arg[i].thread_id = i;
                yatomic_init(&arg[i].perf_atom, 0);
                pthread_create(&tid[i], NULL, __remove2__, &arg[i]);
        }

        for (i = 0; i < thread; i++) {
                pthread_join(tid[i], NULL);
        }

        _gettimeofday(&now, NULL);
        used = _time_used(&last, &now);

        perf_total = 0;
        for (i = 0; i < thread; i++) {
                yatomic_get(&arg[i].perf_atom, &perf);
                perf_total += perf;
                printf("remove thread %d perf %lu used %lu\n", i, perf, arg[i].time_used/(1000*1000));
        }

        printf("remove ops %llu, used %llu\n", (LLU)(perf_total)*1000*1000/used, (LLU)used/(1000*1000));

        return 0;
/*err_ret:*/
        /*return ret;*/
}

void *__create2__(void *_arg)
{
        int ret;
        char name[MAX_NAME_LEN];
        schedule2_arg_t *arg = _arg;
        const fileid_t *parent = &arg->parent;
        uint64_t idx;
        struct timeval last, now;
        fileinfo_t md;

        _gettimeofday(&last, NULL);
        idx = 0;
        while (idx < arg->range) {
                snprintf(name, MAX_NAME_LEN, "u_%d_%lu", arg->thread_id, idx++);
                ret = inode_create(&md, parent, name, 0644, 0, 0, time(NULL), time(NULL));
                if(ret) {
                        if (ret == EEXIST) {
                        } else {
                                fprintf(stderr, "create file %s, failed:%d\n", name, ret);
                        }
                }
                yatomic_get_and_inc(&arg->perf_atom, NULL);
        }

        _gettimeofday(&now, NULL);
        arg->time_used = _time_used(&last, &now);

        return NULL;
/*err_ret:*/
        /*YASSERT(0);*/
        /*return NULL;*/
}

int __create2(const fileid_t *parent, uint64_t range, int thread)
{
        int i;
        uint64_t perf, perf_total, used;
        struct timeval last, now;
        pthread_t tid[THREAD_MAX];
        schedule2_arg_t arg[THREAD_MAX];

        printf("begin create ...\n");
        _gettimeofday(&last, NULL);

        (void)arg;
        (void)tid;
        for (i = 0; i < thread; i++) {
                arg[i].parent = *parent;
                arg[i].range = range;
                arg[i].time_used = 0;
                arg[i].thread_id = i;
                yatomic_init(&arg[i].perf_atom, 0);
                pthread_create(&tid[i], NULL, __create2__, &arg[i]);
        }

        for (i = 0; i < thread; i++) {
                pthread_join(tid[i], NULL);
        }

        _gettimeofday(&now, NULL);
        used = _time_used(&last, &now);

        perf_total = 0;
        for (i = 0; i < thread; i++) {
                yatomic_get(&arg[i].perf_atom, &perf);
                perf_total += perf; 
                printf("create thread %d perf %lu used %lu\n", i, perf, arg[i].time_used/(1000*1000));
        }

        printf("ops %llu, used %llu\n", (LLU)(perf_total)*1000*1000/used, (LLU)used/(1000*1000));

        return 0;
/*err_ret:*/
        /*return ret;*/
}

typedef struct {
        fileid_t parent;
        char name[MAX_NAME_LEN];
        uss_leveldb_t *uss_db;
        yatomic_t *perf_atom;
} perf_ctx_t;

static void __perf_create__(void *arg)
{
        int ret, reqlen, replen, now;
        char _buf[MAX_BUF_LEN];
        perf_ctx_t *ctx = arg; 
        mdp_create_req_t *req = (void *)_buf;
        fileinfo_t md;

        ANALYSIS_BEGIN(0);

        req->parent = ctx->parent;
        req->name.len = strlen(ctx->name) + 1;
        snprintf(req->name.buf, MAX_NAME_LEN, "%s", ctx->name);
        req->uid = 0;
        req->gid = 0;
        req->mode = 0755;
        now = time(NULL);
        req->atime = now;
        req->mtime = now;
        reqlen = sizeof(*req) + req->name.len;
        replen = sizeof(md);
        ret = mds_queue_create(req, reqlen, (void *)&md, (uint32_t *)&replen);
        if (ret) {
                GOTO(err_ret, ret);
        }

        yatomic_get_and_inc(ctx->perf_atom, NULL);

        ANALYSIS_QUEUE(0, 1000*1000, "__perf_create__");
        DBUG("create parent "FID_FORMAT", name %s\n", FID_ARG(&ctx->parent), ctx->name);

        yfree((void **)&arg);

#if 0
        mdp_unlink_req_t *req2 = (void *)&_buf;
        req2->parent = ctx->parent;
        req2->name.len = strlen(ctx->name) + 1;
        snprintf(req2->name.buf, MAX_NAME_LEN, "%s", ctx->name);
        reqlen = sizeof(*req2) + req2->name.len;
        replen = sizeof(md);
        ret = mds_queue_unlink(req2, reqlen, (void *)&md, (uint32_t *)&replen);
        if (ret)
                /*YASSERT(0);*/
#endif

        return;
err_ret:
        DERROR("parent "FID_FORMAT", name %s, ret %d\n", FID_ARG(&ctx->parent), ctx->name, ret);
        YASSERT(0);

        yfree((void **)&arg);
        return;
}

void *__create_schedule_worker__(void *_arg)
{
        int ret, i, efd;
        int master = 0;
        int slave = 0;
        schedule_t *schedule;
        schedule2_arg_t *arg = _arg;
        const fileid_t *parent = &arg->parent;
        uint64_t idx, range = arg->range; 
        struct timeval last, now;
        perf_ctx_t *ctx = NULL;

        cpuset_getcpu_by_physical_id(&master, &slave,
                        mdsconf.schedule_physical_package_id);
        ret = cpuset("test_create", master);
        if (unlikely(ret)) {
                DWARN("set cpu fail\n");
        }
        DINFO("set cpu master %d, slave %d\n", master, slave);

        ret = schedule_create(&efd, "perf_create", NULL, &schedule);
        if (ret) {
                GOTO(err_ret, ret);
        }

        sem_post(&arg->sem);
        _gettimeofday(&last, NULL);
        idx = 0;
        while (idx < range) {
                for (i = 0; i < 10; i++) {
                        ret = ymalloc((void **)&ctx, sizeof(*ctx));
                        if (ret) {
                                GOTO(err_ret, ret);
                        }

                        ctx->parent = *parent;
                        ctx->perf_atom = &arg->perf_atom;
                
                        /*snprintf(ctx->name, MAX_NAME_LEN, "u_%d_%lu", arg->thread_id, idx%range);*/
                        snprintf(ctx->name, MAX_NAME_LEN, "u_%d_%lu", arg->thread_id, idx);
                        DBUG("task_new name %s\n", ctx->name);
                        schedule_task_new("prof_task", __perf_create__, ctx, -1);
                        idx++;
                }

                schedule_run();
        }

        _gettimeofday(&now, NULL);
        arg->time_used = _time_used(&last, &now);

        return NULL;
err_ret:
        YASSERT(0);
        return NULL;
}

int __create_schedule__(const fileid_t *parent, uint64_t range, int thread)
{
        int i, ret;
        pthread_t tid[THREAD_MAX];
        schedule2_arg_t arg[THREAD_MAX];
        struct timeval last, now;
        uint64_t used, perf, perf_total;

        _gettimeofday(&last, NULL);

        for (i = 0; i < thread; i++) {
                arg[i].parent = *parent;
                arg[i].range = range;
                arg[i].time_used = 0;
                arg[i].thread_id = i;
                yatomic_init(&arg[i].perf_atom, 0);

                ret = sem_init(&arg[i].sem, 0, 0);
                if (unlikely(ret))
                        YASSERT(0);

                pthread_create(&tid[i], NULL, __create_schedule_worker__, &arg[i]);
                sem_wait(&arg[i].sem);
        }

        for (i = 0; i < thread; i++) {
                pthread_join(tid[i], NULL);
        }

        _gettimeofday(&now, NULL);
        used = _time_used(&last, &now);

        perf_total = 0;
        for (i = 0; i < thread; i++) {
                ret = yatomic_get(&arg[i].perf_atom, &perf);
                if (ret) {
                        GOTO(err_ret, ret);
                }
                perf_total += perf;
                printf("thread %d perf %llu used %llu\n", i, (LLU)perf, (LLU)arg[i].time_used/(1000*1000));
        }

        printf("ops %llu, used %llu\n", (LLU)(perf_total)*1000*1000/used, (LLU)used/(1000*1000));

        return 0;
err_ret:
        return ret;
}

static void __create_schedule(struct perf_args *perf)
{
        int ret, thread;
        fileid_t parent;
        char tmp_filename[MAX_NAME_LEN];
        uint64_t range;

        thread = perf->thread_num;
        range = perf->file_num;

        ret = __split_path_mds(perf->path, &parent, tmp_filename);
        if (ret) {
                exit(EPERM);
        }

        printf("begin clean...\n");
        ret = __remove2(&parent, range, thread);
        if (ret) {
                GOTO(err_ret, ret);
        }

        /*sleep(100);*/
        printf("begin perf create ...\n");
        ret = __create_schedule__(&parent, range, thread);
        if (ret) {
                GOTO(err_ret, ret);
        }

        if (perf->should_remove) {
                printf("cleanup ...\n");
                ret = __remove2(&parent, range, thread);
                if (ret) {
                        GOTO(err_ret, ret);
                }
        }

        printf("create test parent "FID_FORMAT" range (0-%d)\n", FID_ARG(&parent), perf->file_num);
        return;
err_ret:
        printf("create test parent "FID_FORMAT" range (0-%d), fail\n", FID_ARG(&parent), perf->file_num);
}

static void __perf_lookup__(void *arg)
{
        int ret, reqlen, replen;
        char _buf[MAX_BUF_LEN];
        perf_ctx_t *ctx = arg; 
        mdp_lookup_req_t *req = (void *)_buf;
        fileid_t fileid;

        ANALYSIS_BEGIN(0);

        req->parent = ctx->parent;
        req->name.len = strlen(ctx->name) + 1;
        snprintf(req->name.buf, MAX_NAME_LEN, "%s", ctx->name);
        reqlen = sizeof(*req) + req->name.len;
        replen = sizeof(fileid);
        ret = mds_queue_lookup(req, reqlen, (void *)&fileid, (uint32_t *)&replen);
        if (ret) {
                GOTO(err_ret, ret);
        }

        yatomic_get_and_inc(ctx->perf_atom, NULL);
        yfree((void **)&arg);

        ANALYSIS_QUEUE(0, 1000, "__perf_lookup__");

        return;
err_ret:
        if (ret != ENOENT) {
                YASSERT(0);
        }
        return;
}

void *__lookup_schedule_worker(void *_arg)
{
        int ret, i, efd, master, slave;
        uint64_t idx;
        perf_ctx_t *ctx = NULL;
        struct timeval last, now;
        schedule_t *schedule;
        schedule2_arg_t *arg = _arg;
        const fileid_t *parent = &arg->parent;
        uint64_t range = arg->range;
        int thread_id = arg->thread_id;

        cpuset_getcpu_by_physical_id(&master, &slave,
                        mdsconf.schedule_physical_package_id);
        ret = cpuset("test_lookup", master);
        if (unlikely(ret)) {
                DWARN("set cpu fail\n");
        }
        DWARN("set cpu master %d, slave %d\n", master, slave);

        ret = schedule_create(&efd, "perf_lookup", NULL, &schedule);
        if (ret) {
                GOTO(err_ret, ret);
        }

        _gettimeofday(&last, NULL);
        idx = 0;
        while (idx < range) {
        /*while (1) {*/
                for (i = 0; i < 10; i++) {
                        ret = ymalloc((void **)&ctx, sizeof(*ctx));
                        if (ret) {
                                GOTO(err_ret, ret);
                        }

                        ctx->parent = *parent;
                        ctx->perf_atom = &arg->perf_atom;

                        /*snprintf(ctx->name, MAX_NAME_LEN, "u_%d_%lu", thread_id, _random_range(0, range));*/
                        snprintf(ctx->name, MAX_NAME_LEN, "u_%d_%lu", thread_id, idx%range);

                        schedule_task_new("prof_task", __perf_lookup__, ctx, -1);
                        idx++;
                }
                schedule_run(NULL);
        }

        _gettimeofday(&now, NULL);

        return NULL;
err_ret:
        YASSERT(0);
        return NULL;
}

int __lookup_schedule__(const fileid_t *parent, uint64_t range, int thread)
{
        int ret, i;
        uint64_t perf, perf_total, used;
        pthread_t tid[THREAD_MAX];
        schedule2_arg_t arg[THREAD_MAX];
        struct timeval last, now;

        _gettimeofday(&last, NULL);

        for (i = 0; i < thread; i++) {
                arg[i].parent = *parent;
                arg[i].range = range;
                arg[i].time_used = 0;
                arg[i].thread_id = i;
                yatomic_init(&arg[i].perf_atom, 0);
                pthread_create(&tid[i], NULL, __lookup_schedule_worker, &arg[i]);
        }

        for (i = 0; i < thread; i++) {
                pthread_join(tid[i], NULL);
        }

        _gettimeofday(&now, NULL);
        used = _time_used(&last, &now);

        perf_total = 0;
        for (i = 0; i < thread; i++) {
                ret = yatomic_get(&arg[i].perf_atom, &perf);
                if (ret) {
                        GOTO(err_ret, ret);
                }
                printf("thread %d perf %llu used %llu\n", i, (LLU)perf, (LLU)arg[i].time_used/(1000*1000));
                perf_total += perf;
        }

        printf("ops %llu, used %llu\n", (LLU)(perf_total)*1000*1000/used, (LLU)used/(1000*1000));

        return 0;
err_ret:
        return ret;
}

static void __lookup_schedule(struct perf_args *perf)
{
        int ret;
        fileid_t parent;
        char tmp_filename[MAX_NAME_LEN];
        
        ret = __split_path_mds(perf->path, &parent, tmp_filename);
        if (ret) {
                exit(EPERM);
        }

        printf("prep lookup...\n");
        __create2(&parent, perf->file_num, perf->thread_num);

        printf("lookup test parent "FID_FORMAT" range (0-%d)\n", FID_ARG(&parent), perf->file_num);
        __lookup_schedule__(&parent, perf->file_num, perf->thread_num);

        if (perf->should_remove) {
                printf("clean up ...\n");
                __remove2(&parent, perf->file_num, perf->thread_num);
        }
}

static void __perf_threadpool__(void *arg)
{
        int ret;
        perf_ctx_t *ctx = arg; 
        uss_leveldb_t *uss_db;

        uss_db = uss_get_leveldb(ctx->parent.volid);
        ret = leveldb_queue_perf(uss_db);
        if (ret) {
                GOTO(err_ret, ret);
        }

        yatomic_get_and_inc(ctx->perf_atom, NULL);
        yfree((void **)&arg);

        return;
err_ret:
        YASSERT(0);
        return;
}

void *__threadpool_schedule_worker(void *_arg)
{
        int ret, i, efd, master, slave;
        uint64_t idx;
        perf_ctx_t *ctx = NULL;
        struct timeval last, now;
        schedule_t *schedule;
        schedule2_arg_t *arg = _arg;
        const fileid_t *parent = &arg->parent;
        uint64_t range = arg->range;
        int thread_id = arg->thread_id;

        cpuset_getcpu_by_physical_id(&master, &slave,
                        mdsconf.schedule_physical_package_id);
        ret = cpuset("test_threadpool", master);
        if (unlikely(ret)) {
                DWARN("set cpu fail\n");
        }
        DINFO("set cpu master %d, slave %d\n", master, slave);

        ret = schedule_create(&efd, "perf_threadpool", NULL, &schedule);
        if (ret) {
                GOTO(err_ret, ret);
        }

        _gettimeofday(&last, NULL);
        idx = 0;
        while (idx < range) {
                for (i = 0; i < 10; i++) {
                        ret = ymalloc((void **)&ctx, sizeof(*ctx));
                        if (ret) {
                                GOTO(err_ret, ret);
                        }

                        ctx->parent = *parent;
                        ctx->perf_atom = &arg->perf_atom;
                        snprintf(ctx->name, MAX_NAME_LEN, "u_%d_%lu", thread_id, idx);
                        idx++;

                        schedule_task_new("prof_task", __perf_threadpool__, ctx, -1);
                }
                schedule_run();
        }

        _gettimeofday(&now, NULL);

        return NULL;
err_ret:
        YASSERT(0);
        return NULL;
}

int __threadpool_schedule__(const fileid_t *parent, uint64_t range, int thread)
{
        int ret, i;
        uint64_t perf, perf_total, used;
        pthread_t tid[THREAD_MAX];
        schedule2_arg_t arg[THREAD_MAX];
        struct timeval last, now;

        _gettimeofday(&last, NULL);

        for (i = 0; i < thread; i++) {
                arg[i].parent = *parent;
                arg[i].range = range;
                arg[i].time_used = 0;
                arg[i].thread_id = i;
                yatomic_init(&arg[i].perf_atom, 0);
                pthread_create(&tid[i], NULL, __threadpool_schedule_worker, &arg[i]);
        }

        for (i = 0; i < thread; i++) {
                pthread_join(tid[i], NULL);
        }

        _gettimeofday(&now, NULL);
        used = _time_used(&last, &now);

        perf_total = 0;
        for (i = 0; i < thread; i++) {
                ret = yatomic_get(&arg[i].perf_atom, &perf);
                if (ret) {
                        GOTO(err_ret, ret);
                }
                printf("thread %d perf %llu used %llu\n", i, (LLU)perf, (LLU)arg[i].time_used/(1000*1000));
                perf_total += perf;
        }

        printf("ops %llu, used %llu\n", (LLU)(perf_total)*1000*1000/used, (LLU)used/(1000*1000));

        return 0;
err_ret:
        return ret;
}

static void __threadpool_schedule(struct perf_args *perf)
{
        int ret;
        fileid_t parent;
        char tmp_filename[MAX_NAME_LEN];
        
        ret = __split_path_mds(perf->path, &parent, tmp_filename);
        if (ret) {
                exit(EPERM);
        }

        printf("threadpool perf ...\n");
        printf("parent "FID_FORMAT" range (0-%d)\n", FID_ARG(&parent), perf->file_num);
        __threadpool_schedule__(&parent, perf->file_num, perf->thread_num);
}

void __test(struct perf_args *perf)
{
        int ret, cmd_type = perf->cmd;

        ret = variable_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = schedule_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if(cmd_type == CREATE_SCHEDULE) {
                __create_schedule(perf);
        } else if(cmd_type == LOOKUP_SCHEDULE) {
                __lookup_schedule(perf);
        } else if(cmd_type == PERF_THREAD_POOL_SCHEDULE) {
                __threadpool_schedule(perf);
        } else {
                fprintf(stderr, "INVALID_CMD...\n");
                exit(EINVAL);
        }

        return;
err_ret:
        YASSERT(0);
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

static void usage() {
        fprintf(stderr,
                        "uss.perf {create|lookup|...} \n"
                        "--create [--file nums] [--thread nums] [--remove] --dir /path 创建文件\n"
                        "--lookup [--file nums] [--thread nums] [--remove] [--random] --dir /path 查找文件\n"
                        "--perf_threadpool [--file nums] [--thread nums] [--remove] [--random] --dir /path 查找文件\n"
                        "-h|-?|--help                                               print usage\n"
               );
}

int main(int argc, char *argv[])
{
        int opt, ret, daemon, given_dir = 0;
        int options_index, metano = 0;
        char *prog;
        char path[MAX_PATH_LEN], home[MAX_PATH_LEN], name[MAX_NAME_LEN];
        //cmd | should_remove | should_random | file_nums | size| block_size | thread_nums | directory
        struct perf_args perf = {INVALID_CMD, 0, 0, 1, 1, 4, 1, "/"};

        static const struct option long_options[] = {
                {"create",no_argument,NULL,'c'},
                {"lookup",no_argument,NULL,'l'},
                {"perf_threadpool",no_argument,NULL,'p'},
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

        /*dbg_info(0);*/

        //获取程序名字
        prog = strrchr(argv[0], '/');
        if(prog)
                prog++;
        else
                prog = argv[0];

        while((opt = getopt_long(argc,argv,"h?",long_options,&options_index)) != -1) {

                switch(opt) {
                        case 'c':
                                perf.cmd = CREATE_SCHEDULE;
                                break;
                        case 'l':
                                perf.cmd = LOOKUP_SCHEDULE;
                                break;
                        case 'p':
                                perf.cmd = PERF_THREAD_POOL_SCHEDULE;
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
                                snprintf(perf.path, MAX_PATH_LEN, "%s/u", path);
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


        daemon = 2;
        ret = ly_prep(daemon, name, -1);
        if (ret)
                GOTO(err_ret, ret);

        snprintf(name, MAX_NAME_LEN, "mds/%d", metano);
        snprintf(home, MAX_PATH_LEN, "%s/%s/%d", gloconf.workdir,
                 YFS_MDS_DIR_DISK_PRE, metano);

        printf("home %s, name %s\n", home, name);

        ret = path_validate(home, 1, 1);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init(daemon, name, -1);
        if (ret)
                GOTO(err_ret, ret);

        signal(SIGUSR1, mds_signal_handler);

        ret = mds_primary();
        if (ret) {
                GOTO(err_ret, ret);
        }

        metano = 0;
        ret = mds_init(home, metano);
        if (ret)
                GOTO(err_ret, ret);

        //开始测试
        start_test(&perf);

        return 0;
err_ret:
        return ret;
}

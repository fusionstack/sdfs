

#include <sys/types.h>
#include <sys/shm.h>
#include <getopt.h>

#define DBG_SUBSYS S_PROXY

#include "../../proxy/include/proxy_lib.h"
#include "../../proxy/include/yfs_proxy_file.h"
#include "dbg.h"

typedef struct {
        int idx;
        sem_t sem;
        int max;
        int create;
        int read;
        int write;
        int delete;
        char *home;
} args_t;

void *__test(void *arg)
{
        int ret, i, fd, cp;
        args_t *args = arg;
        char path[MAX_PATH_LEN], buf[MAX_BUF_LEN];

        memset(buf, 0x0, MAX_BUF_LEN);

        if (args->create) {
                for (i = 0; i < args->max;i++) {
                        snprintf(path, MAX_PATH_LEN, "%s/t_%d_%d", args->home,
                                 args->idx, i);
                        fd = yfs_create(path, 0644);
                        if (fd < 0) {
                                ret = errno;
                                DWARN("path %s idx %u\n", path, args->idx);
                                GOTO(err_ret, ret);
                        }

                        if (args->write) {
                                cp = random() % 100;
                                if (cp == 0)
                                        cp = 100 / 2;

                                ret = yfs_pwrite(fd, buf, cp, 0);
                                if (ret < 0) {
                                        ret = -ret;
                                        GOTO(err_ret, ret);
                                }
                        }

                        yfs_close(fd);
                }
        }

        sem_post(&args->sem);
        return NULL;
err_ret:
        sem_post(&args->sem);
        return NULL;
}

int main(int argc, char *argv[])
{
        int ret, i, max, len, t = 1;
        char c_opt;
        char *home, path[MAX_PATH_LEN], buf[MAX_BUF_LEN];
        int64_t offset;
        int __create = 0, __read = 0, __write = 0, __delete = 0;
        args_t *args;
        pthread_t th;
        pthread_attr_t ta;

        while(1) {
                int option_index = 0;

                static struct option long_options[] = {
                        {"create", 0, 0, 0},
                        {"read", 0, 0, 0},
                        {"write", 0, 0, 0},
                        {"delete", 0, 0, 0},
                };
                
                c_opt = getopt_long(argc, argv, "c:ht:",
                                    long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 0:
                        switch (option_index) {
                        case 0:
                                __create = 1;
                                break;
                        case 1:
                                __read = 1;
                                UNIMPLEMENTED(__WARN__);
                                break;
                        case 2:
                                __create = 1;
                                __write = 1;
                                break;
                        case 3:
                                __delete = 1;
                                UNIMPLEMENTED(__WARN__);
                                break;
                        default:
                                fprintf(stderr, "Hoops, wrong op got!\n");
                        }

                        break;

                case 'c':
                        max = atoi(optarg);
                        break;
                case 'h':
                        UNIMPLEMENTED(__WARN__);
                case 't':
                        t = atoi(optarg);
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op (%c) got!\n", c_opt);
                        exit(1);
                }
        }

        home = argv[argc - 1];

        DINFO("cycle %u thread %u home %s\n", max, t, home);

        ret = proxy_client_init("io_test");
        if (ret)
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&args, sizeof(args_t) * t);
        if (ret)
                GOTO(err_ret, ret);

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        for (i = 0; i < t; i++) {
                args[i].idx = i;
                args[i].create = __create;
                args[i].read = __read;
                args[i].write = __write;
                args[i].delete = __delete;
                args[i].max = max;
                args[i].home = home;

                ret = sem_init(&args[i].sem, 0, 0);
                if (ret < 0) {
                        ret == errno; 
                        GOTO(err_ret, ret);
                }

                ret = pthread_create(&th, &ta, __test, &args[i]);
                if (ret)
                        GOTO(err_ret, ret);
        }

        for (i = 0; i < t; i++) {
                ret = _sem_wait(&args[i].sem);
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

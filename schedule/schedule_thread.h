#ifndef __SCHEDULE_THREAD__
#define __SCHEDULE_THREAD__


#define __SCHE_THREAD_MAX__ 64

typedef enum {
        SCHE_THREAD_MISC,
        SCHE_THREAD_ETCD,
        SCHE_THREAD_REDIS,
        SCHE_THREAD_REPLICA,
        SCHE_THREAD_MAX,
} sche_thread_type_t;

struct sche_thread_ops {
        sche_thread_type_t type;
        int off;
        int size;

        void (*begin_trans)(int idx);
        void (*commit_trans)(int idx);
};

/* for sche_thread_ops_register */
extern int disk_maping_register();
extern int etcd_ops_register();

int sche_thread_ops_register(struct sche_thread_ops *hook, int type, int size);
struct sche_thread_ops *sche_thread_ops_get(int type);

int schedule_thread_init();
int schedule_newthread(sche_thread_type_t type, const int hash, int trans,
                       const char *name, int timeout, func_va_t exec, ...);


#endif


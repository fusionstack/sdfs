#ifndef __VARIABLE_H__
#define __VARIABLE_H__

typedef enum {
        VARIABLE_MAPING,
        VARIABLE_MEMCACHE,
        VARIABLE_HUGEPAGE,
        VARIABLE_CORENET_TCP,
        VARIABLE_CORENET_RDMA,
        VARIABLE_CORERPC,
        VARIABLE_CORE,
        VARIABLE_SCHEDULE,
        VARIABLE_AIO,
        VARIABLE_CLOCK,
        VARIABLE_TIMER,
        VARIABLE_GETTIME,
        VARIABLE_REDIS,
        VARIABLE_ANALYSIS,
        VARIABLE_ATTR_QUEUE,
        VARIABLE_MAX,
} variable_type_t;

int variable_init();
void variable_exit();

void * IO_FUNC variable_get(variable_type_t type);
void variable_set(variable_type_t type, void *variable);
void variable_unset(variable_type_t type);

int variable_newthread();
int variable_thread();
void * IO_FUNC variable_get_byctx(void *ctx, int type);
void * IO_FUNC variable_get_ctx();

#endif

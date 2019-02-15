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
        //VARIABLE_REDIS,
        VARIABLE_MAX,
} variable_type_t;

int variable_init();
void variable_exit();

void * IO_FUNC variable_get(int type);
void variable_set(int type, void *variable);
void variable_unset(int type);

int variable_newthread();
int variable_thread();

#endif

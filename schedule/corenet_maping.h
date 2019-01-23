#ifndef __CORENET_MAPING_H__
#define __CORENET_MAPING_H__

#include "core.h"

typedef struct {
        sockid_t sockid;
        sy_spinlock_t lock;

        struct list_head list;
        char loading;
} corenet_maping_t;

int corenet_maping_init(corenet_maping_t **_maping);
int corenet_maping(const nid_t *nid, sockid_t *sockid);
int corenet_maping_loading(const nid_t *nid);
int corenet_maping_accept(core_t *core, const nid_t *nid, const sockid_t *sockid);
void corenet_maping_close(const nid_t *nid);

#endif

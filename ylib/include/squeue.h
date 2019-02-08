#ifndef __SQUEUE_H__
#define __SQUEUE_H__

#include <stdint.h>
#include <sys/types.h>
#include <stdint.h>
#include <semaphore.h>
#ifndef __CYGWIN__

#endif

#include "sdfs_list.h"
#include "htable.h"
#include "sdfs_id.h"
#include "sdfs_conf.h"
#include "dbg.h"

typedef struct {
        htable_t *table;
        struct list_head list;
} squeue_t;

typedef struct {
        struct list_head hook;
        void *ent;
} squeue_entry_t;

int squeue_init(squeue_t *queue, int group, int (*cmp_func)(const void *,const void *),
                uint32_t (*hash_func)(const void *));
int squeue_insert(squeue_t *queue, const void *id, void *_ent);
int squeue_get(squeue_t *queue, const void *id, void **_ent);
int squeue_getfirst(squeue_t *queue, void **_ent);
int squeue_remove(squeue_t *queue, const void *id, void **_ent);
int squeue_move_tail(squeue_t *queue, const void *id);
int squeue_pop(squeue_t *queue, void **_ent);

#endif

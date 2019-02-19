#ifndef __ATTR_QUEUE__
#define __ATTR_QUEUE__

#include <stdint.h>

#include "ylib.h"
#include "dbg.h"

int attr_queue_init();
int attr_queue_destroy();
void attr_queue_run(void *var);
int attr_queue_update(const volid_t *volid, const fileid_t *fileid, void *md);
int attr_queue_extern(const volid_t *volid, const fileid_t *fileid, uint64_t size);
int attr_queue_truncate(const volid_t *volid, const fileid_t *fileid, uint64_t size);
int attr_queue_settime(const volid_t *volid, const fileid_t *fileid, const void *setattr);


#endif

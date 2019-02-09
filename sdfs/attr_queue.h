#ifndef __ATTR_QUEUE__
#define __ATTR_QUEUE__

#include <stdint.h>

#include "ylib.h"
#include "dbg.h"

#define ENABLE_ATTR_QUEUE 1

int attr_queue_init();
int attr_queue_update(const fileid_t *fileid, md_proto_t *md);
int attr_queue_extern(const fileid_t *fileid, uint64_t size);
int attr_queue_truncate(const fileid_t *fileid, uint64_t size);
int attr_queue_settime(const fileid_t *fileid, const setattr_t *setattr);


#endif

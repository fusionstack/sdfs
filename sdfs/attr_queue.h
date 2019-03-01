#ifndef __ATTR_QUEUE__
#define __ATTR_QUEUE__

#include <stdint.h>

#include "ylib.h"
#include "yfs_md.h"
#include "dbg.h"

int attr_queue_init();
int attr_queue_destroy();
void attr_queue_run(void *var);
int attr_queue_update(const volid_t *volid, const fileid_t *fileid, void *md);
int attr_queue_extern(const volid_t *volid, const fileid_t *fileid, uint64_t size);
int attr_queue_truncate(const volid_t *volid, const fileid_t *fileid, uint64_t size);
int attr_queue_settime(const volid_t *volid, const fileid_t *fileid, const void *setattr);

int attr_cache_update(const volid_t *volid, const chkid_t *chkid, const md_proto_t *md);
int attr_cache_get(const volid_t *volid, const chkid_t *chkid, md_proto_t *md);

#endif

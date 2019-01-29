#ifndef __MOND_KV_H__
#define __MOND_KV_H__

#include <sys/statvfs.h>

#include "disk_proto.h"
#include "md_proto.h"

int mond_kv_init();
int mond_kv_set(const char *key, const void *value, uint32_t valuelen);
int mond_kv_get(const char *key, int offset, void *entry, uint32_t *valuelen);

#endif

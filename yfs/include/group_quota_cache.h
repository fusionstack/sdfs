#ifndef _G_QUOTA_CACHE_H_
#define _G_QUOTA_CACHE_H_

#include <stdint.h>
#include "sdfs_quota.h"

typedef struct {
        uint64_t volid;
        gid_t gid;
}entry_group_key_t;

typedef struct {
        entry_group_key_t key;
        quota_t quota;
}group_quota_entry_t;

extern int group_quota_cache_init();
extern int group_quota_cache_get(const gid_t gid, const uint64_t volid, cache_entry_t **_cent);
extern int group_quota_cache_update(cache_entry_t *cent, const quota_t *quota);
extern int group_quota_cache_create(const gid_t gid, const uint64_t volid, const quota_t *quota);
extern int group_quota_cache_drop(const gid_t gid, const uint64_t volid);

#endif

#ifndef _D_QUOTA_CACHE_H_
#define _D_QUOTA_CACHE_H_

#include "sdfs_quota.h"

typedef struct {
        uint64_t quotaid;
        quota_t quota;
}dir_quota_entry_t;

extern int dir_quota_cache_init();
extern int dir_quota_cache_get(const uint64_t quotaid, cache_entry_t **_cent);
extern int dir_quota_cache_update(cache_entry_t *cent, const quota_t *quota);
extern int dir_quota_cache_create(const uint64_t quotaid, quota_t *quota);
extern int dir_quota_cache_drop(const uint64_t quotaid);

#endif


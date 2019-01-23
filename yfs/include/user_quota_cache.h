#ifndef _U_QUOTA_CACHE_H_
#define _U_QUOTA_CACHE_H_

#include <stdint.h>
#include "sdfs_quota.h"

typedef struct {
        uint64_t volid;
        uid_t uid;
}entry_user_key_t;

typedef struct {
        entry_user_key_t key;
        quota_t quota;
}user_quota_entry_t;

extern int user_quota_cache_init();
extern int user_quota_cache_get(const uid_t uid, const uint64_t volid, cache_entry_t **_cent);
extern int user_quota_cache_update(cache_entry_t *cent, const quota_t *quota);
extern int user_quota_cache_create(const uid_t uid, const uint64_t volid, const quota_t *quota);
extern int user_quota_cache_drop(const uid_t uid, const uint64_t volid);

#endif

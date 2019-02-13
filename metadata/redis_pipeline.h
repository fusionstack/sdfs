#ifndef __REDIS_PIPELINE__
#define __REDIS_PIPELINE__

#define ENABLE_REDIS_PIPELINE 0

#if ENABLE_REDIS_PIPELINE
int redis_pipeline_init();
int pipeline_hget(const volid_t *volid, const fileid_t *fileid, const char *key, void *buf, size_t *len);
int pipeline_hset(const volid_t *volid, const fileid_t *fileid, const char *key, const void *value, size_t size, int flag);
int pipeline_hdel(const volid_t *volid, const fileid_t *fileid, const char *key);
int pipeline_hlen(const volid_t *volid, const fileid_t *fileid, uint64_t *count);
int pipeline_kget(const volid_t *volid, const fileid_t *fileid, void *buf, size_t *len);
int pipeline_kset(const volid_t *volid, const fileid_t *fileid, const void *value,
                  size_t size, int flag, int _ttl);
int pipeline_kdel(const volid_t *volid, const fileid_t *fileid);
int pipeline_klock(const volid_t *volid, const fileid_t *fileid, int ttl, int block);
int pipeline_kunlock(const volid_t *volid, const fileid_t *fileid);
#endif

#endif

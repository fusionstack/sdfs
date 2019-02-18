#ifndef __REDIS_CO__
#define __REDIS_CO__

int redis_co_init();
void redis_co_destroy();
int redis_co_run(void *ctx);
int co_hget(const volid_t *volid, const fileid_t *fileid, const char *key,
                  void *buf, size_t *len);
int co_hset(const volid_t *volid, const fileid_t *fileid, const char *key,
                  const void *value, size_t size, int flag);
int co_hdel(const volid_t *volid, const fileid_t *fileid, const char *key);
int co_hlen(const volid_t *volid, const fileid_t *fileid, uint64_t *count);
int co_kget(const volid_t *volid, const fileid_t *fileid, void *buf, size_t *len);
int co_kset(const volid_t *volid, const fileid_t *fileid, const void *value,
                  size_t size, int flag, int _ttl);
int co_kdel(const volid_t *volid, const fileid_t *fileid);
int co_klock(const volid_t *volid, const fileid_t *fileid, int ttl, int block);
int co_kunlock(const volid_t *volid, const fileid_t *fileid);
int co_newsharing(const volid_t *volid, uint8_t *idx);

#endif

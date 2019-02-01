#ifndef __REDIS_PIPELINE__
#define __REDIS_PIPELINE__

int redis_pipeline_init();
int pipeline_hget(const fileid_t *fileid, const char *key, void *buf, size_t *len);

#endif

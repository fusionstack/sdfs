#ifndef __REDIS_CONN_H__
#define __REDIS_CONN_H__

#include <hiredis/hiredis.h>
//#include <hircluster.h>
#include "sdfs_id.h"
#include "redis_util.h"
#include "cJSON.h"
#include "zlib.h"

typedef struct {
        int magic;
        int sharding;
        int idx;
        volid_t volid;
        redis_conn_t *conn;
} redis_handler_t;

int redis_conn_init();
int redis_conn_release(const redis_handler_t *handler);
int redis_conn_get(const volid_t *volid, int sharding, int worker, redis_handler_t *handler);
int redis_conn_new(const volid_t *volid, uint8_t *idx);
int redis_conn_close(const redis_handler_t *handler);
int redis_conn_vol(const volid_t *volid);

extern int redis_vol_get(const volid_t *volid, void **conn);
extern int redis_vol_release(const volid_t *volid);
extern int redis_vol_insert(const volid_t *volid, void *conn);
extern int redis_vol_init();

#endif

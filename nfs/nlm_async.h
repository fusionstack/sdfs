#ifndef __NLM4_ASYNC_H__
#define __NLM4_ASYNC_H__


#include <sys/types.h>
#include <sys/stat.h>
#include <rpc/rpc.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>


#define DBG_SUBSYS S_YNFS

#include "yfs_conf.h"
#include "ylib.h"
#include "sdfs_lib.h"

typedef struct {
        int uppid;
        int ownerlen;
        int callerlen;
        char buf[0];
} nlm_ext_t;

#define MAX_LOCK_LEN (sizeof(sdfs_lock_t) + sizeof(nlm_ext_t) + 2048)

int nlm4_async_init();
int nlm4_async_unreg(const fileid_t *fileid, const sdfs_lock_t *lock);
int nlm4_async_reg(const fileid_t *fileid, const sdfs_lock_t *lock);
int nlm4_async_canceled(const fileid_t *fileid, const sdfs_lock_t *lock);
int nlm4_async_cancel(const fileid_t *fileid, const sdfs_lock_t *lock);

#endif

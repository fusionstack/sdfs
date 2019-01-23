#ifndef __YWEB_STATUS_MACHINE_H__
#define __YWEB_STATUS_MACHINE_H__

#include "chk_proto.h"
#include "yfs_chunk.h"
#include "net_global.h"

typedef enum {
        HTTP_REQ_COMING,
        HTTP_REQ_READING,
        HTTP_REP_GET
} yweb_status;

#endif


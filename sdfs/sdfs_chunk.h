#ifndef __SDFS_CHUNK_H__
#define __SDFS_CHUNK_H__

#include <stdint.h>

#include "ylib.h"
#include "chk_proto.h"
#include "net_proto.h"
#include "dbg.h"

int sdfs_chunk_read(const chkid_t *chkid, buffer_t *buf, int count,
                    int offset, const ec_t *ec);
int sdfs_chunk_write(const fileinfo_t *md, const chkid_t *objid,
                     const buffer_t *buf, int count,
                     int offset, const ec_t *ec);
int sdfs_chunk_check(const chkid_t *chkid);
int sdfs_chunk_recovery(const chkid_t *chkid);


#endif 

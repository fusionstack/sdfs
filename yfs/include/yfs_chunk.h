#ifndef __YFS_CHUNK_H__
#define __YFS_CHUNK_H__

#include <sys/types.h>
#include <stdint.h>

#include "chk_proto.h"
#include "cd_proto.h"
#include "ynet_rpc.h"
#include "job.h"

struct yfs_chunk {
        /* filled by mdc */
        chkid_t chkid;
        uint64_t version;
        //uint32_t no;
        uint32_t rep;          /* rep number */
        uint32_t validrep;
        net_handle_t *nid;   /* rep*nid */
        time_t ltime;

        /* filled by ly */
        int loaded;
};

extern void yfs_chunk_dump(struct yfs_chunk *chk);

#endif

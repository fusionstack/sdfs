#ifndef __CHK_META_H__
#define __CHK_META_H__

#include <stdint.h>

#include "chk_proto.h"
#include "file_proto.h"

#define YFS_CDS_MET_OFF (sizeof(uint32_t))

#pragma pack(8)

typedef struct {
        crc_t crc;
        chkid_t chkid;
        uint64_t __pad0__;
        uint64_t chk_version;
        uint32_t chklen;
        uint32_t chkoff;
        uint32_t chkmax;
        uint32_t __pad1__;
        uint32_t status;
        uint64_t seq;
        uint32_t __pad__2;
        uint64_t __pad__3;/*lost version*/
} chkmeta2_t_old;

typedef struct {
        crc_t crc;
        uint64_t chk_version;
        chkid_t chkid;
        uint64_t __pad__1[2];
} chkmeta2_t;

#pragma pack()

//#define YFS_CDS_CHK_OFF (sizeof(chkmeta2_t) + YFS_CDS_CRC_LEN)
#define YFS_CDS_CHK_OFF (PAGE_SIZE * 3)

#define CHUNK_OFFSET (PAGE_SIZE * 2)

#endif

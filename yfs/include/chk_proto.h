#ifndef __CHK_PROTO_H__
#define __CHK_PROTO_H__

#include <stdint.h>

#include "configure.h"
#include "yfs_conf.h"
#include "sdfs_id.h"
#include "file_proto.h"
#include "ylib.h"

/* chunk id 0 means no that chunk, all chunk id begins from 1 */
#define CHKID_NULL 0
#define CHKVOLID_NULL 0
#define CHKID_FROM 1

#define CHKVER_NULL 0

#define CDS_JNL_MAGIC 0xcd5fe2a0

#define YFS_CHK_LEN_MAX   (sizeof(char) * 1024 * 1024 * 64)     /* 64MB */

#define YFS_CRC_SEG_LEN (8192 * 8)

#if 0

#define YFS_CHK_LEN_DEF   (sizeof(char) * 1024 * 1024 * 4)     /* 4MB */

#else

#define YFS_CHK_LEN_DEF   (sizeof(char) * 1024 * 1024 * 64)     /* 64MB */

#endif


#define YFS_CDS_CRC_COUNT (YFS_CHK_LEN_MAX / YFS_CRC_SEG_LEN) 
#define YFS_CDS_CRC_LEN (YFS_CDS_CRC_COUNT * sizeof(uint32_t))

#pragma pack(8)

typedef enum {
        CHKOP_WRITE = 1,
        CHKOP_DEL = 2,
        CHKOP_TRUNC = 3,
} chkop_type_t;

typedef struct {
        chkid_t chkid;
        uint64_t max_version;
} chkproto_t;

typedef struct {
        chkop_type_t op;
        int32_t increase;
        uint32_t time;
        uint32_t __pad__[3];
        chkid_t chkid;
        uint64_t __pad1__;
} chkjnl_t;

typedef struct {
        chkid_t chkid;
        uint32_t mtime;
        uint64_t version;
        uint32_t offset;
        uint32_t size; /*for write*/
        time_t ltime;
} chkop_t; 

#pragma pack()

#endif

#ifndef __DIR_H__
#define __DIR_H__

#include "md_proto.h"
#include "file_proto.h"
#include "sdfs_lib.h"

#pragma pack(8)

typedef struct {
        fileid_t fileid;
        //uint32_t d_off;
        //uint16_t d_reclen;
        uint16_t d_type;
} dir_entry_t;

#pragma pack()

#endif

#ifndef __YFS_DIR_H__
#define __YFS_DIR_H__

#include "dir.h"

typedef struct {
        fileid_t dirid;
        dirkey_t dir_cached; 
        uint64_t idx_cached;
} dir_t;

#endif

#ifndef __ALIGN_H__
#define __ALIGN_H__

#include "ylib.h"

#define __ALIGN__ 8

#pragma pack(8)

typedef struct {
        uint64_t len;
        char buf[0];
} abuf_t;

#pragma pack()

#endif

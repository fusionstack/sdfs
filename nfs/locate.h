#ifndef __LOCATE_H__
#define __LOCATE_H__

#include <stdint.h>

extern int locate_pfx(const char *pfx, uint32_t dev, uint64_t ino, char *result);

#endif

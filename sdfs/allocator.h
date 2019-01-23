#ifndef __ALLOCATOR__
#define __ALLOCATOR__

#include <stdint.h>

#include "ylib.h"
#include "dbg.h"

int allocator_init();
int allocator_new(int repnum, int hardend, int tier, nid_t *disks);

#endif

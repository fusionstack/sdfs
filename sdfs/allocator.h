#ifndef __ALLOCATOR__
#define __ALLOCATOR__

#include <stdint.h>

#include "ylib.h"
#include "dbg.h"

#define ENABLE_ALLOCATE_BALANCE 1

int allocator_init();
int allocator_new(int repnum, int hardend, int tier, nid_t *disks);

#endif

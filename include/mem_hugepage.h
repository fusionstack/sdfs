#ifndef __MEM_HUGEPAGE_H_
#define __MEM_HUGEPAGE_H_

#include "sdfs_buffer.h"

int mem_hugepage_init();
int mem_hugepage_private_init();  // used by core thread mbuffer_t
int mem_hugepage_private_destoy();

int mem_hugepage_new(uint32_t size, mem_handler_t *mem_handler);

int mem_hugepage_ref(mem_handler_t *mem_handler);
int mem_hugepage_deref(mem_handler_t *mem_handler);

#endif

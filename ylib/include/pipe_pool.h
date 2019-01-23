#ifndef __PIPE_POOL_H__
#define __PIPE_POOL_H__

#include <stdint.h>
#include <errno.h>


extern int ppool(int idx, int io);
int ppool_put(int *no, int len);
int ppool_get();
int ppool_init(int size);
void ppool_status();

#endif

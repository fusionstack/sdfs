#ifndef __BITMAP_H__
#define __BITMAP_H__

#include "sdfs_conf.h"
#include "dbg.h"

int bh_init();
int bh_register(const char *name, int (*_exec)(void *), void *args, int step);

#endif

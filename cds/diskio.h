#ifndef __DISKIO_H__
#define __DISKIO_H__

#include <libaio.h>
#include "ylib.h"

int diskio_submit(struct iocb *iocb, func1_t func);
int diskio_init();

#endif

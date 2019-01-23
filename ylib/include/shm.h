
#ifndef __SHM_H__
#define __SHM_H__

#include "sdfs_conf.h"

int shm_init(const char *name);
int shm_new(int *_fd, off_t *offset, void **addr, uint32_t size);
int shm_ref(int fd);
int shm_unref(int fd);

#endif

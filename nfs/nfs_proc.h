#ifndef __NFS_PROC_H__
#define __NFS_PROC_H__

#include "fnotify.h"

extern sy_spinlock_t nfs_conf_lock;
void *handler_ynfsstate(void *arg);
int __nfs_conf_register(fnotify_callback mod_callback, fnotify_callback del_callback, void *contex);
int __conf_modified(void *contex, uint32_t mask);
int __conf_delete(void *contex, uint32_t mask);

#endif

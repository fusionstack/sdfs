#ifndef __FNOTIFY_H__
#define __FNOTIFY_H__

#include <stdint.h>
#include "sdfs_conf.h"

typedef int (*fnotify_callback)(void *context, uint32_t mask);

int fnotify_register(const char *path, fnotify_callback mod_callback,
                     fnotify_callback del_callback, void *context);
int fnotify_create(const char *path, const char *value,
                       int (*callback)(const char *buf, uint32_t flag),
                       uint32_t flag);
int fnotify_unregister(const char *path);
int fnotify_init(void);

int quorum_fnotify_register(const char *path, fnotify_callback mod_callback, fnotify_callback del_callback, void *context);
int quorum_fnotify_unregister(int md);

#endif

#ifndef __NODECTL_H__
#define __NODECTL_H__

#include <sys/epoll.h>
#include <semaphore.h>
#include <linux/aio_abi.h> 
#include <pthread.h>

#include "fnotify.h"


int nodectl_get_int(const char *key, const char *_default);
int nodectl_get(const char *key, char *value, const char *_default);       
int nodectl_set(const char *key, const char *value);
int nodectl_register(const char *key, const char  *_default, fnotify_callback mod_callback,
                     fnotify_callback del_callback, void *context);
int nodectl_unregister(const char *key);
void nodectl_unlink(const char *key);

#endif



#ifndef __LICH_AIO__
#define __LICH_AIO__

#include <sys/epoll.h>
#include <semaphore.h>
#include <libaio.h> 
#include <pthread.h>

int aio_getevent();
void aio_submit();
int aio_commit(struct iocb *iocb, int prio);
int aio_create(const char *name, int cpu, int *_eventfd);
void aio_destroy();

#endif

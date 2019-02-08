#ifndef __LICH_AIO__
#define __LICH_AIO__

#include <sys/epoll.h>
#include <semaphore.h>
#include <libaio.h> 
#include <pthread.h>

/**
 * 每个core线程,创建一个aio线程和队列
 *
 * @param name
 * @param cpu
 * @param _eventfd
 * @return
 */

#define AIO_MODE_SYNC 0
#define AIO_MODE_DIRECT 1
#define AIO_MODE_SIZE 2

int aio_create(const char *name, int cpu);
void aio_destroy();
int aio_commit(struct iocb *iocb, size_t size, int prio, int mode);

/**
 * @brief 相当于flush或sync操作,在一次调度器循环中,提交剩余的iocb
 *
 * @see aio_commit 配合使用
 */
void aio_submit();

/**
 * @brief iocb事件完成后,唤醒相应的task
 *
 * @return
 */

#endif

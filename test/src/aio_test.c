

#include <sys/statvfs.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <semaphore.h>
#include <pthread.h>
#include <errno.h>
#include <signal.h>
#include <aio.h>

#define DBG_SUBSYS S_YFSLIB

#include "sdfs_lib.h"
#include "dbg.h"
#include "ylib.h"

int fd, fd_out;
void *ptr;
off_t size = 3114787;


int open_callback(void *arg)
{
        fd = *(int *)arg;

        DBUG("file opened\n");
        return 0;
}

void splice_callback(union sigval foo)
{
        (void) foo;

        _write(fd_out, ptr, size);

        close(fd_out);

        exit(0);
}

int main ()
{
        int ret;
        off_t off_in = 0;
//        off_t size = 64546643;
        struct aiocb iocb;
        (void) size;
        (void) off_in;
        (void) ret;
        ly_init(0);

        sleep(1);

        ret = ymalloc(&ptr, size);
        if (ret) {
                DERROR("malloc error\n");
                exit(1);
        }

        fd = ly_open("/zz/data/31m");

        fd_out = open("31m", O_CREAT | O_EXCL | O_RDWR,
                  S_IRUSR | S_IWUSR | S_IRGRP);

        iocb.aio_fildes = fd;
        iocb.aio_nbytes = size;
        iocb.aio_offset = 0;
        iocb.aio_buf = ptr;
        iocb.aio_sigevent.sigev_notify_function = splice_callback;
        iocb.aio_sigevent.sigev_notify_attributes = NULL;
        iocb.aio_sigevent.sigev_value.sival_ptr = NULL;
        iocb.aio_sigevent.sigev_notify = SIGEV_THREAD;

        ret = ly_read_aio(&iocb);

        sleep(100);

        close(fd_out);

        printf("ok\n");

        sleep(1000);

        return 0;
}

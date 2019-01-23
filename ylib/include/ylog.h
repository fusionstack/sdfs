#ifndef __YLOG_H__
#define __YLOG_H__

#include <stdint.h>
#ifndef __CYGWIN__
#include <linux/limits.h>
#endif
#include <semaphore.h>

#include "ylock.h"
#include "sdfs_list.h"

typedef enum {
        YLOG_STDERR,
        YLOG_FILE,
        YLOG_SYSLOG,
} logmode_t;

typedef struct {
        int logfd;
        int count;
        time_t time;
        sy_rwlock_t lock;
        logmode_t logmode;
} ylog_t;

typedef enum {
        YLOG_TYPE_STD, /*standard log optput*/
        YLOG_TYPE_PERF, /*performance log optput*/
        YLOG_TYPE_BALANCE, /*balance log type*/
        YLOG_TYPE_RAMDISK, /*ramdisk log type, record each io crc */
        YLOG_TYPE_MAX,
} logtype_t;

extern ylog_t *ylog;

extern int ylog_init(logmode_t, const char *file);
extern int ylog_destroy(void);
extern int ylog_write(logtype_t type, const char *msg);

#define YROC_ROOT "/dev/shm/uss/proc"
#define FILEMODE (S_IREAD | S_IWRITE | S_IRGRP | S_IROTH)

int yroc_create(const char *, int *);
int yroc_write(int, const void *, size_t);

#endif

#ifndef __IOTEST_H__
#define __IOTEST_H__

#include <limits.h>
#include <stdint.h>
#include <linux/types.h>

typedef struct _arg {
        char    *dir;
        int     number;
        int     size;
	uint64_t total;
} pthread_arg;

void    *thr_fn(void *);

void    write_test(char *, int, int);
void    read_test (char *);

#endif

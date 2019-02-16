

#include <stdint.h>
#include <sys/time.h>
#include <time.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "ylib.h"
#include "ytime.h"
#include "dbg.h"

#define TIME_ZONE 8

ytime_t ytime_gettime()
{
        int ret;
        struct timeval tv;

        ret = _gettimeofday(&tv, NULL);
        if (ret)
                YASSERT(0);

        return tv.tv_sec * 1000000 + tv.tv_usec;
}

int ytime_getntime(struct timespec *ntime)
{
        int ret;
        struct timeval tv;

        ret = _gettimeofday(&tv, NULL);
        if (ret)
                GOTO(err_ret, ret);

        ntime->tv_sec = tv.tv_sec;
        ntime->tv_nsec = tv.tv_usec * 1000;

        return 0;
err_ret:
        return ret;
}

void ytime_2ntime(ytime_t ytime, struct timespec *ntime)
{
        ntime->tv_sec = ytime / 1000000;
        ntime->tv_nsec = (ytime % 1000000) * 1000;
}



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

struct tm *localtime_safe(time_t *_time, struct tm *tm_time)
{
        time_t time = *_time;
        long timezone = TIME_ZONE;
        const char Days[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        uint32_t n32_Pass4year;
        uint32_t n32_hpery;

        time = time + (timezone * 60 * 60);

        if(time < 0)
        {
                time = 0;
        }
        tm_time->tm_sec=(int)(time % 60);
        time /= 60;
        tm_time->tm_min=(int)(time % 60);
        time /= 60;
        n32_Pass4year=((unsigned int)time / (1461L * 24L));
        tm_time->tm_year=(n32_Pass4year << 2)+70;
        time %= 1461L * 24L;
        for (;;)
        {
                n32_hpery = 365 * 24;
                if ((tm_time->tm_year & 3) == 0)
                {
                        n32_hpery += 24;
                }
                if (time < n32_hpery)
                {
                        break;
                }
                tm_time->tm_year++;
                time -= n32_hpery;
        }
        tm_time->tm_hour=(int)(time % 24);
        time /= 24;
        time++;
        if ((tm_time->tm_year & 3) == 0)
        {
                if (time > 60)
                {
                        time--;
                }
                else
                {
                        if (time == 60)
                        {
                                tm_time->tm_mon = 1;
                                tm_time->tm_mday = 29;
                                return tm_time;
                        }
                }
        }
        for (tm_time->tm_mon = 0; Days[tm_time->tm_mon] < time;tm_time->tm_mon++)
        {
                time -= Days[tm_time->tm_mon];
        }

        tm_time->tm_mday = (int)(time);
        return tm_time;
}

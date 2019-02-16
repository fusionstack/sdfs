#include <sys/mman.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <time.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <sys/vfs.h>
#include <ustat.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <unistd.h>
#include <netdb.h>
#include <pthread.h>


#define DBG_SUBSYS S_LIBYLIB

#include "sdfs_conf.h"
#include "sysutil.h"
#include "configure.h"
#include "schedule.h"
#include "adt.h"
//#include "sdfs.h"
#include "ylib.h"
#include "analysis.h"
#include "dbg.h"

#define TIME_ZONE 8

typedef struct {
        time_t time;
        struct timeval tv;
        uint32_t cycle;
} gettime_t;

static __thread gettime_t *__gettime__ = NULL;

#define GETTIME_CYCLE 10
#define GETTIME_CYCLE_PRINT 100000000

void  gettime_refresh()
{
        gettime_t *gettime = __gettime__;

        YASSERT(gettime);
        gettime->cycle++;
        if (unlikely((gettime->cycle % GETTIME_CYCLE == 0) || gloconf.performance_analysis))
                gettimeofday(&__gettime__->tv, NULL);

        if (gettime->cycle % GETTIME_CYCLE_PRINT == 0) {
                DINFO("gettime cycle\n");
        }
}

time_t gettime()
{
        gettime_t *gettime = __gettime__;
        
        if (likely(gettime && (gloconf.polling_timeout == 0 || gloconf.rdma))) {
                return gettime->tv.tv_sec;
        } else {
                return time(NULL);
        }
}

int _gettimeofday(struct timeval *tv, struct timezone *tz)
{
        gettime_t *gettime = __gettime__;

        (void) tz;
        
        if (likely(gettime && (gloconf.polling_timeout == 0 || gloconf.rdma))) {
                *tv = gettime->tv;
        } else {
                gettimeofday(tv, NULL);
        }

        return 0;
}

int gettime_private_init()
{
        int ret;
        
        YASSERT(__gettime__ == NULL);

        ret = ymalloc((void **)&__gettime__, sizeof(*__gettime__));
        if (ret)
                GOTO(err_ret, ret);

        memset(__gettime__, 0x0, sizeof(*__gettime__));
        gettimeofday(&__gettime__->tv, NULL);
        
        return 0;
err_ret:
        return ret;
}

struct tm *localtime_safe(time_t *_time, struct tm *tm_time)
{
        time_t time = *_time;
        long timezone = TIME_ZONE;
        const char Days[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        uint32_t n32_Pass4year;
        uint32_t n32_hpery;

        time=time + (timezone * 60 * 60);

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

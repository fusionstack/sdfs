#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "dbg.h"
#include "fnotify.h"
#include "ylib.h"
#include "sysutil.h"

int __d_info__ = __D_INFO;
int __d_goto__ = 1;
int __syslog_interval__ = 10;
int __d_bug__ = 0;
int __d_level__ = __D_FATAL | __D_ERROR | __D_WARNING | __D_INFO;
int __shutdown__  = 0;
uint32_t ylib_dbg = 0;
uint32_t ylib_sub = 0;

void dbg_sub_init()
{
        ylib_dbg = ~0;
        ylib_sub = 0;
}

void dbg_info(int i)
{
#if 1
        if (gloconf.testing) {
                __d_info__ = __D_INFO;
        } else {
                __d_info__ = i;
        }
#else
        __d_info__ = i;
#endif
}

void dbg_goto(int i)
{
        __d_goto__ = i;
}

void dbg_bug(int i)
{
        __d_bug__ = i ? __D_BUG : 0;

}

void dbg_level(int i)
{
        switch(i) {
                case 1:
                        __d_level__ = __D_FATAL | __D_ERROR | __D_WARNING | __D_INFO | __D_BUG;
                        break;
                case 2:
                        __d_level__ = __D_FATAL | __D_ERROR | __D_WARNING | __D_INFO;
                        break;
                case 3:
                        __d_level__ = __D_FATAL | __D_ERROR | __D_WARNING;
                        break;
                case 4:
                        __d_level__ = __D_FATAL | __D_ERROR;
                        break;
                case 5:
                        __d_level__ = __D_FATAL;
                        break;
                default:
                        break;
        }
}

static int __dmsg_init(const char *path, const char *value,
                       int (*callback)(const char *buf, uint32_t flag),
                       uint32_t flag)
{
        int ret;

        ret = fnotify_create(path, value, callback, flag);
        if (ret) {
                if (ret == ENOENT) {
                        //pass
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __dmsg_goto(const char *buf, uint32_t extra)
{
        (void) extra;
        DINFO("set goto %s\n", buf);

        if (strcmp(buf, "0") == 0) {
                dbg_goto(0);
        } else {
                dbg_goto(1);
        }

        return 0;
}

inline static int __dmsg_level(const char *buf, uint32_t extra)
{
        int level;

        (void) extra;
        DINFO("set level %s\n", buf);

        level = atoi(buf);
        if (level >= 1 && level <= 5) {
                dbg_level(level);
        } else {
                DERROR("set level %d fail, must in 1~5\n", level);
        }

        return 0;
}

inline static int __dmsg_syslog_inteval(const char *buf, uint32_t extra)
{
        (void) extra;
        DINFO("set syslog inteval to %s\n", buf);

        __syslog_interval__  = atoi(buf);

        return 0;
}

inline static int __dmsg_sub(const char *buf, uint32_t extra)
{       
        int ret, on;

        on = atoi(buf);

        if (on == 0) {
                ylib_sub &= ~extra;
        } else if (on == 1) {
                ylib_sub |= extra;
        } else {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        DINFO("fnotify file modified: mask (0x%08x) %s\n", extra, on ? "on" : "off");

        return 0;
err_ret:
        return ret;
}       

int dmsg_init()
{
        int ret;

        DBUG("dmsg init\n");

        if (gloconf.backtrace) {
                ret = __dmsg_init(DGOTO_PATH, "1", __dmsg_goto, 0);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                //dbg_goto(1);
        } else {
                ret = __dmsg_init(DGOTO_PATH, "0", __dmsg_goto, 0);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                //dbg_goto(0);
        }

#if 0
        ret = __dmsg_init(DLEVEL_PATH, "1", __dmsg_level, 1);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __dmsg_init(SYSLOG_INTEVAL_PATH, "10", __dmsg_syslog_inteval, 10);
        if (ret)
                GOTO(err_ret, ret);

        ret = __dmsg_init(DBUG_YLIB_PATH, "0", __dmsg_sub, S_LIBYLIB);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __dmsg_init(DBUG_SCHEDULE_PATH, "0", __dmsg_sub, S_LIBSCHEDULE);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ret = __dmsg_init(DBUG_YNET_PATH, "0", __dmsg_sub, S_LIBYNET);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __dmsg_init(DBUG_CLUSTER_PATH, "0", __dmsg_sub, S_LIBCLUSTER);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __dmsg_init(DBUG_INTERFACE_PATH, "0", __dmsg_sub, S_LIBINTERFACE);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __dmsg_init(DBUG_LSV_PATH, "0", __dmsg_sub, S_LIBLSV);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ret = __dmsg_init(DBUG_REPLICA_PATH, "0", __dmsg_sub, S_LIBREPLICA);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __dmsg_init(DBUG_CHUNK_PATH, "0", __dmsg_sub, S_LIBCHUNK);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __dmsg_init(DBUG_CONTROL_PATH, "0", __dmsg_sub, S_LIBCONTROL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = __dmsg_init(DBUG_STORAGE_PATH, "0", __dmsg_sub, S_LIBSTORAGE);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ret = __dmsg_init(DBUG_TASK_PATH, "0", __dmsg_sub, S_LIBTASK);
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif
        
        return 0;
err_ret:
        return ret;
}

void  __attribute__((noinline)) dbg_ylog_write(const int logtype, const int size, const int mask,
                const char *filename, const int line, const char *function,
                const char *format, ...)
{
        va_list arg;
        time_t __t = gettime();
        char *__d_msg_buf, *__d_msg_info, *__d_msg_time;
        struct tm __tm;
        int _s_id_, _taskid_, _rq_, _r_, _w_, _c_;

        (void) mask;
        
        /**
         * | __d_msg_buf(size) | __d_msg_info(size) | __d_msg_time(32) |
         */
        __d_msg_buf = malloc(size * 2 + 32);
        __d_msg_info = (void *)__d_msg_buf + size;
        __d_msg_time = (void *)__d_msg_buf + size * 2;

        va_start(arg, format);
        vsnprintf(__d_msg_info, size, format, arg);
        va_end(arg);

        strftime(__d_msg_time, 32, "%F %T", localtime_safe(&__t, &__tm));
        schedule_stat(&_s_id_, &_taskid_, &_rq_, &_r_, &_w_, &_c_);

        snprintf(__d_msg_buf, size,
                        "%s/%lu %lu/%lu %d/%d %d/%d/%d/%d %s:%d %s %s",
                        __d_msg_time, (unsigned long)__t,
                        (unsigned long)getpid( ),
                        (unsigned long)syscall(SYS_gettid),
                        _s_id_, _taskid_, _rq_, _r_, _w_, _c_,
                        filename, line, function,
                        __d_msg_info);

        (void) ylog_write(logtype, __d_msg_buf);

        free(__d_msg_buf);
}

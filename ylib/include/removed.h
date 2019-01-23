#ifndef __REMOVED__
#define __REMOVED__

extern int srv_running;

/**********************need move **********************************/
#ifndef likely
#define likely(x)       __builtin_expect(!!(x), 1)
#endif

#ifndef unlikely
#define unlikely(x)     __builtin_expect(!!(x), 0)
#endif

#define SHM_ROOT "/dev/shm/sdfs"

int rdma_running;

static inline void _fence_test1()
{
        return;
}

static inline int _fence_test()
{
        return 0;
}

static inline void _fence_test1_init(const char *arg)
{
        (void) arg;
        return;
}

static inline void time2str(char *date_time, time_t *unix_time)
{
        struct tm *tm_time;

        tm_time = localtime(unix_time);
        strftime(date_time, 24, "%Y-%m-%d %H:%M:%S", tm_time);
}

#ifndef bool
#define bool char
#endif

#ifndef true
#define true 1
#endif
#ifndef false
#define false 0
#endif

int _open_for_samba(const char *path, int flag, mode_t mode);
int _set_value_for_samba(const char *path, const char *value, int size, int flag);
int is_digit_str(const char *str);
int date_check(const char *date_time);
bool is_valid_name (const char *name);
void str2time(const char *date_time, uint32_t *unix_time);
int pattern_ismatch(const char *pattern, const char *compare, int *match);
bool is_valid_password(const char *name);
int _popen(const char *cmd);
void exit_handler(int sig);
int select_popen_fd(int fd, long timeout);
int sy_is_mountpoint(const char *path, long f_type);

/**********************need move **********************************/

#endif



#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <stdint.h>

#define DBG_SUBSYS S_LIBYLIB

#include "sysutil.h"
#include "ytime.h"

void mode_to_permstr(uint32_t mode, char *perms)
{
        _memset(perms, '-', 11);

        perms[0] = '?';

        switch (mode & S_IFMT) {
        case S_IFREG:
                perms[0] = '-';
                break;
        case S_IFDIR:
                perms[0] = 'd';
                break;
        case S_IFLNK:
                perms[0] = 'l';
                break;
        case S_IFIFO:
                perms[0] = 'p';
                break;
        case S_IFSOCK:
                perms[0] = 's';
                break;
        case S_IFCHR:
                perms[0] = 'c';
                break;
        case S_IFBLK:
                perms[0] = 'b';
                break;
        }

        if (mode & S_IRUSR)
                perms[1] = 'r';
        if (mode & S_IWUSR)
                perms[2] = 'w';
        if (mode & S_IXUSR)
                perms[3] = 'x';
        if (mode & S_IRGRP)
                perms[4] = 'r';
        if (mode & S_IWGRP)
                perms[5] = 'w';
        if (mode & S_IXGRP)
                perms[6] = 'x';
        if (mode & S_IROTH)
                perms[7] = 'r';
        if (mode & S_IWOTH)
                perms[8] = 'w';
        if (mode & S_IXOTH)
                perms[9] = 'x';
        if (mode & S_ISUID)
                perms[3] = (perms[3] == 'x') ? 's' : 'S';
        if (mode & S_ISGID)
                perms[6] = (perms[6] == 'x') ? 's' : 'S';
        if (mode & S_ISVTX)
                perms[9] = (perms[9] == 'x') ? 't' : 'T';
        perms[10] = '\0';
}

void stat_to_datestr(struct stat *stbuf, char *date)
{
        struct timeval local_time;;
        struct tm tm;
        const char *date_fmt = "%b %d %H:%M";

        _gettimeofday(&local_time, NULL);

        /*
         * The gmtime() function converts the calendar time timep
         * to broken-down time representation,
         * expressed  in  Coordinated  Universal Time  (UTC).
        */
        //tm = gmtime(&stbuf->st_mtime);

        localtime_safe(&stbuf->st_mtime, &tm);

        /* is this a future or 6 months old date?
           If so, we drop to year format
        */
        if (stbuf->st_mtime > local_time.tv_sec
            || (local_time.tv_sec - stbuf->st_mtime) > 60*60*24*182)
                date_fmt = "%b %d  %Y";

        strftime(date, 64, date_fmt, &tm);

        date[63] = '\0';
}

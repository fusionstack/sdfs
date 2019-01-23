

#include <sys/time.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <errno.h>

#define DBG_SUBSYS S_YWEB

#include "sysutil.h"
#include "dbg.h"

struct strlong {
        char *s;
        long l;
};

extern int srv_running;

static int is_leap(int year)
{
        return year % 400? (year % 100 ? (year % 4 ? 0 : 1) : 0) : 1;
}

/* basically the same as mktime() */
void tm_to_time(struct tm* tm, time_t *tv)
{
        time_t t;
        static int monthtab[12] = {
                0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

        /* years since epoch, converted to days */
        t = (tm->tm_year - 70) * 365;

        /* leap days for previous years */
        t += (tm->tm_year - 69) / 4;

        /* days for the beginning of this month */
        t += monthtab[tm->tm_mon];

        /* leap day for this year */
        if (tm->tm_mon >= 2 && is_leap(tm->tm_year + 1900))
                ++t;

        /* days since the beginning of this month */
        t += tm->tm_mday - 1;   /* 1-based field */

        /* hours, minutes, and seconds */
        t = t * 24 + tm->tm_hour;
        t = t * 60 + tm->tm_min;
        t = t * 60 + tm->tm_sec;

        *tv = t;
}

static int strlong_search(char *str, struct strlong *tab, int n, long *num)
{
        int ret, i, h, l, r;

        l = 0;
        h = n - 1;

        while (srv_running) {
                i = (h + l) / 2;

                r = _strcmp(str, tab[i].s);
                if (r < 0)
                        h = i - 1;
                else if (r > 0)
                        l = i + 1;
                else {
                        *num = tab[i].l;
                        break;
                }

                if (h < l) {
                        ret = ENOENT;
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

static void pound_case(char *str)
{
        for (; *str != '\0'; ++str) {
                if (isupper((int)*str))
                        *str = tolower((int)*str);
        }
}

static int strlong_cmp(const void *v1, const void *v2)
{
        return _strcmp(((struct strlong*)v1)->s, ((struct strlong*)v2)->s);
}

static int scan_mon(char *str_mon, long *tm_mon)
{
        int ret;
        static struct strlong mon_tab[] = {
                {"jan", 0}, {"january", 0},
                {"feb", 1}, {"february", 1},
                {"mar", 2}, {"march", 2},
                {"apr", 3}, {"april", 3},
                {"may", 4},
                {"jun", 5}, {"june", 5},
                {"jul", 6}, {"july", 6},
                {"aug", 7}, {"august", 7},
                {"sep", 8}, {"september", 8},
                {"oct", 9}, {"october", 9},
                {"nov", 10}, {"november", 10},
                {"dec", 11}, {"december", 11},
        };
        static int sorted = 0;

        if (!sorted) {
                qsort(mon_tab, sizeof(mon_tab)/sizeof(struct strlong),
                      sizeof(struct strlong), strlong_cmp);
                sorted = 1;
        }

        pound_case(str_mon);

        ret = strlong_search(str_mon, mon_tab,
                             sizeof(mon_tab)/sizeof(struct strlong), tm_mon);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int scan_wday(char *str_wday, long *tm_wday)
{
        int ret;
        static struct strlong wday_tab[] = {
                {"sun", 0}, {"sunday", 0},
                {"mon", 1}, {"monday", 1},
                {"tue", 2}, {"tuesday", 2},
                {"wed", 3}, {"wednesday", 3},
                {"thu", 4}, {"thursday", 4},
                {"fri", 5}, {"friday", 5},
                {"sat", 6}, {"saturday", 6},
        };
        static int sorted = 0;

        if (!sorted) {
                qsort(wday_tab, sizeof(wday_tab)/sizeof(struct strlong),
                      sizeof(struct strlong), strlong_cmp);
                sorted = 1;
        }

        pound_case(str_wday);

        ret = strlong_search(str_wday, wday_tab,
                             sizeof(wday_tab)/sizeof(struct strlong), tm_wday);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int tdate_parse(char* str, time_t *tv)
{
        struct tm tm;
        char *cp, str_mon[500], str_wday[500];
        int tm_sec, tm_min, tm_hour, tm_mday, tm_year;
        long tm_mon, tm_wday;

        /* initialize */
        _memset((char *)&tm, 0x0, sizeof(struct tm));

        /* skip initial whitespace ourselves - sscanf() is clumsy at this */
        for (cp = str; *cp == ' ' || *cp == '\t'; ++cp)
                continue;

        /*
         * and do the sscanfs. WARNING: you can add more formats here,
         * but be careful! You can easily screw up the parsing of existing
         * formats when you add new ones. The order is important.
         */

        /* DD-mth-YY HH:MM:SS GMT */
        if (sscanf(cp, "%d-%400[a-zA-Z]-%d %d:%d:%d GMT", &tm_mday, str_mon,
                   &tm_year, &tm_hour, &tm_min, &tm_sec )
            == 6 && scan_mon(str_mon, &tm_mon) == 0) {
                tm.tm_mday = tm_mday;
                tm.tm_mon = tm_mon;
                tm.tm_year = tm_year;
                tm.tm_hour = tm_hour;
                tm.tm_min = tm_min;
                tm.tm_sec = tm_sec;
        } else /* DD mth YY HH:MM:SS GMT */
               if (sscanf(cp, "%d %400[a-zA-Z] %d %d:%d:%d GMT", &tm_mday,
                          str_mon, &tm_year, &tm_hour, &tm_min, &tm_sec)
                   == 6 && scan_mon(str_mon, &tm_mon) == 0) {
                tm.tm_mday = tm_mday;
                tm.tm_mon = tm_mon;
                tm.tm_year = tm_year;
                tm.tm_hour = tm_hour;
                tm.tm_min = tm_min;
                tm.tm_sec = tm_sec;
        } else /* HH:MM:SS GMT DD-mth-YY */
               if (sscanf(cp, "%d:%d:%d GMT %d-%400[a-zA-Z]-%d", &tm_hour,
                          &tm_min, &tm_sec, &tm_mday, str_mon, &tm_year)
                   == 6 && scan_mon(str_mon, &tm_mon) == 0) {
                tm.tm_hour = tm_hour;
                tm.tm_min = tm_min;
                tm.tm_sec = tm_sec;
                tm.tm_mday = tm_mday;
                tm.tm_mon = tm_mon;
                tm.tm_year = tm_year;
        } else /* HH:MM:SS GMT DD mth YY */
               if (sscanf(cp, "%d:%d:%d GMT %d %400[a-zA-Z] %d", &tm_hour,
                          &tm_min, &tm_sec, &tm_mday, str_mon, &tm_year)
                   == 6 && scan_mon(str_mon, &tm_mon) == 0) {
                tm.tm_hour = tm_hour;
                tm.tm_min = tm_min;
                tm.tm_sec = tm_sec;
                tm.tm_mday = tm_mday;
                tm.tm_mon = tm_mon;
                tm.tm_year = tm_year;
        } else /* wdy, DD-mth-YY HH:MM:SS GMT */
               if (sscanf(cp, "%400[a-zA-Z], %d-%400[a-zA-Z]-%d %d:%d:%d GMT",
                          str_wday, &tm_mday, str_mon, &tm_year, &tm_hour,
                          &tm_min, &tm_sec)
                   == 7 && scan_wday(str_wday, &tm_wday) == 0
                   && scan_mon(str_mon, &tm_mon) == 0) {
                tm.tm_wday = tm_wday;
                tm.tm_mday = tm_mday;
                tm.tm_mon = tm_mon;
                tm.tm_year = tm_year;
                tm.tm_hour = tm_hour;
                tm.tm_min = tm_min;
                tm.tm_sec = tm_sec;
        } else /* wdy, DD mth YY HH:MM:SS GMT */
               if (sscanf(cp, "%400[a-zA-Z], %d %400[a-zA-Z] %d %d:%d:%d GMT",
                          str_wday, &tm_mday, str_mon, &tm_year, &tm_hour,
                          &tm_min, &tm_sec)
                   == 7 && scan_wday(str_wday, &tm_wday) == 0
                   && scan_mon(str_mon, &tm_mon) == 0) {
                tm.tm_wday = tm_wday;
                tm.tm_mday = tm_mday;
                tm.tm_mon = tm_mon;
                tm.tm_year = tm_year;
                tm.tm_hour = tm_hour;
                tm.tm_min = tm_min;
                tm.tm_sec = tm_sec;
        } else /* wdy mth DD HH:MM:SS GMT YY */
               if (sscanf(cp, "%400[a-zA-Z] %400[a-zA-Z] %d %d:%d:%d GMT %d",
                          str_wday, str_mon, &tm_mday, &tm_hour, &tm_min,
                          &tm_sec, &tm_year)
                   == 7 && scan_wday(str_wday, &tm_wday) == 0
                   && scan_mon(str_mon, &tm_mon) == 0) {
                tm.tm_wday = tm_wday;
                tm.tm_mon = tm_mon;
                tm.tm_mday = tm_mday;
                tm.tm_hour = tm_hour;
                tm.tm_min = tm_min;
                tm.tm_sec = tm_sec;
                tm.tm_year = tm_year;
        } else
                *tv = (time_t)-1;

        if (tm.tm_year > 1900)
                tm.tm_year -= 1900;
        else if (tm.tm_year < 70)
                tm.tm_year += 100;

        tm_to_time(&tm, tv);

        return 0;
}

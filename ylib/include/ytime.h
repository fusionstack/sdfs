#ifndef __YTIME_H__
#define __YTIME_H__

#define ytime_t uint64_t /*time in microsecond*/

ytime_t ytime_gettime();
int ytime_getntime(struct timespec *ntime);
void ytime_2ntime(ytime_t ytime, struct timespec *ntime);
struct tm *localtime_safe(time_t *_time, struct tm *tm_time);

#endif

#ifndef __YWEB_CONF_H__
#define __YWEB_CONF_H__

#define YWEB_SERVICE_DEF "80"
#define YWEB_QLEN_DEF  256

#define YWEB_LOCKFILE "/dev/shm/uss/yweb/lock/yweb"
#define YWEB_PIDFILE "/dev/shm/uss/yweb/lock/yweb.pid"

#define YWEB_LOGFILE "/var/log/yweb.log"

#define YWEB_EPOLL_SIZE 1024

#define YWEB_THR_MAX 1024

/* "\012" "\n" */
/* "\015" "\r" */

#define HTTP_REQ_END1 "\015\012\015\012"
#define HTTP_REQ_END2 "\012\012"

#define YWEB_CHARSET_DEF "iso-8859-1"

#define SERVER_SOFTWARE "httpd/uss-1.9.10"
#define SERVER_URL "http://www.meidisen.com/"

#endif

#ifndef __YFTP_CONF_H__
#define __YFTP_CONF_H__

#define YFTP_SERVICE_DEF "21"
#define YFTP_QLEN_DEF  256

#define YFTP_LOCKFILE "/dev/shm/uss/yftp/lock/yftp"
#define YFTP_PIDFILE "/dev/shm/uss/yftp/lock/yftp.pid"

#define YFTP_LOGFILE "/var/log/yftp.log"

#define YFTP_EPOLL_SIZE 32

#define YFTP_THR_MAX 1024

#define YFTP_MAX_CMD_LINE 4096
#define YFTP_MAX_PATH_LINE 4096
#define YFTP_MAX_DIR_BUFSIZE  16384

#define YFTP_IDLE_SESSION_TIMEOUT (1000 * 1000)

#define YFTP_CHROOT "/"

/* "\012" "\n" */

#define FTP_CMD_END '\012'

#define YFTP_PASV_SERVICE_BASE "2021"
#define YFTP_PASV_SERVICE_RANG YFTP_THR_MAX

/* XXX max upload rate -gj */
#define YFTP_MAX_RATE 0

#define SERVER_SOFTWARE "yftpd/0.01 09oct2006"
#define SERVER_URL "http://www.joybeta.com/"

#define YFTP_ACCESS_LOG 0

#endif

#ifndef __SESSION_H__
#define __SESSION_H__

#include <stdint.h>

#include "sdfs_conf.h"
#include "yftp_conf.h"
#define SESSION_READONLY  0
#define SESSION_READWRITE 0xFF

struct yftp_session {
        int epoll_fd;
        int ctrl_fd;
        int pasv_sd;

        char ctrl_line[YFTP_MAX_CMD_LINE + 1];
        char *cmd;
        char *arg;

        char user[YFTP_MAX_CMD_LINE + 1];
        uint32_t uid;
	    uint32_t mode;
        char *fakedir;

        char pwd[MAX_PATH_LEN + 1];

        char rnfr_filename[MAX_PATH_LEN + 1];

        int is_ascii;

        uint64_t offset;
};

extern long yftp_session_running;

extern int session_init(struct yftp_session *, int ctrl_fd);
extern int session_destroy(struct yftp_session *);
extern int session_clearpasv(struct yftp_session *);
extern int session_pasvactive(struct yftp_session *);

#endif

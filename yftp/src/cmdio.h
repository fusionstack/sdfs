#ifndef __CMDIO_H__
#define __CMDIO_H__

#include "session.h"
#include "dbg.h"

extern int cmdio_write(struct yftp_session *, int status, const char *msg);

extern int cmdio_get_cmd_and_arg(struct yftp_session *);

#endif

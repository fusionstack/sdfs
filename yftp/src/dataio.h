#ifndef __DATAIO_H__
#define __DATAIO_H__

#include "session.h"

extern int dataio_get_pasv_fd(struct yftp_session *ys, int *pfd);

extern int dataio_transfer_dir(struct yftp_session *ys, int rfd, char *path,
                               char *opt, char *filter, int full_details);

#endif

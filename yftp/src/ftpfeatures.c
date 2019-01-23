

#define DBG_SUBSYS S_YFTP

#include "cmdio.h"
#include "features.h"
#include "ftpcodes.h"
#include "session.h"
#include "dbg.h"

int handle_feat(struct yftp_session *ys)
{
        int ret;

        ret = cmdio_write(ys, 0, "211-Features:");
        ret = cmdio_write(ys, 0, " EPSV\r\n");
        ret = cmdio_write(ys, 0, " MDTM\r\n");
        ret = cmdio_write(ys, 0, " PASV\r\n");
        ret = cmdio_write(ys, 0, " REST\r\n");
        ret = cmdio_write(ys, 0, " STREAM\r\n");
        ret = cmdio_write(ys, 0, " SIZE\r\n");
        ret = cmdio_write(ys, 0, " TVFS\r\n");
        ret = cmdio_write(ys, 0, " UTF8\r\n");
        ret = cmdio_write(ys, FTP_FEAT, "End");
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

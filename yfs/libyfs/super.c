

#include <sys/types.h>
#include <regex.h>
#include <sys/statvfs.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSLIB

#include "md_lib.h"
#include "network.h"
#include "sdfs_lib.h"
#include "dbg.h"

#if 1
int ly_statvfs(const char *path, struct statvfs *svbuf)
{
        int ret;
        fileid_t parent, fileid;
        char name[MAX_PATH_LEN];

        ret = sdfs_splitpath(path, &parent, name);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_lookup(NULL, &parent, name, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_statvfs(NULL, &fileid, svbuf);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}
#endif

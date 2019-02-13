

#include <sys/types.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSLIB

#include "md_lib.h"
#include "net_global.h"
#include "sdfs_lib.h"
#include "dbg.h"

#if 1
int ly_readdir(const char *path, off_t offset, void **de, int *delen, int prog_type)
{
        int ret;
        fileid_t fileid;

        (void) prog_type;

        if (offset == 2147483647) {  /* 2GB - 1 */
                *delen = 0;
                return 0;
        }

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_readdir1(NULL, &fileid, offset, de, delen);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int ly_readdirplus(const char *path, off_t offset, void **de, int *delen, int prog_type)
{
        int ret;
        fileid_t fileid;

        (void) prog_type;
        
        if (offset == 2147483647) {  /* 2GB - 1 */
                *delen = 0;
                return 0;
        }

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        DBUG("dir %s "FID_FORMAT"\n", path, FID_ARG(&fileid));

        ret = sdfs_readdirplus(NULL, &fileid, offset, de, delen);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int ly_readdirplus_with_filter(const char *path, off_t offset, void **de, int *delen,
                               const filter_t *filter)
{
        int ret;
        fileid_t fileid;

        if (offset == 2147483647) {  /* 2GB - 1 */
                *delen = 0;
                return 0;
        }
        
        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        DBUG("dir %s "FID_FORMAT"\n", path, FID_ARG(&fileid));

        ret = sdfs_readdirplus_with_filter(NULL, &fileid, offset, de, delen, filter);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}
#endif

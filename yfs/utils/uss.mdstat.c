

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "uss_sysstat.h"
#include "sdfs_lib.h"
#include "dbg.h"

void print_clusterid(const char *clusterid)
{
        printf("cluster id : %s\n", clusterid);
}

int main(int argc, char *argv[])
{
        int ret;

        (void) argc;
        (void) argv;

        dbg_info(0);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init_simple("uss.mdstat");
        if (ret)
                GOTO(err_ret, ret);

        ret = uss_globestat();
        if (ret)
                GOTO(err_ret, ret);

        UNIMPLEMENTED(__WARN__);
#if 0
        ret = uss_mdsstat();
        if (ret)
                GOTO(err_ret, ret);
#endif

        return 0;
err_ret:
        return ret;
}

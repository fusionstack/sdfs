

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "uss_sysstat.h"
#include "sdfs_lib.h"
#include "dbg.h"

int main(int argc, char *argv[])
{
        int ret;

        (void) argc;
        (void) argv;

        dbg_info(0);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init_simple("uss.cdstat");
        if (ret)
                GOTO(err_ret, ret);

        ret = uss_globestat();
        if (ret)
                GOTO(err_ret, ret);

        ret = uss_cdsstat();
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

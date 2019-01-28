#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "file_proto.h"
#include "md_lib.h"

int main(int argc, char *argv[])
{
        int ret;
        char hostname[MAX_NAME_LEN];

        (void) argc;
        (void) argv;

        dbg_info(0);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret) {
                fprintf(stderr, "conf_init() %s\n", strerror(ret));
                exit(1);
        }

        ret = gethostname(hostname, MAX_NAME_LEN);
        if (ret) {
                if (ret == ECONNREFUSED)
                        strcpy(hostname, "N/A");
                else
                        GOTO(err_ret, ret);
        }

        printf("globals.clustername:%s\n"
               "globals.hostname:%s\n"
               "globals.home:%s\n"
               "globals.workdir:%s\n"
               "globals.wmem_max:%d\n"
               "globals.rmem_max:%d\n"
               "globals.check_version:%d\n"
               "globals.io_dump:%d\n"
               "globals.master_vip:%s\n"
               "globals.testing:%d\n"
               "globals.valgrind:%d\n"
               "globals.check_mountpoint:%d\n"
               "globals.solomode:%d\n"
               "globals.nfs_srv:%s\n"
               "mdsconf.redis_baseport:%d\n"
               "mdsconf.redis_sharding:%d\n"
               "mdsconf.redis_replica:%d\n",
               gloconf.cluster_name,
               hostname,
               SDFS_HOME,
               gloconf.workdir,
               gloconf.wmem_max,
               gloconf.rmem_max,
               gloconf.check_version,
               gloconf.io_dump,
               gloconf.master_vip,
               gloconf.testing,
               gloconf.valgrind,
               gloconf.check_mountpoint,
               gloconf.solomode,
               gloconf.nfs_srv,
               mdsconf.redis_baseport,
               mdsconf.redis_sharding,
               mdsconf.redis_replica);

        return 0;
err_ret:
        return ret;
}



#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>

#include "dbg.h"
#include "jnl_proto.h"
#include "configure.h"
#include "chk_meta.h"
#include "file_proto.h"
#include "md_proto.h"
#include "ylib.h"
#include "sdfs_lib.h"

#include <errno.h>


#pragma pack(8)
typedef struct deljnl_s{
        time_t time;
        fileid_t fileid;
}deljnl_t;
#pragma pack()

static int __recycle(const void *arg, int len, void *arg1)
{
        const deljnl_t *j = arg;
        (void) arg1;
        (void) len;

        fprintf(stdout,"time %lu Vid %llu_v%u \n",j->time, 
                (LLU)j->fileid.id,j->fileid.version);
        raw_recycle((fileid_t *)&j->fileid);

        return 0;
}

int main(int argc, char *argv[])
{
        int ret;
        jnl_handle_t jnl;
        char *jnlpath;
        char c_opt;

        jnlpath = 0;

        while ((c_opt = getopt(argc, argv, "p:")) > 0)
                switch (c_opt) {
                case 'p':
                        jnlpath = optarg;
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        exit(1);
                }

        if (jnlpath)
                sprintf(jnl.path_prefix, "%s", jnlpath);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret) {
                fprintf(stderr, "conf_init() %s\n", strerror(ret));
                exit(1);
        }

        ret = ly_init_simple("yrecycle");
        if (ret)
                GOTO(err_ret, ret);


        ret = jnl_open(jnl.path_prefix, &jnl, 0);
        if (ret)
                GOTO(err_ret, ret);

        ret = jnl_iterator(&jnl, 0, __recycle, NULL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

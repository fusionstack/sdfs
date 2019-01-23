

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#include "chk_meta.h"
#include "jnl_proto.h"
#include "ylib.h"
#include "dbg.h"

static int __find(const void *arg, int len, int64_t offset, void *arg1)
{
        chkid_t *chkid = arg1;
        const chkjnl_t *j = arg;
        (void) len;
        (void) offset;

        if (chkid->id == j->chkid.id
            && chkid->version == j->chkid.version) {
                printf("chk %llu_v%u op (%u) increase %d\n",
                       (long long unsigned int)chkid->id, chkid->version,
                       j->op, j->increase);
        }

        return 0;
}

int main(int argc, char *argv[])
{
        int ret;
        jnl_handle_t jnl;
        char *jnlpath;
        chkid_t chkid;
        int diskno;
        char c_opt;

        jnlpath = 0;

        while ((c_opt = getopt(argc, argv, "n:c:p:")) > 0)
                switch (c_opt) {
                case 'p':
                        jnlpath = optarg;
                case 'n':
                        diskno = atoi(optarg);
                        break;
                case 'c':
#ifdef __x86_64__
                        ret = sscanf(optarg, "%lu_v%u", &chkid.id, &chkid.version);
#else
                        ret = sscanf(optarg, "%llu_v%u", &chkid.id, &chkid.version);
#endif
                        YASSERT(ret == 2);

                        DINFO("chkid %llu_v%u\n", (LLU)chkid.id, chkid.version);
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        exit(1);
                }

        if (jnlpath)
                sprintf(jnl.home, "%s", jnlpath);
        else
                sprintf(jnl.home, "/sysy/yfs/cds/%d/chkinfo/jnl/", diskno);

        ret = jnl_open(jnl.home, &jnl, 0);
        if (ret)
                GOTO(err_ret, ret);

        ret = jnl_iterator(&jnl, 0, __find, &chkid);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

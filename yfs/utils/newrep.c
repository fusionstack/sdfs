

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "../libyfs/chunk.h"

int main(int argc, char *argv[])
{
        int ret;
        uint32_t newrep;
        chkid_t chkid;

        if ((argc != 4)) {
                fprintf(stderr, "Usage: %s <chkid->id> <chkid->version> <newrep>\n", argv[0]);
                exit(1);
        }

        chkid.id = atoi(argv[1]);
        chkid.version = atoi(argv[2]);
        newrep = atoi(argv[3]);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("newrep");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        }

        ret = ly_newrep(&chkid, newrep);
        if (ret) {
                fprintf(stderr, "ly_newrep() %s\n", strerror(ret));
                exit(1);
        }

        return 0;
}

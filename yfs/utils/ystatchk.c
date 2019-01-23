

#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include "configure.h"
#include "chk_proto.h"
#include "sdfs_lib.h"

int main(int argc, char *argv[])
{
        int ret;
        unsigned long long chkid;
        unsigned long version;

        if(argc < 3)
        {
                printf("usage:%s id version\n", argv[0]);
        }

        chkid = strtoull(argv[1], NULL, 10);
        version = strtoul(argv[2], NULL, 10);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("ystatchk");
        if(ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
        }
        
        ret = ly_statchk(chkid, version);
        if(ret) {
                fprintf(stderr, "ly_printchk() %s\n", strerror(ret));
        }

        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ly_destroy() %s\n", strerror(ret));
                exit(1);
        } 

        return 0;

}

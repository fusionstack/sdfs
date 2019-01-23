

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include "configure.h"
#include "sdfs_lib.h"

int main(int argc, char *argv[])
{
        int ret, num ;
        char *path, flag, c;

        if ((argc < 2) || (argc > 3)) {
                fprintf(stderr, "Usage: %s <path> | <path> <num>\n", argv[0]);
                exit(1);
        }

        if (argc == 2) {
                path = argv[1];
                c = (char)(*path);
                if (c != '/') {
                        fprintf(stderr,"The <path> should start with '/'\n");
                        exit(1);
                } else 
                        flag = 'p';
        } else if (argc == 3) {// XXX here , the num should less than a value;
                flag = 's';
                path = argv[1];
                num = atoi(argv[2]);
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("yrep");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        }

        if (flag == 'p') {
                ret = ly_getrepnum(path);
                fprintf(stdout, "Under path <%s> there are  %d replicas\n"
                        , path, ret);
        } else if (flag == 's') {
                ret = ly_setrepnum(path, num);
                fprintf(stdout, "Set path <%s> replica number = %d\n", 
                        path , num);
                if (ret) 
                        fprintf(stderr, "ly_setrepnum error ! \n");
        }

        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ly_destroy() %s\n", strerror(ret));
                exit(1);
        }

        return 0;
}

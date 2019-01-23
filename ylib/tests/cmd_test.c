

#include <sys/types.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "dbg.h"

int __run(const char *path, ...)
{
        int ret, i, son, status;
        va_list ap;
        char *argv[128];

        son = fork();

        switch (son) {
        case -1:
                ret = errno;
                GOTO(err_ret, ret);
        case 0:
                va_start(ap, path);

                for (i = 0; i < 128; i++) {
                        argv[i] = va_arg(ap, char *);
                        if (argv[i] == NULL)
                                break;
                }

                va_end(ap);

                ret = execv(path, argv);
                if (ret) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                break;
        default:
                DINFO("wait\n");

                ret = wait(&status);

                if (WIFEXITED(status)) {
                        DINFO("wait1\n");
                        ret = WEXITSTATUS(status);
                        if (ret)
                                GOTO(err_ret, ret);

                        DINFO("ret %u\n", ret);
                } else {
                        DINFO("wait2\n");
                }
        }

        return 0;
err_ret:
        return ret;
}


int main(int argc, char *argv[])
{
        (void) argc;
        (void) argv;

        return __run("/bin/ls", "ls", "-l", NULL);
}

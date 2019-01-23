#include <sys/file.h>
#include <errno.h>

#include "dbg.h"

int main(int argc, char *argv[])
{
        int ret, fd;
        char *key;

        key = argv[argc - 1];

        fd = open(key, O_CREAT | O_RDONLY, 0640);
        if (fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        //DINFO("fd %d\n", fd);

        ret = flock(fd, LOCK_EX | LOCK_NB);
        if (ret == -1) {
                ret = errno;
                if (ret == EWOULDBLOCK) {
                        DINFO("lock %s fail\n", key);
                        goto err_fd;
                } else
                        GOTO(err_ret, ret);
        }

        DINFO("lock %s success\n", key);

        close(fd);

        return 0;
err_fd:
        close(fd);
err_ret:
        return ret;
}

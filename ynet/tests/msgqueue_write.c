

#include <stdlib.h>
#include <unistd.h>

#include "msgqueue.h"
#include "job_tracker.h"

jobtracker_t *jobtracker;

#define WBUF_LEN (MAX_BUF_LEN / 2)
#define RBUF_LEN (MAX_BUF_LEN * 2)

int main(int argc, char *argv[])
{
        int ret, fd, args = 1;
        msgqueue_t queue;
        ynet_net_nid_t nid;
        char *src, *dest, buf[WBUF_LEN];
        struct stat stbuf;
        uint32_t off, cp, left;

        if (argc < 3) {
                fprintf(stderr, "msgqueue_cp /file1 /file2\n");
                EXIT(1);
        }

        src = argv[args++];
        dest = argv[args++];

        ret = sscanf(dest, "%lu_v%u", &nid.id, &nid.version);
        if (ret != 2) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = msgqueue_init(&queue, "/tmp/msgqueue_test_file", &nid);
        if (ret)
                GOTO(err_ret, ret);

        ret = stat(src, &stbuf);
        if (ret)
                GOTO(err_ret, ret);

        fd = open(src, O_RDONLY);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        off = 0;

        while (off < stbuf.st_size) {
                left = stbuf.st_size - off;

                if (left > WBUF_LEN)
                        cp = random() % (WBUF_LEN / 2) + WBUF_LEN / 2;
                else
                        cp = left;

                DBUG("cp %u\n", cp);

                ret = pread(fd, buf, cp, off);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                off += cp;

                ret = msgqueue_push(&queue, buf, cp);
                if (ret)
                        GOTO(err_ret, ret);
        }

        close(fd);

        return 0;
err_ret:
        return ret;
}

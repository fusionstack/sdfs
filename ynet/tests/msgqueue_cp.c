

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
        char *src, *dest, buf[WBUF_LEN], rbuf[RBUF_LEN];
        struct stat stbuf;
        uint32_t off, cp, left;
        msgqueue_msg_t *msg;

        nid.id = 1;
        nid.version = time(NULL);

        ret = msgqueue_init(&queue, "/tmp/msgqueue_test_file", &nid);
        if (ret)
                GOTO(err_ret, ret);

        if (argc < 3) {
                fprintf(stderr, "msgqueue_cp /file1 /file2\n");
                EXIT(1);
        }

        src = argv[args++];
        dest = argv[args++];

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

        fd = creat(dest, 0644);
        if (fd < 0) {
              ret = errno;
              GOTO(err_ret, ret);
        }

        while (1) {
                cp = RBUF_LEN;

                ret = msgqueue_get(&queue, rbuf, cp);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                cp = ret;

                YASSERT(cp <= RBUF_LEN);

                if (cp == 0)
                        break;

                DBUG("get %u\n", cp);

                msg = (void *)rbuf;
                left = cp;

                msg_for_each(msg, left) {
                        DBUG("msg %u left %u\n", msg->len, left);
                        ret = write(fd, msg->buf, msg->len);
                        if (ret < (int)msg->len) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }
                }

                YASSERT(left <= RBUF_LEN);

                ret = msgqueue_pop(&queue, NULL, cp - left);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                YASSERT((unsigned)ret == cp - left);
                YASSERT(ret);

                DBUG("write %u cp %u\n", ret, cp);
        }

        return 0;
err_ret:
        return ret;
}

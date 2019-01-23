

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
        char *src, *dest, rbuf[RBUF_LEN];
        uint32_t cp, left;
        msgqueue_msg_t *msg;

        if (argc < 3) {
                fprintf(stderr, "msgqueue_cp /file1 /file2\n");
                EXIT(1);
        }

        src = argv[args++];
        dest = argv[args++];

        ret = sscanf(src, "%lu_v%u", &nid.id, &nid.version);
        if (ret != 2) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = msgqueue_load(&queue, "/tmp/msgqueue_test_file", &nid);
        if (ret)
                GOTO(err_ret, ret);

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

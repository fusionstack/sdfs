

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <getopt.h>
#include <errno.h>

#include "chk_meta.h"
#include "ylib.h"
#include "dbg.h"
#include "net_global.h"
#include "cd_proto.h"
#include "yfscds_conf.h"

#define YFS_CDS_DIR_DISK_PRE "/sysy/yfs/cds"

net_global_t ng;

int main(int argc, char *argv[])
{
        int ret, fd, fd1, left, offset, size;
        int c_opt;
        struct stat stbuf;
        uint32_t count;
        chkmeta2_t md;
        char cascade[MAX_PATH_LEN];
        char chkpath[MAX_PATH_LEN];
        chkid_t chkid;
        objinfo_t *objinfo;
        char buf[MAX_BUF_LEN], *dist;

        objinfo = (void *)buf;

	int cn;

        dist = argv[argc - 1];

        while ((c_opt = getopt(argc, argv, "c:n:")) > 0) {
                switch (c_opt) {
                case 'c':
#ifdef  __x86_64_
                        ret = sscanf(optarg, "%lu_v%u/%u", &chkid.id, &chkid.version, &chkid.idx);
#else
                        ret = sscanf(optarg, "%llu_v%u/%u", (LLU *)&chkid.id, &chkid.version, &chkid.idx);
#endif
                        if (ret != 3) {
                                ret = EINVAL;
                                GOTO(err_ret, ret);
                        }

                        break;
                case 'n':
                        cn = atoi(optarg);
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op (%c) got!\n", c_opt);
                        exit(1);
                }
        }

	cascade_id2path(cascade, MAX_PATH_LEN, chkid.id);

	sprintf(chkpath, "%s/%d/%s%s_v%u/%u", YFS_CDS_DIR_DISK_PRE,
		cn,"ychunk",cascade, chkid.version, chkid.idx);
        sprintf(ng.home, "%s/%d", YFS_CDS_DIR_DISK_PRE,cn);

        fd = open(chkpath, O_RDWR);
        if (fd == -1) {
                ret = errno;
                fprintf(stderr, "open(%s, ...) %s\n", chkpath, strerror(ret));
                return 1;
        }

        ret = fstat(fd, &stbuf);
        if (ret == -1) {
                ret = errno;
                fprintf(stderr, "fstat(%s, ...) %s\n", chkpath, strerror(ret));
                return 1;
        }

        count = sizeof(chkmeta2_t);

        ret = _pread(fd, (void *)&md, count, 0);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        ret = _pread(fd, objinfo, OBJINFO_SIZE(YFS_CHK_REP_MAX), sizeof(chkmeta2_t));
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        fd1 = creat(dist, 0755);
        if (fd1 < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        left = md.chklen;
        offset = 0;
        while (left) {
                size = left < MAX_BUF_LEN ? left : MAX_BUF_LEN;
                ret = _pread(fd, buf, size, offset + YFS_CDS_CHK_OFF);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                ret = _pwrite(fd1, buf, size, offset);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                left -= size;
                offset += size;
        }


        return 0;
err_ret:
        return ret;
}

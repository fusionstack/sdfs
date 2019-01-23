

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#include "sdfs_buffer.h"
#include "chk_meta.h"
#include "ylib.h"
#include "jnl_proto.h"
#include "chk_proto.h"
#include "chk_meta.h"
#include "yfscds_conf.h"
#include "dbg.h"

int readop(int fd, int offset, chkmeta2_t *md, int bfd)
{
        int ret;
        char buf[sizeof(chkop_t) + Y_BLOCK_MAX], *str;
        chkop_t *op;
        uint32_t crc;

        ret = _pread(fd, buf, sizeof(chkop_t) + Y_BLOCK_MAX, offset);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        op = (void *)buf;
        str = buf + sizeof(chkop_t);

#if 0
        op->__pad__ = 0;

        ret = _pwrite(fd, op, sizeof(chkop_t), offset);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }
#endif

        crc = crc32_sum(str, op->size);
        
        printf("op %u version %llu offset %u size %u read size %u crc %u\n",
              op->op, (LLU)op->version, op->offset, op->size, ret, crc);

        if (bfd != -1) {
                if (op->op == CHKOP_WRITE) {
                        ret = _pwrite(bfd, str, op->size, op->offset + YFS_CDS_CHK_OFF);
                        if (ret < 0) {
                                ret = -ret;
                                GOTO(err_ret, ret);
                        }

                        sy_vec_update(op->offset, op->size, &md->chkoff,
                                      &md->chklen);

                        md->chk_version = op->version;

                        crc32_md(md, sizeof(chkmeta2_t));

                        ret = _pwrite(bfd, (void *)md, sizeof(chkmeta2_t), 0);
                        if (ret < 0) {
                                ret = -ret;
                                GOTO(err_ret, ret);
                        }
                        
                } else if (op->op == CHKOP_TRUNC) {
                        YASSERT(0);
                } else {
                        DINFO("op %u\n", op->op);
                }
        }

        return 0;
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret, fd, bfd;
        char c_opt;
        char *file, *build = NULL;
        struct stat stbuf;
        uint32_t off, chkno, volid;
        chkmeta2_t md;
        fileid_t fileid;
        chkid_t chkid;

        while ((c_opt = getopt(argc, argv, "j:b:f:c:n:v:")) > 0)
                switch (c_opt) {
                case 'j':
                        file = optarg;
                        break;
                case 'b':
                        build = optarg;
                        break;
                case 'f':
#ifdef __x86_64__
                        ret = sscanf(optarg, "%lu_v%u", &fileid.id, &fileid.version);
#else
                        ret = sscanf(optarg, "%llu_v%u", &fileid.id, &fileid.version);
#endif
                        YASSERT(ret == 2);
                        break;
                case 'c':
#ifdef __x86_64__
                        ret = sscanf(optarg, "%lu_v%u", &chkid.id, &chkid.version);
#else
                        ret = sscanf(optarg, "%llu_v%u", &chkid.id, &chkid.version);
#endif
                        YASSERT(ret == 2);
                        break;
                case 'n':
                        chkno = atoi(optarg);
                        break;
                case 'v':
                        volid = atoi(optarg);
                        break;
                default:
                        exit(1);
                }


        fd = open(file, O_RDWR);
        if (fd < 0) {
                ret = errno;
                DWARN("open %s fail, ret %u\n", file, ret);

                GOTO(err_ret, ret);
        }

        ret = stat(file, &stbuf);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        if (build) {
                bfd = open(build, O_CREAT | O_RDWR, 0755);
                if (bfd < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                memset(&md, 0x0, sizeof(chkmeta2_t));
                md.chkid = chkid;
                md.chk_version = 0;
                md.crc.version = META_VERSION0;

                crc32_md(&md, sizeof(chkmeta2_t));

                ret = _pwrite(bfd, (void *)&md, sizeof(chkmeta2_t), 0);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }
        } else
                bfd = -1;

        for (off = 0; off < stbuf.st_size; off += sizeof(chkop_t) + Y_BLOCK_MAX) {
                ret = readop(fd, off, &md, bfd);
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}


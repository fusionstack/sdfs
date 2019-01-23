

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <getopt.h>
#include <errno.h>

#include "chk_meta.h"
#include "sysutil.h"
#include "ylib.h"
#include "dbg.h"
#include "net_global.h"
#include "cd_proto.h"
#include "yfscds_conf.h"

net_global_t ng;

int main(int argc, char *argv[])
{
        int ret, fd, i;
        int c_opt;
        struct stat stbuf;
        char cascade[MAX_PATH_LEN];
        char chkpath[MAX_PATH_LEN], path[MAX_PATH_LEN];
        chkid_t chkid;
        chkmeta2_t *md;
        objinfo_t *objinfo;
        char buf[MAX_BUF_LEN];

        objinfo = (void *)buf;

	int cn = 0;

        while (1) {
                int option_index = 0;

                static struct option long_options[] = {
                        {"clear", 0, 0, 0},
                };
                
                c_opt = getopt_long(argc, argv, "c:n:",
                        long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 0:
                        switch (option_index) {
                        case 0:
                                break;
                        default:
                                fprintf(stderr, "Hoops, wrong op got!\n");
                        }

                        break;
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

        sprintf(ng.home, "%s/%d", YFS_CDS_DIR_DISK_PRE,cn);


        i = 0;
        while (1) {
                sprintf(path, "%s/%d/disk/%u", YFS_CDS_DIR_DISK_PRE,
                        cn, i);

                ret = stat(path, &stbuf);
                if (ret < 0) {
                        ret = errno;
                        if (ret == ENOENT) {
                                goto err_ret;
                        } else
                                GOTO(err_ret, ret);
                }

                sprintf(chkpath, "%s%s_v%u/%u", path, cascade, chkid.version, chkid.idx);
                fd = open(chkpath, O_RDWR);
                if (fd == -1) {
                        ret = errno;
                        if (ret == ENOENT) {
                                i++;
                                continue;
                        }
                }

                break;
        }

        ret = fstat(fd, &stbuf);
        if (ret == -1) {
                ret = errno;
                fprintf(stderr, "fstat(%s, ...) %s\n", chkpath, strerror(ret));
                return 1;
        }

        ret = _pread(fd, buf, PAGE_SIZE * 2, 0);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        md = (void *)buf;
        objinfo = (void *)buf + PAGE_SIZE;

        if (md->crc.crc != 0) {
                ret = crc32_md_verify((void *)md, sizeof(chkmeta2_t));
                if (ret) {
                        ret = EIO;
                        DERROR("bad file %s\n", chkpath);
                        GOTO(err_ret, ret);
                }
        }

        printf("path: %s\nmd version %x, crc %x version %llu id "OBJID_FORMAT"\n",
               chkpath, md->crc.version, md->crc.crc, (LLU)md->chk_version, OBJID_ARG(&md->chkid));

        printf("object: id %llu_v%u[%u] md_version %llu, mode %u, size %u repnum %u status %x writeback:%s\n",
               (LLU)objinfo->id.id, objinfo->id.version, objinfo->id.idx,
               (LLU)objinfo->info_version, objinfo->mode, objinfo->size,
               objinfo->repnum, objinfo->status, (objinfo->status & __S_WRITEBACK) ? "true" : "false");

        (void) sy_close(fd);

        return 0;
err_ret:
        return ret;
}

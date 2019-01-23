

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>

#include "configure.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "ynet_rpc.h"
#include "net_global.h"
#include "sdfs_buffer.h"
#include "dbg.h"

int main(int argc, char *argv[])
{
        int ret, args, verbose = 0, fd;
        char c_opt, *prog, *_id;
        ynet_net_nid_t nid;
        char buf[MAX_BUF_LEN];
        ynet_net_info_t *info;
        net_handle_t nh;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;

        if (argc < 2) {
                fprintf(stderr, "%s [-v] <dirpath>\n", prog);
                exit(1);
        }

        _id = NULL;

        while ((c_opt = getopt(argc, argv, "vi:")) > 0)
                switch (c_opt) {
                case 'i':
                        _id = optarg;
                case 'v':
                        verbose = 1;
                        args++;
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        exit(1);
                }

        if (_id == NULL) {
                printf("need nid like 1_v1 or file /sysy/cds/1/nid\n");

                exit(1);
        }
#ifdef __x86_64__
        ret = sscanf(_id, "%lu_v%u", &nid.id, &nid.version);
#else
        ret = sscanf(_id, "%llu_v%u", &nid.id, &nid.version);
#endif
        if (ret == 0) {
                DINFO("open nid file %s\n", _id);

                fd = open(_id, O_RDONLY);
                if (fd < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                ret = _read(fd, (void *)&nid, sizeof(ynet_net_nid_t));
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                if (ret != sizeof(ynet_net_nid_t)) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }

                close(fd);
        }

        DINFO("nid %llu_%u\n", (LLU)nid.id, nid.version);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("cmd");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_init()'ed\n");

        info = (void *)buf;
        ret = rpc_request_getinfo(&ng.tracker, &nid, (void *)info);
        if (ret)
                GOTO(err_ret, ret);

        ynet_net_info_dump(info);

        ret = rpc_info2nid(&nh, info);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ly_destroy() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_destroy()'ed\n");

        return 0;
err_ret:
        return ret;
}

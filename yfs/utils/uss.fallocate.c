

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "file_proto.h"
#include "../objc/objc.h"
#include "md_lib.h"

void show_help(char *prog) 
{
        fprintf(stderr, "%s [-v] path\n", prog);
        fprintf(stderr, "%s (id)_v(version)\n", prog);
}

int main(int argc, char *argv[])
{
        int ret, args, verbose __attribute__((unused)) = 0;
        fileid_t fileid, chkid;
        char c_opt, *prog, *arg;
        struct stat stbuf;
        fileinfo_t *md;
        char buf[MAX_BUF_LEN], buf1[MAX_BUF_LEN];
	int chknum, i;
        objinfo_t *objinfo;

        dbg_info(0);

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;

        if (argc < 2) {
                show_help(prog);
                exit(1);
        }

        while ((c_opt = getopt(argc, argv, "vf")) > 0)
                switch (c_opt) {
                case 'v':
                        verbose = 1;
                        args++;
                        break;
                default:
                        show_help(prog);
                        exit(1);
                }

        arg = argv[argc - 1];

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret) {
                fprintf(stderr, "conf_init() %s\n", strerror(ret));
                exit(1);
        }

        ret = ly_init_simple("uss.fallocate");
        if (ret)
                GOTO(err_ret, ret);

        if (arg[0] == '/') {
                ret = sdfs_lookup_recurive(arg, &fileid);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
#ifdef __x86_64__
                ret = sscanf(arg, "%lu_v%lu", &fileid.id, &fileid.volid);
#else
                ret = sscanf(arg, "%llu_v%llu", &fileid.id, &fileid.volid);
#endif

                if (ret != 2) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }

        md = (void *)buf;
        ret = md_getattr(&fileid, (void *)md);
        if (ret)
                GOTO(err_ret, ret);

        if (!S_ISREG(md->at_mode)) {
                printf("file "FID_FORMAT" is dir\n", FID_ARG(&fileid));
                goto out;       /* not file */
        }

        ret = sdfs_getattr(&fileid, &stbuf);
        if (ret)
                GOTO(err_ret, ret);

        chknum = _get_chknum(stbuf.st_size, md->split);

        objinfo = (void *)buf1;
        for (i = 0; i < chknum; i++) {
                fid2cid(&chkid, &fileid, i);
                ret = objc_open(objinfo, &chkid, O_CREAT);
                if (ret)
                        GOTO(err_ret, ret);

                printf("allocate "OBJID_FORMAT"\n", OBJID_ARG(&chkid));
        }

out:
        return 0;
err_ret:
        return ret;
}

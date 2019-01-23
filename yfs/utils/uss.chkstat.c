

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "file_proto.h"
#include "md_lib.h"

void show_help(char *prog)
{
        fprintf(stderr, "%s [-v] path\n", prog);
        fprintf(stderr, "%s (id)_v(version) -s idx:available\n", prog);
}

int main(int argc, char *argv[])
{
        int ret, args, verbose __attribute__((unused)) = 0;
        fileid_t fileid, chkid;
        char c_opt, *prog, *arg;
        struct stat stbuf;
        fileinfo_t *md;
        char buf[MAX_BUF_LEN], buf1[MAX_BUF_LEN];
	char *set = NULL;
	int idx, available;
        chkinfo_t *chkinfo;

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

        while ((c_opt = getopt(argc, argv, "vfs:")) > 0)
                switch (c_opt) {
                case 'v':
                        verbose = 1;
                        args++;
                        break;
                case 's':
                        set = optarg;
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

        ret = ly_init_simple("ystat");
        if (ret)
                GOTO(err_ret, ret);

#ifdef __x86_64__
	ret = sscanf(arg, "%lu_v%lu[%u]", &chkid.id, &chkid.volid, &chkid.idx);
#else
	ret = sscanf(arg, "%llu_v%llu[%u]", &chkid.id, &chkid.volid, &chkid.idx);
#endif

	if (ret != 3) {
		ret = EINVAL;
		GOTO(err_ret, ret);
	}

	cid2fid(&fileid, &chkid);
        md = (void *)buf;
        ret = md_getattr((void *)md, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        MD2STAT(md, &stbuf);

#if 0
        if (S_ISREG((stbuf).st_mode)) {
                ret = sdfs_getattr(&fileid, &stbuf);
                if (ret) {
                        size = 0;
                } else {
                        size = stbuf.st_size;
                }
        }
#endif

        if (S_ISDIR((stbuf).st_mode)) {
                printf("fileid "FID_FORMAT"\n", FID_ARG(&fileid));
                goto out;       /* not file */
        }

	if (set) {
		ret = sscanf(set, "%d:%d", &idx, &available);
		if (ret != 2) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }

                chkinfo = (void *)buf1;
                ret = md_chkload(chkinfo, &chkid, NULL);
                if (ret)
                        GOTO(err_ret, ret);

                ret = md_chkavailable(NULL, &chkid, &chkinfo->diskid[idx], available);
                if (ret)
                        GOTO(err_ret, ret);

		ret = raw_printfile(&fileid, chkid.idx);
		if (ret)
			GOTO(err_ret, ret);
	} else {
		ret = raw_printfile(&fileid, chkid.idx);
		if (ret)
			GOTO(err_ret, ret);
	}

out:
        return 0;
err_ret:
        return ret;
}

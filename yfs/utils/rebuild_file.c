

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "file_proto.h"
#include "../mdc/md_lib.h"
#include "../cdc/cdc_lib.h"
#include "../cdc/replica.h"

void show_help(char *prog) 
{
        fprintf(stderr, "%s -f filename...\n", prog);
}

static int __ask_keep(const chkid_t *chkid, net_handle_t *nhs, 
                      unsigned char (*sha1)[SHA1_HASH_SIZE], uint32_t count,
                      const fileid_t *fileid, uint32_t volid)
{
        int ret;
        uint32_t i;
        char buf[MAX_NAME_LEN], str[MAX_NAME_LEN];

        printf("==== chk %llu_v%u has rep %u list below ====\n", (LLU)chkid->id,
               chkid->version, count);

        for (i = 0; i < count; i++) {
                _sha1_print(str, sha1[i]);
                printf("rep %u sha1 %s in %s\n", i, str, netable_rname(&nhs[i]));
        }
        
        printf("==== chk list end ====\n");

        for (i = 0; i < count; i++) {
        req:
                _sha1_print(str, sha1[i]);
                printf("keep rep %u ? (sha1 %s in %s) (Yes/No):", i, str, netable_rname(&nhs[i]));
                scanf("%s", buf);

                if (strcmp(buf, "Yes") == 0) {
                } else if (strcmp(buf, "No") == 0) {
                        ret = cdc_repunlink(&nhs[i], chkid, fileid, volid);
                        if (ret)
                                GOTO(err_ret, ret);
                } else {
                        printf("just answer 'Yes' or 'No'\n");
                        goto req;
                }

        }

        return 0;
err_ret:
        return ret;
}

static int __rebuild(const char *filename, uint32_t chkno, uint32_t resume)
{
        int ret;
        uint32_t mode, i;
        fileid_t fileid;
        chkid_t *chkid;
        md_proto_t *_md;
        fileinfo_t *md;
        struct stat stbuf;

        gloconf.rpc_timeout = 60 * 10;

        ret = sdfs_lookup_recurive(filename, &fileid);
        if (ret)
                GOTO(err_ret, ret);
#if 0
        ret = sdfs_getattr(&fileid, &stbuf);
        if (ret)
                GOTO(err_ret, ret);

        YASSERT(S_ISREG(stbuf.st_mode));
#endif

        ret = md_get(&fileid, &_md);
        if (ret)
                GOTO(err_ret, ret);

        YASSERT(S_ISREG(_md->at_mode));

        md = (void *)_md;

        if (chkno == (uint32_t)-1) {
                if (resume != (uint32_t) -1)
                        i = resume;
                else
                        i = 0;
                for (; i < md->chknum; i++) {
                        chkid = &md->chks[i];

                        ret = __rebuild_chk(chkid, &fileid, md->volid);
                        if (ret)
                                GOTO(err_ret, ret);
                }
        } else {
                if (chkno >= md->chknum) {
                        ret = ENOENT;
                        GOTO(err_free, ret);
                }

                chkid = &md->chks[chkno];

                ret = __rebuild_chk(chkid, &fileid, md->volid);
                if (ret)
                        GOTO(err_ret, ret);
        }

        yfree((void **)&md);

        return 0;
err_free:
        yfree((void **)&md);
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret;
        uint32_t chkno = -1, resume = -1;
        char c_opt, *prog, *filename, buf[MAX_NAME_LEN];

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        filename = NULL;
        chkno = -1;

        while ((c_opt = getopt(argc, argv, "f:n:r:")) > 0)
                switch (c_opt) {
                case 'f':
                        filename = optarg;
                        break;
                case 'n':
                        chkno = atoi(optarg);
                        break;
                case 'r':
                        resume = atoi(optarg);
                        break;
                default:
                        show_help(prog);
                        exit(1);
                }

        if (filename == NULL) {
                show_help(prog);
                exit(1);
        }

req:
        printf("you are about to rebuild file %s, you know what are you doing,"
               " do you ??? \n(Yes/No):", filename);

        scanf("%s", buf);

        if (strcmp(buf, "Yes") == 0) {
                printf("ok, let go...\n");
        } else if (strcmp(buf, "No") == 0) {
                exit(1);
        } else {
                printf("just answer 'Yes' or 'No'\n");
                goto req;
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if(ret)
                exit(1);

        ret = ly_init_simple("rebuild_file");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        }

        __rebuild(filename, chkno, resume);

        return 0;
}

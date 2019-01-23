/*
 * =====================================================================================
 *
 *       Filename:  mds_deletedjnl.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  05/12/2010 01:02:52 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Weng Jian Lv (errno), error.right@gmail.com
 *        Company:  MDS
 *
 * =====================================================================================
 */



#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#include "../mdc/md_lib.h"
#include "../libyfs/chunk.h"
#include "sdfs_lib.h"
#include "yfs_md.h"
#include "chk_meta.h"
#include "ylib.h"
#include "jnl_proto.h"
#include "dbg.h"

static jnl_handle_t deljnl;
#pragma pack(8)
typedef struct deljnl_s{
        time_t time;
        fileid_t fileid;
}deljnl_t;
#pragma pack()



void usage(char *prog)
{
        printf("%s: -n cds number\n", prog);
}

int delete_fileid_chunks(fileid_t *fileid)
{
        int i, ret, flag;
        fileinfo_t *fmd;
        md_proto_t *md;
        chkid_t chkid;
#if 0
        struct yfs_chunk *chk;
        ret = ymalloc((void **)chk, sizeof(struct yfs_chunk));
        if (ret)
                GOTO(err_ret, ret);
#endif
        flag = 0;

        ret = md_get(fileid, (md_proto_t **)&fmd);
        if (ret)
                GOTO(err_ret, ret);

        (void)md;
        ///fmd = (fileinfo_t *)md;
        for (i=0; i<(int)fmd->chknum; i++) {
                if (fmd->chks[i].id == 0
                    || fmd->chks[i].version == 0){
                        continue;
                }
                chkid.id = fmd->chks[i].id;
                chkid.version = fmd->chks[i].version;
                DWARN("ly_chkunlink chkid %llu_v%u, \
                       fileid %llu_v%u, volid %u",  \
                       (LLU)chkid.id, chkid.version,     \
                       (LLU)fileid->id, fileid->version,   \
                       fmd->volid);
                flag = ly_chkunlink(&chkid, fileid, fmd->volid);
        }

        if (flag) {
                ret = flag;
                GOTO(err_ret, ret);
        }
        return 0;
err_ret:
        return ret;
}


int main(int argc, char *argv[])
{
        int ret;
        void *buf;
        (void)argc;
        (void)argv;
        uint32_t version;

        deljnl_t *jnl;
#if 0

        char c_opt;
        while ((c_opt = getopt(argc, argv, "n:h")) > 0)
                switch (c_opt) {
                case 'n':
                        diskno = atoi(optarg);
                        break;
                case 'h':
                default:
                        usage(argv[1]);
                        exit(1);
                }
        time jizhi
#endif
        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);
        ret = ly_init_simple("mds_deletedjnl");
        if (ret)
                GOTO(err_ret, ret);


        sprintf(deljnl.path_prefix, "/sysy/yfs/mds/1/deleted");

        ret = jnl_open(deljnl.path_prefix, JNL_READ, &deljnl, 0);
        if (ret)
                GOTO(err_ret, ret);

        while((buf = jnl_next(&deljnl, &version, NULL))!=NULL){
                jnl = (deljnl_t *)buf;
                printf("time is %s, fileid %llu_v%u\n", ctime(&jnl->time),
                       (LLU)jnl->fileid.id, jnl->fileid.version);
                ret = delete_fileid_chunks(&jnl->fileid);

                if (ret){
                        /*rewrite the jnl*/
                        DWARN("unimplement\n");
                }
                buf = NULL;
        }
        jnl_close(&deljnl);
        return 0;
err_ret:
        return ret;
}

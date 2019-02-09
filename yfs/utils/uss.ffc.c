/*
 * =====================================================================================
 *
 *       Filename:  ffc.c  (file final consistency)
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  05/04/2010 02:16:05 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Weng Jian Lv (errno), error.right@gmail.com
 *        Company:  MDS
 *
 * =====================================================================================
 */

/*open a file, get all the chunk
 * for every chunk, do the below:
 * send md5 message
 * get md5 and compare
 */
#include "md_lib.h"
#include "../cdc/cdc_lib.h"
#include "../libyfs/chunk.h"
#include "yfs_chunk.h"
#include "dbg.h"
#include "chk_proto.h"
#include "md_proto.h"
#include "ynet_rpc.h"
#include "../cdc/replica.h"
#include "ylib.h"
#include "network.h"
#include "../objc/objc.h"
#include "sdfs_lib.h"
#include "sdfs_conf.h"

void help(){
        printf("Usage fcc /path\n");
}

int main(int argc, char *argv[])
{
        int ret, i, idx;
        int c_opt;
        fileinfo_t *md;
        char buf[MAX_BUF_LEN], *arg;
        fileid_t fileid;
        uint32_t chknum, chkno __attribute__((unused));
        objid_t objid;

        dbg_info(0);

        while ((c_opt = getopt(argc, argv, "n")) != -1){
                switch (c_opt) {
                case 'n':
                        chkno = atoi(optarg);
                        break;
                default:
                        help();
                        exit(0);
                        break;
                }
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init_simple("uss.ffc");
        if (ret)
                GOTO(err_ret, ret);

        arg = argv[argc - 1];

        idx = -1;
        if (arg[0] == '/') {
                ret = sdfs_lookup_recurive(arg, &fileid);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
#ifdef __x86_64__
                ret = sscanf(arg, "%lu_v%lu[%u]", &fileid.id, &fileid.volid, &idx);
#else
                ret = sscanf(arg, "%llu_v%llu[%u]", &fileid.id, &fileid.volid, &idx);
#endif

                if (ret != 2 && ret != 3) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }

        md = (void *)buf;
        ret = md_getattr(&fileid, (void *)md);
        if (ret)
                GOTO(err_ret, ret);

        if (!S_ISREG(md->at_mode)) {
                printf("is not file\n");
                return 0;
        }

        if (idx == -1) {
                chknum = _get_chknum(md->at_size, md->split);
                for (i = 0; i < (int)chknum; i++) {
                        fid2cid(&objid, &md->fileid, i);
                        ret = objc_sha1(&objid);
                        if (ret)
                                continue;
                }
        } else {
                fid2cid(&objid, &md->fileid, idx);
                ret = objc_sha1(&objid);
                if (ret)
                        GOTO(err_ret, ret);
        }

        //ly_destroy();

        return 0;
err_ret:
        return ret;
}

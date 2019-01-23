#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <string.h>

#define DBG_SUBSYS S_YFSLIB

#include "configure.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "../mdc/md_lib.h"
#include "../cdc/replica.h"
#include "array.h"
#include "md_array.h"
#include "file_proto.h"
#include "yfs_file.h"
#include "yfs_node.h"
#include "net_global.h"
#include "ynet_rpc.h"
#include "md_proto.h"
#include "job_dock.h"
#include "yfs_chunk.h"
#include "file_table.h"
#include "dbg.h"

extern struct yfs_sb *yfs_sb;

/**
 */
int recovery()
{
        int ret;
        struct yfs_chunk *chk;
        net_handle_t *nid;
        ynet_net_info_t *info;
        int fd;
        struct yfs_file *yf;
        uint32_t chkrep;
        uint64_t chkno;
        uint32_t rep;
        uint32_t i, buflen = MAX_BUF_LEN;
        chkid_t *chkid;
        md_chk_t *mdchk;
        struct yfs_chunk new_chk;
        char *path = "/xaa";
        char buf[MAX_BUF_LEN];

        info = (void *)buf;

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init_simple("recovery");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        } 

        while (srv_running) {
                fd = ly_open(path);
                if (fd < 0) {
                        ret = ENOENT;
                        GOTO(err_ret, ret);
                }

                uint32_t normal_rep = 3;

                DBUG("***************************\n");
                // chunk-by-chunk
                yf = get_file(fd);
                for (chkno = 0; chkno < yf->node->md->chknum; ++chkno) {
                        chk = &yf->chks[chkno];
                        mdchk = &yf->node->md->chks[chkno];

                        if (!chk->loaded) {
                                chkid = &mdchk->chkid;
                                ret = md_chkload(chk, chkid);
                                if (ret) {
                                        DERROR("ret %d\n", ret);
                                        continue;
                                }
                                chk->loaded = 1;
                        }


                        if (chk->rep <= 0 || chk->rep > 2) {
                                DBUG("rep %u\n", chk->rep);
                                continue;
                        }

                        chkrep = normal_rep - chk->rep;
                        DBUG("chkrep %u\n", chkrep);

                        new_chk.chkid.id = chk->chkid.id;
                        new_chk.chkid.version = chk->chkid.version;
                        new_chk.chklen = chk->chklen;
                        /**
                         * XXX
                         */
                        ret = md_chkget(&yf->node->fileid,
                                         context->chkno, yf->node->volid,
                                         normal_rep, &new_chk);
                        if (ret)
                                GOTO(err_ret, ret);

                        for (rep = 0; rep < normal_rep; ++rep) {
                                for (i = 0; i < chk->rep; ++i) {
                                        if (net_handle_cmp(&new_chk.nid[rep], &chk->nid[i]) == 0)
                                                break;
                                }

                                if (i != chk->rep)
                                        continue;

                                // source cds
                                nid = &chk->nid[random()%chk->rep];

                                /* @res info */
                                ret = net_nid2info(info, &buflen, &new_chk.nid[rep]);
                                if (ret)
                                        GOTO(err_ret, ret);
#if 0
                                ret = cdc_repclone(nid, &new_chk.chkid, info);
                                if (ret)
                                        GOTO(err_ret, ret);
#endif
                        }
                }

                sleep(60);
        }

        return 0;
err_ret:
        return ret;
}

/**
 * CDP_CLONE
 * CDP_MERGE
 */
int main(int argc, char **argv) 
{
        int ret;
        char *prog;
        int son;
        int status;

        (void)argc;
        (void)argv;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

#if 0
        if (argc != 1) {
                fprintf(stderr, "Usage: yrecover <path>\n");
                exit(1);
        }
#endif

        ///////////////////////////////////////////////////////////////
        // main loop
        ///////////////////////////////////////////////////////////////
        while (srv_running) 
        {
                son = fork();
                if (-1 == son) {
                        ret = errno;
                        GOTO(err_ret, ret);
                } else if (0 == son) {
                        ret = recovery();
                        if (ret) 
                                GOTO(err_ret, ret);
                } else {
                        while (srv_running) {
                                ret = wait(&status);
                                if (ret == son)
                                        break;
                                ret = errno;
                                DERROR("Monitor: %d\n", ret);
                        }

                        if (WIFexitED(status)) {
                                if (WexitSTATUS(status) == 100)
                                        goto out;
                                DBUG("Monitor: worker exited normally %d\n",
                                     WexitSTATUS(status));
                                break;
                        } else if (WIFSIGNALED(status)) {
                                DERROR("Monitor: worker exited on signal %d\n"
                                       " restarting...", WTERMSIG(status));
                        } else
                                DERROR("Monitor: worker exited (stopped?) %d\n"
                                       " restarting...", status);
                }
        }

        ///////////////////////////////////////////////////////////////
        // DONE
        ///////////////////////////////////////////////////////////////
out:
        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ly_destroy() %s\n", strerror(ret));
                exit(1);
        }

        return 0;
err_ret:
        return ret;
}

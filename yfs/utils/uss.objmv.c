
#include <getopt.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>

#include "configure.h"
#include "adt.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "sdfs_buffer.h"
#include "ynet.h"
#include "md_lib.h"
#include "../objc/objc.h"

void usage()
{
        fprintf(stderr, "\nusage:\n"
                "uss.objmv --chkid <value> --from <nid>  --to <nid>\n"
                "    --chkid       Id of chunk will be moved, format likes <id>_v<volid>[<idx>]\n"
                "    --from, -f    The location of replica, format likes disk_<id>\n"
                "    --to, -t      The location of replica, format likes disk_<id>\n\n"
               );
}

static int __get_ip_by_diskid(const diskid_t *diskid, char *ip)
{
        int ret, seq, times = 0;
        const char *rname = NULL;

retry:
        rname = netable_rname_nid(diskid);
        if (rname == NULL || strlen(rname) == 0) {
                ret = network_connect1(diskid);
                if (ret)
                        GOTO(err_ret, ret);

                if (times++ == 0)
                        goto retry;
                else {
                        ret = ENOENT;
                        GOTO(err_ret, ret);
                }
        }

        ret = sscanf(rname, "%[^:]:cds/%d", ip, &seq);
        if (ret != 2) {
                DERROR("get diskid "DISKID_FORMAT" ip fail, rname:%s\n", DISKID_ARG(diskid), rname);
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __uss_check_diskid(const objid_t *id, const diskid_t *src, const diskid_t *dist)
{
        int ret = 0, i, src_find = 0;
        chkinfo_t *chkinfo;
        char buf[MAX_BUF_LEN] = "", buf1[MAX_BUF_LEN] = "", src_ip[MAX_NAME_LEN] = "", dist_ip[MAX_NAME_LEN] = "", tmp_ip[MAX_NAME_LEN];
        diskid_t *diskid;
        fileinfo_t *md;

        chkinfo = (void *)buf;
        md = (void *)buf1;

        ret = md_getattr((void *)md, id);
        if (ret)
                GOTO(err_ret, ret);

        if (md->plugin == PLUGIN_EC_ISA) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }

        ret = md_chkload(chkinfo, id, NULL);
        if (ret) {
                fprintf(stderr, "chkid "CHKID_FORMAT" load fail.\n", CHKID_ARG(id));
                GOTO(err_ret, ret);
        }

        if (ynet_nid_cmp(src, &chkinfo->diskid[chkinfo->master]) == 0) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }

        ret = __get_ip_by_diskid(dist, dist_ip);
        if (ret)
                GOTO(err_ret, ret);

        ret = __get_ip_by_diskid(src, src_ip);
        if (ret)
                GOTO(err_ret, ret);

        for (i = 0; i < (int)chkinfo->repnum; i++) {
                diskid = &chkinfo->diskid[i];

                if (gloconf.testing) {
                        if (ynet_nid_cmp(dist, diskid) == 0) {
                                ret = EPERM;
                                GOTO(err_ret, ret);
                        }
                } else {
                        __get_ip_by_diskid(diskid, tmp_ip);
                        if (strcmp(tmp_ip, dist_ip) == 0) {
                                if (strcmp(src_ip, dist_ip) == 0 && ynet_nid_cmp(src, dist) != 0) { //from 192.168.1.81/cds0 to 192.168.1.81/cds1 is permited
                                        ;
                                } else {
                                        ret = EPERM;
                                        GOTO(err_ret, ret);
                                }
                        }
                }

                if (src_find == 0 && ynet_nid_cmp(diskid, src) == 0) {
                        //if (i == (int)chkinfo->master || (diskid->status & __S_DIRTY)) {
                        if (diskid->status & __S_DIRTY) {
                                ret = EPERM;
                                GOTO(err_ret, ret);
                        }

                        src_find = 1;
                }

        }

        if (src_find)
                return 0;
        else
                return ENOENT;

err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret = 0;
        char c_opt;
        objid_t objid;
        diskid_t src = {0}, dist = {0};
        char src_ip[64] = "", dist_ip[64] = "";

        dbg_info(0);

        while (srv_running) {
                int option_index = 0;

                static struct option long_options[] = {
                        {"chkid", required_argument, 0, 0},
                        {"from", required_argument, 0, 'f'},
                        {"to", required_argument, 0, 't'},
                        { 0, 0, 0, 0 },
                };

                c_opt = getopt_long(argc, argv, "f:t:",
                                long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 0:
                        switch (option_index) {
                        case 0:
                                ret = sscanf(optarg, "%lu_v%lu[%u]", &objid.id, &objid.volid, &objid.idx);
                                if (ret != 3) {
                                        fprintf(stderr, "chkid error, format likes '<id>_v<version>[<idx>]', ret:%d\n", ret);
                                        exit(EINVAL);
                                }
                                break;
                        default:
                                fprintf(stderr, "Hoops, wrong op got !\n");
                                break;
                        }
                        break;
                case 'f':
                        ret = sscanf(optarg, DISKID_FORMAT, (LLU *)&src.id);
                        if (ret != 1) {
                                fprintf(stderr, "diskid %s error, format likes 'disk_<id>\n", optarg);
                                exit(EINVAL);
                        }
                        break;
                case 't':
                        ret = sscanf(optarg, DISKID_FORMAT, (LLU *)&dist.id);
                        if (ret != 1) {
                                fprintf(stderr, "diskid %s error, format likes 'disk_<id>'\n", optarg);
                                exit(EINVAL);
                        }
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        usage();
                        exit(1);
                }
        }

        if (argc < 6) {
                usage();
                exit(1);
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("uss.objmv");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        }

        __get_ip_by_diskid(&src, src_ip);
        __get_ip_by_diskid(&dist, dist_ip);

        ret = __uss_check_diskid(&objid, &src, &dist);
        if (ret){
                exit(ret);
        }

        printf("disk id check ok, begin move...\n");

        ret =  objc_move(&objid, &src, &dist);
        if (ret)
                exit(ret);

        printf("object "OBJID_FORMAT" move from %s to %s success !\n", OBJID_ARG(&objid), src_ip, dist_ip);
        return 0;
}

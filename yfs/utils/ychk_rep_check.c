
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <dirent.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "yfs_file.h"
#include "file_proto.h"
#include "file_table.h"
#include "chk_proto.h"
#include "chk_proto.h"
#include "job_dock.h"
#include "ylib.h"
#include "net_global.h"
#include "sdfs_lib.h"
#include "yfs_node.h"
#include "yfs_state_machine.h"
#include "yfs_limit.h"
#include "yfscds_conf.h"
#include "dbg.h"

#define YFS_ROOT_DIR    "/sysy/yfs/mds/1/root"

#define RED     "\033[31m"
#define GREEN   "\033[32m"
#define NORMAL  "\033[0m"

extern jobtracker_t *jobtracker;
int verbose = 0;

void show_help(char *prog)
{
        fprintf(stderr, "%s [-v]\n", prog);
}

int chk_locate(ynet_net_info_t *info, chkid_t *chkid, char *dpath)
{
        int ret, diskno;
        char cpath[MAX_PATH_LEN];

        ret = sscanf(info->name, "cds -n %u", &diskno);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        (void) cascade_id2path(cpath, MAX_PATH_LEN, chkid->id);

        (void) snprintf(dpath, MAX_PATH_LEN, "%s/%d/ychunk/%s_v%u",
                        YFS_CDS_DIR_DISK_PRE, diskno, cpath, chkid->version);

        return 0;
err_ret:
        return ret;
}

int get_rep_addr(chkid_t *chkid)
{
        int ret;
        uint32_t reqlen, i, j;
        mdp_chkload_req_t _mdp_req;
        mdp_chkload_req_t *mdp_req = &_mdp_req;
        mdp_chkload_rep_t *rep;
        ynet_net_info_t *info;
        char buf[MAX_BUF_LEN], loc[MAX_PATH_LEN];
        char **addr;
        buffer_t pack;
        nid_t peer;

        mdp_req->op = MDP_CHKLOAD;
        mdp_req->chkid.id = chkid->id;
        mdp_req->chkid.version = chkid->version;
        reqlen = sizeof(mdp_chkload_req_t);

        mbuffer_init(&pack, 0);
        peer = ng.mds_nh.u.nid;
        ret = rpc_request_wait2("mdc_printchk", &peer,
                        mdp_req, reqlen, &pack, MSG_MDP,
                        NIO_NORMAL, _get_timeout());
        if (ret) {
                GOTO(err_ret, ret);
        }

        rep = (void *)buf;
        mbuffer_get(&pack, rep, pack.len);

        addr = malloc(rep->chkrep * sizeof(char *));
        info = (ynet_net_info_t *)rep->info;
        for (i = 0; i < rep->chkrep; i++) {
                struct in_addr sin;
                addr[i] = malloc(128 * sizeof(char));
                for (j = 0; j < info->info_count; j++) {
                        sin.s_addr = htonl(info->info[j].addr);

                        chk_locate(info, chkid, loc);

                        memcpy(addr[i], inet_ntoa(sin), strlen(inet_ntoa(sin)));
                        addr[i][strlen(inet_ntoa(sin))] = 0;
                        if (verbose)
                                fprintf(stderr, "        net[%u]: (%u)%s\n", j,
                                                info->info[j].addr, addr[i]);
                }

                info = (void *)info + sizeof(ynet_net_info_t)
                       + sizeof(ynet_sock_info_t) * info->info_count;
        }

        for (i = 0; i < rep->chkrep; i++)
                for (j = i + 1; j < rep->chkrep; j++) {
                        if (!strcmp(addr[i], addr[j])) {
                                fprintf(stderr, RED"---------Same IP:(%s)\n"NORMAL, addr[i]);
                                break;
                        }
                }

        for (i = 0; i < rep->chkrep; i++)
                free(addr[i]);
        free(addr);

        mbuffer_free(&pack);
        return 0;
err_ret:
        mbuffer_free(&pack);
        return ret;
}

int rep_check(char *subpath)
{
        int ret, fd;
        char path[MAX_PATH_LEN];
        uint64_t  chkno, chkoff, chklen;
        chkid_t chkid;
        struct yfs_file *yf;
        md_chk_t *mdchk;

        snprintf(path, sizeof(path), "%s", subpath + strlen(YFS_ROOT_DIR));
        fprintf(stderr, ">>>> Check: %s\n", path);

        fd = ly_open(path);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        YASSERT(0);
#if 0
        yf = get_file(fd);
        if (yf == NULL) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }
#endif

        for (chkno = 0; chkno < yf->node->md->chknum; chkno++) {
                mdchk = &yf->node->md->chks[chkno];

                chkid  = mdchk->chkid;
                chkoff = mdchk->chkoff;
                chklen = mdchk->chklen;

                if (verbose)
                        fprintf(stderr, "   chk[%d]\n", chkno);

                ret = get_rep_addr(&chkid);
                if (ret)
                        GOTO(err_ret, ret);
        }

        close(fd);

        return 0;
err_ret:
        return ret;
}

int list_dir(char *path)
{
        int ret;
        DIR *pdir;
        struct dirent *ent;
        char subpath[MAX_PATH_LEN];
        struct stat buf;

        pdir = opendir(path);
        if (pdir == NULL) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        memset(subpath, 0x0, sizeof(subpath));

        while ((ent = readdir(pdir)) != NULL) {
                if (ent->d_name[0] == '.')
                        continue;

                snprintf(subpath, sizeof(subpath), "%s/%s", path, ent->d_name);

                memset(&buf, 0x0, sizeof(buf));
                ret = stat(subpath, &buf);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                if (S_ISDIR(buf.st_mode)) {
                        if (verbose)
                                fprintf(stderr, GREEN">>>> * Into subdir: %s\n"NORMAL, subpath);

                        list_dir(subpath);
                } else {
                        rep_check(subpath);
                }
        }
        return 0;
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret;
        char c_opt, *prog;

        dbg_info(0);

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        while ((c_opt = getopt(argc, argv, "v")) > 0)
                switch (c_opt) {
                case 'v':
                        verbose = 1;
                        break;
                default:
                        show_help(prog);
                        exit(1);
                }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init_simple("ychk_rep_check");
        if (ret)
                GOTO(err_ret, ret);

        ret = list_dir(YFS_ROOT_DIR);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

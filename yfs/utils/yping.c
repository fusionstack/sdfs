

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include "yfs_conf.h"
#include "sysutil.h"
#include "../mdc/md_lib.h"
#include "disk_proto.h"
#include "configure.h"
#include "sdfs_lib.h"
#include "cd_proto.h"
#include "configure.h"

void usage(const char *prog)
{
        printf("%s [-h] [-v] <-c|-m> <-s> [p]\n", prog);
}

static inline int  __cmp_load(const void *arg1, const void *arg2)
{
        if (((diskping_stat_t *)arg2)->load > ((diskping_stat_t *)arg1)->load)
                return 1;
        else if (((diskping_stat_t *)arg2)->load == ((diskping_stat_t *)arg1)->load)
                return 0;
        else
                return -1;
}

int main(int argc, char *argv[])
{
        int ret, len, mds = 0, cds = 0, verbose = 0;
        char c_opt, *prog, buf[MAX_BUF_LEN],
                name[MAX_NAME_LEN];
        diskping_stat_t *ds;
        struct sockaddr_in sin;

        dbg_info(0);

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        if (argc == 1) {
                mds = 1;
        } else {
                while ((c_opt = getopt(argc, argv, "chmp:s:v")) > 0)
                        switch (c_opt) {
                        case 'c':
                                cds = 1;
                                break;
                        case 'h':
                                usage(prog);
                                exit(1);
                                break;
                        case 'm':
                                mds = 1;
                                break;
                        case 'v':
                                verbose = 1;
                                break;
                        default:
                                fprintf(stderr, "Hoops, wrong op got!\n");
                                exit(1);
                        }
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if(ret)
                exit(1);

        if (mds == cds) {
                mds = 1;
        }

        if (verbose) {
        }

        ret = ly_init_simple("yping");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_init()'ed\n");

        buf[0] = '\0';

        if (mds == 1) {
                len = MAX_BUF_LEN;

                ret = md_diskinfo(buf, &len);
 
                qsort(buf, len / sizeof(diskping_stat_t),
                      sizeof(diskping_stat_t), __cmp_load);
        } else {
                UNIMPLEMENTED(__DUMP__);
                //ret = ly_pingcds(server, port, buf, MAX_BUF_LEN);
        }
        if (ret) {
                fprintf(stderr, "ly_ping%cds() %s\n", mds ? 'm' : 'c',
                        strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_ping%cds()'ed\n", mds ? 'm' : 'c');

        if (mds == 1) {
                ds = (diskping_stat_t *)buf;
                if (verbose)
                        printf("buffer length %d\n", len);

                _memset(&sin, 0, sizeof(sin));
                sin.sin_family = AF_INET;

                while ((unsigned)len >= sizeof(diskping_stat_t)) {
                        sin.sin_addr.s_addr = htonl(ds->sockaddr);

                        sprintf(name, "%s:%u", inet_ntoa(sin.sin_addr), ds->sockport);

                        printf("%s nid (%llu_v%u) status:%d %llu %llu load %llu\n",
                               strlen(ds->rname) == 0 ? name : ds->rname,
                               (LLU)ds->diskid.id, ds->diskid.version,
                               ds->diskstat,
                               (unsigned long long)ds->disktotal,
                               (unsigned long long)(ds->disktotal
                                                   - ds->diskfree),
                               (LLU)ds->load);

                        len -= sizeof(diskping_stat_t);
                        ds = (void *)ds + sizeof(diskping_stat_t);
                }
        } else {
#if 0
                cdp_ping_rep_t *ping_rep = (cdp_ping_rep_t *)buf;
                fprintf(stdout, "CDS (%s:%d) -- capacity %llu used %llu\n",
                        server, port, (unsigned long long)ping_rep->capacity,
                        (unsigned long long)ping_rep->used);
#endif
        }

        return 0;
}

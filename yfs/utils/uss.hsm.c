

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <dirent.h>

#include "configure.h"
#include "../cdc/replica.h"
#include "sdfs_lib.h"
#include "md_lib.h"

#define PROG    "uss.tgtadm"

/*
 * Global variable
 */
int verbose = 0;

static void usage()
{
        fprintf(stderr,
                "usage: uss.tgtadm [-verbose | -v] [--help | -h]\n"
                "               [--start | -t]\n"
                "               [--stop | -p]\n"
                "               [--info | -i]\n"
                "\n"
                "-v --verbose           Show verbose message\n"
                "-h --help              Show this help\n"
                "-t --start nid"
                "-p --stop nid"
                "-i --info nid"
                "\n"
               );
}

/*
 * Options structure for parse
 */
static struct option long_options[] = {
        { "verbose",    0, 0, 'v' },
        { "help",       0, 0, 'h' },
        { "start",         1, 0, 't' },
        { "stop",       1, 0, 'p' },
        { "info",       1, 0, 'i' },
        { 0, 0, 0, 0 },
};

const char optstr[] = "vht:p:i:";

static  int __str2id(nid_t *nid, const char *arg)
{
        int ret;

#ifdef __x86_64__
        ret = sscanf(arg, "%lu", &nid->id);
#else
        ret = sscanf(arg, "%llu", &nid->id);
#endif
        if (ret != 1) {
                DWARN("arg %s result %u\n", arg, ret);
                ret = EINVAL;
                goto err_ret;
        }

        return 0;
err_ret:
        return ret;
}


typedef enum {
        OP_INFO,
        OP_START,
        OP_STOP,
} op_t;

static int __hsm_start(const nid_t *nid)
{
        int ret;

        ret = cdc_hsmstart(nid);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __hsm_stop(const nid_t *nid)
{
        int ret;

        UNIMPLEMENTED(__WARN__);
        return 0;

        ret = cdc_hsmstop(nid);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __hsm_info(const nid_t *nid)
{
        int ret;

        UNIMPLEMENTED(__WARN__);
        return 0;

        ret = cdc_hsminfo(nid);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret;
        char c_opt;
        nid_t nid;
        op_t op = 0;

        dbg_info(0);

        while (srv_running) {
                int option_index = 0;

               c_opt = getopt_long(argc, argv, optstr, long_options, &option_index);
               if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 'v':
                        verbose = 1;
                        break;
                case 'h':
                        usage();
                        exit(0);
                case 't':
                        printf("hsm start %s\n", optarg);
                        op = OP_START;
                        ret = __str2id(&nid, optarg);
                        if (ret)
                                GOTO(err_ret, ret);

                        break;
                case 'p':
                        printf("hsm stop %s\n", optarg);
                        op = OP_STOP;
                        ret = __str2id(&nid, optarg);
                        if (ret)
                                GOTO(err_ret, ret);

                        break;
                case 'i':
                        printf("hsm info %s\n", optarg);
                        op = OP_INFO;
                        ret = __str2id(&nid, optarg);
                        if (ret)
                                GOTO(err_ret, ret);

                        break;
                default:
                        usage();
                        exit(EINVAL);
                }
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init_simple("uss.hsm");
        if (ret)
                GOTO(err_ret, ret);

        switch (op) {
        case OP_INFO:
                ret = __hsm_info(&nid);
                if (ret)
                        GOTO(err_ret, ret);
                break;
        case OP_START:
                ret = __hsm_start(&nid);
                if (ret)
                        GOTO(err_ret, ret);
                break;
        case OP_STOP:
                ret = __hsm_stop(&nid);
                if (ret)
                        GOTO(err_ret, ret);
                break;
        }

        return 0;
err_ret:
        exit(ret);
}

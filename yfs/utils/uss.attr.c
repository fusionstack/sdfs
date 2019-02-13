#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>

#include "configure.h"
#include "network.h"
#include "net_global.h"
#include "sdfs_lib.h"
#include "file_proto.h"
#include "xattr.h"

typedef enum {
        XATTR_SET,
        XATTR_GET,
        XATTR_REMOVE,
        XATTR_LIST,
        XATTR_INVALID_OP,
} xattr_op_type_t;

char *engine_types[] = {
        "",
        "localfs",
        "leveldb",
};

static inline void _xattr_usage(IN const char *prog)
{
        fprintf(stderr, "usage: %s[options] path or (id)_v(volumeid)\n"
                        "\t-s, --set NAME set the extra attribute of the path by name\n"
                        "\t-g, --get NAME get the extra attribute of the path by name\n"
                        "\t-r, --remove NAME remove the extra attribute of the path by name\n"
                        "\t-l, --list list all extra attribute names of the path\n"
                        "\t-V, --value VALUE the extra attribute value of the path by create or replace command\n"
                        "\t-f, --flags FLAG the flag of set extra attribute operation(0:default, 1:create, 2:replace)\n"
                        "\t-h, --help  help information\n", prog);
}

static inline void _xattr_usage_error(void)
{
        fprintf(stderr, "uss.attr: invalid argument.\n"
                        "Try 'uss.attr --help' for more information.\n");
}

static int _xattr_get_flag(IN const char *optarg)
{
        int flags = USS_XATTR_INVALID;

        if (0 == strcmp("default", optarg) || 0 == strcmp("0", optarg)) {
                flags = USS_XATTR_DEFAULT;
        } else if (0 == strcmp("create", optarg) || 0 == strcmp("1", optarg)) {
                flags = USS_XATTR_CREATE;
        } else if (0 == strcmp("replace", optarg) || 0 == strcmp("2", optarg)) {
                flags = USS_XATTR_REPLACE;
        }

        return flags;
}

static void _xattr_print_listinfo(char *list, size_t size)
{
        int i;
        char *name = list;
        int namelen = 0;
        size_t left = size;

        printf("all xattr names is: \n");
        for (i = 1; left > 0; i++, left -= namelen) {
                printf("%s\t", name);
                namelen = strlen(name) + 1;
                name += namelen;
                if (i % 5 == 0)
                        printf("\n");
        }
        if ((i - 1) % 5 != 0)
                printf("\n");
}

static struct option xattr_options[] = {
        {"set",     required_argument, 0, 's'},
        {"get",     required_argument, 0, 'g'},
        {"remove",  required_argument, 0, 'r'},
        {"list",    no_argument,       0, 'l'},
        {"value",   required_argument, 0, 'v'},
        {"flags",   required_argument, 0, 'f'},
        {"help",    no_argument,       0, 'h'},
        {0,         0,                 0,  0 },
};


int main(int argc, char *argv[])
{
        int ret;
        size_t size = 0;
        char c_opt, *prog, *path = NULL;
        char normal_path[MAX_PATH_LEN];
        fileid_t fileid;
        int flags = USS_XATTR_DEFAULT;
        xattr_op_type_t op_type = XATTR_INVALID_OP;
        char  *name = NULL, *value = NULL;
        char buf[MAX_BUF_LEN];

        dbg_info(0);

        memset(buf, 0, sizeof(buf));

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        while (1) {
                int option_index = 0;

                c_opt = getopt_long(argc, argv, "s:g:r:lV:f:lh", xattr_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 's':
                        name = optarg;
                        op_type = XATTR_SET;
                        break;
                case 'g':
                        name = optarg;
                        op_type = XATTR_GET;
                        break;
                case 'r':
                        name = optarg;
                        op_type = XATTR_REMOVE;
                        break;
                case 'l':
                        op_type = XATTR_LIST;
                        break;
                case 'V':
                        value = optarg;
                        if (strlen(value) > MAX_NAME_LEN - 1) {
                                fprintf(stderr, "value:%s is too long\n", value);
                                exit(1);
                        }
                        break;
                case 'f':
                        flags = _xattr_get_flag(optarg);
                        if (USS_XATTR_INVALID == flags) {
                                _xattr_usage_error();
                                exit(1);
                        }
                        break;
                case 'h':
                        _xattr_usage(prog);
                        exit(0);
                default:
                        _xattr_usage_error();
                        exit(1);
                }
        }

        if ((NULL != name) && (strlen(name) > MAX_NAME_LEN - 1)) {
                fprintf(stderr, "name:%s is too long\n", name);
                exit(1);
        }

        if ((XATTR_SET == op_type) && (NULL == value)) {
                fprintf(stderr, "need enter -v argument for set operation\n");
                exit(1);
        }

        if (optind != argc - 1) {
                _xattr_usage_error();
                exit(1);
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret) {
                fprintf(stderr, "conf_init() %s\n", strerror(ret));
                exit(1);
        }

        ret = ly_init_simple("xattr");
        if (ret)
                GOTO(err_ret, ret);

        path = argv[optind];
        if (path[0] == '/') {
                if(NULL == sdfs_realpath(path, normal_path)) {
                        fprintf(stderr, "invalid path %s\n", path);
                        exit(1);
                }

                ret = sdfs_lookup_recurive(normal_path, &fileid);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
#ifdef __x86_64__
                ret = sscanf(path, "%lu_v%lu", &fileid.id, &fileid.volid);
#else
                ret = sscanf(path, "%llu_v%llu", &fileid.id, &fileid.volid);
#endif
                if (ret != 2) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
                fileid.idx = 0;
        }

        switch (op_type) {
        case XATTR_SET:
                size = strlen(value) + 1;
                ret = sdfs_setxattr(NULL, &fileid, name, value, size, flags);
                if (ret)
                        GOTO(err_ret, ret);
                break;
        case XATTR_GET:
                value = buf;
                size = sizeof(buf);
                ret = sdfs_getxattr(NULL, &fileid, name, value, &size);
                if (ret)
                        GOTO(err_ret, ret);

                if (strcmp(USS_SYSTEM_ATTR_ENGINE, name) == 0)
                        printf("%s\n", engine_types[atoi(value)]);

                else
                        printf("%s\n", value);

                break;
        case XATTR_REMOVE:
                ret = sdfs_removexattr(NULL, &fileid, name);
                if (ret)
                        GOTO(err_ret, ret);
                break;
        case XATTR_LIST:
                value = buf;
                size = sizeof(buf);
                ret = sdfs_listxattr(NULL, &fileid, value, &size);
                if (0 == ret) {
                        if (0 == size)
                                printf("have no xattr names.\n");
                        else
                                _xattr_print_listinfo(value, size);
                }
                break;
        default:
                _xattr_usage_error();
                ret = EINVAL;
                goto err_ret;
        }

        return 0;
err_ret:
        return ret;
}

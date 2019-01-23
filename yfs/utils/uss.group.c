/*
*Date   : 2017.07.28
*Author : jinxx
*uss.group : the command for add group, etc.
*/
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>

#include "dbg.h"
#include "configure.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "group.h"

static void group_usage(void)
{
        fprintf(stderr, "Usage: uss.group [OPTION] GROUP\n"
                "\t-s, --set create or modify a group\n"
                "\t-r, --remove remove a group\n"
                "\t-g, --gid The numerical value of the group's ID.\n"
                "\t-G, --get get a group info\n"
                "\t-l, --list list all groups info\n"
                "\t-h, --help display this help and exits\n");
}

static int list_groups_info(void)
{
        int ret, i, count;
        group_t *group = NULL, *gptr = NULL;

        ret = group_list(&group, &count);
        if (ret) {
                fprintf(stderr, "group list fail, ret:%d, errmsg:%s\n",
                        ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        if (count) {
                for (i = 0; i < count; i++) {
                        gptr = &group[i];
                        printf("group:%-10s\tgid:%u\n", gptr->gname, gptr->gid);
                }

                yfree((void **)&group);
        }
        
        return 0;
err_ret:
        return ret;
}

static int get_gid (const char *gid_str, gid_t *gid)
{
        int ret = EINVAL;
        gid_t gid_tmp;
        char *end_point;

        gid_tmp = strtoul (gid_str, &end_point, 10);
        //valid number
        if (*gid_str != '\0' && *end_point == '\0') {
                *gid = gid_tmp;
                ret = 0;
        } else {
                fprintf (stderr, "invalid numeric argument '%s'\n", optarg);
        }

        return ret;
}


static struct option long_options[] = {
        {"set",     no_argument,       NULL, 's'},
        {"get",     no_argument,       NULL, 'G'},
        {"remove",  no_argument,       NULL, 'r'},
        {"list",    no_argument,       NULL, 'l'},
        {"gid",     required_argument, NULL, 'g'},
        {"help",    no_argument,       NULL, 'h'},
        {NULL,      0,                 NULL, '\0'},
};

int main(int argc, char *argv[])
{
        int ret;
        char c_opt;
        char *group_name = NULL;
        group_t group;
        bool set_gid_flag = false;
        group_op_type_t g_opt = GROUP_INVALID_OP;

        memset(&group, 0, sizeof(group));

        dbg_info(0);

        while (srv_running) {
                int option_index = 0;

                c_opt = getopt_long(argc, argv, "sGrlg:h", long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 's':
                        g_opt = GROUP_SET;
                        break;
                case 'r':
                        g_opt = GROUP_REMOVE;
                        break;
                case 'g':
                        ret = get_gid(optarg, &group.gid);
                        if (ret)
	                        exit(1);
                        set_gid_flag = true;
                        break;
                case 'G':
                        g_opt = GROUP_GET;
                        break;
                case 'l':
                        g_opt = GROUP_LIST;
                        break;
                case 'h':
                        group_usage();
                        exit(0);
                default:
                        group_usage();
                        exit(1);
                }
        }

        if (GROUP_INVALID_OP == g_opt) {
                group_usage();
                exit(1);
        }

        if (GROUP_LIST != g_opt) {
                if (optind != argc - 1) {
                        group_usage();
                        exit(1);
                }

                group_name = argv[optind];
                if (strlen(group_name) < MAX_NAME_LEN &&
                    false == is_valid_name(group_name)) {
                        fprintf(stderr, "group name %s is invalid\n", group_name);
                        exit(1);
                }
        }

        if ((GROUP_SET == g_opt) && (false == set_gid_flag)) {
                fprintf(stderr, "need enter group id.\n");
                exit(1);
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("uss.group");
        if (ret) {
                fprintf(stderr, "ly_init() failed, error: %s.\n", strerror(ret));
                exit(1);
        }

        switch (g_opt) {
        case GROUP_SET:
                strcpy(group.gname, group_name);
                ret = group_set(&group);
                if (EEXIST == ret) {
                        fprintf(stderr, "group name %s, or group id %u is exist.\n",
                                group.gname, group.gid);
                        exit(1);
                }
                break;
        case GROUP_GET:
                ret = group_get(group_name, &group);
                if (ERROR_SUCCESS == ret)
                        printf("group:%s\tgid:%u\n", group.gname, group.gid);
                break;
        case GROUP_REMOVE:
                ret = group_remove(group_name);
                break;
        case GROUP_LIST:
                ret = list_groups_info();
                break;
        default:
                group_usage();
                exit(1);
        }

        if (ERROR_SUCCESS != ret)
                fprintf(stderr, "uss.group failed, error: %s.\n", strerror(ret));

        return ret;
}

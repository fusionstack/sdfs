/*
*Date   : 2017.07.25
*Author : jinxx
*uss.user : the command for add user, etc.
*/
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <dirent.h>
#include <ctype.h>

#include "dbg.h"
#include "configure.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "user.h"
#include "group.h"

static void usage()
{
        fprintf(stderr, "usage: uss.user [OPTION] USER\n"
                "\t-s --set create or modify a user\n"
                "\t-r --remove remove a user\n"
                "\t-g --gid user name or id of primary group\n"
                "\t-u --uid The numerical value of the user's ID.\n"
                "\t-p --password allow [a-z] [A-Z] [-] [_] [0-9] *[$]\n"
                "\t-G --get get a user info\n"
                "\t-l --list list all users info\n"
                "\t-h --help display this help and exits\n");
}

static inline void print_usage_error()
{
        fprintf(stderr, "uss.user: invalid argument.\nTry 'uss.user --help' for more information.\n");
}

#if 0
static int __uss_user_list(const user_op_t *_user_op)
{
        int ret = 0, len = 0, offset = 0;
        user_t *user = NULL, *uptr = NULL, *last_user = NULL;
        user_op_t user_op;

        user_op.opt = _user_op->opt;
        memset(&user_op.user, 0, sizeof(user_op.user));

        while (1) {
                ret = user_get(&user_op, &user, &len);
                if (ret) {
                        fprintf(stderr, "user_get fail, ret:%d, errmsg:%s\n", ret, strerror(ret));
                        GOTO(err_ret, ret);
                } else if (len == 0) {
                        break;
                }

                for (offset = 0, uptr = user; offset < len; offset += sizeof(*user)) {
                        if (strlen(uptr->name) == 0) {
                                yfree((void **)&user);
                                goto out;
                        }

                        printf("user:%s\tuid:%u\tgid:%u\tpassword:%s\n", uptr->name, uptr->uid, uptr->gid, uptr->password);
                        last_user = uptr;
                        uptr ++;
                }

                memcpy(user_op.user.name, last_user->name, sizeof(user_op.user.name));
                yfree((void **)&user);
        }
out:

        return 0;
err_ret:
        return ret;
}
#endif

static int list_users_info(void)
{
        int ret, count, i;
        user_t *user = NULL, *uptr = NULL;

        ret = user_list(&user, &count);
        if (ret) {
                fprintf(stderr, "user list fail, ret:%d, errmsg:%s\n",
                        ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        if (count) {
                for (i = 0; i < count; i++) {
                        uptr = &user[i];
                        printf("user:%s\tuid:%u\tgid:%u\tpassword:%s\n",
                               uptr->name, uptr->uid, uptr->gid, uptr->password);
                }

                yfree((void **)&user);
        }
        
        return 0;
err_ret:
        return ret;
}

static struct option long_options[] = {
        {"set",      no_argument,       NULL, 's'},
        {"get",      no_argument,       NULL, 'G'},
        {"remove",   no_argument,       NULL, 'r'},
        {"list",     no_argument,       NULL, 'l'},
        {"gid",      required_argument, NULL, 'g'},
        {"uid",      required_argument, NULL, 'u'},
        {"password", required_argument, NULL, 'p'},
        {"help",     no_argument,       NULL, 'h'},
        {NULL,       0,                 NULL, '\0'},
};

static int get_unsigned_int(const char *str, unsigned int *_id)
{
        int ret = EINVAL;
        unsigned int id;
        char *end_point;

        id = strtoul(str, &end_point, 10);

        if (*str != '\0' && *end_point == '\0') {
                *_id = id;
                ret = 0;
        } else {
                fprintf (stderr, "invalid numeric argument '%s'\n", str);
        }
        return ret;
}

int main(int argc, char *argv[])
{
        int ret;
        char c_opt;
        char *user_name = NULL;
        bool set_uid_flag = false;
        bool set_gid_flag = false;
        user_t user;
        user_op_type_t u_opt = USER_INVALID_OP;

        memset(&user, 0x0, sizeof(user));

        while (srv_running) {
                int option_index = 0;

                c_opt = getopt_long(argc, argv, "sGrlu:g:p:h", long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 's':
                        u_opt = USER_SET;
                        break;
                case 'r':
                        u_opt = USER_REMOVE;
                        break;
                case 'g':
                        ret = get_unsigned_int(optarg, &user.gid);
                        if (ret)
                                exit(1);
                        set_gid_flag = true;
                        break;
                case 'u':
                        ret = get_unsigned_int(optarg, &user.uid);
                        if (ret)
                                exit(1);
                        set_uid_flag = true;
                        break;
                case 'p':
                        if (!is_valid_password(optarg)) {
                                print_usage_error();
                                exit(1);
                        }
                        strcpy(user.password, optarg);
                        break;
                case 'G':
                        u_opt = USER_GET;
                        break;
                case 'l':
                        u_opt = USER_LIST;
                        break;
                case 'h':
                        usage();
                        exit(0);
                default:
                        usage();
                        exit(1);
                }
        }

        if (USER_INVALID_OP == u_opt) {
                usage();
                exit(1);
        }

        if (USER_LIST != u_opt) {
                if (optind != argc - 1)
                        usage();

                user_name = argv[optind];
                if (strlen(user_name) < MAX_NAME_LEN
                    && false == is_valid_name(user_name)) {
                        fprintf(stderr, "user name %s is invalid\n", user_name);
                        exit(1);
                }
        }

        if ((USER_SET == u_opt)
            && (false == set_uid_flag)
            && (false == set_gid_flag)
            && strlen(user.password) == 0) {
                fprintf(stderr, "set user info need enter uid or gid or password.\n");
                exit(1);
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        dbg_info(0);
        
        ret = ly_init_simple("uss.user");
        if (ret) {
                fprintf(stderr, "ly_init() failed, error: %s.\n", strerror(ret));
                exit(1);
        }

        switch (u_opt) {
        case USER_SET:
                strcpy(user.name, user_name);
                ret = user_set(&user);
                if (EEXIST == ret) {
                        fprintf(stderr, "user name %s, or user id %u is exist.\n",user.name, user.uid);
                        exit(1);
                }
                break;
        case USER_REMOVE:
                ret = user_remove(user_name);
                break;
        case USER_GET:
                ret = user_get(user_name, &user);
                break;
        case USER_LIST:
                ret = list_users_info();
                break;
        default:
                print_usage_error();
                exit(1);
        }

        if (ERROR_SUCCESS != ret) {
                fprintf(stderr, "uss.user failed, error: %s.\n", strerror(ret));
        } else {
                if (u_opt == USER_GET)
                        printf("user:%s\nuid:%u\ngid:%u\npassword:%s\n",
                                user.name, user.uid, user.gid, user.password);
        }

        return ret;
}

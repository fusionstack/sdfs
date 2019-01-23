

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#include "chk_meta.h"
#include "ylib.h"
#include "job_dock.h"
#include "redis_util.h"
#include "redis.h"
#include "dbg.h"
#include "option.h"
#include "sdfs_lib.h"
#include "md_lib.h"
//#include "leveldb_util.h"
#include "atomic_id.h"

#define MAX_SUBCMD_DEPTH 8
#define CMD_NEED_NULL (1 << 0)
#define CMD_NEED_ARG (1 << 1)
#define CMD_NEED_ROOT (1 << 2)

// insert line break
#define CMD_BR "BR"

struct command {
        const char *name;
        const struct subcommand *sub;
        int (*parser)(int, const char *);
};

struct subcommand {
        const char *name;
        const char *arg;
        const char *opts;
        const char *desc;
        const struct subcommand *sub;
        unsigned long flags;
        int (*fn)(int, char **);
        const struct sd_option *options;
};

static const char program_name[] = "uss.admin";
static char system_cmd[] = "admin";
static bool found_system_cmd = false;

int subcmd_depth = -1;
struct subcommand *subcmd_stack[MAX_SUBCMD_DEPTH];

static const struct sd_option uss_admin_options[] = {
        {'v', "verbose", false, "print more information than default", NULL},
        {'h', "help", false, "display this help and exit", NULL},
        { 0, NULL, false, NULL, NULL},
};

/*static struct pool_cmd_data {*/
/*} pool_cmd_data;*/

static struct admin_cmd_data {
        int output_format;
        int mult_thread;
        size_t size;
        char storage_area[MAX_NAME_LEN];
        char protocol_name[MAX_NAME_LEN];
        char image_name[MAX_NAME_LEN];
        char path[MAX_NAME_LEN];
} admin_cmd_data;

struct cmd_flag_data {
        bool all_flag;
        bool mult_thread_flag;
        bool size_flag;
        bool protocol_name_flag;
        bool image_name_flag;
        bool path_flag;
        bool output_format_flag;
        bool storage_area_flag;
        bool thin_flag;
        char priority;
        int arguments;
} cmd_flag_data = {false, false, false, false, false, false, false, false, false, -1, 0};

void subcommand_usage(char *cmd, char *subcmd, int status);

static int (*command_parser)(int, const char *);
static int (*command_fn)(int, char **);
static const char *command_opts;
static const char *command_arg;
static const char *command_desc;
static const struct sd_option *command_options;


/*static char *__get_default_storage_area()*/
/*{*/
        /*return "default";*/
/*}*/

/*
static int __stor_snap_clone(int argc, char **argv)
{
        int ret, arguments = 0;
        char snappath[MAX_NAME_LEN], to[MAX_NAME_LEN];
        char *_snappath = NULL, *_to = NULL;

        arguments = cmd_flag_data.arguments * 2 + 2 + 2;

        if (argc < arguments) {
                ret = EINVAL;
                fprintf(stderr, "lichbd: too few arguments\n");
                GOTO(err_ret, ret);
        } else if (argc > arguments) {
                ret = EINVAL;
                fprintf(stderr, "lichbd: too more arguments\n");
                GOTO(err_ret, ret);
        } else {
                _snappath = argv[argc - 2];
                _to = argv[argc - 1];
        }

        if (!__is_valid_snappath(_snappath)) {
                ret = EINVAL;
                fprintf(stderr, "snap path was invalid\n");
        }

        __add_lichbd_pre(snappath, _snappath, cmd_flag_data);

        if (!__is_valid_image(_to)) {
                ret = EINVAL;
                fprintf(stderr, "_image was invalid\n");
                GOTO(err_ret, ret);
        }
        __add_lichbd_pre(to, _to, cmd_flag_data);

        ret = utils_snapshot_clone(snappath, to, cmd_flag_data.priority, snap_cmd_data.storage_area);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        ret = md_chunk_setsite_path(to, admin_cmd_data.storage_area);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}
*/
static int __check_arg(int argc, char **argv, int arguments)
{
        int ret;

        (void)argv;

        if (argc < arguments) {
                ret = EINVAL;
                fprintf(stderr, "uss.admin: too few arguments\n");
                GOTO(err_ret, ret);
        } else if (argc > arguments) {
                ret = EINVAL;
                fprintf(stderr, "uss.admin: too more arguments\n");
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __init_root(int argc, char **argv)
{
        int ret, arguments = 0;

        arguments = cmd_flag_data.arguments * 2 + 2;

        ret = __check_arg(argc, argv, arguments);
        if (ret)
                GOTO(err_ret, ret);

        ret = md_initroot();
        if (ret)
                GOTO(err_ret, ret);

        printf("init root ok\n");

        return 0;
err_ret:
        return ret;
}

static void usage(const struct command *commands, int status)
{
        int i;
        const struct subcommand *s;
        char name[64];
        char text[MAX_NAME_LEN];

        if (status) {
                fprintf(stderr, "Try '%s help' for more information.\n", program_name);
        } else {
                printf("Usage: %s [OPTIONS] <cmd> ...\n", program_name);
                printf("\nAvailable commands:\n");
                for (i = 0; commands[i].name; i++) {
                        for (s = commands[i].sub; s->name; s++) {
                                if (!strcmp(s->name, CMD_BR)) {
                                        printf("\n");
                                        continue;
                                }
                                if (!strcmp(commands[i].name, system_cmd)) {
                                        snprintf(name, sizeof(name), "%s", s->name);
                                } else {
                                        snprintf(name, sizeof(name), "%s %s",
                                                commands[i].name, s->name);
                                }
                                sprintf(text, "%s %s", name, s->arg);
                                if (strlen(text) >= 45) {
                                        printf("  %s\n", text);
                                        printf("  %-45s%s\n", "", s->desc);
                                } else {
                                        printf("  %-45s%s\n", text, s->desc);
                                }
                        }
                        // add empty line
                        printf("\n");
                }
                printf("\n");
                /*printf("supported protocol: iscsi, lichbd, nbd\n");*/
                /*printf("if protocol is iscsi, len(iqn) + len(pool) + len(vol) + 2 must be less than 223\n");*/
                /*printf("\tand only accept one layer of pool, character only allow 'a-z', '0-9', '.', '-', ':'\n");*/
                /*printf("For more information, run "*/
                                /*"'%s <command> <subcommand> help'.\n", program_name);*/
        }
        exit(status);
}

static struct sd_option admin_options[] = {
        {'A', "area", true, "storage area", NULL},
        {'S', "single", false, "do not use multi thread", "do not use mult thread"},
        {'s', "size", true, "size of image for create and resize", NULL},
        {'p', "protocol", true, "protocol name", NULL},
        {'P', "path", true, "path name for import/export", NULL},
        {'i', "image", true, "image name", NULL},
        {'I', "image-format", true, "image format: [1|2]", NULL},
        {'f', "format", true, "output format, such as json", NULL},
        {'t', "thin", true, "thin import: [0|1]", NULL},
        {'T', "tier", true, "tier: [0|1]", NULL},
        { 0, NULL, false, NULL, NULL},
};

static struct subcommand admin_cmd[] = {
        {"init_root", "", "", "init root",
                NULL, CMD_NEED_NULL,
                __init_root, admin_options},
        {CMD_BR, NULL, NULL, NULL, NULL, CMD_NEED_NULL, NULL, NULL},
        {NULL, NULL, NULL, NULL, NULL, CMD_NEED_NULL, NULL, NULL},
};

static int admin_parser(int ch, const char *opt)
{
        /*int ret;*/
        DINFO("ch : %d, opt: %s\n", ch, opt);

        switch (ch) {
                case 't':
                        break;
                default:
                        break;
        }

        return 0;
/*err_ret:*/
        /*return ret;*/
}

struct command admin_command = {
        "admin",
        admin_cmd,
        admin_parser
};

static const struct sd_option *find_opt(int ch)
{
        const struct sd_option *opt;

        /* search for common options */
        sd_for_each_option(opt, uss_admin_options) {
                if (opt->ch == ch)
                        return opt;
        }

        /* search for self options */
        if (command_options) {
                sd_for_each_option(opt, command_options) {
                        if (opt->ch == ch)
                                return opt;
                }
        }

        fprintf(stderr, "Internal error\n");
        exit(1);
}

static void init_commands(const struct command **commands)
{
        int ret;
        static struct command *cmds;
        struct command command_list[] = {
                admin_command,
                {NULL, NULL, NULL},
        };

        if (!cmds) {
                ret = ymalloc((void **)&cmds, sizeof(command_list));
                if (unlikely(ret)) {
                        YASSERT(0);
                }
                memcpy(cmds, command_list, sizeof(command_list));
        }

        *commands = cmds;
        return;
}

static const struct subcommand *find_subcmd(const char *cmd, const char *subcmd)
{
        int i, j;
        const struct command *commands;
        const struct subcommand *sub;

        init_commands(&commands);

        for (i = 0; commands[i].name; i++) {
                if (!strcmp(commands[i].name, cmd)) {
                        sub = commands[i].sub;
                        for (j = 0; sub[j].name; j++) {
                                if (!strcmp(sub[j].name, subcmd))
                                        return &sub[j];
                        }
                }
        }

        return NULL;
}

static unsigned long setup_commands(const struct command *commands,
                const char *cmd, const char *subcmd)
{
        int i;
        bool found = false;
        const struct subcommand *s;
        unsigned long flags = 0;

        for (i = 0; commands[i].name; i++) {
                if (!strcmp(commands[i].name, cmd)) {
                        found = true;
                        if (commands[i].parser)
                                command_parser = commands[i].parser;
                        break;
                }
        }

        if (!found) {
                for (i = 0; commands[i].name; i++) {
                        if (!strcmp(commands[i].name, system_cmd)) {
                                break;
                        }
                }

                for (s = commands[i].sub; s->name; s++) {
                        if (!strcmp(s->name, cmd)) {
                                found = true;
                                found_system_cmd = true;
                                if (commands[i].parser)
                                        command_parser = commands[i].parser;
                                break;
                        }
                }
        }

        if (!found) {
                if (cmd && strcmp(cmd, "help") && strcmp(cmd, "--help") &&
                                strcmp(cmd, "-h")) {
                        fprintf(stderr, "Invalid command '%s'\n", cmd);
                        usage(commands, EINVAL);
                }
                usage(commands, 0);
        }

        for (s = commands[i].sub; (!found_system_cmd?subcmd:cmd) && s->name; s++) {
                if (!strcmp(s->name, (!found_system_cmd?subcmd:cmd))) {
                        command_fn = s->fn;
                        command_opts = s->opts;
                        command_arg = s->arg;
                        command_desc = s->desc;
                        command_options = s->options;
                        flags = s->flags;
                        break;
                }
        }

        if (!command_fn) {
                if (found_system_cmd) {
                        usage(commands, 0);
                }

                if (subcmd && strcmp(subcmd, "help") &&
                                strcmp(subcmd, "--help") && strcmp(subcmd, "-h"))
                        fprintf(stderr, "Invalid command '%s %s'\n", cmd, subcmd);

                fprintf(stderr, "Available %s commands:\n", cmd);
                for (s = commands[i].sub; s->name; s++)
                        fprintf(stderr, "  %s %s\n", cmd, s->name);
                exit(EINVAL);
        }

        return flags;
}

void subcommand_usage(char *cmd, char *subcmd, int status)
{
        int i, n, len = strlen(command_opts);
        const struct sd_option *sd_opt;
        const struct subcommand *sub, *subsub;
        char name[64];
        char text[MAX_NAME_LEN];

        printf("Usage: %s %s %s", program_name, found_system_cmd?"":cmd, subcmd);

        if (0 <= subcmd_depth) {
                for (i = 0; i < subcmd_depth + 1; i++)
                        printf(" %s", subcmd_stack[i]->name);

                subsub = subcmd_stack[i - 1]->sub;
        } else {
                sub = find_subcmd(cmd, subcmd);
                subsub = sub->sub;
        }

        if (subsub) {
                n = 0;
                while (subsub[n].name)
                        n++;
                if (n == 1)
                        printf(" %s", subsub[0].name);
                else if (n > 1) {
                        printf(" {%s", subsub[0].name);
                        for (i = 1; i < n; i++)
                                printf("|%s", subsub[i].name);
                        printf("}");
                }
        }

        /**
        for (i = 0; i < len; i++) {
                sd_opt = find_opt(command_opts[i]);
                if (sd_opt->has_arg) {
                        if (sd_opt->ch != 's')
                                printf(" [-%c %s]", sd_opt->ch, sd_opt->name);
                }
                else
                        printf(" [-%c]", sd_opt->ch);
        }
        **/

        if (command_arg)
                printf(" %s", command_arg);

        printf("\n");
        if (subsub) {
                printf("Available subcommands:\n");
                for (i = 0; subsub[i].name; i++) {
                        sprintf(text, "%s %s", subsub[i].name, subsub[i].arg);
                        if (strlen(text) >= 45) {
                                printf("  %s\n", text);
                                printf("  %-45s%s\n", "", subsub[i].desc);
                        } else {
                                printf("  %-45s%s\n", text, subsub[i].desc);
                        }
                }

        }

        for (i = 0; i < len; i++) {
                if (i == 0)
                        printf("Options:\n");
                sd_opt = find_opt(command_opts[i]);
                snprintf(name, sizeof(name), "-%c, --%s",
                                sd_opt->ch, sd_opt->name);
                printf("  %-24s%s\n", name, sd_opt->desc);
        }

        exit(status);
}

static const struct sd_option *build_sd_options(const char *opts)
{
        static struct sd_option sd_opts[256], *p;
        int i, len = strlen(opts);

        p = sd_opts;
        for (i = 0; i < len; i++)
                *p++ = *find_opt(opts[i]);
        memset(p, 0, sizeof(struct sd_option));

        return sd_opts;
}

static int format_args(int argc, char *argv[], char *new[])
{
        int i, c = 0, c1 = 0;
        char *new1[256];

        for (i = 0; i < argc; i++) {
                if ('-' == argv[i][0]) {
                        new1[c1] = (char *)malloc(256);
                        strcpy(new1[c1++], argv[i++]);
                        if (NULL != argv[i]) {
                                new1[c1] = (char *)malloc(256);
                                strcpy(new1[c1++], argv[i]);
                        }
                } else {
                        new[c] = (char*)malloc(256);
                        strcpy(new[c++], argv[i]);
                }
        }

        for (i = 0; i < c1; i++) {
                new[i + c] = (char*)malloc(256);
                strcpy(new[i + c], new1[i]);
        }

        return 0;
}

int main(int argc, char *argv[])
{
        int ch, longindex, ret;
        unsigned long flags;
        struct option *long_options;
        const struct command *commands;
        const char *short_options;
        const struct sd_option *sd_opts;
        bool verbose;
        char argument1[256], argument2[256];
        char *argvnew[256];

        admin_cmd_data.mult_thread = 1;
        admin_cmd_data.output_format = 0;
        strcpy(admin_cmd_data.image_name, "");
        strcpy(admin_cmd_data.protocol_name, "");
        strcpy(admin_cmd_data.path, "");
        admin_cmd_data.size = 0;

        init_commands(&commands);

        if (argc == 1) {
                usage(commands, 0);
                exit(EINVAL);
        }

        format_args(argc, argv, argvnew);

        if (argc > 2) {
                strcpy(argument1, argvnew[1]);
                strcpy(argument2, argvnew[2]);
                flags = setup_commands(commands, argvnew[1], argvnew[2]);
        } else {
                strcpy(argument1, argvnew[1]);
                flags = setup_commands(commands, argvnew[1], NULL);
        }

        optind = 3?!found_system_cmd:2;

        sd_opts = build_sd_options(command_opts);
        long_options = build_long_options(sd_opts);
        short_options = build_short_options(sd_opts);

        while ((ch = getopt_long(argc, argv, short_options, long_options,
                                        &longindex)) >= 0) {
                switch (ch) {
                        case 'v':
                                verbose = true;
                                break;
                        case 'h':
                                subcommand_usage(argv[1], argv[2], 0);
                                break;
                        case '?':
                                usage(commands, EINVAL);
                                break;
                        default:
                                if (command_parser) {
                                        ret = command_parser(ch, optarg);
                                        if (unlikely(ret))
                                                GOTO(err_ret, ret);
                                }
                                else
                                        usage(commands, EINVAL);
                                break;
                }
        }

        (void) verbose;
        if (flags & CMD_NEED_ARG && argc == optind) {
                if (found_system_cmd) {
                        subcommand_usage(system_cmd, argument1, EINVAL);
                } else {
                        subcommand_usage(argument1, argument2, EINVAL);
                }
        }

        int i;
        for (i = 0; i< argc; i++) {
                if (!strcmp(argv[i], "help")) {
                        if (found_system_cmd) {
                                subcommand_usage(system_cmd, argument1, 0);
                        } else {
                                subcommand_usage(argument1, argument2, 0);
                        }
                }
        }

        ret = ly_init_simple2("uss.admin");
        if (unlikely(ret))
                GOTO(err_ret, ret);

        //dbg_info(0);
        
        ret = command_fn(argc, argv);
        if (unlikely(ret)) {
                if (ret == EINVAL) {
                        if (found_system_cmd) {
                                subcommand_usage(system_cmd, argument1, EINVAL);
                        } else {
                                subcommand_usage(argument1, argument2, EINVAL);
                        }
                } else {
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        /*exit(_errno(ret));*/
        return ret;
}

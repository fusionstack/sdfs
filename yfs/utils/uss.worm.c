#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>


#include "sdfs_id.h"
#include "yfs_md.h"
#include "worm_cli_lib.h"
#include "sdfs_ec.h"
#include "sdfs_lib.h"
#include "xattr.h"

#if ENABLE_WORM

#define WORM_ARG_MIN_PROTECT        0x00000001
#define WORM_ARG_MAX_PROTECT        0x00000002
#define WORM_ARG_DEFAULT_PROTECT    0x00000004
#define WORM_ARG_AUTO_LOCK          0x00000008
#define WORM_ARG_PATH               0x0000000A

uint32_t global_modify_mask = 0;

enum {
        CMD_NULL,
        /* 初始化worm时钟 */
        CMD_INIT,
        CMD_CLOCK,
        /* 创建worm根目录 */
        CMD_CREATE,
        CMD_GET,
        CMD_MODIFY,
        CMD_REMOVE,
        /* 更新worm时钟 */
        CMD_UPDATE,
        CMD_LIST
};

struct worm_configure {
        int cmd_type;
        uint32_t min_protect_period;
        uint32_t default_protect_period;
        uint32_t max_protect_period;
        uint32_t auto_lock_period;
        char path[MAX_PATH_LEN];
        char username[MAX_NAME_LEN];
        char password[MAX_NAME_LEN];
};

static struct worm_configure worm_conf;

static const struct option long_options[] = {
        {"init",no_argument,NULL,'i'},
        {"clock",no_argument,NULL,'C'},
        {"create",no_argument,NULL,'c'},
        {"modify",no_argument,NULL,'o'},
        {"remove",no_argument,NULL,'r'},
        {"get",no_argument,NULL,'g'},
        {"update",no_argument,NULL,'u'},
        {"list",no_argument,NULL,'l'},
        {"min",required_argument,NULL,'m'},
        {"max",required_argument,NULL,'M'},
        {"default",required_argument,NULL,'d'},
        {"autolock",required_argument,NULL,'a'},
        {"path",required_argument,NULL,'p'},
        {"username",required_argument,NULL,'U'},
        {"password",required_argument,NULL,'P'},
        {"help",no_argument,NULL,'h'},
        {0,0,0,0}
};

static void usage() {
        fprintf(stderr,
                        "uss.worm {init, create, modify, get, update}                               \n"
                            "--init                                 initialize worm clock\n"
                            "--create                               create a worm root directory info\n"
                            "    --min <integer><time>              minimum protect period\n"
                            "    --max <integer><time>              maximum protect period\n"
                            "    --default <integer><time>          default protect period\n"
                            "    --autolock <integer><time>         auto lock period\n"
                            "    --path PATH                        specify a path for worm root\n"
                            "--modify                               modify a worm root directory info by super user\n"
                            "    --min <integer><time>              minimum protect period\n"
                            "    --max <integer><time>              maximum protect period\n"
                            "    --default <integer><time>          default protect period\n"
                            "    --autolock <integer><time>         auto lock period\n"
                            "    --username USERNAME                specify a username to modify worm attributes\n"
                            "    --password PASSWORD                specify a password to modify worm attributes\n"
                            "    --path PATH                        specify a path for worm root\n"
                            "--remove                               remove a worm root directory or worm state file\n"
                            "    --username USERNAME                specify a username\n"
                            "    --password PASSWORD                specify a password\n"
                            "    --path PATH                        worm root directory or worm state file\n"
                            "--get                                  retrive a file or directory worm info\n"
                            "    --path PATH                        specify a path for lookup\n"
                            "--update                               update worm clock for one hour\n"
                            "--list                                 list all worm root info\n"
                            "                                                               \n"
                            "Times are expressed in the following format:\n"
                            " y      Specifies years\n"
                            " m      Specifies months\n"
                            " d      Specifies days\n"
                            " h      Specifies hours\n"
                            " n      Specifies minutes\n"
               );
}

static int valid_worm_attr(uint32_t min, uint32_t max, uint32_t _default, uint32_t autolock)
{
        (void)min;
        (void)_default;
        (void)autolock;

        /* 70*YEAR = 2177280000 */
        if(max > 2177280000) {
                return 0;
        }

        return 1;
}

static int __get_wormfileid_by_fid(const uint64_t fid,
                const fileid_t *subfileid,
                fileid_t *fileid)
{
        if(fileid == NULL || subfileid == NULL) {
                return EINVAL;
        }

        if(fid == WORM_FID_NULL) {
                return ENOKEY;
        }

        fileid->id = fid;
        fileid->idx = 0;
        fileid->volid = subfileid->volid;

        return 0;
}

static int second_to_string(uint32_t sec, char *buf)
{
        uint32_t t;

        if(buf == NULL) {
                return  EINVAL;
        }

        if(sec % YEAR == 0) {
                t = sec / YEAR;
                snprintf(buf, MAX_U64_LEN, "%u year", t);
        } else if(sec % MONTH == 0) {
                t = sec / MONTH;
                snprintf(buf, MAX_U64_LEN, "%u month", t);
        } else if(sec % DAY == 0) {
                t = sec / DAY;
                snprintf(buf, MAX_U64_LEN, "%u day", t);
        } else if(sec % HOUR == 0) {
                t = sec / HOUR;
                snprintf(buf, MAX_U64_LEN, "%u hour", t);
        } else if(sec % MINUTE == 0) {
                t = sec / MINUTE;
                snprintf(buf, MAX_U64_LEN, "%u minute", t);
        } else {
                t = sec;
                snprintf(buf, MAX_U64_LEN, "%u second", t);
        }

        return 0;
}

static void print_worm_root(const worm_t *worm)
{
        char min[MAX_U64_LEN] = {0};
        char max[MAX_U64_LEN] = {0};
        char _default[MAX_U64_LEN] = {0};
        char autolock[MAX_U64_LEN] = {0};

        second_to_string(worm->min_protect_period, min);
        second_to_string(worm->max_protect_period, max);
        second_to_string(worm->default_protect_period, _default);
        second_to_string(worm->auto_lock_period, autolock);

        fprintf(stdout, "FILEID: "FID_FORMAT"\n"
                        "Root Path: %s\n"
                        "Default Protect Period: %s\n"
                        "Min Protect Period: %s\n"
                        "Max Protect Period: %s\n"
                        "Auto Lock Period: %s\n",
                        FID_ARG(&(worm->fileid)),
                        worm->path,
                        min, max, _default, autolock
               );
}

static void print_worm_file(const worm_t *worm,
                const char *path, const worm_file_t *worm_file, const md_proto_t *md)
{
        char buf_time[MAX_U64_LEN];
        time_t timestamp;
        struct tm *pt = NULL;

        fprintf(stdout, "worm info for : %s\n", path);
        fprintf(stdout, "fileid:"FID_FORMAT"\n", FID_ARG(&(worm->fileid)));
        fprintf(stdout, "worm root directory: %s\n", worm->path);
        fprintf(stdout, "worm state: %s\n",
                        strlen(worm_file->status) == 0 ? "not commited" : worm_file->status);

        timestamp = (time_t)md->at_mtime;
        pt = localtime(&timestamp);
        memset(buf_time, 0, MAX_U64_LEN);
        strftime(buf_time, MAX_U64_LEN, "%Y-%m-%d %H:%M:%S", pt);
        fprintf(stdout, "last modified: %s\n", buf_time);

        if(convert_string_to_worm_status(worm_file->status) == WORM_IN_PROTECT) {
                timestamp = (time_t)md->at_atime;
                pt = localtime(&timestamp);
                memset(buf_time, 0, MAX_U64_LEN);
                strftime(buf_time, MAX_U64_LEN, "%Y-%m-%d %H:%M:%S", pt);
        } else {
                if(worm_file->set_atime != 0) {
                        timestamp = (time_t)worm_file->set_atime;
                        pt = localtime(&timestamp);
                        memset(buf_time, 0, MAX_U64_LEN);
                        strftime(buf_time, MAX_U64_LEN, "%Y-%m-%d %H:%M:%S", pt);
                }
        }

        fprintf(stdout, "worm expire date: %s\n", strlen(buf_time) == 0 ? "not set" : buf_time);

        return;
}

static void print_worm_list(worm_t *worm, int count)
{
        int i;
        worm_t *ptr = worm;
        char min[MAX_U64_LEN] = {0};
        char max[MAX_U64_LEN] = {0};
        char _default[MAX_U64_LEN] = {0};
        char autolock[MAX_U64_LEN] = {0};

        fprintf(stdout, "%-32s %-32s %-32s %-32s %-32s\n",
                        "Root Path",
                        "Default Protect Period",
                        "Min Protect Period",
                        "Max Protect Period",
                        "Auto Lock Period"
                        );

        for(i=0; i<count; ++i, ptr++) {
                memset(min, 0, MAX_U64_LEN);
                memset(max, 0, MAX_U64_LEN);
                memset(_default, 0, MAX_U64_LEN);
                memset(autolock, 0, MAX_U64_LEN);
                second_to_string(ptr->min_protect_period, min);
                second_to_string(ptr->max_protect_period, max);
                second_to_string(ptr->default_protect_period, _default);
                second_to_string(ptr->auto_lock_period, autolock);

                fprintf(stdout, "%-32s %-32s %-32s %-32s %-32s\n",
                                ptr->path,
                                _default, min, max, autolock
                       );
        }
}

int worm_init()
{
        int ret = 0;
        fileid_t fileid;
        time_t timestamp = 0;

        ret = worm_init_wormclock_dir(&fileid);
        if(ret)
                GOTO(err_ret, ret);

        ret = worm_get_clock_time(&fileid, &timestamp);
        if(ret && ret != ENOKEY)
                GOTO(err_ret, ret);

        if(ret == ENOKEY) {
                timestamp = time(NULL);
                ret = worm_set_clock_time(&fileid, timestamp);
                if(ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int worm_clock()
{
        int ret = 0;
        fileid_t fileid;
        time_t timestamp = 0;
        struct tm *pt = NULL;
        char buf_time[MAX_U64_LEN] = {0};

        ret = worm_init_wormclock_dir(&fileid);
        if(ret)
                GOTO(err_ret, ret);

        ret = worm_get_clock_time(&fileid, &timestamp);
        if(ret && ret != ENOKEY)
                GOTO(err_ret, ret);

        pt = localtime(&timestamp);
        strftime(buf_time, MAX_U64_LEN, "%Y-%m-%d %H:%M:%S", pt);

        fprintf(stdout, "worm clock: %s\n", buf_time);

        return 0;
err_ret:
        return ret;
}

int worm_create()
{
        int ret, is_empty = 0;
        fileid_t fileid;
        worm_t worm;
        md_proto_t *md;
        char buf[MAX_BUF_LEN];

        memset(&worm, 0, sizeof(worm_t));
        memset(&md, 0, sizeof(md_proto_t));

        ret = sdfs_lookup_recurive(worm_conf.path, &fileid);
        if(ret)
                GOTO(err_ret, ret);

        ret = raw_is_directory_empty(&fileid, &is_empty);
        if(ret)
                GOTO(err_ret, ret);

        if(!is_empty) {
                fprintf(stderr, "worm is only allowed to set on empty directory\n");
                exit(EPERM);
        }

        md = (void *)buf;
        ret = md_getattr(&fileid, md);
        if(ret)
                GOTO(err_ret, ret);

        if(md->wormid != WORM_FID_NULL) {
                fprintf(stderr, "worm root directory not allowed to be nested\n");
                exit(EPERM);
        }

        worm.worm_type = WORM_ROOT;
        worm.fileid = fileid;
        worm.min_protect_period = worm_conf.min_protect_period;
        worm.max_protect_period = worm_conf.max_protect_period;
        worm.default_protect_period = worm_conf.default_protect_period;
        worm.auto_lock_period = worm_conf.auto_lock_period;
        strncpy(worm.path, worm_conf.path, strlen(worm_conf.path));

        ret = raw_create_worm(&fileid, &worm);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __get_changed_worm(worm_t *worm)
{
        if(worm == NULL) {
                return EINVAL;
        }

        if(global_modify_mask & WORM_ARG_MIN_PROTECT) {
                worm->min_protect_period = worm_conf.min_protect_period;
        }

        if(global_modify_mask & WORM_ARG_MAX_PROTECT) {
                worm->max_protect_period = worm_conf.max_protect_period;
        }

        if(global_modify_mask & WORM_ARG_DEFAULT_PROTECT) {
                worm->default_protect_period = worm_conf.default_protect_period;
        }

        if(global_modify_mask & WORM_ARG_AUTO_LOCK) {
                worm->auto_lock_period = worm_conf.auto_lock_period;
        }

        return 0;
}

int worm_modify()
{
        int ret;
        fileid_t fileid;
        worm_t worm;

        memset(&worm, 0, sizeof(worm_t));

        ret = sdfs_lookup_recurive(worm_conf.path, &fileid);
        if(ret)
                GOTO(err_ret, ret);

        ret = worm_get_attr(&fileid, &worm);
        if(ret)
                GOTO(err_ret, ret);

        ret = __get_changed_worm(&worm);
        if(ret)
                GOTO(err_ret, ret);

        ret = raw_modify_worm(&fileid, &worm, worm_conf.username, worm_conf.password);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int worm_remove()
{
        int ret;
        fileid_t parent, fileid;
        char name[MAX_NAME_LEN] = {0};
        struct stat st;

        ret = sdfs_lookup_recurive(worm_conf.path, &fileid);
        if(ret)
                GOTO(err_ret, ret);

        ret = sdfs_getattr(&fileid, &st);
        if(ret)
                GOTO(err_ret, ret);

        ret = sdfs_splitpath(worm_conf.path, &parent, name);
        if(ret)
                GOTO(err_ret, ret);

        if(S_ISDIR(st.st_mode)) {
                ret = raw_rmdir_with_worm(&parent, name, worm_conf.username, worm_conf.password);
                if(ret)
                        GOTO(err_ret, ret);
        } else if(S_ISREG(st.st_mode)) {
                ret = raw_unlink_with_worm(&parent, name, worm_conf.username, worm_conf.password);
                if(ret)
                        GOTO(err_ret, ret);
        } else {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int worm_get()
{
        int ret;
        fileid_t fileid;
        fileid_t worm_fileid;
        md_proto_t *md;
        struct stat st;
        worm_t worm;
        worm_file_t worm_file;
        size_t size = sizeof(worm_file_t);
        char buf[MAX_BUF_LEN];

        memset(&worm, 0, sizeof(worm_t));
        memset(&worm_file, 0, size);
        memset(&md, 0, sizeof(md_proto_t));

        ret = sdfs_lookup_recurive(worm_conf.path, &fileid);
        if(ret)
                GOTO(err_ret, ret);

        md = (void *)buf;
        ret = md_getattr(&fileid, md);
        if(ret)
                GOTO(err_ret, ret);

        MD2STAT(md, &st);

        if(S_ISDIR(st.st_mode)) {
                ret = worm_get_attr(&fileid, &worm);
                if(ret && ret != ENOKEY)
                        GOTO(err_ret, ret);

                if(ret == ENOKEY) {
                        DWARN("this is not a worm root directory\n");
                        goto err_ret;
                }

                print_worm_root(&worm);
        } else if(S_ISREG(st.st_mode)) {
                if(md->wormid == WORM_FID_NULL) {
                        ret = ENOENT;
                        DWARN("%s not belong to any worm root directory\n", worm_conf.path);
                        goto err_ret;
                }

                __get_wormfileid_by_fid(md->wormid, &fileid, &worm_fileid);

                ret = worm_get_attr(&worm_fileid, &worm);
                if(ret)
                        GOTO(err_ret, ret);

                ret = worm_get_file_attr(&fileid, &worm_file);
                if(ret && ret != ENOKEY)
                        GOTO(err_ret, ret);

                print_worm_file(&worm, worm_conf.path, &worm_file, &md);
        } else {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int worm_update()
{
        int ret;

        ret = worm_update_clock_time();
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int worm_list()
{
        int ret;
        char list[BIG_BUF_LEN] = {0};
        size_t size = BIG_BUF_LEN;
        int count = 0;

        ret = raw_list_worm((worm_t*)list, size, &count);
        if(ret)
                GOTO(err_ret, ret);

        print_worm_list((worm_t*)list, count);

        return 0;
err_ret:
        return ret;
}

int valid_unit(const char u)
{
        switch(u) {
                case 'y':
                case 'm':
                case 'd':
                case 'h':
                case 'n':
                case 's':
                        return 1;
                default:
                        return 0;
        }
}

int convert_to_second(uint32_t num, const char u)
{
        switch(u) {
                case 'y':
                        return num * YEAR;
                case 'm':
                        return num * MONTH;
                case 'd':
                        return num * DAY;
                case 'h':
                        return num * HOUR;
                case 'n':
                        return num * MINUTE;
                case 's':
                        return num;
                default:
                        return num;
        }
}

int parse_period_arg(const char *str, uint32_t *_num)
{
        char buf[MAX_U64_LEN] = {0};
        size_t len = strlen(str);
        size_t last_index = len - 1;
        char unit;
        uint32_t num;

        if(len > MAX_U64_LEN) {
                return EINVAL;
        }

        memcpy(buf, str, len);
        unit = buf[last_index];
        buf[last_index] = '\0';

        if(valid_unit(unit) == 0) {
                return EINVAL;
        }

        errno = 0;
        num = strtoul(buf, NULL, 10);
        if(num == 0 && errno == EINVAL) {
                return EINVAL;
        }

        *_num = convert_to_second(num, unit);

        return 0;
}

static void main_init(int argc, char *argv[])
{
        int opt, options_index, ret;
        uint32_t arg_mask = 0;
        uint32_t default_protect_period = 0;
        uint32_t min_protect_period = 0;
        uint32_t max_protect_period = 0;
        uint32_t auto_lock_period = 0;

        while((opt = getopt_long(argc,argv,"h?",long_options,&options_index)) != -1) {

                switch(opt) {
                        case 'i':
                                worm_conf.cmd_type = CMD_INIT;
                                break;
                        case 'c':
                                worm_conf.cmd_type = CMD_CREATE;
                                break;
                        case 'o':
                                worm_conf.cmd_type = CMD_MODIFY;
                                break;
                        case 'r':
                                worm_conf.cmd_type = CMD_REMOVE;
                                break;
                        case 'C':
                                worm_conf.cmd_type = CMD_CLOCK;
                                break;
                        case 'u':
                                worm_conf.cmd_type = CMD_UPDATE;
                                break;
                        case 'g':
                                worm_conf.cmd_type = CMD_GET;
                                break;
                        case 'l':
                                worm_conf.cmd_type = CMD_LIST;
                                break;
                        case 'm':
                                arg_mask |= WORM_ARG_MIN_PROTECT;
                                global_modify_mask |= WORM_ARG_MIN_PROTECT;
                                ret = parse_period_arg(optarg, &min_protect_period);
                                if(ret) {
                                        fprintf(stderr, "parse min_protect_period failed\n");
                                        exit(ret);
                                }

                                break;
                        case 'M':
                                arg_mask |= WORM_ARG_MAX_PROTECT;
                                global_modify_mask |= WORM_ARG_MAX_PROTECT;
                                ret = parse_period_arg(optarg, &max_protect_period);
                                if(ret) {
                                        fprintf(stderr, "parse max_protect_period failed\n");
                                        exit(ret);
                                }
                                break;
                        case 'd':
                                arg_mask |= WORM_ARG_DEFAULT_PROTECT;
                                global_modify_mask |= WORM_ARG_DEFAULT_PROTECT;
                                ret = parse_period_arg(optarg, &default_protect_period);
                                if(ret) {
                                        fprintf(stderr, "parse default_protect_period failed\n");
                                        exit(ret);
                                }
                                break;
                        case 'a':
                                arg_mask |= WORM_ARG_AUTO_LOCK;
                                global_modify_mask |= WORM_ARG_AUTO_LOCK;
                                ret = parse_period_arg(optarg, &auto_lock_period);
                                if(ret) {
                                        fprintf(stderr, "parse default_protect_period failed\n");
                                        exit(ret);
                                }
                                break;
                        case 'p':
                                arg_mask |= WORM_ARG_PATH;
                                path_normalize(optarg, worm_conf.path, MAX_PATH_LEN);
                                break;
                        case 'U':
                                strncpy(worm_conf.username, optarg, strlen(optarg));
                                break;
                        case 'P':
                                strncpy(worm_conf.password, optarg, strlen(optarg));
                                break;
                        case 'h':
                        case '?':
                                usage();exit(-1);
                                break;
                        default:
                                printf("wrong argument\n");
                                break;
                }
        }

        if (optind < argc) {
                printf("non-option ARGV-elements: ");
                while (optind < argc)
                        printf("%s ", argv[optind++]);
                printf("\n");
        }

        if(worm_conf.cmd_type == CMD_CREATE) {
                if(arg_mask & (WORM_ARG_MIN_PROTECT | WORM_ARG_MAX_PROTECT |
                                        WORM_ARG_DEFAULT_PROTECT | WORM_ARG_AUTO_LOCK | WORM_ARG_PATH)) {
                        worm_conf.min_protect_period = min_protect_period;
                        worm_conf.max_protect_period = max_protect_period;
                        worm_conf.default_protect_period = default_protect_period;
                        worm_conf.auto_lock_period = auto_lock_period;

                        if(valid_worm_attr(min_protect_period,
                                                max_protect_period,
                                                default_protect_period,
                                                auto_lock_period) == 0) {
                                fprintf(stdout, "not allowed period value\n");
                                exit(EINVAL);
                        }
                } else {
                        fprintf(stderr, "please specify argument min_protect_period, \
                                        max_protect_period, default_protect_period auto_lock_period and directory\n");
                        exit(EINVAL);
                }
        } else if(worm_conf.cmd_type == CMD_GET) {
                if((arg_mask & WORM_ARG_PATH)) {
                } else {
                        fprintf(stderr, "please specify argument path\n");
                        exit(EINVAL);
                }
        } else if(worm_conf.cmd_type == CMD_MODIFY) {
                if((arg_mask & WORM_ARG_PATH)) {
                        if(arg_mask & WORM_ARG_MIN_PROTECT) {
                                worm_conf.min_protect_period = min_protect_period;
                        }

                        if(arg_mask & WORM_ARG_MAX_PROTECT) {
                                worm_conf.max_protect_period = max_protect_period;
                        }

                        if(arg_mask & WORM_ARG_DEFAULT_PROTECT) {
                                worm_conf.default_protect_period = default_protect_period;
                        }

                        if(arg_mask & WORM_ARG_AUTO_LOCK) {
                                worm_conf.auto_lock_period = auto_lock_period;
                        }

                        if(valid_worm_attr(min_protect_period,
                                                max_protect_period,
                                                default_protect_period,
                                                auto_lock_period) == 0) {
                                fprintf(stdout, "not allowed period value\n");
                                exit(EINVAL);
                        }
                } else {
                        fprintf(stderr, "please specify argument path\n");
                        exit(EINVAL);
                }
        } else if(worm_conf.cmd_type == CMD_REMOVE) {
                if((arg_mask & WORM_ARG_PATH)) {
                } else {
                        fprintf(stderr, "please specify argument path\n");
                        exit(EINVAL);
                }
        }
}

int main(int argc, char *argv[])
{
        int ret;

        dbg_info(0);

        if(argc == 1) {
                usage();
                exit(EINVAL);
        }

        main_init(argc, argv);

        ret = ly_init_simple("uss_worm");
        if(ret)
                GOTO(err_ret, ret);

        switch(worm_conf.cmd_type) {
                case CMD_INIT:
                        ret = worm_init();
                        break;
                case CMD_CREATE:
                        ret = worm_create();
                        break;
                case CMD_MODIFY:
                        ret = worm_modify();
                        break;
                case CMD_REMOVE:
                        ret = worm_remove();
                        break;
                case CMD_CLOCK:
                        ret = worm_clock();
                        break;
                case CMD_GET:
                        ret = worm_get();
                        break;
                case CMD_UPDATE:
                        ret = worm_update();
                        break;
                case CMD_LIST:
                        ret = worm_list();
                        break;
                default:
                        ret = EINVAL;
                        fprintf(stdout, "invalid worm command\n");
                        break;
        }

        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

#else

int main(int argc, char *argv[])
{
        (void) argc;
        (void) argv;
        
        DERROR("worm disabled");
        return 0;
}

#endif

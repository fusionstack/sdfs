#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <stdint.h>
#include <errno.h>

#include "sdfs_lib.h"
#include "sdfs_id.h"
#include "sdfs_quota.h"
#include "xattr.h"
#include "md_lib.h"

enum {
        CMD_INVALID,
        CMD_CREATE,
        CMD_GET,
        CMD_MODIFY,
        CMD_REMOVE,
        CMD_LIST,
};

enum {
        UNIT_INVALID,
        UNIT_BYTES,
        UNIT_KILO,
        UNIT_MEGA,
        UNIT_GIGA,
        UNIT_TERA
};

typedef struct {
        int cmd_type;
        int quota_type;
        uint32_t modify_mask;
        uint64_t space_hard;
        uint64_t inode_hard;
        uid_t uid;
        gid_t gid;
        char directory[MAX_PATH_LEN];
}quota_configure_t ;

static const struct option long_options[] = {
        {"create",no_argument,NULL,'C'},
        {"get",no_argument,NULL,'R'},
        {"modify",no_argument,NULL,'M'},
        {"list",no_argument,NULL,'l'},
        {"remove",no_argument,NULL,'r'},
        {"space-hardlimit",required_argument,NULL,'s'},
        {"inode-hardlimit",required_argument,NULL,'i'},
        {"bytes",no_argument,NULL,'b'},
        {"kilo",no_argument,NULL,'k'},
        {"mega",no_argument,NULL,'m'},
        {"giga",no_argument,NULL,'G'},
        {"tera",no_argument,NULL,'T'},
        {"uid",required_argument,NULL,'u'},
        {"gid",required_argument,NULL,'g'},
        {"directory",required_argument,NULL,'d'},
        {"help",no_argument,NULL,'h'},
        {0,0,0,0}
};

static int __get_quotaid(const fileid_t *fileid, fileid_t *quotaid)
{
        int ret;
        md_proto_t *md;
        char buf[MAX_BUF_LEN];

        md = (void *)buf;

        ret = md_getattr(NULL, fileid, md);
        if (ret) {
                GOTO(err_ret, ret);
        }

        *quotaid = md->quotaid;

        return 0;
err_ret:
        return ret;
}

bool is_lvm(const char *path)
{
        int i = 0;
        int is_slash = 0;
        int dir_level = 0;

        YASSERT(NULL != path);
        if ('/' != path[0])
                return false;

        is_slash = 1;
        for (i = 1; '\0' != path[i]; i++) {
                if (is_slash) {
                        if ('/' != path[i]) {
                                is_slash = 0;
                                dir_level++;
                                if (2 == dir_level)
                                        break;
                        }
                }
                else {
                        if ('/' == path[i])
                                is_slash = 1;
                }
        }

        if (2 == dir_level)
                return false;
        else
                return true;
}

static quota_configure_t quota_conf;

static void usage()
{
        fprintf(stderr, "usage : uss.quota {--create|--remove|--get|--modify}                   \n"
                        "--create                                                               \n"
                        "   --space-hardlimit SPACE --bytes|--kilo|--mega|--giga|--tera         \n"
                        "   --inode-hardlimit INODE                                             \n"
                        "   --uid userid                                                        \n"
                        "   --gid groupid                                                       \n"
                        "   --directory PATH                                                    \n"
                        "                                                                       \n"
                        "--remove                                                               \n"
                        "   --uid userid                                                        \n"
                        "   --gid groupid                                                       \n"
                        "   --directory PATH                                                    \n"
                        "                                                                       \n"
                        "--get                                                                  \n"
                        "   --uid userid                                                        \n"
                        "   --gid groupid                                                       \n"
                        "   --directory PATH                                                    \n"
                        "                                                                       \n"
                        "--modify                                                               \n"
                        "   --space-hardlimit SPACE --bytes|--kilo|--mega|--giga|--tera         \n"
                        "   --inode-hardlimit INODE                                             \n"
                        "   --uid userid                                                        \n"
                        "   --gid groupid                                                       \n"
                        "   --directory PATH                                                    \n"
                        "                                                                       \n"
                        "创建配额                                                               \n"
                        "uss.quota --create --space-hardlimit 10 --giga --inode-hardlimit 100 --directory /home/a\n"
                        "uss.quota --create --gid 5000 --space-hardlimit 10 --giga --inode-hardlimit 100 --directory /home\n"
                        "uss.quota --create --uid 5000 --space-hardlimit 10 --giga --inode-hardlimit 100 --directory /home\n"
                        "                                                                       \n"
                        "删除配额                                                               \n"
                        "uss.quota --remove --directory /home/a                                 \n"
                        "uss.quota --remove --uid 5000 --directory /home                        \n"
                        "uss.quota --remove --gid 5000 --directory /home                        \n"
                        "                                                                       \n"
                        "查找配额                                                               \n"
                        "uss.quota --get --directory /home/a                                    \n"
                        "uss.quota --get --uid 5000 --directory /home                           \n"
                        "uss.quota --get --gid 5000 --directory /home                           \n"
                        "                                                                       \n"
                        "修改配额                                                               \n"
                        "uss.quota --modify --space-hardlimit 10 --giga --inode-hardlimit 100 --directory /home/a\n"
                        "uss.quota --modify --gid 5000 --space-hardlimit 10 --giga --inode-hardlimit 100 --directory /home\n"
                        "uss.quota --modify --uid 5000 --space-hardlimit 10 --giga --inode-hardlimit 100 --directory /home\n"
                        );
}

uint64_t convert_to_bytes(uint64_t space_hard, int unit_type)
{
        switch(unit_type) {
                case UNIT_BYTES:
                        return space_hard;
                case UNIT_KILO:
                        return space_hard * KB;
                case UNIT_MEGA:
                        return space_hard * MB;
                case UNIT_GIGA:
                        return space_hard * GB;
                case UNIT_TERA:
                        return space_hard * TB;
                default:
                        return space_hard *GB;
        }
}

static int is_quota_valid()
{
        uint64_t space_hard = quota_conf.space_hard;
        uint64_t inode_hard = quota_conf.inode_hard;

        if(space_hard > MAX_SPACE_LIMIT ||
                        inode_hard > MAX_INODE_LIMIT) {
                return 0;
        }

        return 1;
}

static char *get_quota_type(int quota_type)
{
        switch(quota_type) {
                case QUOTA_DIR:
                        return "directory";
                case QUOTA_USER:
                        return "user";
                case QUOTA_GROUP:
                        return "group";
                default:
                        return "";
        }
}

static void print_quota_info(const char *path, const fileid_t *fileid, const quota_t *quota)
{
        fprintf(stdout, "path:%s fileid: %llu_v%llu\n",
                        path, (LLU)fileid->id, (LLU)fileid->volid);

        fprintf(stdout, "-----------------------------------\n");

        fprintf(stdout, "quota_type: %s\n"
                "quotaid: "CHKID_FORMAT"\n"
                "pquotaid: "CHKID_FORMAT"\n"
                "quota-fileid: %llu_v%llu\n"
                "uid: %llu\n"
                "gid: %llu\n"
                "inode_used: %llu\n"
                "inode_hard: %llu\n"
                "space_used: %llu\n"
                "space_hard: %llu\n",
                get_quota_type(quota->quota_type),
                CHKID_ARG(&quota->quotaid),
                CHKID_ARG(&quota->pquotaid),
                (LLU)quota->dirid.id,
                (LLU)quota->dirid.volid,
                (LLU)quota->uid,
                (LLU)quota->gid,
                (LLU)quota->inode_used,
                (LLU)quota->inode_hard,
                (LLU)quota->space_used,
                (LLU)quota->space_hard
                        );
}

int quota_create()
{
        int ret, is_empty = 0, level = 0, exist = 0;
        fileid_t fileid, parent;
        quota_t quota, test_quota;
        fileid_t quotaid, pquotaid;
        md_proto_t *md;
        char buf[MAX_BUF_LEN];

        md = (md_proto_t *)buf;

        memset(&quota, 0, sizeof(quota_t));
        memset(&test_quota, 0, sizeof(quota_t));

        ret = get_dir_level(quota_conf.directory, &level);
        if(ret)
                GOTO(err_ret, ret);

        if(level > QUOTA_MAX_LEVEL) {
                fprintf(stderr, "8 level is maximum\n");
                ret = EPERM;
                GOTO(err_ret, ret);
        }

        /* 获取目录的fileid */
        ret = sdfs_lookup_recurive(quota_conf.directory, &fileid);
        if(ret)
                GOTO(err_ret, ret);

        quota.dirid = fileid;
        DBUG("directory:%s, level:%d, chkid:"CHKID_FORMAT"\n", quota_conf.directory, level, CHKID_ARG(&fileid));

        /* 判断目录是否为空 */
        ret = raw_is_directory_empty(&fileid, &is_empty);
        if(ret)
                GOTO(err_ret, ret);

        if(is_empty == 0) {
                fprintf(stdout, "quota is only allowed to set on empty directory\n");
                exit(EPERM);
        }

        if(!is_quota_valid()) {
                fprintf(stderr, "The given capacity or inode is too large\n");
                exit(EPERM);
        }

        if(quota_conf.quota_type == QUOTA_GROUP ||
                        quota_conf.quota_type == QUOTA_USER) {
#if 1
                if(!is_lvm(quota_conf.directory)) {
                        fprintf(stderr, "QUOTA_GROUP, QUOTA_USER is only allowed to set on lvm directory\n");
                        exit(EPERM);
                }
#endif
        }

        if(quota_conf.quota_type == QUOTA_DIR) {
                ret = __get_quotaid(&fileid, &quotaid);
                if(ret)
                        GOTO(err_ret, ret);

                if(quotaid.id != QUOTA_NULL) {
                        test_quota.quota_type = QUOTA_DIR;
                        ret = raw_get_quota(&quotaid, &test_quota);
                        if(ret)
                                GOTO(err_ret, ret);

                        if(fileid_cmp(&fileid, &test_quota.dirid) == 0) {
                                exist = 1;
                        } else {
                                exist = 0;
                        }
                } else {
                        exist = 0;
                }

                if (!exist) {
			ret = md_getattr(NULL, &fileid, md);
			if (ret) {
				GOTO(err_ret, ret);
			}

                        parent = md->parent;

			ret = __get_quotaid(&parent, &pquotaid);
			if(ret)
				GOTO(err_ret, ret);

			quota.pquotaid = pquotaid;
                }
        } else if(quota_conf.quota_type == QUOTA_GROUP ||
                        quota_conf.quota_type == QUOTA_USER) {
                exist = 0;
                quota.uid = quota_conf.uid;
                quota.gid = quota_conf.gid;
        }

        quota.space_hard = quota_conf.space_hard;
        quota.inode_hard = quota_conf.inode_hard;
        quota.quota_type = quota_conf.quota_type;

        if(exist == 0) {
                ret = raw_create_quota(&quota);
                if(ret)
                        GOTO(err_ret, ret);
        } else {
                fprintf(stderr, "The given directory is already created quota\n");
        }

        return 0;
err_ret:
        return ret;
}

/* QUOTA_REMOVE */
int quota_remove()
{
        //directory,uid,gid
        int ret;
        fileid_t fileid;
        fileid_t quotaid;
        quota_t quota;

        if(quota_conf.quota_type == QUOTA_GROUP ||
                        quota_conf.quota_type == QUOTA_USER) {
#if 1
                if(!is_lvm(quota_conf.directory)) {
                        fprintf(stderr, "QUOTA_GROUP, QUOTA_USER is only available on lvm directory\n");
                        exit(EPERM);
                }
#endif
        }

        ret = sdfs_lookup_recurive(quota_conf.directory, &fileid);
        if(ret)
                GOTO(err_ret, ret);

        quota.dirid = fileid;

        /* 如果是目录配额其实不需要提前获取quotaid */
        /* 获取配额ID */
        ret = __get_quotaid(&fileid, &quotaid);
        if(ret)
                GOTO(err_ret, ret);

        quota.uid = quota_conf.uid;
        quota.gid = quota_conf.gid;
        quota.quota_type = quota_conf.quota_type;

        ret = raw_remove_quota(&quotaid, &quota);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

/* QUOTA_GET */
int quota_get()
{
        int ret;
        fileid_t fileid;
        fileid_t quotaid;
        quota_t quota;

        memset(&quota, 0, sizeof(quota_t));
        if(quota_conf.quota_type == QUOTA_GROUP ||
                        quota_conf.quota_type == QUOTA_USER) {
#if 1
                if(!is_lvm(quota_conf.directory)) {
                        fprintf(stderr, "QUOTA_GROUP, QUOTA_USER is only available on lvm directory\n");
                        exit(EPERM);
                }
#endif
        }

        ret = sdfs_lookup_recurive(quota_conf.directory, &fileid);
        if(ret)
                GOTO(err_ret, ret);

        quota.dirid = fileid;

        /* 获取配额ID */
        ret = __get_quotaid(&fileid, &quotaid);
        if(ret)
                GOTO(err_ret, ret);

        quota.uid = quota_conf.uid;
        quota.gid = quota_conf.gid;
        quota.quota_type = quota_conf.quota_type;


        if (chkid_null(&quotaid)) {
                if (quota_conf.quota_type == QUOTA_DIR) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                } else {
                        ret = raw_get_quota(NULL, &quota);
                        if(ret)
                                GOTO(err_ret, ret);
                }
        } else {
                ret = raw_get_quota(&quotaid, &quota);
                if(ret)
                        GOTO(err_ret, ret);
        }

        print_quota_info(quota_conf.directory, &fileid, &quota);

        return 0;
err_ret:
        return ret;
}

/* QUOTA_MODIFY */
int quota_modify()
{
        int ret;
        uint32_t modify_mask = 0;
        fileid_t quotaid;
        fileid_t fileid;
        quota_t quota;

        if(!is_quota_valid()) {
                fprintf(stderr, "The given capacity or inode is too large\n");
                exit(EPERM);
        }

        if(quota_conf.quota_type == QUOTA_GROUP ||
                        quota_conf.quota_type == QUOTA_USER) {
#if 1
                if(!is_lvm(quota_conf.directory)) {
                        fprintf(stderr, "QUOTA_GROUP, QUOTA_USER is only available on lvm directory\n");
                        exit(EPERM);
                }
#endif
        }

        ret = sdfs_lookup_recurive(quota_conf.directory, &fileid);
        if(ret)
                GOTO(err_ret, ret);

        quota.dirid = fileid;

        /* 获取配额ID */
        ret = __get_quotaid(&fileid, &quotaid);
        if(ret)
                GOTO(err_ret, ret);

        quota.space_hard = quota_conf.space_hard;
        quota.inode_hard = quota_conf.inode_hard;
        quota.uid = quota_conf.uid;
        quota.gid = quota_conf.gid;
        quota.quota_type = quota_conf.quota_type;

        modify_mask = quota_conf.modify_mask;

        ret = raw_modify_quota(&quotaid, &quota, modify_mask);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static void main_init(int argc, char *argv[])
{
        int opt, set_directory = 0;
        uint32_t modify_mask = 0;
        int cmd_type = CMD_INVALID, unit_type = UNIT_INVALID, quota_type = QUOTA_DIR;
        int options_index;
        uint64_t space_hard = 0, inode_hard = 0;
        uid_t uid = 0;
        gid_t gid = 0;

        memset(&quota_conf, 0, sizeof(quota_configure_t));

        while((opt = getopt_long(argc,argv,"h?",long_options,&options_index)) != -1) {

                switch(opt) {
                        case 'C':
                                cmd_type = CMD_CREATE;
                                break;
                        case 'R':
                                cmd_type = CMD_GET;
                                break;
                        case 'M':
                                cmd_type = CMD_MODIFY;
                                break;
                        case 'l':
                                cmd_type = CMD_LIST;
                                break;
                        case 'r':
                                cmd_type = CMD_REMOVE;
                                break;
                        case 's':
                                modify_mask |= SPACE_HARD_BIT;
                                space_hard = strtoull(optarg, NULL, 10);
                                break;
                        case 'i':
                                modify_mask |= INODE_HARD_BIT;
                                inode_hard = strtoull(optarg, NULL, 10);
                                break;
                        case 'b':
                                unit_type = UNIT_BYTES;
                                break;
                        case 'k':
                                unit_type = UNIT_KILO;
                                break;
                        case 'm':
                                unit_type = UNIT_MEGA;
                                break;
                        case 'G':
                                unit_type = UNIT_GIGA;
                                break;
                        case 'T':
                                unit_type = UNIT_TERA;
                                break;
                        case 'u':
                                quota_type = QUOTA_USER;
                                uid = strtoull(optarg, NULL, 10);
                                break;
                        case 'g':
                                quota_type = QUOTA_GROUP;
                                gid = strtoull(optarg, NULL, 10);
                                break;
                        case 'd':
                                set_directory = 1;
                                path_normalize(optarg, quota_conf.directory, MAX_PATH_LEN);
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

        /* 指定参数--space-hardlimit或--inode-hardlimit */
        /* --dir */
        if(cmd_type == CMD_CREATE) {
                if(space_hard == 0 && inode_hard == 0) {
                        /* 必须指定一个参数表 */
                        fprintf(stderr, "space-hardlimit, inode-hardlimit or both\n");
                        exit(EINVAL);
                }

                if(!set_directory) {
                        fprintf(stderr, "please specify directory\n");
                        exit(EINVAL);
                }

                quota_conf.space_hard = convert_to_bytes(space_hard, unit_type);
                quota_conf.inode_hard = inode_hard;
                quota_conf.gid = gid;
                quota_conf.uid = uid;
                quota_conf.quota_type = quota_type;
                quota_conf.cmd_type = CMD_CREATE;
        } else if(cmd_type == CMD_REMOVE) {
                if(!set_directory) {
                        fprintf(stderr, "please specify directory\n");
                        exit(EINVAL);
                }

                quota_conf.gid = gid;
                quota_conf.uid = uid;
                quota_conf.quota_type = quota_type;
                quota_conf.cmd_type = CMD_REMOVE;
        } else if(cmd_type == CMD_GET) {
                if(!set_directory) {
                        fprintf(stderr, "please specify directory\n");
                        exit(EINVAL);
                }

                quota_conf.gid = gid;
                quota_conf.uid = uid;
                quota_conf.quota_type = quota_type;
                quota_conf.cmd_type = CMD_GET;
        } else if(cmd_type == CMD_MODIFY) {
                if(space_hard == 0 && inode_hard == 0) {
                        /* 必须指定一个参数表 */
                        fprintf(stderr, "space-hardlimit, inode-hardlimit or both\n");
                        exit(EINVAL);
                }

                if(!set_directory) {
                        fprintf(stderr, "please specify directory\n");
                        exit(EINVAL);
                }

                quota_conf.modify_mask = modify_mask;
                quota_conf.space_hard = convert_to_bytes(space_hard, unit_type);
                quota_conf.inode_hard = inode_hard;
                quota_conf.gid = gid;
                quota_conf.uid = uid;
                quota_conf.quota_type = quota_type;
                quota_conf.cmd_type = CMD_MODIFY;
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

        ret = ly_init_simple("uss_quota");
        if(ret)
                GOTO(err_ret, ret);

        switch(quota_conf.cmd_type) {
                case CMD_CREATE:
                        ret = quota_create();
                        break;
                case CMD_REMOVE:
                        ret = quota_remove();
                        break;
                case CMD_GET:
                        ret = quota_get();
                        break;
                case CMD_MODIFY:
                        ret = quota_modify();
                        break;
                default:
                        ret = EINVAL;
                        fprintf(stdout, "invalid quota command\n");
                        break;
        }

        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

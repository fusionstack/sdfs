/*
*Date   : 2017.07.29
*Author : Yang
*uss.share : the command for the directory share
*/
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <dirent.h>
#include <ctype.h>
#include <sys/types.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "sdfs_id.h"
#include "sdfs_quota.h"
#include "md_lib.h"

#define SHARE_USER_BIT    0x00000001
#define SHARE_GROUP_BIT   0x00000002
#define SHARE_HOST_BIT    0x00000004

typedef enum {
        SHARE_SET,
        SHARE_GET,
        SHARE_REMOVE,
        SHARE_LIST,
        SHARE_INVALID_OPER,
}share_oper_type_t;


static char *share_protstr[] = {
        "cifs",
        "ftp",
        "nfs",
        ""
};

static char *share_userstr[] = {
        "user",
        "group",
        "host",
        ""
};

static char *share_modstr[] = {
        "read-only",
        "read-write",
        ""
};

void share_get_key(IN const fileid_t *dirid, IN const char *name,
                   IN const char *share_name, IN share_user_type_t user_type,
                   OUT share_key_t *share_key)
{
        YASSERT(SHARE_INVALID_USER != user_type);

        memset(share_key, 0x0, sizeof(*share_key));
        share_key->dirid = *dirid;

        strncpy(share_key->name, name ,sizeof(share_key->name));
        share_key->usertype = user_type;

        if (share_name) {
                strncpy(share_key->share_name, share_name,
                        sizeof(share_key->share_name));
        }
}


static void _share_usage(void)
{
        fprintf(stderr, "Usage: uss.share [OPTION] PATH\n"
                "\t-g, --group GROUP     the group name of the protocol(cifs/nfs) for the share directory\n"
                "\t-n, --share_name      the share name of the cifs for the share directory\n"
                "\t-m, --mode MOD        the mode of the directory shared, support ro(read-only),rw(read-write)\n"
                "\t-p, --protocol PROTO  the protocol of the share directory, only support cifs/ftp/nfs\n"
                "\t-r, --remove          remove a share record of the share directory\n"
                "\t-s, --set             set the info of the share directory\n"
                "\t-u, --user USER       the user name of the protocol(cifs/ftp) for the share directory\n"
                "\t-G, --get             get the info of the share directory\n"
                "\t-l, --list            list all the info of the share directory\n"
                "\t-H, --host HOST       the host name or IP of the protocol nfs for the directory shared\n"
                "\t-h, --help            help information\n");
}

static share_mode_t _share_get_mode(IN const char *mod_str)
{
        share_mode_t share_mod = SHARE_INVALID_MOD;

        if (0 == strcmp("ro", mod_str))
                share_mod = SHARE_RO;
        else if (0 == strcmp("rw", mod_str))
                share_mod = SHARE_RW;

        return share_mod;
}

static share_protocol_t _share_get_prot(IN const char *prot_str)
{
        share_protocol_t share_prot = SHARE_INVALID_PROT;

        if (0 == strcmp("cifs", prot_str))
                share_prot = SHARE_CIFS;
        else if (0 == strcmp("ftp", prot_str))
                share_prot = SHARE_FTP;
        else if (0 == strcmp("nfs", prot_str))
                share_prot = SHARE_NFS;

        return share_prot;
}

static int _share_args_check(IN share_oper_type_t oper,
                             IN share_protocol_t prot,
                             IN share_mode_t mod,
                             IN const uint32_t share_user_mask,
                             IN const bool share_name_flag)
{
        int i;
        uint32_t share_user_mask_tmp = share_user_mask;
        int bit_set_num = 0;

        if (SHARE_INVALID_OPER == oper) {
                fprintf(stderr, "need enter -r, -s or -G -l argument for determine the type of operation\n");
                return ERROR_FAILED;
        }

        if (SHARE_INVALID_PROT == prot) {
                fprintf(stderr, "need enter -p arugment for determine the share protocol\n");
                return ERROR_FAILED;
        }

        if (SHARE_LIST == oper)
                return ERROR_SUCCESS;

        if ((SHARE_SET == oper) && (SHARE_INVALID_MOD == mod)) {
                fprintf(stderr, "set operation, need enter -m argument for the share mod \n");
                return ERROR_FAILED;
        }

        if (0 == share_user_mask)  {
                fprintf(stderr, "need enter -g, -u or -H argument for the share user\n");
                return ERROR_FAILED;
        }

        for (i = 0; i < SHARE_INVALID_USER; i++) {
                if (share_user_mask_tmp & 1)
                        bit_set_num++;
                share_user_mask_tmp = share_user_mask_tmp >> 1;
        }

        if (bit_set_num > 1) {
                fprintf(stderr, "user group and host cannot be used together.\n");
                return ERROR_FAILED;
        }

        if ((share_user_mask & SHARE_HOST_BIT) &&
            (SHARE_NFS != prot)) {
                fprintf(stderr, "only NFS support -H argument\n");
                return ERROR_FAILED;
        }

        if ((SHARE_CIFS == prot) && (false == share_name_flag)) {
                fprintf(stderr, "CIFS share name can not null error \n");
                return ERROR_FAILED;
        }

        if ((SHARE_NFS == prot) && (share_user_mask & SHARE_USER_BIT)) {
                fprintf(stderr, "NFS not support -u argument\n");
                return ERROR_FAILED;
        }

        if ((SHARE_FTP == prot) && (share_user_mask & ~SHARE_USER_BIT)) {
                fprintf(stderr, "FTP only support -u argument\n");
                return ERROR_FAILED;
        }

        return ERROR_SUCCESS;
}

static void _share_args2info(IN const fileid_t *dirid,
                             IN const char *name,
                             IN const char *share_name,
                             IN share_user_type_t usertype,
                             IN share_mode_t mod,
                             IN const char *path,
                             OUT shareinfo_t * shareinfo)
{
        memset(shareinfo, 0x0, sizeof(*shareinfo));

        shareinfo->dirid = *dirid;
        shareinfo->uid = INVALID_UID;
        shareinfo->gid = INVALID_GID;

        if (usertype == SHARE_USER) {
                strncpy(shareinfo->uname, name ,sizeof(shareinfo->uname));
        } else if (usertype == SHARE_GROUP) {
                strncpy(shareinfo->gname, name ,sizeof(shareinfo->gname));
        } else if (usertype == SHARE_HOST) {
                strncpy(shareinfo->hname, name ,sizeof(shareinfo->hname));
        } else {
                UNIMPLEMENTED(__DUMP__);
        }

        strncpy(shareinfo->share_name, share_name ,sizeof(shareinfo->share_name));
        shareinfo->usertype = usertype;
        shareinfo->mode = mod;
        strncpy(shareinfo->path, path, sizeof(shareinfo->path));
}

static void __print_shareinfo(IN const shareinfo_t *shareinfo, const char *protocol)
{
        printf("protocol   : %s\n"
               "directory  : %s\n"
               "dirid      : "ID_VID_FORMAT"\n"
               "user_name    : %s\n"
               "group_name    : %s\n"
               "host_name    : %s\n"
               "mode       : %s\n"
               "share_name : %s\n",
               protocol,
               shareinfo->path,
               ID_VID_ARG(&shareinfo->dirid),
               (char *)shareinfo->uname,
               (char *)shareinfo->gname,
               (char *)shareinfo->hname,
               share_modstr[shareinfo->mode],
               shareinfo->share_name);
}

static void _print_cifs_share_info(IN const share_cifs_t *share_cifs)
{
        __print_shareinfo(share_cifs, "cifs");
}

static void _print_ftp_share_info(IN const share_ftp_t *share_ftp)
{
        __print_shareinfo(share_ftp, "ftp");
}

static void _print_nfs_share_info(IN const share_nfs_t *share_nfs)
{
        __print_shareinfo(share_nfs, "nfs");
}

static void _print_share_info(IN share_protocol_t prot, IN const char *buf)
{
        share_cifs_t *share_cifs = NULL;
        share_ftp_t *share_ftp = NULL;
        share_nfs_t *share_nfs = NULL;

        if (SHARE_CIFS == prot) {
                share_cifs = (share_cifs_t *)buf;
                _print_cifs_share_info(share_cifs);
        } else if (SHARE_FTP == prot) {
                share_ftp = (share_ftp_t *)buf;
                _print_ftp_share_info(share_ftp);
        } else if (SHARE_NFS == prot) {
                share_nfs = (share_nfs_t *)buf;
                _print_nfs_share_info(share_nfs);
        }
}

static void _list_cifs_share_info(IN const share_cifs_t *shareinfo, IN int count)
{
        int i;
        
        for (i = 0; i < count; i++) {
                printf("-------------------------------------------------------------\n");
                _print_cifs_share_info(&shareinfo[i]);
        }
}

static void _list_ftp_share_info(IN const share_ftp_t *shareinfo, IN int count)
{
        int i;
        
        for (i = 0; i < count; i++) {
                printf("-------------------------------------------------------------\n");
                _print_ftp_share_info(&shareinfo[i]);
        }
}

static void _list_nfs_share_info(IN const share_nfs_t *shareinfo, IN int count)
{
        int i;
        
        for (i = 0; i < count; i++) {
                printf("-------------------------------------------------------------\n");
                _print_nfs_share_info(&shareinfo[i]);
        }
}

static void _list_share_info(IN share_protocol_t prot, IN const shareinfo_t *shareinfo, IN int count)
{
        if (SHARE_CIFS == prot) {
                _list_cifs_share_info(shareinfo, count);
        } else if (SHARE_FTP == prot) {
                _list_ftp_share_info(shareinfo, count);
        } else if (SHARE_NFS == prot) {
                _list_nfs_share_info(shareinfo, count);
        }
}

static int _share_list(share_protocol_t prot)
{
        int ret;
        int count;
        shareinfo_t *shareinfo;

        ret = md_share_list_byprotocal(prot, &shareinfo, &count);
        if (ret)
                GOTO(err_ret, ret);

        _list_share_info(prot, shareinfo, count);

        yfree((void **)&shareinfo);

        return 0;
err_ret:
        return ret;
}

static struct option share_options[] = {
        {"group",       required_argument, 0, 'g'},
        {"share_name",  required_argument, 0, 'n'},
        {"mod",         required_argument, 0, 'm'},
        {"protocol",    required_argument, 0, 'p'},
        {"remove",      no_argument,       0, 'r'},
        {"set",         no_argument,       0, 's'},
        {"user",        required_argument, 0, 'u'},
        {"get",         no_argument,       0, 'G'},
        {"list",        no_argument,       0, 'l'},
        {"host",        required_argument, 0, 'H'},
        {"help",        no_argument,       0, 'h'},
        {0,             0,                 0,  0 },
};


int main(int argc, char *argv[])
{
        int ret;
        char c_opt, *path = NULL;
        char *err_str = NULL;
        //name:user, group, IP or host name;share_name:mount point name;
        char name[MAX_NAME_LEN], share_name[MAX_NAME_LEN];
        char *pshare_name = NULL;
        int name_len = sizeof(name);
        char normal_path[MAX_PATH_LEN];
        fileid_t dirid;
        fileinfo_t *md;
        struct stat stbuf;
        void *req_buf = NULL;
        uint32_t req_buflen = 0;
        char buf[MAX_PATH_LEN * 2];
        uint32_t buflen = sizeof(buf);
        uint32_t share_user_mask = 0;
        share_oper_type_t oper_type = SHARE_INVALID_OPER;
        share_protocol_t share_prot = SHARE_INVALID_PROT;
        share_user_type_t user_type = SHARE_INVALID_USER;
        share_mode_t share_mode = SHARE_INVALID_MOD;
        share_cifs_t share_cifs;
        share_ftp_t share_ftp;
        share_nfs_t share_nfs;
        share_key_t share_key;
        bool share_name_flag = false;

        share_name[0] = 0;
        normal_path[0] = 0;
        memset(&dirid, 0, sizeof(dirid));

        while (1) {
                int option_index = 0;

                c_opt = getopt_long(argc, argv, "g:n:m:p:rsu:GlH:h", share_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                case 'g':
                        if (strlen(optarg) >= MAX_NAME_LEN) {
                                fprintf(stderr, "group name %s is invalid.\n", optarg);
                                exit(1);
                        }
                        strncpy(name, optarg, name_len);
                        share_user_mask |= SHARE_GROUP_BIT;
                        user_type = SHARE_GROUP;
                        break;
                case 'n':
                        if (strlen(optarg) >= MAX_NAME_LEN) {
                                fprintf(stderr, "share name %s is invalid.\n", optarg);
                                exit(1);
                        }
                        strncpy(share_name, optarg, sizeof(share_name));
                        share_name_flag = true;
                        break;

                case 'm':
                        share_mode = _share_get_mode(optarg);
                        if (SHARE_INVALID_MOD == share_mode) {
                                fprintf(stderr, "share mod %s is invalid.\n", optarg);
                                exit(1);
                        }
                        break;
                case 'p':
                        share_prot = _share_get_prot(optarg);
                        if (SHARE_INVALID_PROT == share_prot) {
                                fprintf(stderr, "share protocol %s is invalid.\n", optarg);
                                exit(1);
                        }
                        break;
                case 'r':
                        oper_type = SHARE_REMOVE;
                        break;
                case 's':
                        oper_type = SHARE_SET;
                        break;
                case 'u':
                        if (strlen(optarg) >= MAX_NAME_LEN) {
                                fprintf(stderr, "user name %s is invalid.\n", optarg);
                                exit(1);
                        }
                        strncpy(name, optarg, name_len);
                        share_user_mask |= SHARE_USER_BIT;
                        user_type = SHARE_USER;
                        break;
                case 'G':
                        oper_type = SHARE_GET;
                        break;
                case 'l':
                        oper_type = SHARE_LIST;
                        break;
                case 'H':
                         if (strlen(optarg) >= MAX_NAME_LEN) {
                                fprintf(stderr, "user name %s is invalid.\n", optarg);
                                exit(1);
                        }
                        strncpy(name, optarg, name_len);
                        share_user_mask |= SHARE_HOST_BIT;
                        user_type = SHARE_HOST;
                        break;
                case 'h':
                        _share_usage();
                        exit(0);
                default:
                        _share_usage();
                        exit(1);
                }
        }

        if ((SHARE_LIST != oper_type) && (optind != argc - 1)) {
                fprintf(stderr, "need enter path.\n");
                exit(1);
        }

        if (optind == argc - 1) {
                path = argv[optind];
                if (path && path[0] != '/') {
                        fprintf(stderr, "invalid path %s, must start from /\n", path);
                        exit(1);
                }

                if(NULL == sdfs_realpath(path, normal_path)) {
                        fprintf(stderr, "invalid path %s\n", path);
                        exit(1);
                }
        }

        ret = _share_args_check(oper_type, share_prot, share_mode,
                                share_user_mask, share_name_flag);
        if (ret) {
                ret = EPERM;
                goto err_ret;
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        dbg_info(0);

        ret = ly_init_simple("uss.share");
        if (ret)
                GOTO(err_ret, ret);

        if (0 != normal_path[0]) {
                ret = sdfs_lookup_recurive(normal_path, &dirid);
                if (ret)
                        GOTO(err_ret, ret);

                md = (void *)buf;
                ret = md_getattr(&dirid, (void *)md);
                if (ret)
                        GOTO(err_ret, ret);

                MD2STAT(md, &stbuf);
                if (!S_ISDIR((stbuf).st_mode)) {
                        ret = ENOTDIR;
                        fprintf(stderr, "%s is not a directory.\n", normal_path);
                        goto err_ret;
                }

        }

        if (SHARE_SET == oper_type) {
                //oct 777 decimal 511
                ret = sdfs_chmod(NULL, &dirid, 511);
                if(ret)
                        GOTO(err_ret, ret);

                switch (share_prot) {
                case SHARE_CIFS:
                        _share_args2info(&dirid, name, share_name, user_type, share_mode, path, &share_cifs);
                        req_buf = &share_cifs;
                        req_buflen = sizeof(share_cifs);
                        break;
                case SHARE_FTP:
                        _share_args2info(&dirid, name, share_name, SHARE_USER, share_mode, path, &share_ftp);
                        req_buf = &share_ftp;
                        req_buflen = sizeof(share_ftp);
                        break;
                case SHARE_NFS:
                        _share_args2info(&dirid, name, share_name, user_type, share_mode, path, &share_nfs);
                        req_buf = &share_nfs;
                        req_buflen = sizeof(share_nfs);
                        break;
                default:
                        exit(1);
                        break;
                }
        } else if ((SHARE_GET == oper_type) || (SHARE_REMOVE == oper_type)) {
                //if (SHARE_CIFS == share_prot)
                pshare_name = share_name;
                share_get_key(&dirid, name, pshare_name, user_type, &share_key);
                req_buf = &share_key;
                req_buflen = sizeof(share_key);
        }

        switch (oper_type){
        case SHARE_SET:
                ret = md_set_shareinfo(share_prot, req_buf, req_buflen);
                if (ret) {
                        fprintf(stderr, "set %s share info for %s:%s by %s failed, error is %s.\n",
                                normal_path, share_userstr[user_type], name,
                                share_protstr[share_prot], strerror(ret));
                        goto err_ret;
                }
                break;
        case SHARE_GET:
                ret = md_get_shareinfo(share_prot, req_buf, req_buflen, buf, buflen);
                if (ret) {
                        if (ENOENT == ret)
                                err_str = "not exist";
                        else
                                err_str = strerror(ret);

                        fprintf(stderr, "get %s share info for %s:%s by %s failed, error is %s.\n",
                                normal_path, share_userstr[user_type], name,
                                share_protstr[share_prot], err_str);
                        goto err_ret;
                } else
                        _print_share_info(share_prot, buf);

                break;
        case SHARE_REMOVE:
                ret = md_remove_shareinfo(share_prot, req_buf, req_buflen);
                if (ret) {
                        fprintf(stderr, "remove %s share info for %s:%s by %s failed, error is %s.\n",
                                normal_path, share_userstr[user_type], name,
                                share_protstr[share_prot], strerror(ret));
                        goto err_ret;
                }
                break;
        case SHARE_LIST:
                ret = _share_list(share_prot);
                if (ret)
                        GOTO(err_ret, ret);

                break;
        default:
                break;
        }

        return 0;

err_ret:
        return ret;
}

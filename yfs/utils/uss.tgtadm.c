

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <dirent.h>
#include <ctype.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "xattr.h"
#include "../../yfs/mdc/md_lib.h"

#define PROG    "uss.tgtadm"

/*
 * Global variable
 */
int verbose = 0;

#define ISCSI_USER_MAX          20
#define ISCSI_LUN_ALIAS_MAX     20
#define ISCSI_LUN_MAX           254
#define ISCSI_TGT_NAME_MAX      1024

static void usage()
{
        fprintf(stderr,
                "usage: uss.tgtadm [-verbose | -v] [--help | -h]\n"
                "               [--mode | -m mode]\n"
                "               [--op | -o op]\n"
                "               [--target | -t target]\n"
                "               [--lun | -l lun]\n"
                "               [--alias | -a alias]\n"
                "               [--size | -s size]\n"
                "               [--user|-u user]\n"
                "               [--pass|-p pass]\n"
                "               [--direction|-d dir]\n"
                "\n"
                "-v --verbose           Show verbose message\n"
                "-h --help              Show this help\n"
                "-o --op=op             Valid op: `new' `del' `list'\n"
                "-m --mode=mode         Valid mode: `target' `lun' `account'\n"
                "-t --target=target     The target format is: NAMESPACE-SUBNAME\n"
                "-l --lun=lun           The of logic unit, valid is 0~%u\n"
                "-a --alias=alias       The alias of lun, length is 0~%u\n"
                "-s --size=size         Valid unit: [b|B] [k|K] [m|M] [g|G]\n"
                "-u --user=user         The username to set\n"
                "-p --pass=pass         The password to set\n"
                "-d --direction=dir     The direction of the account to set\n"
                "\n"
                "See `manual of the yiscsi' for more detail.\n"
                "\n",
                ISCSI_LUN_MAX, ISCSI_LUN_ALIAS_MAX
               );
}

/*
 * Options structure for parse
 */
static struct option long_options[] = {
        { "verbose",    0, 0, 'v' },
        { "help",       0, 0, 'h' },
        { "op",         1, 0, 'o' },
        { "mode",       1, 0, 'm' },
        { "target",     1, 0, 't' },
        { "lun",        1, 0, 'l' },
        { "alias",      1, 0, 'a' },
        { "size",       1, 0, 's' },
        { "user",       1, 0, 'u' },
        { "pass",       1, 0, 'p' },
        { "direction",  1, 0, 'd' },
        { 0, 0, 0, 0 },
};

const char optstr[] = "vho:m:t:l:a:s:u:p:d:";

/*
 * The `sdfs_removexattr' is not impliment now, use a special value
 * to express this.
 */
const char *__none_value = "____NONE____";

/*
 * Options enum and tokens for convert
 */
struct ytoken {
        char *key;
        int val;
};

enum tgtop_t {
        OP_NEW,
        OP_DEL,
        OP_LIST,
};

enum tgtmode_t {
        MODE_TARGET,
        MODE_LUN,
        MODE_ALIAS,
        MODE_ACCOUNT,
};

enum tgtdir_t {
        DIR_IN,
        DIR_OUT,
};

struct ytoken target_op_tokens[] = {
        { "new",  OP_NEW  },
        { "del",  OP_DEL  },
        { "list", OP_LIST },
};

struct ytoken lun_op_tokens[] = {
        { "new",  OP_NEW  },
        { "del",  OP_DEL  },
        { "list", OP_LIST },
};

struct ytoken account_op_tokens[] = {
        { "new",  OP_NEW  },
        { "del",  OP_DEL  },
        { "list", OP_LIST },
};

struct ytoken alias_op_tokens[] = {
        { "new",  OP_NEW  },
        { "del",  OP_DEL  },
        { "list", OP_LIST },
};

struct ytoken mode_tokens[] = {
        { "target",  MODE_TARGET },
        { "lun",     MODE_LUN },
        { "account", MODE_ACCOUNT },
        { "alias",   MODE_ALIAS },
};

struct ytoken tgtdir_tokens[] = {
        { "in",  DIR_IN },
        { "out", DIR_OUT },
};

struct ychap_key {
        char *key_user;
        char *key_pass;
};

struct ychap_key ychap_keys[] = {
        { "iscsi.in_user", "iscsi.in_pass" },
        { "iscsi.out_user", "iscsi.out_pass" },
};

#define IQN_FORMAT      "+----------------------------------------------+\n"    \
                        "|                   IQN                        |\n"    \
                        "|----------------------------------------------|\n"    \
                        "| %44s |\n"                                            \
                        "+----------------------------------------------+\n"

#define TGT_FORMAT      "\nTarget (%s) :                                 \n"

#define LUN_TBL_BEGIN   "+----------------------------------------------+\n"    \
                        "|    Lun    |    Size   |         Alias        |\n"    \
                        "|-----------+-----------+----------------------|\n"
#define LUN_TBL_FORMAT  "| %9s | %8llu%c | %20s |\n"
#define LUN_TBL_END     "+----------------------------------------------+\n"

#define ALIAS_FORMAT    "+----------------------------------------------+\n"    \
                        "|    Lun    |   Alias                          |\n"    \
                        "|-----------+----------------------------------|\n"    \
                        "| %9s | %32s |\n"                                      \
                        "+----------------------------------------------+\n"

#define ACCOUNT_FORMAT  "+----------------------------------------------+\n"    \
                        "|      User (In)       |     Pass (In)         |\n"    \
                        "|----------------------+-----------------------|\n"    \
                        "| %20s | %21s |\n"                                     \
                        "|----------------------------------------------|\n"    \
                        "|      User (Out)      |     Pass (Out)        |\n"    \
                        "|----------------------+-----------------------|\n"    \
                        "| %20s | %21s |\n"                                     \
                        "+----------------------------------------------+\n"

const char *yiscsi_lun_alias_key = "iscsi.lun_alias";
const char *yiscsi_tgt_magic_key = "iscsi.is_target";

#define ytoken_size(tokens) (sizeof(tokens)/sizeof(tokens[0]))

static inline int ytoken_match(struct ytoken *tokens, int token_cnt, char *key)
{
        int i;

        for (i = 0; i < token_cnt; ++i)
                if (!strcmp(tokens[i].key, key))
                        return tokens[i].val;

        return -1;
}

/**
 * Misc functions
 */

static int _is_valid_char(char ch)
{
        return ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') ||
                (ch == '.') || (ch == ':') || (ch == '-'));
}

static int _is_valid_name(char *name)
{
        size_t i;

        for (i = 0; i < strlen(name); ++i) {
                if (!_is_valid_char(name[i])) {
                        return 0;
                }
        }

        return 1;
}

static int check_lun_name(char *name)
{
        size_t i;
        uint32_t lun;

        for (i = 0; i < strlen(name); ++i) {
                if (!isdigit(name[i])) {
                        goto err_ret;
                }
        }

        lun = atoll(name);

        if (lun > ISCSI_LUN_MAX) {
                goto err_ret;
        }

        return 0;
err_ret:
        return EINVAL;
}

static int target_name_sep(char *name, char **_ns, char **_sub)
{
        int ret;
        char *ns, *sub;

        ns = name;

        /*         iqn            :          name */
        if (strlen(sanconf.iqn) + 1 + strlen(name) >= ISCSI_TGT_NAME_MAX) {
                fprintf(stderr, "invalid name length, maximum length is %d\n",
                        ISCSI_TGT_NAME_MAX);
                ret = EINVAL;
                goto err_ret;
        }

        if (!_is_valid_name(name)) {
                fprintf(stderr, "invalid char found in name, only 'a-z', '0-9', "
                        "'.', '-', ':' is allowed\n");
                ret = EINVAL;
                goto err_ret;
        }

        sub = strchr(name, '.');
        if (!sub) {
                ret = EINVAL;
                goto err_ret;
        }

        if (sub != strrchr(name, '.')) {
                fprintf(stderr, "only one `.' is allowed in target name\n");
                ret = EINVAL;
                goto err_ret;
        }

        if (name[0] == '.' || name[strlen(name) - 1] == '.') {
                fprintf(stderr, "the `.' is only allowed in the middle of "
                        "the target name\n");
                ret = EINVAL;
                goto err_ret;
        }

        *sub++ = 0;

        if (_ns)
                *_ns = ns;
        if (_sub)
                *_sub = sub;

        return 0;
err_ret:
        fprintf(stderr, "the format of target name is: namespace.subname\n");
        return ret;
}

static int get_size(char *str, uint64_t *_size)
{
        int ret;
        uint64_t size = 0;
        char unit;

        if (strlen(str) < 2) {
                ret = EINVAL;
                goto err_ret;
        }

        unit = str[strlen(str) - 1];
        str[strlen(str) - 1] = 0;

        size = atoll(str);

        switch (unit) {
        case 'b':
        case 'B':
                break;
        case 'k':
        case 'K':
                size *= 1000;
                break;
        case 'm':
        case 'M':
                size *= (1000 * 1000);
                break;
        case 'g':
        case 'G':
                size *= (1000 * 1000 * 1000);
                break;
        default:
                fprintf(stderr, "size unit must be specified, see help for detail\n");
                ret = EINVAL;
                goto err_ret;
        }

        *_size = size;

        return 0;
err_ret:
        return ret;
}

static int uss_lun_list(fileid_t *tgtid)
{
        int ret, delen;
        off_t offset;
        void *de0;
        struct dirent *de;
        struct stat stbuf;
        fileinfo_t *md;
        char lname[MAX_PATH_LEN], unit;
        char alias[MAX_BUF_LEN];
        uint64_t size;
        size_t xattr_size;

        offset = 0;
        de0 = NULL;
        delen = 0;

        printf(LUN_TBL_BEGIN);

        while (1) {
                ret = sdfs_readdirplus(tgtid, offset, &de0, &delen);
                if (ret)
                        GOTO(err_ret, ret);
                else if (delen == 0)
                        break;

                dir_for_each(de0, delen, de, offset) {
                        if (!strcmp(de->d_name, ".") || !strcmp(de->d_name, "..") ||
                            /* Skip Lun if the name start with '.' */
                            de->d_name[0] == '.') {
                                continue;
                        }

                        if (check_lun_name(de->d_name))
                                continue;

                        md = (void *)de + de->d_reclen - sizeof(md_proto_t);
                        MD2STAT(md, &stbuf);

                        if (!S_ISREG(stbuf.st_mode))
                                continue;

                        /* Alias is optional */
                        alias[0] = 0;
                        xattr_size = sizeof(alias);
                        sdfs_getxattr(&md->fileid, yiscsi_lun_alias_key, alias, &xattr_size);
                        if (!strcmp(alias, __none_value))
                                alias[0] = 0;

                        snprintf(lname, MAX_PATH_LEN, "%s", de->d_name);

                        if ((size = stbuf.st_size / (1000 * 1000 * 1000)))
                                unit = 'G';
                        else if ((size = stbuf.st_size / (1000 * 1000)))
                                unit = 'M';
                        else if ((size = stbuf.st_size / 1000))
                                unit = 'K';
                        else
                                unit = 'B';

                        printf(LUN_TBL_FORMAT, lname, (LLU)size, unit, alias);
                }

                yfree((void **)&de0);
        }

        printf(LUN_TBL_END);

        return 0;
err_ret:
        if (de0)
                yfree((void **)&de0);
        return ret;
}

static int uss_tgt_list(fileid_t *nsid, const char *ns, const char *filter)
{
        int ret, delen;
        off_t offset;
        void *de0;
        struct dirent *de;
        struct stat stbuf;
        fileinfo_t *md;
        char tname[MAX_PATH_LEN];
        char value[MAX_BUF_LEN];
        size_t size;

        offset = 0;
        de0 = NULL;
        delen = 0;

        while (1) {
                ret = sdfs_readdirplus(nsid, offset, &de0, &delen);
                if (ret)
                        GOTO(err_ret, ret);
                else if (delen == 0)
                        break;

                dir_for_each(de0, delen, de, offset) {
                        if (!strcmp(de->d_name, ".") || !strcmp(de->d_name, "..")) {
                                continue;
                        }

                        if (filter && strcmp(filter, de->d_name))
                                continue;

                        md = (void *)de + de->d_reclen - sizeof(md_proto_t);
                        MD2STAT(md, &stbuf);

                        if (!S_ISDIR(stbuf.st_mode))
                                continue;

                        size = sizeof(value);
                        memset(value, 0x00, size);
                        ret = sdfs_getxattr(&md->fileid, yiscsi_tgt_magic_key, value, &size);
                        if (ret)
                                continue;

                        if (strcmp(value, "yes") && strcmp(value, "true"))
                                continue;

                        snprintf(tname, sizeof(tname), "%s.%s", ns, de->d_name);

                        printf(TGT_FORMAT, tname);

                        (void) uss_lun_list(&md->fileid);
                }

                yfree((void **)&de0);
        }

        return 0;
err_ret:
        if (de0)
                yfree((void **)&de0);
        return ret;
}

static int uss_ns_list(const char *nf, const char *tf)
{
        int ret;
        uint32_t i, offset;
        buffer_t rep;
        char tmp[MAX_BUF_LEN], *buf = NULL;
        mdp_lvlist_rep_t *lvms;
        lv_entry_t *entry;

        mbuffer_init(&rep, 0);

        ret = md_lvlist1(&rep);
        if (ret)
                GOTO(err_ret, ret);

        if (rep.len > MAX_BUF_LEN) {
                ret = ymalloc((void **)&buf, rep.len);
                if (ret)
                        GOTO(err_ret, ret);
        } else
                buf = tmp;

        mbuffer_get(&rep, buf, rep.len);

        lvms = (void *)buf;

        printf(IQN_FORMAT, sanconf.iqn);

        for (i = 0, offset = 0; i < rep.len / sizeof(lv_entry_t); ++i) {
                entry = (void *)(lvms->buf.buf + offset);

                if (nf && strcmp(nf, entry->vname))
                        goto next_entry;

                (void) uss_tgt_list(&entry->fileid, entry->vname, tf);

next_entry:
                offset += sizeof(lv_entry_t);
        }

        if (buf && buf != tmp)
                yfree((void **)&buf);
        mbuffer_free(&rep);

        return 0;
err_ret:
        if (buf && buf != tmp)
                yfree((void **)&buf);
        mbuffer_free(&rep);
        return ret;
}

/**
 * Target operation process
 */

static int mode_target_new(char *ns, char *subname)
{
        int ret;
        size_t size;
        char *magickey = "true";
        char path[MAX_PATH_LEN], name[MAX_NAME_LEN];
        fileid_t parent, fileid;

        snprintf(path, sizeof(path), "/%s/%s", ns, subname);

        ret = sdfs_splitpath(path, &parent, name);
        if (ret) {
                if (ret == ENOENT) {
                        fprintf(stderr, "namespace `%s' not exists, please create "
                                "it first\n", ns);
                }
                GOTO(err_ret, ret);
        }

        ret = sdfs_mkdir(&parent, name, NULL, 0755, 0, 0);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        size = strlen(magickey) + 1;
        ret = sdfs_setxattr(&fileid, yiscsi_tgt_magic_key, "true", size, USS_XATTR_DEFAULT);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int mode_target_del(char *ns, char *subname)
{
        int ret, delen = 0, offset = 0;
        void *de = NULL;
        char path[MAX_PATH_LEN], name[MAX_NAME_LEN];
        fileid_t parent;

        snprintf(path, sizeof(path), "/%s/%s", ns, subname);

        /*
         * The raw_rmdir has a bug, directory can be deleted
         * even it isn't empty, so check it by raw_readdir here.
         */
        ret = ly_readdir(path, offset, &de, &delen);
        if (ret)
                GOTO(err_ret, ret);

        if (delen) {
                ret = ENOTEMPTY;
                fprintf(stderr, "%s: %s\n", subname, strerror(ret));
                goto err_ret;
        }

        free(de);

        ret = sdfs_splitpath(path, &parent, name);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_rmdir(&parent, name);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int mode_target_list()
{
        return uss_ns_list(NULL, NULL);
}

/**
 * Lun operation process
 */

static int mode_lun_new(char *ns, char *subname, char *lname, char *alias, uint64_t size)
{
        int ret;
        size_t size;
        char path[MAX_PATH_LEN], name[MAX_NAME_LEN];
        fileid_t parent, fileid;

        snprintf(path, sizeof(path), "/%s/%s/%s", ns, subname, lname);

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret) {
                if (ret != ENOENT)
                        GOTO(err_ret, ret);
        } else {
                ret = EEXIST;
                GOTO(err_ret, ret);
        }

        /*
         * Create a hide file first, then rename it after truncate it to the
         * specify size
         */
retry:
        snprintf(path, sizeof(path), "/%s/%s/.__%s__.%u", ns, subname, lname, (uint32_t)random());

        ret = sdfs_splitpath(path, &parent, name);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_create(&parent, name, &fileid, 0755, 0, 0);
        if (ret) {
                if (ret == EEXIST) {
                        DERROR("temporary file exists, retry ...\n");
                        goto retry;
                } else
                        GOTO(err_ret, ret);
        }

        ret = sdfs_truncate(&fileid, size);
        if (ret)
                GOTO(err_unlink, ret);

        if (alias) {
                size = strlen(alias) + 1;
                ret = sdfs_setxattr(&fileid, yiscsi_lun_alias_key, alias,
                                   size, USS_XATTR_DEFAULT);
                if (ret)
                        GOTO(err_unlink, ret);
        }

        ret = sdfs_rename(&parent, name, &parent, lname);
        if (ret)
                GOTO(err_unlink, ret);

        return 0;
err_unlink:
        sdfs_unlink(&parent, name);
err_ret:
        return ret;
}

static int mode_lun_del(char *ns, char *subname, char *lname)
{
        int ret;
        char path[MAX_PATH_LEN], name[MAX_NAME_LEN];
        fileid_t parent;

        snprintf(path, sizeof(path), "/%s/%s/%s", ns, subname, lname);

        ret = sdfs_splitpath(path, &parent, name);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_unlink(&parent, name);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int mode_lun_list(char *ns, char *subname)
{
        return uss_ns_list(ns, subname);
}

/**
 * Alias operation process
 */

static int mode_alias_new(char *ns, char *subname, char *lname, char *alias)
{
        int ret;
        size_t size;
        char path[MAX_PATH_LEN];
        fileid_t fileid;

        snprintf(path, sizeof(path), "/%s/%s/%s", ns, subname, lname);

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        size = strlen(alias) + 1;
        ret = sdfs_setxattr(&fileid, yiscsi_lun_alias_key, alias, size, USS_XATTR_DEFAULT);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int mode_alias_del(char *ns, char *subname, char *lname)
{
        int ret;
        size_t size;
        char path[MAX_PATH_LEN];
        fileid_t fileid;

        snprintf(path, sizeof(path), "/%s/%s/%s", ns, subname, lname);

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        size = strlen(__none_value) + 1;
        ret = sdfs_setxattr(&fileid, yiscsi_lun_alias_key, __none_value, size, USS_XATTR_DEFAULT);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int mode_alias_list(char *ns, char *subname, char *lname)
{
        int ret;
        char path[MAX_PATH_LEN], alias[MAX_PATH_LEN];
        fileid_t fileid;
        size_t size;

        snprintf(path, sizeof(path), "/%s/%s/%s", ns, subname, lname);

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        alias[0] = 0;
        size = sizeof(alias);
        (void) sdfs_getxattr(&fileid, yiscsi_lun_alias_key, alias, &size);

        if (!strcmp(alias, __none_value))
                alias[0] = 0;

        printf(ALIAS_FORMAT, lname, alias);

        return 0;
err_ret:
        return 0;
}

/**
 * Account operation process
 */
static int mode_account_new(char *ns, char *subname,
                            char *user, char *pass, int dir)
{
        int ret;
        size_t size;
        char path[MAX_PATH_LEN];
        fileid_t fileid;

        snprintf(path, sizeof(path), "/%s/%s", ns, subname);

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        size = strlen(user) + 1;
        ret = sdfs_setxattr(&fileid, ychap_keys[dir].key_user, user, size, USS_XATTR_DEFAULT);
        if (ret)
                GOTO(err_ret, ret);

        size = strlen(pass) + 1;
        ret = sdfs_setxattr(&fileid, ychap_keys[dir].key_pass, pass, size, USS_XATTR_DEFAULT);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int mode_account_del(char *ns, char *subname, int dir)
{
        int ret;
        size_t size;
        char path[MAX_PATH_LEN];
        fileid_t fileid;

        snprintf(path, sizeof(path), "/%s/%s", ns, subname);

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        size = strlen(__none_value) + 1;
        ret = sdfs_setxattr(&fileid, ychap_keys[dir].key_user, __none_value,
                           size, USS_XATTR_DEFAULT);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_setxattr(&fileid, ychap_keys[dir].key_pass, __none_value,
                           size, USS_XATTR_DEFAULT);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int mode_account_list(char *ns, char *subname)
{
        int ret;
        char path[MAX_PATH_LEN], user[2][MAX_BUF_LEN], pass[2][MAX_BUF_LEN];
        fileid_t fileid;
        size_t size = MAX_BUF_LEN;

        snprintf(path, sizeof(path), "/%s/%s", ns, subname);

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        memset(user, 0x00, sizeof(user));
        memset(pass, 0x00, sizeof(pass));

        (void) sdfs_getxattr(&fileid, ychap_keys[DIR_IN].key_user,  user[0], &size);
        (void) sdfs_getxattr(&fileid, ychap_keys[DIR_IN].key_pass,  pass[0], &size);
        (void) sdfs_getxattr(&fileid, ychap_keys[DIR_OUT].key_user, user[1], &size);
        (void) sdfs_getxattr(&fileid, ychap_keys[DIR_OUT].key_pass, pass[1], &size);

        if (!strcmp(user[DIR_IN], __none_value))
                user[DIR_IN][0] = 0;
        if (!strcmp(pass[DIR_IN], __none_value))
                pass[DIR_IN][0] = 0;

        if (!strcmp(user[DIR_OUT], __none_value))
                user[DIR_OUT][0] = 0;
        if (!strcmp(pass[DIR_OUT], __none_value))
                pass[DIR_OUT][0] = 0;

        printf(ACCOUNT_FORMAT, user[DIR_IN], pass[DIR_IN], user[DIR_OUT], pass[DIR_OUT]);

        return 0;
err_ret:
        return 0;
}

int main(int argc, char *argv[])
{
        int ret;
        char c_opt;
        char *tname = NULL, *lname = NULL, *alias = NULL;
        char *user = NULL, *pass = NULL;
        char *ns = NULL, *subname = NULL;
        char *option = NULL;
        uint64_t size = 0;
        int op = -1, mode = -1, dir = -1;

        dbg_info(0);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

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
                case 'o':
                        option = optarg;
                        break;
                case 'm':
                        mode = ytoken_match(mode_tokens,
                                            ytoken_size(mode_tokens),
                                            optarg);
                        if (mode == -1) {
                                fprintf(stderr, "invalid argument: `%s'\n", optarg);
                                exit(EINVAL);
                        }
                        break;
                case 't':
                        tname = optarg;
                        ret = target_name_sep(tname, &ns, &subname);
                        if (ret)
                                exit(EINVAL);
                        break;
                case 'l':
                        lname = optarg;
                        ret = check_lun_name(lname);
                        if (ret) {
                                fprintf(stderr, "invalid lun, only digit is allowed, "
                                        "valid lun: 0~%u\n", ISCSI_LUN_MAX);
                                exit(EINVAL);
                        }
                        break;
                case 'a':
                        alias = optarg;
                        if (strlen(alias) > ISCSI_LUN_ALIAS_MAX) {
                                fprintf(stderr, "alias too long, max length %u\n",
                                        ISCSI_LUN_ALIAS_MAX);
                                exit(EINVAL);
                        }
                        break;
                case 's':
                        ret = get_size(optarg, &size);
                        if (ret || !size) {
                                fprintf(stderr, "invalid argument: `%s'\n", optarg);
                                exit(EINVAL);
                        }
                        break;
                case 'u':
                        user = optarg;
                        if (strlen(user) > ISCSI_USER_MAX) {
                                fprintf(stderr, "user too long, max length %u\n",
                                        ISCSI_USER_MAX);
                                exit(EINVAL);
                        }
                        break;
                case 'p':
                        pass = optarg;
                        if (strlen(pass) < 12 || strlen(pass) > 16) {
                                fprintf(stderr, "The length of CHAP passwod must be [12 ~ 16] bytes\n");
                                exit(EINVAL);
                        }
                        break;
                case 'd':
                        dir = ytoken_match(tgtdir_tokens,
                                           ytoken_size(tgtdir_tokens),
                                           optarg);
                        if (dir == -1) {
                                fprintf(stderr, "invalid argument: `%s'\n", optarg);
                                exit(EINVAL);
                        }
                        break;
                default:
                        usage();
                        exit(EINVAL);
                }
        }

        /* Arguments check */
        if (mode == -1) {
                fprintf(stderr, "--mode must be specified\n");
                exit(EINVAL);
        }

        if (option == NULL) {
                fprintf(stderr, "--op must be specified\n");
                exit(EINVAL);
        }

        /* Uss initialization */
        ret = ly_init_simple(PROG);
        if (ret)
                GOTO(err_ret, ret);

        (void) srandom((uint32_t)getpid());

        /* Task distribute */
        switch (mode) {
        case MODE_TARGET:
                op = ytoken_match(target_op_tokens,
                                  ytoken_size(target_op_tokens),
                                  option);
                if (op == -1) {
                        fprintf(stderr, "invalid argument: `%s'\n", option);
                        exit(EINVAL);
                }

                switch (op) {
                case OP_NEW:
                case OP_DEL:
                        if (!tname) {
                                fprintf(stderr, "--target must be specified in "
                                        "the new and del operation of target mode\n");
                                exit(EINVAL);
                        }

                        switch (op) {
                        case OP_NEW:
                                ret = mode_target_new(ns, subname);
                                if (ret)
                                        goto err_ret;
                                break;
                        case OP_DEL:
                                ret = mode_target_del(ns, subname);
                                if (ret)
                                        goto err_ret;
                                break;
                        }
                        break;
                case OP_LIST:
                        ret = mode_target_list();
                        if (ret)
                                goto err_ret;
                        break;
                }
                break;
        case MODE_LUN:
                if (!tname) {
                        fprintf(stderr, "--target must be specified in the "
                                "lun mode\n");
                        exit(EINVAL);
                }

                op = ytoken_match(lun_op_tokens,
                                  ytoken_size(lun_op_tokens),
                                  option);
                if (op == -1) {
                        fprintf(stderr, "invalid argument: `%s'\n", option);
                        exit(EINVAL);
                }

                switch (op) {
                case OP_NEW:
                case OP_DEL:
                        if (!lname) {
                                fprintf(stderr, "--lun must be specified in "
                                        "the new and del operation of lun mode\n");
                                exit(EINVAL);
                        }

                        switch (op) {
                        case OP_NEW:
                                if (!size) {
                                        fprintf(stderr, "--size must be specified in "
                                                "the new operation of lun mode\n");
                                        exit(EINVAL);
                                }

                                ret = mode_lun_new(ns, subname, lname, alias, size);
                                if (ret)
                                        goto err_ret;
                                break;
                        case OP_DEL:
                                ret = mode_lun_del(ns, subname, lname);
                                if (ret)
                                        goto err_ret;
                                break;
                        }
                        break;
                case OP_LIST:
                        ret = mode_lun_list(ns, subname);
                        if (ret)
                                goto err_ret;
                        break;
                }
                break;
        case MODE_ALIAS:
                if (!tname || !lname) {
                        fprintf(stderr, "--target and --lun must be specified in the "
                                "alias mode\n");
                        exit(EINVAL);
                }

                op = ytoken_match(alias_op_tokens,
                                  ytoken_size(alias_op_tokens),
                                  option);
                if (op == -1) {
                        fprintf(stderr, "invalid argument: `%s'\n", option);
                        exit(EINVAL);
                }

                switch (op) {
                case OP_NEW:
                        if (!alias) {
                                fprintf(stderr, "--alias must be specified in the "
                                        "alias mode\n");
                                exit(EINVAL);
                        }

                        ret = mode_alias_new(ns, subname, lname, alias);
                        if (ret)
                                goto err_ret;
                        break;
                case OP_DEL:
                        ret = mode_alias_del(ns, subname, lname);
                        if (ret)
                                goto err_ret;
                        break;
                case OP_LIST:
                        ret = mode_alias_list(ns, subname, lname);
                        if (ret)
                                goto err_ret;
                        break;
                }
                break;
        case MODE_ACCOUNT:
                if (!tname) {
                        fprintf(stderr, "--target must be specified in the "
                                "account mode\n");
                        exit(EINVAL);
                }

                op = ytoken_match(account_op_tokens,
                                  ytoken_size(account_op_tokens),
                                  option);
                if (op == -1) {
                        fprintf(stderr, "invalid argument: `%s'\n", option);
                        exit(EINVAL);
                }

                switch (op) {
                case OP_NEW:
                        if (!user || !pass || dir == -1) {
                                fprintf(stderr, "--user, --pass and --direction "
                                        "must be specified in the new operation "
                                        "of account mode\n");
                                exit(EINVAL);
                        }

                        ret = mode_account_new(ns, subname, user, pass, dir);
                        if (ret)
                                goto err_ret;
                        break;
                case OP_DEL:
                        if (dir == -1) {
                                fprintf(stderr, "--direction must be specified "
                                        "in the del operation of account mode\n");
                                exit(EINVAL);
                        }

                        ret = mode_account_del(ns, subname, dir);
                        if (ret)
                                goto err_ret;
                        break;
                case OP_LIST:
                        ret = mode_account_list(ns, subname);
                        if (ret)
                                goto err_ret;
                        break;
                }
                break;
        default:
                fprintf(stderr, "BUG\n");
                exit(EINVAL);
        }

        return 0;
err_ret:
        exit(ret);
}

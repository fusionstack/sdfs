#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>

#define DBG_SUBSYS S_YFTP

#include "cmdio.h"
#include "ftpcodes.h"
#include "ftpfeatures.h"
#include "postlogin.h"
#include "session.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "yftp_conf.h"
#include "ynet.h"
#include "dbg.h"
#include "cJSON.h"
#include "xattr.h"
#include "sdfs_share.h"
#include "schedule.h"
#include "md_lib.h"

int emit_greeting(struct yftp_session *ys)
{
        /* XXX process connection limits here.
         * such as max_clients, max_per_ip etc. -gj
         */

        return cmdio_write(ys, FTP_GREET, SERVER_SOFTWARE " " SERVER_URL);
}

static int get_valid_mode(const share_ftp_t * ftp, uint32_t *mode){
        if (ftp->mode == SHARE_RO){
                *mode = SESSION_READONLY;
                return 0;
        }
        else if (ftp->mode == SHARE_RW){
                *mode = SESSION_READWRITE;
                return 0;
        }
        else{
                return 1;
        }
}

int user_get(const char *user_name, user_t *user)
{

        int ret;

        ret = md_get_user(user_name, user);
        if(ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

static int handle_login_internal(const char *username, const char *passwd,
                uint32_t *mode, char *fakedir)
{
        int ret, retry;
        user_t user_info;
        fileid_t dirid;
        share_ftp_t *share_info = NULL;
        char buf[MAX_PATH_LEN * 2] = {0};

        memset(&dirid, 0, sizeof(fileid_t));

        /* 根据用户名获取Password */
        retry = 0;
retry:
        ret = user_get(username, &user_info);
        if(ret){
                if (NEED_EAGAIN(ret)) {
                        network_connect_mond(0);
                        USLEEP_RETRY(err_ret, ret, retry, retry, 30, (1000 * 1000));
                } else {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }

        if(memcmp(user_info.password, passwd , strlen(passwd)) != 0){
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        share_info = (share_ftp_t*)buf;

        ret = md_share_get_byname(username, SHARE_USER, share_info);
        if (ret){
                GOTO(err_ret, ret);
        }

        if ((share_info->protocol & SHARE_FTP) == 0) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }

        memcpy(fakedir, share_info->path, strlen(share_info->path));

        ret = get_valid_mode(share_info, mode);
        if (ret){
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int handle_login(const char *user, const char *passwd,
                uint32_t *mode, char *fakedir)
{
        int ret;
        uint32_t len, i;
        /*
         * do not assume PAM can cope with dodgy input, even though it
         * almost certainly can
         */

        len = _strlen(user);
        if (len == 0 || len > MAX_USERNAME_LEN) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        /* throw out dodgy start characters */
        if(!isalnum(user[0])) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        /* throw out non-printable characters and space in username */
        for (i = 0; i < len; i++) {
                if (isspace(user[i])) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }

                if (!isprint(user[i])) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }

        /* throw out excessive length passwords */
        len = _strlen(passwd);
        if (len > MAX_PASSWD_LEN) {
                ret = EINVAL;

                GOTO(err_ret, ret);
        }

        ret = handle_login_internal(user, passwd, mode, fakedir);
        if (ret) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int do_login(struct yftp_session *ys, char *passwd)
{
        int ret;
        char dir[MAX_PATH_LEN];

        memset(dir, 0x0, MAX_PATH_LEN);
        ret = handle_login(ys->user, passwd, &ys->mode, dir);
        if (ret) {
                GOTO(err_ret, ret);
        }

        if (ys->fakedir)
                free(ys->fakedir);

        ys->fakedir = malloc(strlen(dir)+1);
        if(!ys->fakedir)
                return ENOMEM;

        memset(ys->fakedir, 0x0, strlen(dir) + 1);
        memcpy(ys->fakedir, dir, strlen(dir));
        memcpy(ys->pwd, dir, strlen(dir));

        return 0;
err_ret:
        return ret;
}

int process_login(struct yftp_session *ys, char *passwd)
{
        int ret ;
        char user[YFTP_MAX_CMD_LINE];

        ret = do_login(ys, passwd);
        DBUG("in process_login\n");

        /* clear password */
        _memset(passwd, 0x00, _strlen(passwd));

        if (ret) {
                snprintf(user, YFTP_MAX_CMD_LINE, "User %s cannot log in.",
                                ys->user);

                ret = cmdio_write(ys, FTP_LOGINERR, user);
                if (ret)
                        GOTO(err_ret, ret);

                return 0;
        } else {
                snprintf(user, YFTP_MAX_CMD_LINE, "User %s logged in.",
                                ys->user);

                ret = cmdio_write(ys, FTP_LOGINOK, user);
                if (ret)
                        GOTO(err_ret, ret);
        }

        (void) postlogin_process(ys);

        YASSERT(0 == "should not get here:(\n");

        return 0;
err_ret:
        return ret;
}

int handle_pass_cmd(struct yftp_session *ys)
{
        int ret;

        if (ys->user[0] == '\0') {
                ret = cmdio_write(ys, FTP_NEEDUSER, "Login with USER first.");
                if (ret)
                        GOTO(err_ret, ret);

                return 0;
        }

        ret = process_login(ys, ys->arg);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int handle_user_cmd(struct yftp_session *ys)
{
        int ret;
        char rep[YFTP_MAX_CMD_LINE];

        _memcpy(ys->user, ys->arg, _strlen(ys->arg) + 1);
        str_upper(ys->arg);

        snprintf(rep, YFTP_MAX_CMD_LINE, "Password required for %s.", ys->user);

        ret = cmdio_write(ys, FTP_GIVEPWORD, rep);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int handle_addu(struct yftp_session *ys)
{
        char *uname,*passwd;
        int ret;
        fileid_t fileid, fileidp;
        size_t size;
        char name[MAX_NAME_LEN];
        char path[MAX_PATH_LEN];

        uname = ys->arg;
        passwd = strchr(uname, ' ');
        if ( passwd != NULL ) {
                passwd[0] = '\0';
                passwd = passwd + 1;
                DBUG("passwd (%s)\n", passwd);
        } else {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        if(strlen(uname) > MAX_USERNAME_LEN || _strlen(passwd) > MAX_PASSWD_LEN)
        {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }


        //create user
        //set password
        memset(name, 0x00, MAX_NAME_LEN);
        memset(path, 0x00, MAX_PATH_LEN);
        sprintf(path, "/%s", uname);
        ret = sdfs_splitpath(path, &fileidp, name);
        if (ret) {
                DBUG("useradd ret (%d)\n", ret);
                cmdio_write(ys, FTP_USEREXIST, "user exist");
                GOTO(err_ret, ret);
        }

        ret = sdfs_create(NULL, &fileidp, name, &fileid, 0755, 0, 0);
        if (ret) {
                DBUG("useradd ret (%d)\n", ret);
                cmdio_write(ys, FTP_USEREXIST, "user exist");
                GOTO(err_ret, ret);
        }

        size = strlen(passwd) + 1;
        ret = sdfs_setxattr(NULL, &fileid, name + 1, passwd, size, USS_XATTR_CREATE);
        if (ret) {
                DBUG("useradd ret (%d)\n", ret);
                cmdio_write(ys, FTP_USEREXIST, "user exist");
                GOTO(err_ret, ret);
        } else {
                DBUG("useradd ret (%d)\n", ret);
                ret = cmdio_write(ys, FTP_USERADDOK, "user add ok");
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}


int ftp_handle(struct yftp_session *ys)
{
        int ret;

        while (srv_running) {
                ret = cmdio_get_cmd_and_arg(ys);
                if (ret)
                        GOTO(err_session, ret);

                ret = EINVAL;

                switch (ys->cmd[0]) {
                        case 'U':       /* "USER" */
                                if (memcmp(ys->cmd, "USER", 5) != 0)
                                        GOTO(err_cmd, ret);

                                ret = handle_user_cmd(ys);
                                if (ret)
                                        GOTO(err_session, ret);

                                break;
                        case 'Q':       /* "QUIT" */
                                if (memcmp(ys->cmd, "QUIT", 5) != 0)
                                        GOTO(err_cmd, ret);

                                (void) cmdio_write(ys, FTP_GOODBYE, "Goodbye.");

                                goto out;
                                break;
                        case 'F':       /* "FEAT" */
                                if (memcmp(ys->cmd, "FEAT", 5) != 0)
                                        GOTO(err_cmd, ret);

                                ret = handle_feat(ys);
                                if (ret)
                                        GOTO(err_session, ret);

                                break;
                        case 'A':       /* "AUTH" */
                                DBUG("get cmd (%s)\n",ys->cmd);
                                if (memcmp(ys->cmd, "AUTH", 5) == 0){
                                        ret = cmdio_write(ys, FTP_COMMANDNOTIMPL,
                                                        "Bingo, not supported command!");
                                        if (ret)
                                                GOTO(err_session, ret);
                                } else if (memcmp(ys->cmd, "ADDU", 5) == 0) {
                                        ret = handle_addu(ys);
                                        if (ret)
                                                GOTO(err_session, ret);
                                } else
                                        GOTO(err_cmd, ret);

                                break;
                        case 'P':       /* "PASS" "PBSZ" "PROT" */
                                if (memcmp(ys->cmd, "PASS", 5) == 0) {
                                        ret = handle_pass_cmd(ys);
                                        if (ret)
                                                GOTO(err_session, ret);
                                } else if ((memcmp(ys->cmd, "PBSZ", 5) == 0)
                                                || (memcmp(ys->cmd, "PROT", 5) == 0)) {
                                        ret = cmdio_write(ys, FTP_COMMANDNOTIMPL,
                                                        "Bingo, not supported command!");
                                        if (ret)
                                                GOTO(err_session, ret);
                                } else
                                        GOTO(err_cmd, ret);

                                break;
err_cmd:
                        default:
                                ret = cmdio_write(ys, FTP_LOGINERR,
                                                "Please login with USER and PASS.");
                                if(ret)
                                        GOTO(err_session, ret);
                }
        }

out:
        (void) session_destroy(ys);

        return 0;
err_session:
        (void) session_destroy(ys);
        return ret;
}

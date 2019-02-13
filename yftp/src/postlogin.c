

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <semaphore.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/file.h>

#define DBG_SUBSYS S_YFTP
#define LOOPBACK_ADDR 16777343

#include "cmdio.h"
#include "dataio.h"
#include "ftpcodes.h"
#include "ftpfeatures.h"
#include "session.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "yftp_conf.h"
#include "ynet_rpc.h"
#include "configure.h"
#include "dbg.h"
#include "../../ynet/sock/ynet_sock.h"
#include "../../ynet/sock/sock_tcp.h"

static int get_fd(int *pfd, char *lockfile, const char *path);
static int release_fd(int lockfd);
static int lock_file(int lockfd);
static int unlock_file(int lockfd);

#undef BIG_BUF_LEN
#define BIG_BUF_LEN (1024 * 1024)

uint32_t yftp_read(fileid_t *fileid, char *buf,  uint32_t size, uint64_t off)
{
        int ret;
        buffer_t buffer;

        mbuffer_init(&buffer, 0);

        ret = sdfs_read_sync(NULL, fileid, &buffer, size, off);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        mbuffer_get(&buffer, buf, buffer.len);

        mbuffer_free(&buffer);
        return ret;
err_ret:
        mbuffer_free(&buffer);
        return -ret;
}

uint32_t yftp_write(fileid_t *fileid, char *buf,  uint32_t size, uint64_t off)
{
        int ret;
        buffer_t buffer;

        mbuffer_init(&buffer, 0);

        ret = mbuffer_copy(&buffer, buf, size);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_write_sync(NULL, fileid, &buffer, size, off);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_free, ret);
        }

        /*fix should not get!buffer_get(&buffer, buf, obj.ret); */
        mbuffer_free(&buffer);
        return ret;
err_free:
        mbuffer_free(&buffer);
err_ret:
        return -ret;
}

static int get_fd(int *pfd, char *lockfile, const char *path)
{
        int lockfd = -1, ret = 0;
        char filelock[MAX_PATH_LEN] = {'\0'};
        char *ptr = NULL, *ptr2 = NULL;

        snprintf(filelock, MAX_PATH_LEN - 1, "%s", "/dev/shm/");

        ptr = strstr(path, "/");
        ptr++;
        ptr2 = strstr(ptr, "/");
        if (!ptr2) {
                strcat(filelock, ptr);
        } else {
                while(ptr2) {
                        _strncpy(filelock + _strlen(filelock), ptr,
                                        ptr2 - ptr);
                        strcat(filelock, "-");
                        ptr = ++ptr2;
                        ptr2 = strstr(ptr,"/");
                }
                strcat(filelock, ptr);
        }

        snprintf(lockfile, MAX_PATH_LEN - 1, "%s", filelock);

        lockfd = open(filelock, O_RDONLY | O_CREAT, 0644);
        if (lockfd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        *pfd = lockfd;
err_ret:
        return ret;
}

static int release_fd(int fd)
{
        int ret = 0;

        ret = sy_close(fd);

        return ret;
}

static int lock_file(int fdlock)
{
        int ret = 0;

        ret = flock(fdlock, LOCK_EX);
        if (ret) {
                ret = errno;
                GOTO(err_ret, ret);
        }

err_ret:
        return ret;
}

static int unlock_file(int fdlock)
{
        int ret;

        ret = flock(fdlock, LOCK_UN);
        if (ret) {
                ret = errno;
                GOTO(err_ret, ret);
        }
err_ret:
        return ret;
}

//#define YFTP_CACHED_UPLOAD
#if YFTP_ACCESS_LOG

#define ONE_DAY 86400

static int access_log_fd;
time_t savetime;
sem_t opt_lock;

int ylog_access_write(char *uname, char *opt, char *path, off_t size)
{
        int ret;
        char buf[MAX_BUF_LEN], log_file[MAX_PATH_LEN];
        char zero_clock[MAX_PATH_LEN], *p;
        time_t sec, zerotime;
        struct tm tm;

        sem_init(&opt_lock, 0, 0);
        sem_post(&opt_lock);
        ret = time(&sec);
        if (ret == -1){
                ret = errno;
                return ret;
        }
        localtime_safe(&sec, &tm);
        snprintf(zero_clock, MAX_PATH_LEN, "%d-%d-%d-00-00-00",
                        (tm.tm_year + 1900), (tm.tm_mon + 1), tm.tm_mday);
        strptime(zero_clock, "%Y-%m-%d-%H-%M-%S", &tm);
        zerotime = mktime(&tm);
        if (sec - savetime >= ONE_DAY ){
                savetime = zerotime;
                snprintf(log_file, MAX_PATH_LEN, "/var/log/yftp-%d-%d-%d",
                                (tm.tm_year + 1900), (tm.tm_mon + 1), tm.tm_mday );
                close(access_log_fd);
                access_log_fd = open(log_file, O_CREAT | O_RDWR | O_APPEND, 0644);
                if (access_log_fd == -1){
                        ret = errno;
                        GOTO(err_ret, ret);
                }
                _sem_wait(&opt_lock);
        }

        _memset(buf,0,MAX_BUF_LEN);
        /*    /user/data/  */
        path = path + (strlen(uname) + _strlen("/data/"));
        p = path;
        /*   change " "in filename or path to "_"  */
        while (*p != '\0') {
                if (*p == ' ')
                        *p = '_';
                p++;
        }
        snprintf(buf, MAX_BUF_LEN, "%lu %s %s %s %llu\n",
                        sec, uname, opt, path, size);

        ret = _write(access_log_fd, buf, _strlen(buf));

        if (ret < 0){
                ret = -ret;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;

}
#else
#endif

#if 0
int get_absolute_path(const char *pwd, const char *arg, char *path, uint32_t path_len)
{
        char str_pwd[path_len];
        char str_arg[path_len];

        if(arg)
        {
                memset(str_pwd, 0, path_len);
                memset(str_arg, 0, path_len);
                strcpy(str_pwd, pwd);
                strcpy(str_arg, arg);
                if(str_arg[0] == '.' && str_arg[1] != '.')
                {
                        //arg为./something
                        int cur_len = strlen(str_pwd);
                        if(cur_len==1 && str_pwd[0] == '/')
                        {
                                sprintf(path, "/%s", str_arg+2);
                        }
                        else if(cur_len>1)
                        {
                                sprintf(path, "%s%s", str_pwd, str_arg+1);
                        }
                }
                else if(!strncmp(str_arg, "..", 2))
                {
                        int cur_len = strlen(str_pwd);
                        if(cur_len==1 && str_pwd[0] == '/')
                        {
                                sprintf(path, "%s", "/");
                                return(-1);
                        }
                        else if(cur_len > 1)
                        {
                                char *str = strrchr(str_pwd, '/');
                                if(str == str_pwd) //说明str_pwd 为 /something
                                {
                                        sprintf(path, "/%s", str_arg+3);
                                }
                                else
                                {
                                        //说明str_pwd 为/some1/some2/...
                                        str_pwd[strlen(str_pwd)-strlen(str)] = '\0';
                                        sprintf(path, "%s%s", str_pwd, str_arg + 2);
                                }
                        }
                }
                else if(str_arg[0] == '/')
                {
                        sprintf(path, "%s", str_arg);
                }
                else if(str_arg[0] == '~' && str_arg[2] == '\0')
                {
                        sprintf(path, "%s", "/");
                }
                else if(str_arg[0] == '~' && str_arg[1] == '/' && str_arg[2] != '\0')
                {
                        sprintf(path, "/%s", str_arg+2);
                }
                else
                {
                        int cur_len = strlen(str_pwd);
                        if(cur_len==1 && str_pwd[0] == '/')
                        {
                                sprintf(path, "/%s", str_arg);
                                return(-1);
                        }
                        else
                        {
                                sprintf(path, "%s/%s", str_pwd, str_arg);
                        }
                }
                if(path[strlen(path)-1] == '/' && strlen(path) > 1)
                {
                        path[strlen(path)-1] = '\0';
                }
        }
        else
        {
                sprintf(path, "%s", pwd);
        }

        return 0;
}
#else

int get_absolute_path(struct yftp_session *ys, const char *arg, char *path, uint32_t path_len)
{
        char str_pwd[path_len];
        char str_arg[path_len];

        memset(str_pwd, 0, path_len);
        strcpy(str_pwd, ys->pwd);

        // /dir01
        if(arg){
                memset(str_arg, 0, path_len);
                strcpy(str_arg, arg);
                if(str_arg[0] == '.' && str_arg[1] != '.'){
                        //arg为./something
                        if(strcmp(ys->fakedir, str_pwd) == 0){
                                sprintf(path, "%s/%s", ys->fakedir, str_arg+2);
                        } else{
                                sprintf(path, "%s/%s", str_pwd, str_arg+2);
                        }
                } else if(!strncmp(str_arg, "..", 2)){
                        if(strcmp(ys->fakedir, str_pwd) == 0){
                                sprintf(path, "%s", ys->fakedir);
                        } else{
                                char *str = strrchr(str_pwd, '/');
                                //说明str_pwd 为/fakedir/some2/...
                                str_pwd[strlen(str_pwd)-strlen(str)] = '\0';
                                sprintf(path, "%s%s", str_pwd, str_arg + 2);
                        }
                } else if(str_arg[0] == '/'){
                        if(strncmp(ys->fakedir, str_arg, strlen(ys->fakedir)) == 0){
                                sprintf(path, "%s", str_arg);
                        } else{
                                DWARN("%s is not allowed\n", str_arg);
                                return -1;
                                //TODO 改进
                                //sprintf(path, "%s", ys->fakedir);
                        }
                } else if(str_arg[0] == '~' && str_arg[2] == '\0'){
                        sprintf(path, "%s", ys->fakedir);
                } else if(str_arg[0] == '~' && str_arg[1] == '/' && str_arg[2] != '\0'){
                        sprintf(path, "%s/%s", ys->fakedir, str_arg+2);
                } else{
                        if(strcmp(str_pwd, ys->fakedir) == 0){
                                sprintf(path, "%s/%s", ys->fakedir, str_arg);
                        } else{
                                sprintf(path, "%s/%s", str_pwd, str_arg);
                        }
                }
        } else{
                sprintf(path, "%s", str_pwd);
        }
        if(path[strlen(path)-1] == '/' && strlen(path) > 1){
                path[strlen(path)-1] = '\0';
        }


        if(arg) {
                DBUG("fakedir=%s, arg=%s, str_pwd=%s, absolute_path=%s\n",
                                ys->fakedir, arg, str_pwd, path);
        } else {
                DBUG("fakedir=%s, arg=NULL, str_pwd=%s, absolute_path=%s\n",
                                ys->fakedir, str_pwd, path);
        }

        return 0;
}

#endif

int user_pwd_dir_2path(struct yftp_session *ys, const char *pwd,
                const char *dir, char *path, uint32_t pathlen)
{
        char *sla;
        char prefix[MAX_PATH_LEN];


        YASSERT(pwd != NULL);

        DBUG("user_pwd_dir_2path pwd is %s, arg is %s\n",      \
                        pwd, dir)

                _memset(path, 0x0, pathlen);
        /*bug prefix should be volid????think?*/
        if (ys->fakedir)
                snprintf(prefix, MAX_PATH_LEN, "%s", ys->fakedir);
        else
                YASSERT(0);
        if (dir) {
                while ((sla = strstr(dir, "~/")) != NULL)
                        dir = sla + 1;

                if (dir[0] == '/')
                        snprintf(path, pathlen, "/%s%s", prefix, dir);
                else {
                        if (pwd[1] == '\0')
                                snprintf(path, pathlen, "/%s/%s", prefix, dir);
                        else
                                snprintf(path, pathlen, "/%s%s/%s", prefix, pwd,
                                                dir);
                }
        } else {
                if (pwd[1] == '\0')
                        snprintf(path, pathlen, "/%s", prefix);
                else
                        snprintf(path, pathlen, "/%s%s", prefix, pwd);
        }

        DBUG("user (%s), pwd (%s), dir (%s), path (%s) %u\n", ys->user, pwd, dir,
                        path, pathlen);

        return 0;
}

int data_transfer_checks(struct yftp_session *ys)
{
        int ret;

        if (!session_pasvactive(ys)) {
                ret = cmdio_write(ys, FTP_BADSENDCONN,
                                "Use PORT or PASV first.");
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int get_remote_transfer_fd(struct yftp_session *ys, const char *dataconn,
                int *fd)
{
        int ret;

        ret = dataio_get_pasv_fd(ys, fd);
        if (ret)
                GOTO(err_ret, ret);

        ret = cmdio_write(ys, FTP_DATACONN, dataconn);
        if (ret)
                GOTO(err_rfd, ret);

        return 0;
err_rfd:
        (void) sy_close(*fd);
err_ret:
        return ret;
}

int handle_abort(struct yftp_session *ys)
{
        int ret;

        ret = cmdio_write(ys, FTP_ABOR_NOCONN, "No transfer to ABOR.");
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int handle_stat(struct yftp_session *ys)
{
        int ret;

        ret = cmdio_write(ys, FTP_STATOK, "FTP server status: "
                        SERVER_SOFTWARE SERVER_URL);
        if (ret)
                GOTO(err_ret, ret);

        ret = cmdio_write(ys, FTP_STATOK, "End of status");
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int handle_dir_common(struct yftp_session *ys, int full_details, int stat_cmd)
{
        int ret, rfd, havedir = 0;
        char *first_index, *opt, opts[YFTP_MAX_CMD_LINE], *filter, path[MAX_PATH_LEN], *c;
        fileid_t fileid;

        if (!stat_cmd && data_transfer_checks(ys))
                goto out;

        /* option ? "ls -a ..." is fine */
        opt = NULL;
        filter = NULL;
        if (ys->arg != NULL) {
                first_index = ys->arg;
                opt = strchr(ys->arg, '-');
                if (opt != NULL && opt == first_index) {
                        opt += 1;

                        if (stat_cmd)
                                snprintf(opts, YFTP_MAX_CMD_LINE, "%sa", opt);
                        else
                                snprintf(opts, YFTP_MAX_CMD_LINE, "%s", opt);

                        opt = opts;

                        /* a space will separate options from filter (if any) */
                        filter = strchr(opt, ' ');
                        if (filter != NULL) {
                                filter[0] = '\0';
                                filter += 1;
                        }
                } else {
                        snprintf(opts, YFTP_MAX_CMD_LINE, "%s", ys->arg);
                        filter = opts;
                }
        }

        if (filter != NULL) {
                if(get_absolute_path(ys, filter, path, MAX_PATH_LEN)) {

                        ret = cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                        if (ret)
                                GOTO(err_pasv, ret);

                        goto out;
                }

                /* first check - is it an outright directory, as in "ls /pub" */
                ret = sdfs_lookup_recurive(path, &fileid);
                if (ret == 0) {
                        /* listing a directory */
                        filter = NULL;
                        havedir = 1;
                } else {
                        c = strrchr(filter, '/');
                        if (c) {
                                filter = c;
                                filter += 1;
                        }

                        c = strrchr(path, '/');
                        if (c == path) {
                                path[0] = '/';
                                path[1] = '\0';
                        } else
                                c[0] = '\0';
                        ret = sdfs_lookup_recurive(path, &fileid);
                        if (ret == 0)
                                havedir = 1;
                }
        } else {
                if(get_absolute_path(ys, NULL, path, MAX_PATH_LEN)) {

                        ret = cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                        if (ret)
                                GOTO(err_pasv, ret);

                        goto out;
                }

                havedir = 1;
        }

        if (stat_cmd) {
                rfd = ys->ctrl_fd;

                ret = cmdio_write(ys, FTP_STATFILE_OK, "Status follows:");
                if (ret)
                        GOTO(err_pasv, ret);
        } else {
                ret = get_remote_transfer_fd(ys,
                                "Here comes the directory listing.",
                                &rfd);
                if (ret)
                        GOTO(err_pasv, ret);
        }

        if (havedir) {
                ret = dataio_transfer_dir(ys, rfd, path, opt, filter,
                                full_details);
                if (ret)
                        GOTO(err_rfd, ret);
        }

        if (!stat_cmd) {
                (void) sy_close(rfd);
                (void) session_clearpasv(ys);
        }

        if (stat_cmd) {
                ret = cmdio_write(ys, FTP_STATFILE_OK, "End of status");
                if (ret)
                        GOTO(err_ret, ret);
        } else if (havedir == 0) {
                ret = cmdio_write(ys, FTP_TRANSFEROK,
                                "Transfer done (but failed to open directory).");
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                ret = cmdio_write(ys, FTP_TRANSFEROK, "Directory send OK.");
                if (ret)
                        GOTO(err_ret, ret);
        }

out:
        return 0;
err_rfd:
        if (!stat_cmd)
                (void) sy_close(rfd);
err_pasv:
        if (!stat_cmd)
                (void) session_clearpasv(ys);
err_ret:
        return ret;
}

int handle_list(struct yftp_session *ys)
{
        return handle_dir_common(ys, 1, 0);
}

int handle_nlst(struct yftp_session *ys)
{
        return handle_dir_common(ys, 0, 0);
}

int handle_stat_file(struct yftp_session *ys)
{
        return handle_dir_common(ys, 1, 1);
}

#ifdef YFTP_CACHED_UPLOAD

int handle_upload_common(struct yftp_session *ys, int is_append, int is_unique)
{
        int ret, tmpfd, rfd, yfd, nfds, epoll_fd;
        uint64_t offset, flen;
        char tmp_file[] = "/tmp/tmp_XXXXXX", path[MAX_PATH_LEN];
        char buf[MAX_BUF_LEN];
        uint32_t buflen;
        size_t count;
        struct epoll_event ev, events;

        offset = ys->offset;
        ys->offset = 0;

        if (data_transfer_checks(ys))
                goto out;

        if (is_append) {
                ret = cmdio_write(ys, FTP_COMMANDNOTIMPL,
                                "APPE not implemented.");
                if (ret)
                        GOTO(err_pasv, ret);

                goto out_pasv;
        }

        if (is_unique) {
                ret = cmdio_write(ys, FTP_COMMANDNOTIMPL,
                                "STOU not implemented.");
                if (ret)
                        GOTO(err_pasv, ret);

                goto out_pasv;
        }

        /*if (ys->is_ascii) {
          ret = cmdio_write(ys, FTP_FILEFAIL,
          "No support for ASCII transfer.");
          if (ret)
          GOTO(err_pasv, ret);

          goto out_pasv;
          }*/

        tmpfd = mkstemp(tmp_file);
        if (tmpfd == -1) {
                ret = errno;
                DERROR("mkstemp(%s) %s\n", tmp_file, strerror(ret));

                ret = cmdio_write(ys, FTP_UPLOADFAIL, "Could not create file.");
                if (ret)
                        GOTO(err_pasv, ret);

                goto out_pasv;
        }

        ret = unlink(tmp_file);
        if (ret == -1) {
                ret = errno;
                DERROR("unlink(%s) %s\n", tmp_file, strerror(ret));

                ret = cmdio_write(ys, FTP_UPLOADFAIL, "Could not create file.");
                if (ret)
                        GOTO(err_tmpfd, ret);

                goto out_tmpfd;
        }

        ret = get_remote_transfer_fd(ys, "OK to send data.", &rfd);
        if (ret)
                GOTO(err_tmpfd, ret);

        epoll_fd = epoll_create(1);
        if (epoll_fd == -1) {
                ret = errno;
                GOTO(err_tmpfd, ret);
        }

        ev.events = Y_EPOLL_EVENTS;
        ev.data.fd = rfd;

        ret = _epoll_ctl(epoll_fd, EPOLL_CTL_ADD, rfd, &ev);
        if (ret == -1) {
                ret = errno;
                GOTO(err_epoll, ret);
        }

        flen = 0;
        while (srv_running) {
                buflen = MAX_BUF_LEN;

                nfds = _epoll_wait(epoll_fd, &events, 1,
                                gloconf.rpc_timeout * 1000);
                if (nfds == -1) {
                        ret = errno;
                        GOTO(err_epoll, ret);
                } else if (nfds == 0) {
                        ret = ETIME;
                        GOTO(err_epoll, ret);
                }

                ret = _recv(rfd, buf, buflen, 0);
                if (ret < 0) {
                        ret = -ret;
                        DERROR(" (%d) %s\n", ret, strerror(ret));
                } else if (ret == 0)
                        break;
                else {
                        buflen = (uint32_t)ret;

                        ret = _write(tmpfd, buf, buflen);
                        if (ret < 0) {
                                ret = -ret;
                                DERROR("_write() (%d) %s\n", ret,
                                                strerror(ret));
                        } else
                                flen += count;
                }

                if (ret) {
                        ret = cmdio_write(ys, FTP_UPLOADFAIL,
                                        "Could not create file.");
                        if (ret)
                                GOTO(err_rfd, ret);

                        (void) sy_close(rfd);

                        goto out_epoll;
                }
        }

        (void) sy_close(rfd);
        (void) session_clearpasv(ys);

        if(get_absolute_path(ys, ys->arg, path, MAX_PATH_LEN)) {

                ret = cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                if (ret)
                        GOTO(err_rfd, ret);

                goto out;
        }

        ret = ly_depositfile(tmpfd, yfd, offset, flen);
        if (ret) {
                DERROR("deposit(%s) %s\n", path, strerror(ret));

                ret = cmdio_write(ys, FTP_UPLOADFAIL, "Could not create file.");
                if (ret)
                        GOTO(err_tmpfd, ret);

                (void) ly_release(yfd);

                goto out_epoll;
        }

        (void) sy_close(tmpfd);
        ret = cmdio_write(ys, FTP_TRANSFEROK, "Transfer complete\r\n");
        if (ret)
                GOTO(err_ret, ret);

        return 0;

out_epoll:
        (void) sy_close(epoll_fd);
out_tmpfd:
        (void) sy_close(tmpfd);
out_pasv:
        (void) session_clearpasv(ys);
out:
        return 0;
err_rfd:
        (void) sy_close(rfd);
err_epoll:
        (void) sy_close(epoll_fd);
err_tmpfd:
        (void) sy_close(tmpfd);
err_pasv:
        (void) session_clearpasv(ys);
err_ret:
        return ret;
}

#else

int handle_upload_common(struct yftp_session *ys, int is_append, int is_unique)
{
        int ret, rfd, yfd, nfds, epoll_fd, lockfd;
        char path[MAX_PATH_LEN], buf[BIG_BUF_LEN];
        char filename[MAX_PATH_LEN];
        uint32_t buflen;
        yfs_off_t offset;
        struct epoll_event ev, events;

        fileid_t fileidp, fileid;
        char name[MAX_NAME_LEN];
        struct stat stbuf;
        int is_directory;

        (void)yfd;

        offset = ys->offset;
        ys->offset = 0;

        if (data_transfer_checks(ys))
                goto out;


        if (is_unique) {
                ret = cmdio_write(ys, FTP_COMMANDNOTIMPL,
                                "STOU not implemented.");
                if (ret)
                        GOTO(err_pasv, ret);

                goto out_pasv;
        }

        /*if (ys->is_ascii) {
          ret = cmdio_write(ys, FTP_FILEFAIL,
          "No support for ASCII transfer.");
          if (ret)
          GOTO(err_pasv, ret);

          goto out_pasv;
          }*/

        memset(path, 0x00, MAX_PATH_LEN);
        memset(buf, 0x00, BIG_BUF_LEN);
        memset(name, 0x00, MAX_NAME_LEN);
        ret = get_remote_transfer_fd(ys, "OK to send data.", &rfd);
        if (ret)
                GOTO(err_pasv, ret);

        if(get_absolute_path(ys, ys->arg, path, MAX_PATH_LEN)) {

                ret = cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                if (ret)
                        GOTO(err_pasv, ret);

                goto out;
        }

        /* retr fileid of path */
        ret = sdfs_lookup_recurive(path, &fileid);
        if(ret) {
                if(ret == ENOENT) {
                    // if other error occurs
                    goto not_exist;
                } else {
                        GOTO(err_pasv, ret);
                }
        }

        /* check if fileid is a directory */
        ret = raw_is_dir(&fileid, &is_directory);
        if(ret) {
                if(ret == ENOENT) {
                    goto not_exist;
                } else {
                        GOTO(err_pasv, ret);
                        // attr not exist
                }
        }

        if(is_directory) {
                ret = cmdio_write(ys, FTP_UPLOADFAIL, "Could not create file.");
                if (ret)
                        GOTO(err_rfd, ret);

                goto out;
        }

not_exist:
        if (is_append) {
                offset = 0;
                DBUG("Path is %s\n", path);
                ret = sdfs_lookup_recurive(path, &fileid);
                if (ret)
                        goto middle_out;
                ret = sdfs_getattr(NULL, &fileid, &stbuf);
                if (ret)
                        goto middle_out;

                offset = stbuf.st_size;
                DBUG("is_append path is %s \
                                fileid "FID_FORMAT"\n offset %llu\n",
                                path, FID_ARG(&fileid), (LLU)offset);
                goto middle_out1;
middle_out:
                ret = cmdio_write(ys, FTP_UPLOADFAIL, "failed unknow error\n");
                if (ret)
                        GOTO(err_pasv, ret);
                goto out_pasv;
        } else {
                if (offset != 0) {
                        ret = sdfs_lookup_recurive(path, &fileid);
                        if (ret)
                                GOTO(err_rfd, ret);

                        DINFO("resume %s:["FID_FORMAT"] form %llu\n",
                                        path, FID_ARG(&fileid), (LLU)offset);

                } else {
                        ret = sdfs_splitpath(path, &fileidp, name);
                        DBUG("path is %s:["FID_FORMAT"], splitpath is %s\n",
                                        path, FID_ARG(&fileidp), name);
                        if (ret) {
                                DWARN("handle_upload_common :raw_splitpath: %s \n",\
                                                path);
                                ret = cmdio_write(ys, FTP_UPLOADFAIL, "Could not create file.");
                                if (ret)
                                        GOTO(err_rfd, ret);
                                goto out_rfd;
                        }

retry:
                        ret = sdfs_create(NULL, &fileidp, name, &fileid, 0644, 0, 0);
                        if (ret) {
                                if (ret == EEXIST) {
                                        ret = sdfs_unlink(NULL, &fileidp, name);
                                        if (ret)
                                                GOTO(err_rfd, ret);

                                        goto retry;
                                } else {
                                        DWARN("create(%s) %s\n", name, strerror(ret));
                                        ret = cmdio_write(ys, FTP_UPLOADFAIL, "Could not create file.");
                                        if (ret)
                                                GOTO(err_rfd, ret);
                                        goto out_rfd;
                                }
                        }
                }
        }
middle_out1:
        DBUG("%s created\n", path);
        epoll_fd = epoll_create(1);
        if (epoll_fd == -1) {
                ret = errno;
                GOTO(err_rfd, ret);
        }

        _memset(&ev, 0x0, sizeof(struct epoll_event));
        ev.events = Y_EPOLL_EVENTS & ~(EPOLLET);
        ev.data.fd = rfd;

        ret = _epoll_ctl(epoll_fd, EPOLL_CTL_ADD, rfd, &ev);
        if (ret == -1) {
                ret = errno;
                GOTO(err_epoll, ret);
        }

        ret = get_fd(&lockfd, filename, path);
        if (ret) {
                DERROR("get_fd failed\n");
                GOTO(err_epoll, ret);
        }

        ret = lock_file(lockfd);
        if (ret) {
                DERROR("lock_file failed\n");
                GOTO(err_epoll, ret);
        }

        int received = 0;
        while (srv_running) {
                nfds = _epoll_wait(epoll_fd, &events, 1,
                                gloconf.rpc_timeout * 1000);
                if (nfds == -1) {
                        ret = errno;
                        GOTO(err_epoll, ret);
                } else if (nfds == 0) {
                        ret = ETIME;
                        GOTO(err_epoll, ret);
                }

                buflen = BIG_BUF_LEN;

                ret = _recv(rfd, buf+received, buflen - received, 0);
                if (ret < 0) {
                        ret = -ret;
                        DERROR("recv %u offset %d (%d) %s\n",
                               received, offset, ret, strerror(ret));
                        if (received) {
                                ret = yftp_write(&fileid, buf, received, offset);
                                if (ret < 0) {
                                        ret = -ret;

                                        DERROR("ly_pwrite() (%d) %s\n", ret,
                                                        strerror(ret));
                                        GOTO(err_epoll, ret);
                                } else {
                                        offset += received;
                                        ret = 0;
                                        received = 0;
                                        break;
                                }
                        } else {
                                break;
                        }
                } else if (ret == 0) {
                        DINFO("recv %u offset %d\n", received, offset);
                        
                        if (received) {
                                ret = yftp_write(&fileid, buf, received, offset);
                                if (ret < 0) {
                                        ret = -ret;

                                        DERROR("ly_pwrite() (%d) %s\n", ret,
                                                        strerror(ret));
                                } else {
                                        offset += received;
                                        ret = 0;
                                }
                        }

                        break;
                } else {
                        received += (uint32_t)ret;
                        DINFO("yfs_write fileid "FID_FORMAT"\n", FID_ARG(&fileid));
                        if (received == BIG_BUF_LEN) {
                                ret = yftp_write(&fileid, buf, received, offset);
                                if (ret < 0) {
                                        ret = -ret;

                                        DERROR("ly_pwrite() (%d) %s\n", ret,
                                                        strerror(ret));
                                } else {
                                        offset += received;
                                        ret = 0;
                                        received = 0;
                                }
                        } else {
                                ret = 0;
                        }
                }

                if (ret) {
                        ret = cmdio_write(ys, FTP_UPLOADFAIL,
                                        "Could not create file.");
                        if (ret)
                                GOTO(err_epoll, ret);

                        goto out_epoll;
                }
        }

        DINFO("upload: totol %llu\n", (unsigned long long)offset);
        ret = cmdio_write(ys, FTP_TRANSFEROK, "Transfer complete\r\n");
        if (ret)
                GOTO(err_epoll, ret);
#if	YFTP_ACCESS_LOG
        ret = ylog_access_write(ys->user, ys->cmd, path, offset);
        if (ret)
                GOTO(err_epoll, ret);
#else
#endif

out_epoll:
        (void)unlink(filename);
        (void)unlock_file(lockfd);
        (void)release_fd(lockfd);
        (void) sy_close(epoll_fd);
out_rfd:
        (void) sy_close(rfd);
out_pasv:
        (void) session_clearpasv(ys);
out:
        return 0;

err_epoll:
        (void)unlink(filename);
        (void)unlock_file(lockfd);
        (void)release_fd(lockfd);
        (void) sy_close(epoll_fd);
err_rfd:
        (void) sy_close(rfd);
err_pasv:
        (void) session_clearpasv(ys);
        return ret;
}

#endif

int handle_mdtm(struct yftp_session *ys)
{
        int ret;

        /* fake it XXX */
        ret = cmdio_write(ys, FTP_MDTMOK, "File modification time set.");
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int handle_appe(struct yftp_session *ys)
{
        return handle_upload_common(ys, 1, 0);
}

int handle_stou(struct yftp_session *ys)
{
        return handle_upload_common(ys, 0, 1);
}

int handle_stor(struct yftp_session *ys)
{
        if ( ys->mode == SESSION_READONLY) {
                cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                goto err_ret;
        }
        return handle_upload_common(ys, 0, 0);
err_ret:
        return EACCES;
}

int handle_opts(struct yftp_session *ys)
{
        int ret;

        str_upper(ys->arg);

        if (!memcmp(ys->arg, "UTF8 ON", 7)) {
                ret = cmdio_write(ys, FTP_OPTSOK, "Always in UTF8 mode.");
                if (ret)
                        GOTO(err_ret, ret);
        }
        else {
                ret = cmdio_write(ys, FTP_BADOPTS, "Option not understood.");
                if (ret)
                        GOTO(err_ret, ret);
        }
        return ret;
err_ret:
        return ret;
}

#define BIG_BUFFER 524200

int handle_retr(struct yftp_session *ys)
{
        int ret, yfd, rfd;
        struct stat stbuf;
        char dataconn[YFTP_MAX_CMD_LINE], path[MAX_PATH_LEN];
        yfs_off_t offset;
        yfs_size_t size, toread;
        char buf[BIG_BUFFER];
        fileid_t fileid;

        (void)yfd;

        offset = ys->offset;
        ys->offset = 0;

        if (data_transfer_checks(ys))
                goto out;

        if (ys->arg == NULL || ys->arg[0] == '\0') {
                ret = cmdio_write(ys, FTP_BADCMD, "bad RETR command.");
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        }

        if (ys->is_ascii && offset != 0) {
                ret = cmdio_write(ys, FTP_FILEFAIL,
                                "No support for resume of ASCII transfer.");
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        }

        if(get_absolute_path(ys, ys->arg, path, MAX_PATH_LEN)) {

                ret = cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        }

        ret = sdfs_lookup_recurive(path, &fileid);
        DBUG("path is %s, fileid is "FID_FORMAT" \n", \
                        path, FID_ARG(&fileid));
        if (ret) {
                DERROR("handle_retr open (%s ...) %s\n", path, strerror(ret));
                ret = cmdio_write(ys, FTP_FILEFAIL, "Failed to open file.");
                if (ret)
                        GOTO(err_ret, ret);
                goto out_yfd;
        }

        ret = sdfs_getattr(NULL, &fileid, &stbuf);
        if (ret)
                GOTO(err_ret, ret);


        if (!(S_ISREG(stbuf.st_mode))) {
                DERROR("not regular file\n");

                ret = cmdio_write(ys, FTP_FILEFAIL, "Not regular file.");
                if (ret)
                        GOTO(err_yfd, ret);

                goto out_yfd;
        }

        snprintf(dataconn, YFTP_MAX_CMD_LINE,
                        "Opening %s mode data connection for %s (%llu bytes).",
                        ys->is_ascii ? "ASCII" : "BINARY", path,
                        (unsigned long long)stbuf.st_size);

        ret = get_remote_transfer_fd(ys, dataconn, &rfd);
        if (ret)
                GOTO(err_pasv, ret);

        size = stbuf.st_size - offset;

        while (size) {
                toread = size < (yfs_off_t)BIG_BUFFER ? size
                        : (yfs_off_t)BIG_BUFFER;

                ret = yftp_read(&fileid, buf, toread, offset);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_pasv, ret);
                } else if ((yfs_off_t)ret != toread) {
                        ret = EBADF;
                        GOTO(err_pasv, ret);
                }

                ret = _write(rfd, buf, toread);
                if (ret < 0) {
                        ret = -ret;
                        if (ret == EPIPE)
                                goto err_pasv;
                        else
                                GOTO(err_pasv, ret);
                }

                size -= toread;
                offset += toread;
        }

        (void) sy_close(rfd);

        if (ret) {
                ret = cmdio_write(ys, FTP_BADSENDNET,
                                "Failure reading local file." " or "
                                "Failure writing network stream.");
                if (ret)
                        GOTO(err_pasv, ret);
        } else {
                ret = cmdio_write(ys, FTP_TRANSFEROK, "File send OK.");
                if (ret)
                        GOTO(err_pasv, ret);

#if	YFTP_ACCESS_LOG
                ret = ylog_access_write(ys->user, ys->cmd, path, size);
                if (ret)
                        GOTO(err_pasv, ret);
#else
#endif
        }

        (void) session_clearpasv(ys);

out_yfd:
out:
        return 0;
err_pasv:
        (void) session_clearpasv(ys);
err_yfd:
err_ret:
        cmdio_write(ys, FTP_FILEFAIL, "Failed to open file.");
        return ret;
}

int handle_size(struct yftp_session *ys)
{
        int ret, lockfd;
        struct stat stbuf;
        char size_ok[YFTP_MAX_CMD_LINE], path[MAX_PATH_LEN];
        char filename[MAX_PATH_LEN] = {'\0'};
        fileid_t fileid;

        if (ys->arg == NULL || ys->arg[0] == '\0') {
                ret = cmdio_write(ys, FTP_BADCMD, "bad SIZE command.");
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        }

        if(get_absolute_path(ys, ys->arg, path, MAX_PATH_LEN)) {

                ret = cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        }

        ret = get_fd(&lockfd, filename, path);
        if (ret) {
                DERROR("get_fd failed\n");
                GOTO(err_ret, ret);
        }

        ret = lock_file(lockfd);
        if (ret) {
                DERROR("lock_file failed\n");
                GOTO(err_ret, ret);
        }

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret) {
                DBUG("open (%s ...) %s\n", path, strerror(ret));
                ret = cmdio_write(ys, FTP_FILEFAIL, "Failed to open file.");
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        }

        ret = sdfs_getattr(NULL, &fileid, &stbuf);
        if (ret) {
                DBUG("open (%s ...) %s\n", path, strerror(ret));
                ret = cmdio_write(ys, FTP_FILEFAIL, "Failed to open file.");
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        }

        if (!S_ISREG(stbuf.st_mode)) {
                ret = cmdio_write(ys, FTP_FILEFAIL, "Could not get file size.");
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        }

        snprintf(size_ok, YFTP_MAX_CMD_LINE, "%llu",
                        (unsigned long long)stbuf.st_size);

        ret = cmdio_write(ys, FTP_SIZEOK, size_ok);
        if (ret)
                GOTO(err_ret, ret);


out:
        (void)unlink(filename);
        (void)unlock_file(lockfd);
        (void)release_fd(lockfd);
        return 0;
err_ret:
        (void)unlink(filename);
        (void)unlock_file(lockfd);
        (void)release_fd(lockfd);
        return ret;
}

int handle_site_chmod(struct yftp_session *ys, char *chmod_args)
{
        int ret;
        char *arg, *mode_str, *path_str;
        mode_t mode;
        fileid_t fileid;
        char path[MAX_PATH_LEN];

        if (chmod_args == NULL || chmod_args[0] == '\0') {
                ret = cmdio_write(ys, FTP_BADCMD,
                                "SITE CHMOD needs 2 arguments.");
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        }

        mode_str = chmod_args;

        arg = strchr(chmod_args, ' ');
        if (arg != NULL) {
                path_str = arg + 1;
                arg[0] = '\0';
        } else
                path_str = NULL;

        if (path_str == NULL || path_str[0] == '\0') {
                ret = cmdio_write(ys, FTP_BADCMD,
                                "SITE CHMOD needs 2 arguments.");
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        }

        mode = str_octal_to_uint(mode_str);

        /*
           (void) user_pwd_dir_2path(ys, ys->pwd, path_str, path,
           MAX_PATH_LEN);
           */
        if(get_absolute_path(ys, path_str, path, MAX_PATH_LEN)) {

                ret = cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        }

        ret = sdfs_lookup_recurive(path, &fileid);
        DBUG("handle_site_chmod path is %s, fileid is "FID_FORMAT" \n",
                        path, FID_ARG(&fileid));
        if (ret) {
                DERROR("open (%s ...) %s\n", path, strerror(ret));
                ret = cmdio_write(ys, FTP_FILEFAIL, "Failed to open file.");
                if (ret)
                        GOTO(err_ret, ret);
                goto out;
        }

        ret = sdfs_chmod(NULL, &fileid, mode);
        if (ret) {
                DERROR("chmod(%s, ...) %s\n", path, strerror(ret));

                ret = cmdio_write(ys, FTP_FILEFAIL,
                                "SITE CHMOD command failed.");
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        }

        ret = cmdio_write(ys, FTP_CHMODOK, "SITE CHMOD command ok.");
        if (ret)
                GOTO(err_ret, ret);

out:
        return 0;
err_ret:
        return ret;
}

int handle_site(struct yftp_session *ys)
{
        int ret;
        char *arg, *site_args;

        /* what SITE sub-command is it ? */
        arg = strchr(ys->arg, ' ');
        if (arg != NULL) {
                site_args = arg + 1;
                arg[0] = '\0';
        } else
                site_args = NULL;

        DBUG("SITE command (%s), SITE args (%s)\n", ys->arg, site_args);

        if (memcmp(ys->arg, "CHMOD", 6) == 0)
                ret = handle_site_chmod(ys, site_args);
        else if (memcmp(ys->arg, "HELP", 5) == 0) {
                ret = cmdio_write(ys, FTP_SITEHELP, "CHMOD HELP");
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                ret = cmdio_write(ys, FTP_BADCMD, "Unknown SITE command.");
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int handle_rest(struct yftp_session *ys)
{
        int ret;
        char rest_ok[YFTP_MAX_CMD_LINE];

        ys->offset = (uint64_t) strtoull(ys->arg, NULL, 10);

        snprintf(rest_ok, YFTP_MAX_CMD_LINE,
                        "Restart position accepted (%llu).",
                        (unsigned long long)ys->offset);

        ret = cmdio_write(ys, FTP_RESTOK, rest_ok);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int handle_rnfr(struct yftp_session *ys)
{
        int ret;
        char path[MAX_PATH_LEN];
        struct stat stbuf;
        fileid_t fileid;

        ys->rnfr_filename[0] = '\0';

        if(get_absolute_path(ys, ys->arg, path, MAX_PATH_LEN)) {
                ret = cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                if (ret)
                        GOTO(err_ret, ret);

                return 0;
        }

        ret = sdfs_lookup_recurive(path, &fileid);
        DBUG("handle_rnfr path is %s, fileid is "FID_FORMAT" \n", \
                        path, FID_ARG(&fileid));
        if (ret) {
                DERROR("open (%s ...) %s\n", path, strerror(ret));
                ret = cmdio_write(ys, FTP_FILEFAIL, "Failed to open file.");
                if (ret)
                        GOTO(err_ret, ret);
                goto err_ret;
        }

        ret = sdfs_getattr(NULL, &fileid, &stbuf);

        if (ret == 0) {
                snprintf(ys->rnfr_filename, MAX_PATH_LEN, "%s", path);

                ret = cmdio_write(ys, FTP_RNFROK, "Ready for RNTO.");
        } else
                ret = cmdio_write(ys, FTP_FILEFAIL, "RNFR command failed.");

        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int handle_rnto(struct yftp_session *ys)
{
        int ret = 0;
        char path[MAX_PATH_LEN];
        char namef[MAX_NAME_LEN];
        char namet[MAX_NAME_LEN];
        fileid_t fileidf, fileidt;

        if (ys->mode == SESSION_READONLY) {
                cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                goto err_ret;
        }

        if (ys->rnfr_filename[0] == '\0') {
                ret = cmdio_write(ys, FTP_NEEDRNFR, "RNFR required first.");
                if (ret)
                        GOTO(err_ret, ret);
        }

        if(get_absolute_path(ys, ys->arg, path, MAX_PATH_LEN)) {
                ret = cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                if (ret)
                        GOTO(err_ret, ret);

                return 0;
        }

        memset(namef, 0x00, MAX_NAME_LEN);
        memset(namet, 0x00, MAX_NAME_LEN);
        ret = sdfs_splitpath(ys->rnfr_filename, &fileidf, namef);
        DINFO("handle_rnto path is %s, fileid is "FID_FORMAT" \n", path, FID_ARG(&fileidf));
        if (ret) {
                ret = cmdio_write(ys, FTP_FILEFAIL, "Rename failed.");
                GOTO(err_ret, ret);
        }

        ret = sdfs_splitpath(path, &fileidt, namet);
        if (ret) {
                ret = cmdio_write(ys, FTP_FILEFAIL, "Rename failed.");
                GOTO(err_ret, ret);
        }

        ret = sdfs_rename(NULL, &fileidf, namef, &fileidt, namet);
        if (ret)
                ret = cmdio_write(ys, FTP_FILEFAIL, "Rename failed.");
        ys->rnfr_filename[0] = '\0';

        if (ret == 0)
                ret = cmdio_write(ys, FTP_RENAMEOK, "Rename successful.");
        else
                ret = cmdio_write(ys, FTP_FILEFAIL, "Rename failed.");

        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

#if 0
int select_nic(unsigned long *accessable_addr, ynet_net_info_t *netinfo,
                unsigned long local_addr)
{
        int i, n, num = 0;
        struct sockaddr_in tmp;

        n = netinfo->info_count;
        for (i = 0; i < n; i++) {
                tmp.sin_addr.s_addr = netinfo->corenet[i].addr;
                if (local_addr == (unsigned long) inet_netof(tmp.sin_addr)) {
                        accessable_addr[num] = tmp.sin_addr.s_addr;
                        num += 1;
                }
        }
        return num;
}
#endif

static int get_coninfo(char *buf, uint32_t *bufsize, int connfd, uint32_t port)
{
        int ret;
        struct sockaddr_in saddr;
        int size;
        uint32_t addr;
        char aaddr[128];
        char *paddr;

        memset(&saddr, 0, sizeof(struct sockaddr_in));
        size = sizeof(struct sockaddr_in);

        ret = getsockname(connfd, (struct sockaddr *) &saddr, (socklen_t *)&size);
        if (ret) {
                GOTO(err_ret, ret);
        }

        paddr = inet_ntoa(saddr.sin_addr);
        memcpy(aaddr, paddr, strlen(paddr));
        addr = ntohl(saddr.sin_addr.s_addr);
        memcpy(buf, &addr, sizeof(uint32_t));
        memcpy((char *) buf + sizeof(uint32_t), &port, sizeof(uint32_t));

        *bufsize = sizeof(uint32_t) * 2;

        DBUG("listen address: %s\n", paddr);

        return 0;
err_ret:
        return ret;
}

int handle_pasv(struct yftp_session *ys, int is_epsv)
{
        int ret, pasv_sd;
        uint32_t listen_addr, port;
        struct in_addr sin;
        char addr[16];
        char pasv_ok[YFTP_MAX_CMD_LINE];
        char tmp[60];

        (void) session_clearpasv(ys);

        ynet_sock_info_t info[100];
        uint32_t info_count;
        info_count = 0;

        ret = rpc_portlisten(&pasv_sd, 0, &port, YFTP_QLEN_DEF, YNET_RPC_BLOCK);
        if (ret)
                GOTO(err_ret, ret);

        ret = get_coninfo((void *)info, &info_count, ys->ctrl_fd, port);
        if (ret) {
                GOTO(err_sd, ret);
        }

        listen_addr = info[0].addr;
        sin.s_addr = htonl(listen_addr);
        DBUG(" ip is %s\n", inet_ntoa(sin));
        memset(tmp, 0x00, 60);
        sprintf(tmp, "%s", inet_ntoa(sin));

        DBUG("port %u\n", port);

        ys->pasv_sd = pasv_sd;

        if (is_epsv) {
                snprintf(pasv_ok, YFTP_MAX_CMD_LINE,
                                "Entering Extended Passive Mode (|||%u|)", port);

                ret = cmdio_write(ys, FTP_EPSVOK, pasv_ok);
                if (ret)
                        GOTO(err_sd, ret);
        } else {
                snprintf(addr, 16, "%s", tmp);

                str_replace_char(addr, '.', ',');

                snprintf(pasv_ok, YFTP_MAX_CMD_LINE,
                                "Entering Passive Mode (%s,%u,%u)", addr, port >> 8,
                                port & 255);

                ret = cmdio_write(ys, FTP_PASVOK, pasv_ok);
                if (ret)
                        GOTO(err_sd, ret);
        }

        return 0;
err_sd:
        (void) session_clearpasv(ys);
err_ret:
        return ret;
}

int handle_type(struct yftp_session *ys)
{
        int ret;

        str_upper(ys->arg);

        if (memcmp(ys->arg, "I", 2) == 0
                        || memcmp(ys->arg, "L8", 3) == 0
                        || memcmp(ys->arg, "L 8", 4) == 0) {
                ys->is_ascii = 0;

                ret = cmdio_write(ys, FTP_TYPEOK, "Switching to Binary mode.");
                if (ret)
                        GOTO(err_ret, ret);
        } else if (memcmp(ys->arg, "A", 2) == 0
                        || memcmp(ys->arg, "A N", 4) == 0) {
                ys->is_ascii = 1;

                ret = cmdio_write(ys, FTP_TYPEOK, "Switching to ASCII mode.");
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                ret = cmdio_write(ys, FTP_BADCMD, "Unrecognised TYPE command.");
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int handle_cwd(struct yftp_session *ys)
{
        int ret;
        char path[MAX_PATH_LEN];
        fileid_t fileid;

        if(get_absolute_path(ys,ys->arg, path, MAX_PATH_LEN)) {
                ret = cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                if (ret)
                        GOTO(err_ret, ret);

                return 0;
        }

        DINFO("last_path=%s, arguments=%s,  changed_path=%s\n", ys->pwd, ys->arg, path);


        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret) {
                ret = cmdio_write(ys, FTP_FILEFAIL,
                                "Failed to change directory.");
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                snprintf(ys->pwd, MAX_PATH_LEN, "%s", path);

                ret = cmdio_write(ys, FTP_CWDOK,
                                "Directory successfully changed.");
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int handle_cdup(struct yftp_session *ys)
{
        char arg_buf[MAX_BUF_LEN] = {0};

        ys->arg = arg_buf;
        _strncpy(arg_buf, "..", 2);

        int ret = handle_cwd(ys);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int handle_mkdir(struct yftp_session *ys)
{
        int ret = 0;
        char path[MAX_PATH_LEN], *sla, *mkd_ok;
        char name[MAX_NAME_LEN];
        fileid_t fileid;


        if(get_absolute_path(ys, ys->arg, path, MAX_PATH_LEN)) {
                ret = cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                if(ret) {
                        GOTO(err_ret, ret);
                }

                return 0;
        }

        if (ys->mode == SESSION_READONLY) {
                cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                goto err_ret;
        }

        memset(name, 0x00, MAX_NAME_LEN);
        ret = sdfs_splitpath(path, &fileid, name);
        DBUG("handle_mkdir path is %s, fileid is "FID_FORMAT" \n", \
                        path, FID_ARG(&fileid));
        if (ret) {
                ret = cmdio_write(ys, FTP_FILEFAIL,
                                "Create directory operation failed.");
                goto err_ret;
        }
        ret = sdfs_mkdir(NULL, &fileid, name, NULL, NULL, 0755, 0, 0);
        if (ret) {
                ret = cmdio_write(ys, FTP_FILEFAIL,
                                "Create directory operation failed.");
        } else {
                sla = path + 1;      /* "/user/dir" */
                sla = strchr(sla, '/');

                YASSERT(sla != NULL);

                mkd_ok = path + _strlen(path);

                snprintf(mkd_ok, MAX_PATH_LEN - _strlen(path), " created");

                ret = cmdio_write(ys, FTP_MKDIROK, sla);
        }

        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int handle_rmdir(struct yftp_session *ys)
{
        int ret;
        char path[MAX_PATH_LEN];
        fileid_t fileid;
        char name[MAX_NAME_LEN];

        if(get_absolute_path(ys, ys->arg, path, MAX_PATH_LEN)) {
                ret = cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                if (ret)
                        GOTO(err_ret, ret);

                return 0;
        }

        memset(name, 0x00, MAX_NAME_LEN);
        ret = sdfs_splitpath(path, &fileid, name);
        DBUG("handle_mkdir path is %s, fileid is "FID_FORMAT" \n", path, FID_ARG(&fileid));
        if (ys->mode == SESSION_READONLY) {
                cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                goto err_ret;
        }

        if (ret) {
                ret = cmdio_write(ys, FTP_FILEFAIL,
                                "Remove directory operation failed.");
                GOTO(err_ret, ret);
        }

        ret = sdfs_rmdir(NULL, &fileid, name);
        if (ret)
                ret = cmdio_write(ys, FTP_FILEFAIL,
                                "Remove directory operation failed.");
        else
                ret = cmdio_write(ys, FTP_RMDIROK,
                                "Remove directory operation successful.");

        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int handle_dele(struct yftp_session *ys)
{
        int ret;
        char path[MAX_PATH_LEN];
        struct stat stbuf;
        fileid_t fileid;
        fileid_t fileidp;

        char name[MAX_NAME_LEN];

        if(get_absolute_path(ys, ys->arg, path, MAX_PATH_LEN)) {
                ret = cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        }

        if (ys->mode == SESSION_READONLY) {
                cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                ret = EACCES;
                goto err_ret;
        }
        memset(name, 0x00, MAX_NAME_LEN);
        ret = sdfs_splitpath(path, &fileidp, name);
        DBUG("handle_mkdir path is %s, fileid is "FID_FORMAT" \n", path, FID_ARG(&fileidp));

        if (ret) {
                ret = cmdio_write(ys, FTP_FILEFAIL, "Delete operation failed.");
                GOTO(err_ret, ret);
        }

        ret = sdfs_lookup(NULL, &fileidp, name, &fileid);
        if (ret) {
                DERROR("open (%s ...) %s\n", path, strerror(ret));
                ret = cmdio_write(ys, FTP_FILEFAIL, "Failed to delete file.");
                if (ret)
                        GOTO(err_ret, ret);
                /*
                 * if raw_lookup return error
                 * go out directly
                 */
                goto out;
        }

        ret = sdfs_getattr(NULL, &fileid, &stbuf);
        if (ret)
                GOTO(err_ret, ret);

        if (S_ISDIR(stbuf.st_mode)){
                ret = cmdio_write(ys, FTP_FILEFAIL,
                                "rm: access failed : 550 Delete operation failed.");
                goto out;
        }

        ret = sdfs_unlink(NULL, &fileidp, name);
        if (ret)
                ret = cmdio_write(ys, FTP_FILEFAIL, "Delete operation failed.");
        else
                ret = cmdio_write(ys, FTP_DELEOK,
                                "Delete operation successful.");
        if (ret)
                GOTO(err_ret, ret);

#if	YFTP_ACCESS_LOG
        ret = ylog_access_write(ys->user, ys->cmd, path, stbuf.st_size);
        if (ret)
                GOTO(err_ret, ret);
#endif

out:
        return 0;
err_ret:
        return ret;
}

int handle_pwd(struct yftp_session *ys)
{
        int ret;
        char pwd_ok[YFTP_MAX_CMD_LINE];

        snprintf(pwd_ok, YFTP_MAX_CMD_LINE, "\"%s\" is current directory.",
                        ys->pwd);

        ret = cmdio_write(ys, FTP_PWDOK, pwd_ok);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int postlogin_process(struct yftp_session *ys)
{
        int ret;

        while (srv_running) {
                ret = cmdio_get_cmd_and_arg(ys);
                if (ret)
                        GOTO(err_session, ret);

                if (memcmp(ys->cmd, "QUIT", 5) == 0) {
                        (void) cmdio_write(ys, FTP_GOODBYE, "Goodbye.");

                        goto out;
                } else if (memcmp(ys->cmd, "PWD", 4) == 0
                                || memcmp(ys->cmd, "XPWD", 5) == 0)
                        ret = handle_pwd(ys);
                else if(memcmp(ys->cmd, "ABOR", 5) == 0)
                        ret = handle_abort(ys);
                else if (memcmp(ys->cmd, "CWD", 4) == 0
                                || memcmp(ys->cmd, "XCWD", 5) == 0)
                        ret = handle_cwd(ys);
                else if (memcmp(ys->cmd, "CDUP", 5) == 0
                                || memcmp(ys->cmd, "XCUP", 5) == 0)
                        ret = handle_cdup(ys);
                else if (memcmp(ys->cmd, "PASV", 5) == 0
                                || memcmp(ys->cmd, "P@SW", 5) == 0)
                        ret = handle_pasv(ys, 0);
                else if (memcmp(ys->cmd, "EPSV", 5) == 0)
                        ret = handle_pasv(ys, 1);
                else if (memcmp(ys->cmd, "RETR", 5) == 0)
                        ret = handle_retr(ys);
                else if (memcmp(ys->cmd, "NOOP", 5) == 0)
                        ret = cmdio_write(ys, FTP_NOOPOK, "NOOP ok.");
                else if (memcmp(ys->cmd, "SYST", 5) == 0)
                        ret = cmdio_write(ys, FTP_SYSTOK, "UNIX Type: L8");
                else if (memcmp(ys->cmd, "LIST", 5) == 0)
                        ret = handle_list(ys);
                else if (memcmp(ys->cmd, "TYPE", 5) == 0)
                        ret = handle_type(ys);
                else if (memcmp(ys->cmd, "STOR", 5) == 0)
                        ret = handle_stor(ys);
                else if (memcmp(ys->cmd, "OPTS", 4) == 0)
                        ret = handle_opts(ys);
                else if (memcmp(ys->cmd, "MKD", 4) == 0
                                || memcmp(ys->cmd, "XMKD", 5) == 0)
                        ret = handle_mkdir(ys);
                else if (memcmp(ys->cmd, "RMD", 4) == 0
                                || memcmp(ys->cmd, "XRMD", 5) == 0)
                        ret = handle_rmdir(ys);
                else if (memcmp(ys->cmd, "DELE", 5) == 0)
                        ret = handle_dele(ys);
                else if (memcmp(ys->cmd, "REST", 5) == 0)
                        ret = handle_rest(ys);
                else if (memcmp(ys->cmd, "RNFR", 5) == 0)
                        ret = handle_rnfr(ys);
                else if (memcmp(ys->cmd, "RNTO", 5) == 0)
                        ret = handle_rnto(ys);
                else if (memcmp(ys->cmd, "NLST", 5) == 0)
                        ret = handle_nlst(ys);
                else if (memcmp(ys->cmd, "SIZE", 5) == 0)
                        ret = handle_size(ys);
                else if (memcmp(ys->cmd, "SITE", 5) == 0)
                        ret = handle_site(ys);
                else if (memcmp(ys->cmd, "APPE", 5) == 0)
                        ret = handle_appe(ys);
                else if (memcmp(ys->cmd, "MDTM", 5) == 0)
                        ret = handle_mdtm(ys);
                else if (memcmp(ys->cmd, "STRU", 5) == 0) {
                        str_upper(ys->arg);

                        if (memcmp(ys->arg, "F", 2) == 0)
                                ret = cmdio_write(ys, FTP_STRUOK,
                                                "Structure set to F.");
                        else
                                ret = cmdio_write(ys, FTP_BADSTRU,
                                                "Bad STRU command.");
                } else if (memcmp(ys->cmd, "MODE", 5) == 0) {
                        str_upper(ys->arg);

                        if (memcmp(ys->arg, "S", 2) == 0)
                                ret = cmdio_write(ys, FTP_MODEOK,
                                                "Mode set to S.");
                        else
                                ret = cmdio_write(ys, FTP_BADMODE,
                                                "Bad MODE command.");
                } else if (memcmp(ys->cmd, "STOU", 5) == 0)
                        ret = handle_stou(ys);
                else if (memcmp(ys->cmd, "ALLO", 5) == 0)
                        ret = cmdio_write(ys, FTP_ALLOOK,
                                        "ALLO command ignored.");
                else if (memcmp(ys->cmd, "REIN", 5) == 0)
                        ret = cmdio_write(ys, FTP_COMMANDNOTIMPL,
                                        "REIN not implemented.");
                else if (memcmp(ys->cmd, "ACCT", 5) == 0)
                        ret = cmdio_write(ys, FTP_COMMANDNOTIMPL,
                                        "ACCT not implemented.");
                else if (memcmp(ys->cmd, "SMNT", 5) == 0)
                        ret = cmdio_write(ys, FTP_COMMANDNOTIMPL,
                                        "SMNT not implemented.");
                else if (memcmp(ys->cmd, "FEAT", 5) == 0)
                        ret = handle_feat(ys);
                else if (memcmp(ys->cmd, "OPTS", 5) == 0)
                        ret = cmdio_write(ys, FTP_BADOPTS,
                                        "Option not understood.");
                else if (memcmp(ys->cmd, "STAT", 5) == 0) {
                        if (ys->arg == NULL)
                                ret = handle_stat(ys);
                        else
                                ret = handle_stat_file(ys);
                } else if (memcmp(ys->cmd, "PASV", 5) == 0
                                || memcmp(ys->cmd, "PORT", 5) == 0
                                || memcmp(ys->cmd, "STOR", 5) == 0
                                || memcmp(ys->cmd, "MKD", 4) == 0
                                || memcmp(ys->cmd, "XMKD", 5) == 0
                                || memcmp(ys->cmd, "RMD", 4) == 0
                                || memcmp(ys->cmd, "XRMD", 5) == 0
                                || memcmp(ys->cmd, "DELE", 5) == 0
                                || memcmp(ys->cmd, "RNFR", 5) == 0
                                || memcmp(ys->cmd, "RNTO", 5) == 0
                                || memcmp(ys->cmd, "SITE", 5) == 0
                                || memcmp(ys->cmd, "APPE", 5) == 0
                                || memcmp(ys->cmd, "EPSV", 5) == 0
                                || memcmp(ys->cmd, "EPRT", 5) == 0
                                || memcmp(ys->cmd, "RETR", 5) == 0
                                || memcmp(ys->cmd, "LIST", 5) == 0
                                || memcmp(ys->cmd, "NLST", 5) == 0
                                || memcmp(ys->cmd, "STOU", 5) == 0
                                || memcmp(ys->cmd, "ALLO", 5) == 0
                                || memcmp(ys->cmd, "REIN", 5) == 0
                                || memcmp(ys->cmd, "ACCT", 5) == 0
                                || memcmp(ys->cmd, "SMNT", 5) == 0
                                || memcmp(ys->cmd, "FEAT", 5) == 0
                                || memcmp(ys->cmd, "OPTS", 5) == 0
                                || memcmp(ys->cmd, "STAT", 5) == 0
                                || memcmp(ys->cmd, "PBSZ", 5) == 0
                                || memcmp(ys->cmd, "PROT", 5) == 0)
                                ret = cmdio_write(ys, FTP_NOPERM, "Permission denied.");
                else
                        ret = cmdio_write(ys, FTP_BADCMD, "Unknown command.");

                if (ret) {
                        if (ret == EPIPE)
                                goto err_session;
                        else
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



#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <dirent.h>
#include <errno.h>

#define DBG_SUBSYS S_YFTP

#include "cmdio.h"
#include "ftpcodes.h"
#include "session.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "yftp_conf.h"
#include "ynet_rpc.h"
#include "configure.h"
#include "dbg.h"

/*
 * eg: *.tgz, abc.tgz, abc.abc
 *     *.*
 *     a.*
 */
/*
 * eg2: a*, a.tgz, aaa.ttt
 */
int should_be_filter_out(char *wild_card, char *filename, int *filtered)
{
        size_t i;
        int ret, have_star = 0;
        char *str_index = NULL, *last_index = NULL;

        if(wild_card == NULL) {
                *filtered = 0;
                return 0;
        }

        if(filename == NULL) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        if(index(wild_card, '*') == NULL) {
                if(_strcmp(wild_card, filename) == 0) {
                        *filtered = 0;
                        return 0;
                } else {
                        *filtered = 1;
                        return 0;
                }
        }

        str_index = wild_card;
        for(i=0; i<strlen(filename); ++i) {
                if(*str_index != '\0') {
                        while(*str_index == '*') {
                                str_index++;
                                have_star = 1;
                        }
                        if(*str_index == filename[i]) {
                                last_index = str_index;
                                str_index++;
                        } else if( last_index != NULL && *last_index != '*' &&
                                        *str_index != filename[i] && *str_index != '\0') {
                                *filtered = 1;
                                return 0;
                        } else if(*str_index == '\0') {
                                *filtered = 0;
                                return 0;
                        } else if(have_star == 0) {
                                *filtered = 1;
                                return 0;
                        }
                } else {
                        break;
                }
        }

        if(last_index == NULL)
                *filtered = 1;
        else
                *filtered = 0;

        return 0;
err_ret:
        return ret;
}


int dataio_get_pasv_fd(struct yftp_session *ys, int *pfd)
{
        int ret, clifd, nfds, epoll_fd;
        struct epoll_event ev, events;

        epoll_fd = epoll_create(1);
        if (epoll_fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        _memset(&ev, 0x0, sizeof(struct epoll_event));
        ev.events = Y_EPOLL_EVENTS;
        ev.data.fd = ys->pasv_sd;

        ret = _epoll_ctl(epoll_fd, EPOLL_CTL_ADD, ys->pasv_sd, &ev);
        if (ret == -1) {
                ret = errno;
                GOTO(err_epoll, ret);
        }

        /* TODO: the value of timeout need adjust carefully */
        nfds = _epoll_wait(epoll_fd, &events, 1,
                          gloconf.rpc_timeout * 1000);
        if (nfds == -1) {
                ret = errno;
                GOTO(err_epoll, ret);
        } else if (nfds == 0) {
                ret = ETIME;
                GOTO(err_epoll, ret);
        }

        ret = rpc_accept(&clifd, ys->pasv_sd, 1, YNET_RPC_BLOCK);
        if (ret) {
                int rc;

                rc = cmdio_write(ys, FTP_BADSENDCONN,
                                 "Failed to establish connection.");
                if (rc)
                        GOTO(err_epoll, rc);

                GOTO(err_epoll, ret);
        }

        *pfd = clifd;
        (void) sy_close(epoll_fd);
        return 0;
err_epoll:
        (void) sy_close(epoll_fd);
err_ret:
        return ret;
}

int check_if_dir(const char *_path, int *is_dir, fileid_t *fileid)
{
        int ret;
        struct stat stbuf;

        ret = sdfs_lookup_recurive(_path, fileid);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_getattr(NULL, fileid, &stbuf);
        if (ret)
                GOTO(err_ret, ret);

        if(S_ISDIR(stbuf.st_mode)) {
                *is_dir = 1;
        } else {
                *is_dir = 0;
        }

        return 0;
err_ret:
        return ret;
}

int dataio_transfer_dir(struct yftp_session *ys, int rfd, char *dirname,
                        char *opt, char *filter, int full_details)
{
        int ret, delen, is_dir, filter_out;
        off_t offset;
        void *de0, *ptr;
        struct dirent *de;
        struct stat stbuf;
        char buf[YFTP_MAX_DIR_BUFSIZE], perms[11], date[64];
        char depath[MAX_PATH_LEN], deline[YFTP_MAX_DIR_BUFSIZE];
        size_t buflen, len;
        fileid_t fileidp;
        fileid_t fileidc;

        DBUG("dirname %s, opt %s, filter %s, full %d\n", dirname, opt, filter,
             full_details);

        (void) opt;

        offset = 0;
        de0 = NULL;
        delen = 0;
        buflen = 0;

        ret = check_if_dir(dirname, &is_dir, &fileidp);
        if(ret) {
                cmdio_write(ys, FTP_TRANSFEROK,
                               "Transfer done (but failed to open directory).");
                GOTO(err_ret, ret);
        }

        if(!is_dir) {
                //this is a file
                ret = sdfs_getattr(NULL, &fileidp, &stbuf);
                if (ret)
                        GOTO(err_ret, ret);

                stat_to_datestr(&stbuf, date);
                mode_to_permstr((uint32_t)stbuf.st_mode, perms);
                snprintf(deline, YFTP_MAX_DIR_BUFSIZE,
                                "%s %s %s %-12llu %s %s\r\n", perms,
                                ys->user, ys->user,
                                (unsigned long long)stbuf.st_size,
                                date, ys->arg);
                ret = _write(rfd, deline, strlen(deline));
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                goto out;

        }

        while (srv_running) {
                ret = sdfs_readdir1(NULL, &fileidp, offset, &de0, &delen);
                if (ret) {
                        ret = cmdio_write(ys, FTP_TRANSFEROK,
                               "Transfer done (but failed to open directory).");
                        if (ret)
                                GOTO(err_ret, ret);

                        break;
                } else if (delen == 0) {
                        if (buflen > 0) {
                                ret = _write(rfd, buf, buflen);
                                if (ret < 0) {
                                        ret = -ret;
                                        GOTO(err_ret, ret);
                                }
                        }

                        break;
                }

                ptr = de0;
                while (delen > 0) {
                        de = (struct dirent *)ptr;

                        if (de->d_reclen > delen) {
                                ret = EINVAL;
                                DERROR("reclen %u > delen %lu\n", de->d_reclen,
                                       (unsigned long)delen);

                                yfree((void **)&de);

                                ret = cmdio_write(ys, FTP_TRANSFEROK,
                               "Transfer done (but failed to open directory).");
                                if (ret)
                                        GOTO(err_ret, ret);

                                goto out;
                        }

                        len = _strlen(de->d_name);
                        if ((len == 1 && de->d_name[0] == '.')
                            || (len == 2 && de->d_name[0] == '.'
                                && de->d_name[1] == '.'))
                                goto next;

                        ret = should_be_filter_out(filter, de->d_name, &filter_out);
                        if(ret)
                                goto next;

                        if(filter_out == 1)
                                goto next;

                        if(full_details) {
                                if(strlen(dirname) == 1 && dirname[0] == '/')
                                    snprintf(depath, MAX_PATH_LEN, "/%s", de->d_name);
                                else
                                    snprintf(depath, MAX_PATH_LEN, "%s/%s", dirname, de->d_name);

                                _memset(&stbuf, 0x0, sizeof(struct stat));

                                /* stat() the file. Of course there's a race condition -
                                 * the directory entry may have gone away while we
                                 * read it, so ignore failure to stat
                                 */
                                ret = sdfs_lookup_recurive(depath, &fileidc);
                                if (ret)
                                        goto next;
                                ret = sdfs_getattr(NULL, &fileidc, &stbuf);
                                if (ret)
                                        goto next;

                                stat_to_datestr(&stbuf, date);
                                mode_to_permstr((uint32_t)stbuf.st_mode, perms);
                                snprintf(deline, YFTP_MAX_DIR_BUFSIZE,
                                                "%s %s %s %-12llu %s %s\r\n", perms,
                                                ys->user, ys->user,
                                                (unsigned long long)stbuf.st_size,
                                                date, de->d_name);
                        } else {
                                snprintf(deline, YFTP_MAX_DIR_BUFSIZE, "%s\r\n", de->d_name);
                        }

                        len = _strlen(deline);

                        if (buflen + len > YFTP_MAX_DIR_BUFSIZE) {
                                ret = _write(rfd, buf, buflen);
                                if (ret < 0) {
                                        ret = -ret;
                                        GOTO(err_de0, ret);
                                }

                                buflen = 0;
                        }

                        _memcpy(buf + buflen, deline, len);
                        buflen += len;

next:
                        offset = de->d_off;
                        ptr += de->d_reclen;
                        delen -= de->d_reclen;

                }

                yfree((void **)&de0);
                delen = 0;
                if(offset == 0)
                {
                    if (buflen > 0) {
                        ret = _write(rfd, buf, buflen);
                        if (ret < 0) {
                                ret = -ret;
                                GOTO(err_ret, ret);
                        }
                    }
                    break;
                }
        }

out:
        return 0;
err_de0:
        yfree((void **)&de0);
err_ret:
        return ret;
}

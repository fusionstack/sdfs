

#include <rpc/rpc.h>

#define DBG_SUBSYS S_YNFS

#include "attr.h"
#include "file_proto.h"
#include "nfs_state_machine.h"
#include "sdfs_lib.h"
#include "md_attr.h"
#include "attr_queue.h"
#include "schedule.h"
#include "dbg.h"

/*
 * check whether stat_cache is for a regular file
 * fh_decomp must be called before to fill the stat cache
 */

int sattr_utime(const fileid_t *fileid, int at, int mt, int ct)
{
        int ret;
        setattr_t setattr;

        setattr_init(&setattr, -1, -1, NULL, -1, -1, -1);
        setattr_update_time(&setattr,
                            at ? __SET_TO_SERVER_TIME : __DONT_CHANGE, NULL,
                            mt ? __SET_TO_SERVER_TIME : __DONT_CHANGE, NULL,
                            ct ? __SET_TO_SERVER_TIME : __DONT_CHANGE, NULL);

#if ENABLE_ATTR_QUEUE
        ret = attr_queue_settime(NULL, fileid, &setattr);
        if (ret)
                GOTO(err_ret, ret);
#else
        ret = sdfs_setattr(NULL, fileid, &setattr, 0);
        if (ret)
                GOTO(err_ret, ret);
#endif
        
        return 0;
err_ret:
        return ret;
}

int sattr_set(const fileid_t *fileid, const sattr *attr, const nfs3_time *ctime)
{
        int ret, update = 0, settime;
        setattr_t setattr, time;
        struct timespec t;

        DBUG("attr set %u %u %u %u %u %u %u %p\n",
              attr->uid.set_it,
              attr->gid.set_it,
              attr->mode.set_it,
              attr->size.set_it,
              attr->atime.set_it,
              attr->mtime.set_it,
              ctime);

        setattr_init(&setattr, -1, -1, NULL, -1, -1, -1);
        setattr_init(&time, -1, -1, NULL, -1, -1, -1);

        if (attr->uid.set_it == TRUE) {
                setattr.uid.set_it = 1;
                setattr.uid.val = attr->uid.uid;
                update = 1;
        }

        if (attr->gid.set_it == TRUE) {
                setattr.gid.set_it = 1;
                setattr.gid.val = attr->gid.gid;
                update = 1;
        }

        if (attr->mode.set_it == TRUE) {
                setattr.mode.set_it = 1;
                setattr.mode.val = attr->mode.mode;
                update = 1;
        }

        if (attr->size.set_it == TRUE) {
                setattr.size.set_it = 1;
                setattr.size.size = attr->size.size;

                ret = sdfs_truncate(NULL, fileid, setattr.size.size);
                if (ret)
                        GOTO(err_ret, ret);
        }

        if (attr->atime.set_it == SET_TO_SERVER_TIME) {
                setattr_update_time(&time,
                                    __SET_TO_SERVER_TIME, NULL,
                                    __DONT_CHANGE, NULL,
                                    __DONT_CHANGE, NULL);
                settime = 1;
        } else if (attr->atime.set_it == SET_TO_CLIENT_TIME) {
                t.tv_sec = attr->atime.time.seconds;
                t.tv_nsec = attr->atime.time.nseconds;
                
                setattr_update_time(&time,
                                    __SET_TO_CLIENT_TIME, &t,
                                    __DONT_CHANGE, NULL,
                                    __DONT_CHANGE, NULL);
                settime = 1;
        }

        if (attr->mtime.set_it == SET_TO_SERVER_TIME) {
                setattr_update_time(&time,
                                    __DONT_CHANGE, NULL,
                                    __SET_TO_SERVER_TIME, NULL,
                                    __DONT_CHANGE, NULL);
                settime = 1;
        } else if (attr->mtime.set_it == SET_TO_CLIENT_TIME) {
                t.tv_sec = attr->mtime.time.seconds;
                t.tv_nsec = attr->mtime.time.nseconds;

                setattr_update_time(&time,
                                    __DONT_CHANGE, NULL,
                                    __SET_TO_CLIENT_TIME, &t,
                                    __DONT_CHANGE, NULL);
                settime = 1;
        }

        if (ctime) {
                t.tv_sec = ctime->seconds;
                t.tv_nsec = ctime->nseconds;

#if 0
                char _time[MAX_PATH_LEN];
                struct tm tm;
                strftime(_time, 32, "%F %T", localtime_safe(&t.tv_sec, &tm));
                DINFO("time %s\n", _time);
#endif
                
                setattr_update_time(&time,
                                    __DONT_CHANGE, NULL,
                                    __DONT_CHANGE, NULL,
                                    __SET_TO_CLIENT_TIME, &t);
                settime = 1;
        }

        if (update) {
                ret = sdfs_setattr(NULL, fileid, &setattr, 1);
                if (ret)
                        GOTO(err_ret, ret);
        }

        if (settime) {
                ret = sdfs_setattr(NULL, fileid, &time, 1);
                if (ret)
                        GOTO(err_ret, ret);
                        
        }
        
        return 0;
err_ret:
        return ret;
}

int sattr_tomode(mode_t *mode, sattr *attr)
{
        if (attr->mode.set_it == TRUE)
                *mode = attr->mode.mode;
        else
                *mode = 0644;

        return 0;
}

/* compute post operation attributes given a stat buffer */
int get_postopattr_stat(post_op_attr *attr, struct stat *stbuf)
{
        attr->attr_follow = TRUE;

        if (S_ISDIR(stbuf->st_mode))
                attr->attr.type = NFS3_DIR;
        else if (S_ISBLK(stbuf->st_mode))
                attr->attr.type = NFS3_BLK;
        else if (S_ISCHR(stbuf->st_mode))
                attr->attr.type = NFS3_CHR;
        else if (S_ISLNK(stbuf->st_mode))
                attr->attr.type = NFS3_LNK;
        else if (S_ISSOCK(stbuf->st_mode))
                attr->attr.type = NFS3_SOCK;
        else if (S_ISFIFO(stbuf->st_mode))
                attr->attr.type = NFS3_FIFO;
        else
                attr->attr.type = NFS3_REG;

        attr->attr.mode = stbuf->st_mode & 0xFFFF;
        attr->attr.nlink = stbuf->st_nlink;
        attr->attr.uid = stbuf->st_uid;
        attr->attr.gid = stbuf->st_gid;
        attr->attr.size = stbuf->st_size;
        attr->attr.used = stbuf->st_blocks * FAKE_BLOCK;
        attr->attr.rdev.data1 = (stbuf->st_rdev >> 8) & 0xFF;
        attr->attr.rdev.data2 = stbuf->st_rdev & 0xFF;
        attr->attr.fsid = stbuf->st_dev;
        attr->attr.fileid = stbuf->st_ino;
        attr->attr.atime.seconds = stbuf->st_atim.tv_sec;
        attr->attr.atime.nseconds = stbuf->st_atim.tv_nsec;
        attr->attr.mtime.seconds = stbuf->st_mtim.tv_sec;
        attr->attr.mtime.nseconds = stbuf->st_mtim.tv_nsec;
        attr->attr.ctime.seconds = stbuf->st_ctim.tv_sec;
        attr->attr.ctime.nseconds = stbuf->st_ctim.tv_nsec;

        DBUG("type %u fsid %llu fileid %llu mode %d %d, size %llu, used %llu\n",
             attr->attr.type,
             (LLU)attr->attr.fsid,
             (LLU)attr->attr.fileid,
             stbuf->st_mode,
             attr->attr.mode,
             (LLU)attr->attr.size, (LLU)attr->attr.used);

        return 0;
}

void get_postopattr1(const fileid_t *fileid, post_op_attr *attr)
{
        int ret, retry;
        struct stat stbuf;

        DBUG("post op attr\n");

        attr->attr_follow = FALSE;
        retry = 0;
retry:
        ret = sdfs_getattr(NULL, fileid, &stbuf);
        if (ret) {
                if (NEED_EAGAIN(ret)) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 30, (1000 * 1000));
                } else {
                        GOTO(err_ret, ret);
                }
        }

        attr->attr_follow = TRUE;
        get_postopattr_stat(attr, &stbuf);

        return;
err_ret:
        DBUG(""FID_FORMAT"\n", FID_ARG(fileid));
        return;
}

void get_preopattr1(const fileid_t *fileid, preop_attr *attr)
{
        int ret, retry;
        struct stat stbuf;

        DBUG("pre op attr\n");

        attr->attr_follow = FALSE;
        retry = 0;
retry:
        ret = sdfs_getattr(NULL, fileid, &stbuf);
        if (ret) {
                if (NEED_EAGAIN(ret)) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 30, (1000 * 1000));
                } else {
                        GOTO(err_ret, ret);
                }
        }

        attr->attr_follow = TRUE;

        attr->attr.size = stbuf.st_size;
        attr->attr.mtime.seconds = stbuf.st_mtime;
        attr->attr.mtime.nseconds = 0;
        attr->attr.ctime.seconds = stbuf.st_ctime;
        attr->attr.ctime.nseconds = 0;

        return;
err_ret:
        DWARN(""FID_FORMAT"\n", FID_ARG(fileid));
        return;
}

#include "sdfs_id.h"
#include "worm_cli_lib.h"
#include "xattr.h"
#include "file_proto.h"
#include "yfs_md.h"
#include "yfs_chunk.h"
#include "cache.h"
#include "net_proto.h"
#include "network.h"
#include "lvm.h"
#include "sdfs_lib.h"



worm_status_t convert_string_to_worm_status(const char *string)
{
        size_t size = strlen(string);

        if(strncmp(string, "WORM_NOT_SET", size) == 0) {
                return WORM_NOT_SET;
        } else if(strncmp(string, "WORM_BEFORE_PROTECT", size) == 0) {
                return WORM_BEFORE_PROTECT;
        } else if(strncmp(string, "WORM_IN_PROTECT", size) == 0) {
                return WORM_IN_PROTECT;
        } else if(strncmp(string, "WORM_AFTER_PROTECT", size) == 0) {
                return WORM_AFTER_PROTECT;
        } else {
                return WORM_NOT_SET;
        }
}

int get_wormfileid_by_fid_cli(const uint64_t fid,
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

int worm_auth_valid(const char *username, const char *password)
{
        size_t user_len, pass_len;

        if(username == NULL || password == NULL) {
                return 0;
        }

        user_len = strlen(username);
        pass_len = strlen(password);

        if(user_len == WORM_ADMIN_LEN && pass_len == WORM_ADMIN_LEN) {
                if((strncmp(username, WORM_ADMIN_USER, user_len) == 0) &&
                                (strncmp(password, WORM_ADMIN_PASS, pass_len) == 0)) {
                        return 1;
                } else {
                        return 0;
                }
        } else {
                return 0;
        }
}

int worm_init_wormclock_dir(fileid_t *_fileid)
{
        int ret;
        fileid_t fileid;
        fileid_t parent;
        char name[MAX_NAME_LEN] = {0};

retry:
        ret = sdfs_lookup_recurive(WORM_CLOCKDIR, &fileid);
        if(ret && ret != ENOENT)
                GOTO(err_ret, ret);

        if(ret == ENOENT) {
                ret = sdfs_splitpath(WORM_CLOCKDIR, &parent, name);
                if (ret)
                        GOTO(err_ret, ret);

                ret = sdfs_mkdir(NULL, &parent, name, NULL, NULL, 0755, 0, 0);
                if(ret)
                        GOTO(err_ret, ret);

                ret = lvm_set_engine(WORM_CLOCKDIR, ENGINE_LOCALFS);
                if(ret)
                        GOTO(err_ret, ret);

                goto retry;
        }

        if(_fileid != NULL) {
                *_fileid = fileid;
        }

        return 0;
err_ret:
        return ret;
}

int worm_set_clock_time(const fileid_t *fileid, const time_t wormclock_timestamp)
{
        int ret;
        char buf_time[MAX_U64_LEN] = {0};
        size_t size = 0;

        snprintf(buf_time, MAX_U64_LEN, "%ld", wormclock_timestamp);
        size = strlen(buf_time) + 1;

        ret = sdfs_setxattr(fileid, WORM_CLOCK_KEY, buf_time, size, USS_XATTR_DEFAULT);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int worm_get_clock_time(const fileid_t *fileid, time_t *wormclock_timestamp)
{
        int ret;
        char buf_time[MAX_U64_LEN] = {0};
        size_t size;

        size = sizeof(buf_time);
        ret = sdfs_getxattr(fileid, WORM_CLOCK_KEY, buf_time, &size);
        if(ret) {
                if(ret == ENOKEY)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        if(wormclock_timestamp != NULL)
                *wormclock_timestamp = (time_t)strtol(buf_time, NULL, 10);

        return 0;
err_ret:
        return ret;
}

int worm_update_clock_time(void)
{
        int ret;
        fileid_t fileid;
        time_t wormclock_time;

        ret = sdfs_lookup_recurive(WORM_CLOCKDIR, &fileid);
        if(ret)
                GOTO(err_ret, ret);

        ret = worm_get_clock_time(&fileid, &wormclock_time);
        if(ret)
                GOTO(err_ret, ret);

        wormclock_time += ONE_HOUR;
        ret = worm_set_clock_time(&fileid, wormclock_time);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int worm_set_file_attr(const fileid_t *fileid, worm_file_t *_worm_file)
{
        int ret;

        if(_worm_file == NULL) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        ret = sdfs_setxattr(fileid, WORM_FILE_KEY, _worm_file,
                        sizeof(worm_file_t), USS_XATTR_DEFAULT);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int worm_get_file_attr(const fileid_t *fileid, worm_file_t *_worm_file)
{
        int ret;
        worm_file_t worm_file;
        size_t size = sizeof(worm_file_t);

        ret = sdfs_getxattr(fileid, WORM_FILE_KEY, &worm_file, &size);
        if(ret) {
                if(ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        if(_worm_file != NULL)
                memcpy(_worm_file, &worm_file, sizeof(worm_file_t));

        return 0;
err_ret:
        return ret;
}

int worm_remove_file_attr(const fileid_t *fileid)
{
        int ret;

        ret = sdfs_removexattr(fileid, WORM_FILE_KEY);
        if(ret) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int worm_get_attr(const fileid_t *fileid, worm_t *_worm)
{
        int ret;
        worm_t worm;
        size_t size = sizeof(worm_t);

        ret = sdfs_getxattr(fileid, WORM_ATTR_KEY, &worm, &size);
        if(ret) {
                if(ret == ENOKEY)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        if(_worm != NULL)
                memcpy(_worm, &worm, sizeof(worm_t));

        return 0;
err_ret:
        return ret;
}

int worm_set_attr(const fileid_t *fileid, worm_t *_worm)
{
        int ret;

        if(_worm == NULL) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        ret = sdfs_setxattr(fileid, WORM_ATTR_KEY, _worm,
                        sizeof(worm_t), USS_XATTR_DEFAULT);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int worm_remove_attr(const fileid_t *fileid)
{
        int ret;

        ret = sdfs_removexattr(fileid, WORM_ATTR_KEY);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

worm_status_t worm_get_status(IN const fileid_t *fileid)
{
        int ret;
        worm_file_t worm_file;
        size_t size = sizeof(worm_file_t);
        worm_status_t status = WORM_NOT_SET;

        memset(&worm_file, 0, size);

        ret = worm_get_file_attr(fileid, &worm_file);
        if(ret) {
                DBUG("get worm attr failed fileid "FID_FORMAT", error : %s\n",
                                FID_ARG(fileid), strerror(ret));
                return status;
        }

        status = convert_string_to_worm_status(worm_file.status);

        return status;
}

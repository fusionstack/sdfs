

#include <sys/types.h>
#include <sys/wait.h>
#include <getopt.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSMDS

#include "configure.h"
#include "net_global.h"
#include "job_dock.h"
#include "get_version.h"
#include "shadow.h"
#include "ylib.h"
#include "yfsmds_conf.h"
#include "ylog.h"
#include "dbg.h"
#include "nodectl.h"

#define LOG_DRBD_PRIMARY  SDFS_HOME"/log/drbd_primary.log"
#define LOG_DRBD_SECONDARY  SDFS_HOME"/log/drbd_secondary.log"
#define LOG_DRBD_CHECK  SDFS_HOME"/log/drbd_check.log"
#define LOG_REDIS  SDFS_HOME"/log/redis.log"

#define NODECTL_PREFIX "/dev/shm/sdfs/"


int nodectl_get(const char *key, char *value, const char *_default)
{
        int ret;
        char path[MAX_PATH_LEN];

        snprintf(path, MAX_PATH_LEN, "%s/%s", NODECTL_PREFIX, key);

        ret = _get_value(path, value, MAX_BUF_LEN);
        if (ret < 0) {
                ret = -ret;
                YASSERT(ret == ENOENT);
                ret = _set_value(path, _default, strlen(_default) + 1, O_CREAT | O_TRUNC);
                if (ret)
                        GOTO(err_ret, ret);

                strcpy(value, _default);
        }

        return 0;
err_ret:
        return ret;
}

int nodectl_get_int(const char *key, const char *_default)
{
        int ret;
        char value[MAX_BUF_LEN];

        ret = nodectl_get(key, value, _default);
        if(ret) {
                GOTO(err_ret, ret);
        }

        if (value[strlen(value) - 1] == '\n')
                value[strlen(value) - 1] = '\0';

        if (false == is_digit_str(value))
                return atoi(_default);

        return atoi(value);
err_ret:
        return atoi(_default);
}

int nodectl_set(const char *key, const char *value)
{
        int ret;
        char path[MAX_PATH_LEN];

        snprintf(path, MAX_PATH_LEN, "%s/%s", NODECTL_PREFIX, key);

        ret = _set_value(path, value, strlen(value) + 1, O_CREAT | O_TRUNC);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int nodectl_register(const char *key, const char  *_default, fnotify_callback mod_callback,
                     fnotify_callback del_callback, void *context)
{
        int ret;
        char path[MAX_PATH_LEN];

        ret = nodectl_set(key, _default);
        if (ret)
                GOTO(err_ret, ret);

        snprintf(path, MAX_PATH_LEN, "%s/%s", NODECTL_PREFIX, key);
        ret = fnotify_register(path, mod_callback, del_callback, context);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int nodectl_unregister(const char *key)
{
        int ret;
        char path[MAX_PATH_LEN];

        snprintf(path, MAX_PATH_LEN, "%s/%s", NODECTL_PREFIX, key);
        ret = fnotify_unregister(path);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}


void nodectl_unlink(const char *key)
{
        char path[MAX_PATH_LEN];

        snprintf(path, MAX_PATH_LEN, "%s/%s", NODECTL_PREFIX, key);
        unlink(path);
}

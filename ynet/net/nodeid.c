#include <sys/types.h>
#include <regex.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <getopt.h>
#include <dirent.h>
#include <sys/statvfs.h>

#define DBG_SUBSYS S_YFSMDS

#include "ylib.h"
#include "configure.h"
#include "bmap.h"
#include "etcd.h"
#include "net_global.h"
#include "nodeid.h"
#include "net_table.h"
#include "schedule.h"
#include "ylog.h"
#include "dbg.h"
#include "bh.h"

//node id for 1 to NODEID_MAX, avoid 0

static int __nodeid_newid__(nodeid_t id, const char *name)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];

        snprintf(key, MAX_NAME_LEN, "%u", id);

retry:
        ret = etcd_create_text(ETCD_NID, key, name, 0);
        if (unlikely(ret)) {
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 10, (100 * 1000));
                } else {
                        GOTO(err_ret, ret);
                }
        }
        
        return 0;
err_ret:
        return ret;
}

static int __nodeid_newid(nodeid_t *_id, const char *name)
{
        int ret;
        nodeid_t i;

        for (i = 1; i < NODEID_MAX; i++) {
                if (!nodeid_used(i)) {
                        ret = __nodeid_newid__(i, name);
                        if (ret) {
                                if (ret == EEXIST) {
                                        DINFO("try %u fail, continue\n", i);
                                        continue;
                                } else
                                        GOTO(err_ret, ret);
                        }

                        *_id = i;
                        break;
                }
        }

        if (i == NODEID_MAX) {
                ret = ENOSPC;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int nodeid_load(nodeid_t *_id)
{
        int ret;
        char key[MAX_NAME_LEN], value[MAX_BUF_LEN];
        nid_t nid;

        sprintf(key, "%s/%s/nid", ng.home, YFS_STATUS_PRE);

        ret = _get_text(key, value, MAX_BUF_LEN);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        str2nid(&nid, value);

        *_id = nid.id;

        return 0;
err_ret:
        return ret;
}

int nodeid_init(nodeid_t *_id, const char *name)
{
        int ret;
        char key[MAX_NAME_LEN], value[MAX_BUF_LEN];
        nid_t nid;
        nodeid_t id;

        char nodename[MAX_NAME_LEN], hostname[MAX_NAME_LEN];
        ret = net_gethostname(hostname, MAX_NAME_LEN);
        if (ret)
                GOTO(err_ret, ret);
                
        snprintf(nodename, MAX_NAME_LEN, "%s:%s", hostname, name);
        
        ret = nodeid_load(&id);
        if (ret) {
                if (ret == ENOENT) {
                        //pass
                } else
                        GOTO(err_ret, ret);
        } else {
                ret = EEXIST;
                GOTO(err_ret, ret);
        }

        ret = __nodeid_newid(&id, nodename);
        if (ret)
                GOTO(err_ret, ret);
        
        sprintf(key, "%s/%s/nid", ng.home, YFS_STATUS_PRE);

        ret = path_validate(key, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (ret)
                GOTO(err_ret, ret);

        nid.id = id;
        nid2str(value, &nid);
        
        ret = _set_text(key, value, strlen(value) + 1, O_EXCL | O_CREAT);
        if (ret)
                GOTO(err_ret, ret);

        if (_id)
                *_id = id;
        
        return 0;
err_ret:
        return ret;
}

int nodeid_drop(nodeid_t id)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN];

        snprintf(key, MAX_NAME_LEN, "%u", id);
retry:
        ret = etcd_del(ETCD_NID, key);
        if (unlikely(ret)) {
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 10, (100 * 1000));
                } else {
                        UNIMPLEMENTED(__DUMP__);
                }
        }
        
        return 0;
err_ret:
        return ret;
}


int nodeid_used(nodeid_t id)
{
        int ret, retry = 0;
        char key[MAX_PATH_LEN], value[MAX_BUF_LEN];

        snprintf(key, MAX_NAME_LEN, "%u", id);
retry:
        ret = etcd_get_text(ETCD_NID, key, value, NULL);
        if (unlikely(ret)) {
                if (ret == ENOKEY) {
                        return 0;
                } else if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 10, (100 * 1000));
                } else {
                        GOTO(err_ret, ret);
                }
        }
        
        return 1;
err_ret:
        return 1;
}

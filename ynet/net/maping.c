#include <sys/types.h>
#include <dirent.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "net_global.h"
#include "ynet_rpc.h"
#include "job_dock.h"
#include "etcd.h"
#include "conn.h"
#include "schedule.h"
#include "configure.h"
#include "dbg.h"

#define MAPING_PATH     SHM_ROOT"/maping"

typedef struct {
        sy_spinlock_t lock;
        char prefix[MAX_PATH_LEN];
} maping_t;

static maping_t maping;

static void __replace(char *new, const char *old, char from, char to)
{
        char *tmp;
        strcpy(new, old);
        tmp = strchr(new, from);
        *tmp = to;
}

int maping_init()
{
        int ret;

        DINFO("maping init\n");
        ret = sy_spin_init(&maping.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        strcpy(maping.prefix, MAPING_PATH);

        if (ng.daemon) {
                ret = path_validate(maping.prefix, YLIB_ISDIR, YLIB_DIRCREATE);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int maping_get(const char *type, const char *_key, char *value, time_t *ctime)
{
        int ret, retry = 0;
        char path[MAX_PATH_LEN], tmp[MAX_PATH_LEN], crc_value[MAX_PATH_LEN];
        const char *key;
        uint32_t crc, _crc;
        struct stat stbuf;

        if (strchr(_key, '/')) {
                __replace(tmp, _key, '/', ':');
                YASSERT(strchr(tmp, '/') == 0);
                key = tmp;
        } else {
                key = _key;
        }

        snprintf(path, MAX_NAME_LEN, "%s/%s/%s", maping.prefix, type, key);

        ret = sy_spin_lock(&maping.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = _get_text(path, crc_value, MAX_BUF_LEN);
        if (ret < 0) {
                ret = -ret;
                goto err_lock;
        }

        if (ctime) {
                ret = stat(path, &stbuf);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_lock, ret);
                }      

                *ctime = stbuf.st_ctime;
        }
        
retry:
        if (!strcmp(NAME2NID, type)) {
                sscanf(crc_value, "%x %s", &crc, value);
                _crc = crc32_sum(value, strlen(value));
                if (_crc != crc) {
                        USLEEP_RETRY(err_unlink, ENOENT, retry, retry, 2, (100 * 1000));
                }
        } else {
                strcpy(value, crc_value);
        }
        //DWARN("path %s\n", path);

        sy_spin_unlock(&maping.lock);

        return 0;
err_unlink:
        DWARN("remove %s\n", path);
        unlink(path);
err_lock:
        sy_spin_unlock(&maping.lock);
err_ret:
        return ret;
}

int maping_set(const char *type, const char *_key, const char *value)
{
        int ret;
        char path[MAX_PATH_LEN], tmp[MAX_PATH_LEN], crc_value[MAX_PATH_LEN] = {0};
        const char *key;
        uint32_t crc;

        DBUG("set %s %s\n", type, _key);

        if (strchr(_key, '/')) {
                __replace(tmp, _key, '/', ':');
                YASSERT(strchr(tmp, '/') == 0);
                key = tmp;
        } else {
                key = _key;
        }

        snprintf(path, MAX_NAME_LEN, "%s/%s/%s", maping.prefix, type, key);

        crc = crc32_sum(value, strlen(value));
        if (!strcmp(NAME2NID, type)) {
                sprintf(crc_value, "%x %s", crc, value);
        } else {
                strcpy(crc_value, value);
        }

        ret = sy_spin_lock(&maping.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ret = _set_text(path, crc_value, strlen(crc_value), O_CREAT | O_TRUNC | O_SYNC);
        if (unlikely(ret))
                GOTO(err_lock, ret);

        sy_spin_unlock(&maping.lock);

        return 0;
err_lock:
        sy_spin_unlock(&maping.lock);
err_ret:
        return ret;
}

int maping_drop(const char *type, const char *_key)
{
        int ret;
        char path[MAX_PATH_LEN], tmp[MAX_PATH_LEN];
        const char *key;
        struct stat stbuf;

        DBUG("remove %s %s\n", type, _key);

        if (strchr(_key, '/')) {
                __replace(tmp, _key, '/', ':');
                YASSERT(strchr(tmp, '/') == 0);
                key = tmp;
        } else {
                key = _key;
        }

        snprintf(path, MAX_NAME_LEN, "%s/%s/%s", maping.prefix, type, key);

        ret = sy_spin_lock(&maping.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = stat(path, &stbuf);
        if (ret == 0) {
                DBUG("remove %s\n", path);
                unlink(path);
        }

        sy_spin_unlock(&maping.lock);

        return 0;
err_ret:
        return ret;
}

static int __maping_cleanup(const char *parent, const char *key, void *args)
{
        char path[MAX_PATH_LEN];

        (void) args;

        snprintf(path, MAX_NAME_LEN, "%s/%s", parent, key);

        DBUG("cleanup %s\n", path);

        unlink(path);

        return 0;
}

int maping_cleanup(const char *type)
{
        int ret;
        char path[MAX_PATH_LEN];

        DBUG("cleanup %s\n", type);

        snprintf(path, MAX_NAME_LEN, "%s/%s", maping.prefix, type);

        ret = sy_spin_lock(&maping.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = _dir_iterator(path, __maping_cleanup, NULL);
        if (unlikely(ret)) {
                //pass
        }

        sy_spin_unlock(&maping.lock);

        return 0;
err_ret:
        return ret;
}

static int __maping_setnetinfo__(const ynet_net_info_t *info)
{
        int ret;
        char buf[MAX_BUF_LEN], nidstr[MAX_NAME_LEN];

        netinfo2str(buf, info);
        nid2str(nidstr, &info->id);
        ret = maping_set(NID2NETINFO, nidstr, buf);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = maping_set(HOST2NID, info->name, nidstr);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int maping_host2nid(const char *hostname, nid_t *nid)
{
        int ret;
        char nidstr[MAX_NAME_LEN];

        ret = maping_get(HOST2NID, hostname, nidstr, NULL);
        if (unlikely(ret)) {
                YASSERT(ret == ENOENT);

                ret = etcd_get_text(ETCD_NODE, hostname, nidstr, NULL);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }

                ret = maping_set(HOST2NID, hostname, nidstr);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        str2nid(nid, nidstr);
        
        return 0;
err_ret:
        return ret;
}

#if 0

int maping_addr2nid(const char *addr, nid_t *nid)
{
        int ret;
        char hostname[MAX_NAME_LEN] = "";

        ret = ip2hostname(addr, hostname);
        if (ret <= 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        ret = maping_host2nid(hostname, nid);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

#endif

int maping_nid2host(const nid_t *nid, char *hostname)
{
        int ret;
        char buf[MAX_BUF_LEN];
        ynet_net_info_t *info;

        info = (void *)buf;
        ret = maping_nid2netinfo(nid, info);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        strcpy(hostname, info->name);
        
        return 0;
err_ret:
        return ret;
}

int maping_nid2netinfo(const nid_t *nid, ynet_net_info_t *info)
{
        int ret;
        char buf[MAX_BUF_LEN], nidstr[MAX_NAME_LEN];
        time_t ctime;

        nid2str(nidstr, nid);
        ret = maping_get(NID2NETINFO, nidstr, buf, &ctime);
        if (unlikely(ret)) {
                YASSERT(ret == ENOENT);

        retry:
                ret = conn_getinfo(nid, info);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }

                ret = __maping_setnetinfo__(info);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        } else {
                if (gettime() -  ctime > gloconf.rpc_timeout / 2) {
                        DBUG("drop %s\n", nidstr);
                        goto retry;
                }

                ret = str2netinfo(info, buf);
                YASSERT(ret == 0);
        }
        
        return 0;
err_ret:
        return ret;
}

static int __maping_getmaster__(nid_t *nid)
{
        int ret;
        etcd_lock_t lock;
        uint32_t magic;
        char host[MAX_NAME_LEN];

        ret = etcd_lock_init(&lock, ROLE_MOND, "master", gloconf.rpc_timeout, -1, -1);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = etcd_locker(&lock, host, nid, &magic, NULL);
        if (unlikely(ret)) {
                if (ret == ENOKEY) {
                        if (ng.daemon || gloconf.testing)
                                ret = EAGAIN;
                        else
                                ret = EHOSTDOWN;
                }

                GOTO(err_ret, ret);
        }

        (void) magic;

#if 0
        if (!ng.daemon) {
                ng.master_magic = magic;
        }
#endif
        
        return 0;
err_ret:
        return ret;
}

static int __maping_getmaster(nid_t *nid)
{
        int ret, retry = 0;
        time_t ctime, now;
        char nidstr[MAX_NAME_LEN];

retry:
        ret = maping_get(ROLE_MOND, "master", nidstr, &ctime);
        if (unlikely(ret)) {
                DBUG("get master fail\n");

                ret = __maping_getmaster__(nid);
                if (unlikely(ret))
                        GOTO(err_ret, ret);

                nid2str(nidstr, nid);
                ret = maping_set(ROLE_MOND, "master", nidstr);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        } else {
                now = gettime();
                if (ctime + gloconf.rpc_timeout / 2 < now || ctime > now) {
                        YASSERT(retry == 0);
                        DBUG("update master\n");
                        maping_drop(ROLE_MOND, "master");
                        retry = 1;
                        goto retry;
                } else {
                        DBUG("cached master\n");
                }

                str2nid(nid, nidstr);
        }

        return 0;
err_ret:
        return ret;
}

int maping_getmaster(nid_t *nid, int force)
{
        if (force || ng.master_magic == (uint32_t)-1) {
                return __maping_getmaster__(nid);
        } else {
                return __maping_getmaster(nid);
        }
}

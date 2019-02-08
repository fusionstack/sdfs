

#include <sys/types.h>
#include <sys/stat.h>
#include <rpc/rpc.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#define DBG_SUBSYS S_YNFS

#include "ylib.h"
#include "yfs_conf.h"
#include "ynfs_conf.h"
#include "net_proto.h"
#include "attr.h"
#include "error.h"
#include "dbg.h"


typedef struct {
        struct list_head list;
        net_handle_t nh;
        char host[MAX_PATH_LEN];
} mount_body_t;

typedef struct {
        sy_rwlock_t rwlock;

/*
 * number of active mounts
 *
 * only a guess since clients can crash and/or not sent UMNT calls
 */
        int mount_cnt;
        int orphan_cnt;
/* list of currently mounted host */
        struct list_head list;
} mount_list_t;

static mount_list_t mlist;

int mountlist_init(void)
{
        int ret;

        ret = sy_rwlock_init(&mlist.rwlock, NULL);
        if (ret)
                GOTO(err_ret, ret);

        mlist.mount_cnt = 0;
        mlist.orphan_cnt = 0;
        INIT_LIST_HEAD(&mlist.list);

        return 0;
err_ret:
        return ret;
}

int add_client(net_handle_t *nh)
{
        int ret;
        void *ptr;
        mount_body_t *mb;

        ret = ymalloc(&ptr, sizeof(mount_body_t));
        if (ret)
                GOTO(err_ret, ret);

        mb = (mount_body_t *)ptr;

        INIT_LIST_HEAD(&mb->list);
        mb->nh = *nh;
        _memset(mb->host, 0x0, MAX_PATH_LEN);

        ret = sy_rwlock_wrlock(&mlist.rwlock);
        if (ret)
                GOTO(err_ptr, ret);

        list_add_tail(&mb->list, &mlist.list);
        mlist.mount_cnt++;
        mlist.orphan_cnt++;

        sy_rwlock_unlock(&mlist.rwlock);

        return 0;
err_ptr:
        (void) yfree((void **)&ptr);
err_ret:
        return ret;
}

#if 0
static void reg_mount(net_handle_t *nh, ynfs_fh3_t *fh)
{
        int ret;
        char path[MAX_PATH_LEN];
        mount_body_t *mb;
        struct list_head *pos;

        (void) fh;

        if (mlist.orphan_cnt) {
                UNIMPLEMENTED(__WARN__);
#if 0
                ret = fh_decomroot(fh, path);
                if (ret) {
                        DERROR("invalid root\n");
                        return;
                }
#endif
 
                ret = sy_rwlock_wrlock(&mlist.rwlock);
                if (ret)
                        YASSERT(0);

                if (mlist.orphan_cnt) {
                        DINFO("orphan client %u, try to seek\n", mlist.orphan_cnt);

                        list_for_each(pos, &mlist.list) {
                                mb = (mount_body_t *)pos;
                                if (mb->nh.type == nh->type
                                    && mb->nh.u.sd.sd == nh->u.sd.sd
                                    && mb->nh.u.sd.seq == nh->u.sd.seq)
                                {
                                        if (strlen(mb->host) == 0) {
                                                strcpy(mb->host, path);
                                                mlist.orphan_cnt--;
                                        }
                                }
                        }
                }

                sy_rwlock_unlock(&mlist.rwlock);
        }
}
#endif

/*
 * add entry to mount list
 */
int add_mount(const char *path, struct in_addr sin)
{
        int ret, len;
        char *host;
        void *ptr;
        mount_body_t *mb;
	(void) path;

        /*
         * XXX The string is returned in a statically allocated buffer,
         * which  subsequent calls will overwrite.
         *
         * is it thread-safe ? -- yf
         */
        host = inet_ntoa(sin);
        if (host == NULL) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        DINFO("mount path %s from client %s\n", path, host);

        len = _strlen(host);

        ret = ymalloc(&ptr, sizeof(mount_body_t) + len + 1);
        if (ret)
                GOTO(err_ret, ret);

        mb = (mount_body_t *)ptr;

        INIT_LIST_HEAD(&mb->list);
        _memcpy(mb->host, host, len);

        ret = sy_rwlock_wrlock(&mlist.rwlock);
        if (ret)
                GOTO(err_ptr, ret);

        list_add_tail(&mb->list, &mlist.list);
        mlist.mount_cnt++;

        sy_rwlock_unlock(&mlist.rwlock);

        return 0;
err_ptr:
        (void) yfree((void **)&ptr);
err_ret:
        return ret;
}

/*
 * remove entries from mount list
 */
int remove_mount(const char *path, struct in_addr sin)
{
        int ret;
        char *host;
        mount_body_t *pos, *mb;
	(void) path;

        host = inet_ntoa(sin);

        mb = NULL;

        DINFO("umount path %s from client %s\n", path, host);

        ret = sy_rwlock_wrlock(&mlist.rwlock);
        if (ret)
                GOTO(err_ret, ret);

        list_for_each_entry(pos, &mlist.list, list)
                if (_strcmp(pos->host, host) == 0) {
                        mb = pos;
                        list_del_init(&mb->list);

                        if (mlist.mount_cnt > 0)
                                mlist.mount_cnt--;

                        break;
                }

        sy_rwlock_unlock(&mlist.rwlock);

        if (mb != NULL)
                yfree((void **)&mb);

        return 0;
err_ret:
        return ret;
}


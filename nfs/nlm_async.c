
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


#include "yfs_conf.h"
#include "ylib.h"
#include "nlm_async.h"
#include "attr.h"
#include "error.h"
#include "nlm_job_context.h"
#include "net_global.h"
#include "nfs_conf.h"
#include "nlm_events.h"
#include "nlm_state_machine.h"
#include "nlm_nsm.h"
#include "../sock/sock_tcp.h"
#include "sunrpc_proto.h"
#include "sunrpc_reply.h"
#include "sdfs_lib.h"
#include "xdr_nlm.h"
#include "configure.h"
#include "sdfs_conf.h"
#include "nlm_nsm.h"
#include "core.h"
#include "nfs_events.h"
#include "yfs_limit.h"
#include "sdfs_id.h"
#include "dbg.h"

#define NLM4_ASYNC_HASH  10;

typedef struct {
        struct list_head hook;
        fileid_t fileid;
        int cancel;
        sdfs_lock_t *lock;
        char _lock[MAX_LOCK_LEN];
} nlm4_async_entry_t;

typedef struct {
        sy_spinlock_t lock;
        struct list_head list;
} nlm4_async_t;

static nlm4_async_t *__nlm4_async__;


int nlm4_async_cancel(const fileid_t *fileid, const sdfs_lock_t *lock)
{
        int ret, found = 0;
        nlm4_async_t *nlm4_async = __nlm4_async__;
        struct list_head *pos;
        nlm4_async_entry_t *ent;
        
        ret = sy_spin_lock(&nlm4_async->lock);
        if (ret)
                GOTO(err_ret, ret);

        list_for_each(pos, &nlm4_async->list) {
                ent = (void *)pos;

                if (sdfs_lock_equal(fileid, lock, &ent->fileid, ent->lock)) {
                        ent->cancel = 1;
                        found = 1;
                }
        }
        
        sy_spin_unlock(&nlm4_async->lock);

        if (found == 0) {
                DINFO("cancel "CHKID_FORMAT", not found \n", CHKID_ARG(fileid));
        }
        
        return 0;
err_ret:
        return ret;
}

int nlm4_async_canceled(const fileid_t *fileid, const sdfs_lock_t *lock)
{
        int ret;
        nlm4_async_t *nlm4_async = __nlm4_async__;
        struct list_head *pos;
        nlm4_async_entry_t *ent;
        
        ret = sy_spin_lock(&nlm4_async->lock);
        if (ret)

        list_for_each(pos, &nlm4_async->list) {
                ent = (void *)pos;

                if (sdfs_lock_equal(fileid, lock, &ent->fileid, ent->lock)) {
                        sy_spin_unlock(&nlm4_async->lock);
                        return 1;
                }
        }
        
        sy_spin_unlock(&nlm4_async->lock);

        return 0;
}

int nlm4_async_reg(const fileid_t *fileid, const sdfs_lock_t *lock)
{
        int ret;
        nlm4_async_t *nlm4_async = __nlm4_async__;
        nlm4_async_entry_t *ent;

        DINFO("reg "CHKID_FORMAT" \n", CHKID_ARG(fileid));
        ret = ymalloc((void**)&ent, sizeof(*ent));
        if (ret)
                GOTO(err_ret, ret);

        ent->cancel = 0;
        ent->fileid = *fileid;
        ent->lock = (void *)ent->_lock;
        memcpy(ent->lock, lock, SDFS_LOCK_SIZE(lock));
       
        ret = sy_spin_lock(&nlm4_async->lock);
        if (ret)
                GOTO(err_ret, ret);

        list_add_tail(&ent->hook, &nlm4_async->list);

        sy_spin_unlock(&nlm4_async->lock);
        
        return 0;
err_ret:
        return ret;
}

int nlm4_async_unreg(const fileid_t *fileid, const sdfs_lock_t *lock)
{
        int ret, found = 0;
        nlm4_async_t *nlm4_async = __nlm4_async__;
        struct list_head *pos, *n;
        nlm4_async_entry_t *ent;
        
        ret = sy_spin_lock(&nlm4_async->lock);
        if (ret)
                GOTO(err_ret, ret);

        list_for_each_safe(pos, n, &nlm4_async->list) {
                ent = (void *)pos;

                if (sdfs_lock_equal(fileid, lock, &ent->fileid, ent->lock)) {
                        YASSERT(ent->cancel == 0);
                        list_del(&ent->hook);
                        yfree((void **)&ent);
                        found = 1;
                        break;
                }
        }
        
        sy_spin_unlock(&nlm4_async->lock);

        YASSERT(found == 1);
        
        return 0;
err_ret:
        return ret;
}

int nlm4_async_init()
{
        int ret;
        nlm4_async_t *nlm4_async;

        ret = ymalloc((void**)&nlm4_async, sizeof(*nlm4_async));
        if (ret)
                GOTO(err_ret, ret);

        memset(nlm4_async, 0x0, sizeof(*nlm4_async));

        INIT_LIST_HEAD(&nlm4_async->list);

        ret = sy_spin_init(&nlm4_async->lock);
        if (ret)
                GOTO(err_ret, ret);
        
        __nlm4_async__ = nlm4_async;


        return 0;
err_ret:
        return ret;
}

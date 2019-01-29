#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/resource.h>
#include <unistd.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <semaphore.h>
#include <poll.h> 
#include <pthread.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSLIB

#include "ynet_rpc.h"
#include "job_dock.h"
#include "net_global.h"
#include "ynet_rpc.h"
#include "rpc_proto.h"
#include "mond_rpc.h"
#include "md_lib.h"
#include "network.h"
#include "diskpool.h"
#include "nodepool.h"
#include "mond_kv.h"
#include "mem_cache.h"
#include "schedule.h"
#include "dbg.h"

extern net_global_t ng;

typedef enum {
        MOND_NULL = 400,
        MOND_GETSTAT,
        MOND_DISKHB,
        MOND_NEWDISK,
        MOND_DISKJOIN,
        MOND_STATVFS,
        MOND_SET,
        MOND_GET,
        MOND_MAX,
} mond_op_t;

typedef struct {
        uint32_t op;
        uint32_t buflen;
        chkid_t  chkid;
        char buf[0];
} msg_t;

extern int mond_ismaster();

extern disk_stat_t getdiskstat(const diskinfo_stat_t *diskstat);

static __request_handler_func__  __request_handler__[MOND_MAX - MOND_NULL];
static char  __request_name__[MOND_MAX - MOND_NULL][__RPC_HANDLER_NAME__ ];

static void __request_set_handler(int op, __request_handler_func__ func, const char *name)
{
        YASSERT(strlen(name) + 1 < __RPC_HANDLER_NAME__ );
        strcpy(__request_name__[op - MOND_NULL], name);
        __request_handler__[op - MOND_NULL] = func;
}

static void __request_get_handler(int op, __request_handler_func__ *func, const char **name)
{
        *func = __request_handler__[op - MOND_NULL];
        *name = __request_name__[op - MOND_NULL];
}

inline static void __getmsg(buffer_t *buf, msg_t **_req, int *buflen, char *_buf)
{
        msg_t *req;

        YASSERT(buf->len <= MEM_CACHE_SIZE4K);

        req = (void *)_buf;
        *buflen = buf->len - sizeof(*req);
        mbuffer_get(buf, req, buf->len);

        *_req = req;
}

static void __request_handler(void *arg)
{
        int ret;
        msg_t req;
        sockid_t sockid;
        msgid_t msgid;
        buffer_t buf;
        __request_handler_func__ handler;
        const char *name;

        request_trans(arg, NULL, &sockid, &msgid, &buf, NULL);

        if (buf.len < sizeof(req)) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        mbuffer_get(&buf, &req, sizeof(req));

        DBUG("new op %u from %s, id (%u, %x)\n", req.op,
             _inet_ntoa(sockid.addr), msgid.idx, msgid.figerprint);

        __request_get_handler(req.op, &handler, &name);
        if (handler == NULL) {
                ret = ENOSYS;
                DWARN("error op %u\n", req.op);
                GOTO(err_ret, ret);
        }

        schedule_task_setname(name);

        ret = handler(&sockid, &msgid, &buf);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        mbuffer_free(&buf);

        DBUG("reply op %u from %s, id (%u, %x)\n", req.op,
              _inet_ntoa(sockid.addr), msgid.idx, msgid.figerprint);

        return ;
err_ret:
        mbuffer_free(&buf);
        rpc_reply_error(&sockid, &msgid, ret);
        DBUG("error op %u from %s, id (%u, %x)\n", req.op,
             _inet_ntoa(sockid.addr), msgid.idx, msgid.figerprint);
        return;
}

static int __mond_srv_getstat(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf)
{
        int ret;
        msg_t *req;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t buflen;
        const nid_t *nid;
        instat_t instat;

        req = (void *)buf;
        mbuffer_get(_buf, req, sizeof(*req));
        buflen = req->buflen;
        ret = mbuffer_popmsg(_buf, req, buflen + sizeof(*req));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        _opaque_decode(req->buf, buflen, &nid, NULL, NULL);

        DINFO("getstat of %s\n", network_rname(nid));

        instat.nid = *nid;
        ret = network_connect(nid, NULL,0 , 0);
        if (unlikely(ret)) {
                instat.online = 0;
        } else {
                instat.online = 1;
        }

        rpc_reply(sockid, msgid, &instat, sizeof(instat));

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

int mond_rpc_getstat(const nid_t *nid, instat_t *instat)
{
        int ret, size = sizeof(*instat);
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t count;
        msg_t *req;

        ret = network_connect_mond(0);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ANALYSIS_BEGIN(0);

        req = (void *)buf;
        req->op = MOND_GETSTAT;
        _opaque_encode(&req->buf, &count, nid, sizeof(*nid), NULL);

        req->buflen = count;

        ret = rpc_request_wait("mond_rpc_getstat", net_getadmin(),
                               req, sizeof(*req) + count, instat, &size,
                               MSG_MOND, 0, _get_timeout());
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

static int __mond_srv_diskhb(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf)
{
        int ret;
        msg_t *req;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t buflen;
        const nid_t *nid;
        int *tier;
        const diskinfo_stat_diff_t *diff;
        const uuid_t *uuid;
        const volinfo_t *volinfo;

        req = (void *)buf;
        mbuffer_get(_buf, req, sizeof(*req));
        buflen = req->buflen;
        ret = mbuffer_popmsg(_buf, req, buflen + sizeof(*req));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        _opaque_decode(req->buf, buflen,
                       &nid, NULL,
                       &tier, NULL,
                       &uuid, NULL,
                       &diff, NULL,
                       &volinfo, NULL,
                       NULL);

        char _uuid[MAX_NAME_LEN];
        uuid_unparse(*uuid, _uuid);
        DINFO("hb disk %s uuid %s\n", network_rname(nid), _uuid);
        
        ret = diskpool_hb(nid, *tier, diff, volinfo->volreptnum,
                          volinfo->volrept, uuid);
        if (ret)
                GOTO(err_ret, ret);
        
        rpc_reply(sockid, msgid, NULL, 0);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

int mond_rpc_diskhb(const nid_t *nid, int tier, const uuid_t *uuid,
                    const diskinfo_stat_diff_t *diff,
                    const volinfo_t *volinfo)
{
        int ret;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t count;
        msg_t *req;

        char _uuid[MAX_NAME_LEN];
        uuid_unparse(*uuid, _uuid);
        DBUG("hb disk %s uuid %s\n", network_rname(nid), _uuid);
        
        ret = network_connect_mond(0);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ANALYSIS_BEGIN(0);
        
        req = (void *)buf;
        req->op = MOND_DISKHB;
        _opaque_encode(&req->buf, &count,
                       nid, sizeof(*nid),
                       &tier, sizeof(tier),
                       uuid, sizeof(*uuid),
                       diff, sizeof(*diff),
                       volinfo, sizeof(*volinfo),
                       NULL);

        req->buflen = count;

        ret = rpc_request_wait("mond_rpc_diskhb", net_getadmin(),
                               req, sizeof(*req) + count, NULL, NULL,
                               MSG_MOND, 0, _get_timeout());
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

static int __mond_srv_newdisk(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf)
{
        int ret;
        msg_t *req;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t buflen;
        const nid_t *nid;
        const uint32_t *repnum, *hardend, *tier;

        req = (void *)buf;
        mbuffer_get(_buf, req, sizeof(*req));
        buflen = req->buflen;
        ret = mbuffer_popmsg(_buf, req, buflen + sizeof(*req));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        _opaque_decode(req->buf, buflen,
                       &nid, NULL,
                       &tier, NULL,
                       &repnum, NULL,
                       &hardend, NULL,
                       NULL);

        char _array[MAX_BUF_LEN], _diskid[MAX_BUF_LEN];
        net_handle_t *array = (void *)_array;
        diskid_t *diskid = (void *)_diskid;

        DBUG("repnum %u hardend %u tier %u\n", *repnum, *hardend, *tier);

        YASSERT(*tier == 0);
        
        ret = nodepool_get(*repnum, array, *hardend, *tier);
        if (ret) {
                GOTO(err_ret, ret);
        }

        for (uint32_t i = 0; i < *repnum; i++) {
                diskid[i] = array[i].u.nid;
        }
        
        rpc_reply(sockid, msgid, diskid, sizeof(*diskid) * (*repnum));

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

int mond_rpc_newdisk(const nid_t *nid, uint32_t tier, uint32_t repnum,
                     uint32_t hardend, diskid_t *disks)
{
        int ret, replen;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t count;
        msg_t *req;

        ANALYSIS_BEGIN(0);
        
        ret = network_connect_mond(0);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        req = (void *)buf;
        req->op = MOND_NEWDISK;
        _opaque_encode(&req->buf, &count,
                       nid, sizeof(*nid),
                       &tier, sizeof(tier),
                       &repnum, sizeof(repnum),
                       &hardend, sizeof(hardend),
                       NULL);

        req->buflen = count;

        replen = sizeof(*disks) * repnum;
        ret = rpc_request_wait("mond_rpc_newdisk", net_getadmin(),
                               req, sizeof(*req) + count, disks, &replen,
                               MSG_MOND, 0, _get_timeout());
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

static int __mond_srv_diskjoin(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf)
{
        int ret;
        msg_t *req;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t buflen;
        const nid_t *nid;
        int *tier;
        const uuid_t *uuid;
        const diskinfo_stat_t *stat;

        req = (void *)buf;
        mbuffer_get(_buf, req, sizeof(*req));
        buflen = req->buflen;
        ret = mbuffer_popmsg(_buf, req, buflen + sizeof(*req));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        _opaque_decode(req->buf, buflen,
                       &nid, NULL,
                       &tier, NULL,
                       &uuid, NULL,
                       &stat, NULL,
                       NULL);

        char _uuid[MAX_NAME_LEN];
        uuid_unparse(*uuid, _uuid);
        DINFO("hb disk %s uuid %s\n", network_rname(nid), _uuid);

        ret = diskpool_join(nid, stat);
        if (ret) {
                GOTO(err_ret, ret);
        }

        disk_stat_t diskstat;
        diskstat = getdiskstat(stat);
        if (diskstat == DISK_STAT_FREE) {
                ret = nodepool_addisk(uuid, nid, *tier);
                if (ret)
                        GOTO(err_ret, ret);
        }
        
        rpc_reply(sockid, msgid, NULL, 0);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

int mond_rpc_diskjoin(const nid_t *nid, uint32_t tier, const uuid_t *uuid,
                      const diskinfo_stat_t *stat)
{
        int ret;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t count;
        msg_t *req;

        ret = network_connect_mond(0);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ANALYSIS_BEGIN(0);

        char _uuid[MAX_NAME_LEN];
        uuid_unparse(*uuid, _uuid);
        DINFO("join disk %s uuid %s\n", network_rname(nid), _uuid);
        
        req = (void *)buf;
        req->op = MOND_DISKJOIN;
        _opaque_encode(&req->buf, &count,
                       nid, sizeof(*nid),
                       &tier, sizeof(tier),
                       uuid, sizeof(*uuid),
                       stat, sizeof(*stat),
                       NULL);

        req->buflen = count;

        ret = rpc_request_wait("mond_rpc_diskjoin", net_getadmin(),
                               req, sizeof(*req) + count, NULL, NULL,
                               MSG_MOND, 0, _get_timeout());
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

static int __mond_srv_statvfs(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf)
{
        int ret;
        msg_t *req;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t buflen;
        const nid_t *nid;
        const fileid_t *fileid;
        struct statvfs svbuf;
        diskinfo_stat_t stat;

        req = (void *)buf;
        mbuffer_get(_buf, req, sizeof(*req));
        buflen = req->buflen;
        ret = mbuffer_popmsg(_buf, req, buflen + sizeof(*req));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        _opaque_decode(req->buf, buflen,
                       &nid, NULL,
                       &fileid, NULL,
                       NULL);
  
        _memset(&stat, 0x0, sizeof(diskinfo_stat_t));

        ret = diskpool_statvfs(&stat);
        if (ret)
                GOTO(err_ret, ret);

        DUMP_DISKSTAT(&stat);

        DBUG("frsize %llu, bsize %llu\n", (LLU)stat.ds_frsize, (LLU)stat.ds_bsize);
        if (stat.ds_frsize == 0) {
                memset(&stat, 0x0, sizeof(struct statvfs));
        }

        DISKSTAT2FSTAT(&stat, &svbuf);
        DBUG("total %llu free %llu avail %llu\n", (LLU)stat.ds_bsize * stat.ds_blocks,
             (LLU)stat.ds_bfree * stat.ds_bsize, (LLU)stat.ds_bavail*stat.ds_bsize);

        rpc_reply(sockid, msgid, &svbuf, sizeof(svbuf));

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

int mond_rpc_statvfs(const nid_t *nid, const fileid_t *fileid, struct statvfs *stbuf)
{
        int ret;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t count;
        msg_t *req;

        ret = network_connect_mond(0);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ANALYSIS_BEGIN(0);

        req = (void *)buf;
        req->op = MOND_STATVFS;
        _opaque_encode(&req->buf, &count,
                       nid, sizeof(*nid),
                       fileid, sizeof(*fileid),
                       NULL);

        req->buflen = count;

        int replen = sizeof(*stbuf);
        ret = rpc_request_wait("mond_rpc_statvfs", net_getadmin(),
                               req, sizeof(*req) + count, stbuf, &replen,
                               MSG_MOND, 0, _get_timeout());
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

static int __mond_srv_null(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf)
{
        int ret;
        msg_t *req;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t buflen;

        req = (void *)buf;
        mbuffer_get(_buf, req, sizeof(*req));
        buflen = req->buflen;
        ret = mbuffer_popmsg(_buf, req, buflen + sizeof(*req));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (!mond_ismaster()) {
                ret = ENOSYS;
                GOTO(err_ret, ret);
        }
        
        rpc_reply(sockid, msgid, NULL, 0);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

int mond_rpc_null(const nid_t *mond)
{
        int ret;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t count;
        msg_t *req;
        
        ANALYSIS_BEGIN(0);

        req = (void *)buf;
        req->op = MOND_NULL;
        count = 0;
        req->buflen = count;

        ret = rpc_request_wait("mond_rpc_null", mond,
                               req, sizeof(*req) + count, NULL, NULL,
                               MSG_MOND, 0, _get_timeout());
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

static int __mond_srv_set(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf)
{
        int ret;
        msg_t *req;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t buflen;
        const nid_t *nid;
        const char *path;
        const char *value;
        uint32_t valuelen;

        req = (void *)buf;
        mbuffer_get(_buf, req, sizeof(*req));
        buflen = req->buflen;
        ret = mbuffer_popmsg(_buf, req, buflen + sizeof(*req));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        _opaque_decode(req->buf, buflen,
                       &nid, NULL,
                       &path, NULL,
                       &value, &valuelen,
                       NULL);

        DINFO("set %s, valuelen %u\n", path, valuelen);

        ret = mond_kv_set(path, value, valuelen);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        rpc_reply(sockid, msgid, NULL, 0);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

int mond_rpc_set(const nid_t *nid, const char *path, const char *value, uint32_t valuelen)
{
        int ret;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t count;
        msg_t *req;

        ret = network_connect_mond(0);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ANALYSIS_BEGIN(0);

        req = (void *)buf;
        req->op = MOND_SET;
        _opaque_encode(&req->buf, &count,
                       nid, sizeof(*nid),
                       path, strlen(path) + 1,
                       value, valuelen,
                       NULL);

        req->buflen = count;

        ret = rpc_request_wait("mond_rpc_set", net_getadmin(),
                               req, sizeof(*req) + count, NULL, NULL,
                               MSG_MOND, 0, _get_timeout());
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

#if 1
static int __mond_srv_get(const sockid_t *sockid, const msgid_t *msgid, buffer_t *_buf)
{
        int ret;
        msg_t *req;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t buflen;
        const nid_t *nid;
        const char *path;
        const uint64_t *offset;
        char *value;
        uint32_t valuelen;

        req = (void *)buf;
        mbuffer_get(_buf, req, sizeof(*req));
        buflen = req->buflen;
        ret = mbuffer_popmsg(_buf, req, buflen + sizeof(*req));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        _opaque_decode(req->buf, buflen,
                       &nid, NULL,
                       &path, NULL,
                       &offset, NULL,
                       NULL);

        DINFO("get %s\n", path);

        ret = ymalloc((void **)&value, MON_ENTRY_MAX);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ret = mond_kv_get(path, *offset, value, &valuelen);
        if (unlikely(ret))
                GOTO(err_free, ret);
        
        rpc_reply(sockid, msgid, value, valuelen);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_free:
        yfree((void **)&value);
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}

int mond_rpc_get(const nid_t *nid, const char *path, uint64_t offset, void *value, int *valuelen)
{
        int ret;
        char *buf = mem_cache_calloc1(MEM_CACHE_4K, PAGE_SIZE);
        uint32_t count;
        msg_t *req;

        ret = network_connect_mond(0);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ANALYSIS_BEGIN(0);

        req = (void *)buf;
        req->op = MOND_GET;
        _opaque_encode(&req->buf, &count,
                       nid, sizeof(*nid),
                       path, strlen(path) + 1,
                       &offset, sizeof(offset),
                       NULL);

        req->buflen = count;

        ret = rpc_request_wait("mond_rpc_get", net_getadmin(),
                               req, sizeof(*req) + count, value, valuelen,
                               MSG_MOND, 0, _get_timeout());
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        mem_cache_free(MEM_CACHE_4K, buf);

        return 0;
err_ret:
        mem_cache_free(MEM_CACHE_4K, buf);
        return ret;
}
#endif


int mond_rpc_init()
{
        int ret;
        
        DINFO("mond rpc init\n");

        ret = mond_kv_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        //__request_set_handler(MOND_READ, __mond_srv_read, "mond_srv_read");
        __request_set_handler(MOND_NULL, __mond_srv_null, "mond_srv_null");
        __request_set_handler(MOND_GETSTAT, __mond_srv_getstat, "mond_srv_getstat");
        __request_set_handler(MOND_DISKHB, __mond_srv_diskhb, "mond_srv_diskhb");
        __request_set_handler(MOND_NEWDISK, __mond_srv_newdisk, "mond_srv_newdisk");
        __request_set_handler(MOND_DISKJOIN, __mond_srv_diskjoin, "mond_srv_diskjoin");
        __request_set_handler(MOND_STATVFS, __mond_srv_statvfs, "mond_srv_statvfs");
        __request_set_handler(MOND_GET, __mond_srv_get, "mond_srv_get");
        __request_set_handler(MOND_SET, __mond_srv_set, "mond_srv_set");
        
        if (ng.daemon) {
                rpc_request_register(MSG_MOND, __request_handler, NULL);

#if 0
                corerpc_register(MSG_MOND, __request_handler, NULL);
#endif
        }
        
        return 0;
err_ret:
        return ret;
}

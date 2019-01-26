
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>

#define DBG_SUBSYS S_YFSLIB

#include "sdfs_id.h"
#include "aiocb.h"
#include "md_lib.h"
#include "chk_proto.h"
#include "network.h"
#include "net_global.h"
#include "chk_proto.h"
#include "file_table.h"
#include "job_dock.h"
#include "ylib.h"
#include "net_global.h"
#include "yfs_file.h"
#include "cache.h"
#include "sdfs_lib.h"
#include "sdfs_chunk.h"
#include "network.h"
#include "yfs_limit.h"
#include "schedule.h"
#include "worm_cli_lib.h"
#include "main_loop.h"
#include "posix_acl.h"
#include "io_analysis.h"
#include "flock.h"
#include "xattr.h"
#include "dbg.h"


typedef struct {
        sem_t sem;
        int   ret;
} __block_t;

#define YFS_WRITE_SEG_MAX 8

#pragma pack(8)

typedef struct {
        chkop_type_t op;
        uint32_t __pad__;
        chkid_t chkid;
        uint32_t offset;
        uint32_t size;
        uint32_t crc_count;
        uint32_t __pad1__;
        uint64_t version;
        uint32_t crc[0];
} wseg_head_t;

#pragma pack()

typedef struct {
        struct list_head hook;
        wseg_head_t head;
        uint32_t __pad__[(Y_BLOCK_MAX / YFS_CRC_SEG_LEN) + 1];
        buffer_t buf;
        chkinfo_t chkinfo;
        ynet_net_nid_t __pad__1[YFS_CHK_REP_MAX];
} wseg_t;

typedef struct {
        fileid_t fileid;
        buffer_t *buf;
        uint32_t size;
        uint64_t offset;
        int (*callback)(void *, int);
        void *arg;
} sdfs_read_ctx_t;

typedef struct {
        fileid_t fileid;
        const buffer_t *buf;
        uint32_t size;
        uint64_t offset;
        int (*callback)(void *, int);
        void *arg;
} sdfs_write_ctx_t;

int sdfs_read(const fileid_t *fileid, buffer_t *_buf, uint32_t size, uint64_t offset)
{
        int ret, retry = 0, chkno = -1;
        fileinfo_t _md;
        fileinfo_t *md = &_md;
        chkid_t chkid;
        uint32_t chk_size;
        uint32_t chk_off;
        ec_t ec;
        buffer_t buf;

        ANALYSIS_BEGIN(0);
        
        DBUG("fileid "FID_FORMAT" size %llu off %llu size %u\n", FID_ARG(&md->fileid),
              (LLU)md->at_size, (LLU)offset, size);

retry:
        ret = md_getattr((void *)md, fileid);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        if (offset > md->at_size) {
                DWARN("fileid "FID_FORMAT" size %llu off %llu size %u\n", FID_ARG(&md->fileid),
                                (LLU)md->at_size, (LLU)offset, size);
                goto out;
        }

        if (size + offset > md->at_size) {
                DWARN("fileid "FID_FORMAT" size %llu off %llu size %u\n", FID_ARG(&md->fileid),
                                (LLU)md->at_size, (LLU)offset, size);
                size = md->at_size - offset;
        }

        ec.plugin = md->plugin;
        ec.tech = md->tech;
        ec.m = md->m;
        ec.k = md->k;

        while (size) {
                chkno = offset / md->split;
                fid2cid(&chkid, fileid, chkno);

                chk_off = offset % md->split;
                chk_size = (chk_off + size)
                        < md->split ? size
                        : (md->split - chk_off);
                chk_size = chk_size < Y_BLOCK_MAX ? chk_size : Y_BLOCK_MAX;
                YASSERT(chk_size <= md->split);

                mbuffer_init(&buf, 0);
                ret = sdfs_chunk_read(&chkid, &buf, chk_size, chk_off, &ec);
                if (ret) {
                        if (ret == ENOENT) {
                                mbuffer_appendzero(&buf, chk_size);
                        } else {
                                GOTO(err_ret, ret);
                        }
                }

                size -= chk_size;
                YASSERT(buf.len == chk_size);
                mbuffer_merge(_buf, &buf);
        }

out:
        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        ret = io_analysis(ANALYSIS_READ, size);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}


static void __sdfs_read_async(void *_arg)
{
        int ret;
        sdfs_read_ctx_t *ctx = _arg;

        ret = sdfs_read(&ctx->fileid, ctx->buf, ctx->size, ctx->offset);
        if (ret)
                GOTO(err_ret, ret);

        ctx->callback(ctx->arg, ctx->buf->len);
        yfree((void **)&ctx);

        return;
err_ret:
        ret = -ret;
        ctx->callback(ctx->arg, ret);
        yfree((void **)&ctx);
        return;
        
}

int sdfs_read_async(const fileid_t *fileid, buffer_t *buf, uint32_t size,
                  uint64_t off, int (*callback)(void *, int), void *obj)
{
        int ret;
        sdfs_read_ctx_t *ctx;

        YASSERT(fileid->id);
        YASSERT(fileid->volid);

        DBUG("read off %llu size %u\n", (LLU)off, size);

        ret = ymalloc((void **)&ctx, sizeof(*ctx));
        if (ret)
                GOTO(err_ret, ret);

        ctx->fileid = *fileid;
        ctx->buf = buf;
        ctx->size = size;
        ctx->offset = off;
        ctx->callback = callback;
        ctx->arg = obj;

        ret = main_loop_request(__sdfs_read_async, ctx, "sdfs_read_async");
        if (ret)
                GOTO(err_free, ret);

        return 0;
err_free:
        yfree((void **)&ctx);
err_ret:
        return ret;
}

static int __resume_sem(void *obj, int retval)
{
        __block_t *blk;

        blk = obj;
        blk->ret = retval;
        sem_post(&blk->sem);

        return 0;
}

int sdfs_read_sync(fileid_t *fileid, buffer_t *buf, uint32_t size, uint64_t off)
{
        int ret;
        __block_t blk;

        mbuffer_init(buf, 0);
        sem_init(&blk.sem, 0, 0);
        blk.ret = 0;

        ret = sdfs_read_async(fileid, buf, size, off,
                              __resume_sem, &blk);
        if (ret)
                GOTO(err_ret, ret);

        ret = _sem_wait(&blk.sem);
        if (ret) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        ret = blk.ret;
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        return blk.ret;
err_ret:
        return -ret;
}

static int __sdfs_write_split(wseg_t *segs, int *count, buffer_t *buf, uint32_t _size,
                   uint64_t _offset, uint32_t chk_max, const fileid_t *fileid)
{
        int ret, i;
        uint32_t size;
        uint64_t offset;
        wseg_t *seg;

        offset = _offset;
        size = _size;
        YASSERT(buf->len == _size);

        for (i = 0; size > 0; i++) {
                YASSERT(i < YFS_WRITE_SEG_MAX);

                seg = &segs[i];
                fid2cid(&seg->head.chkid, fileid, offset / chk_max);
                seg->head.op = CHKOP_WRITE;
                seg->head.__pad__ = 0;
                seg->head.offset = offset % chk_max;
                seg->head.size = (seg->head.offset + size) < chk_max
                        ? size : (chk_max - seg->head.offset);
                seg->head.size = seg->head.size < Y_BLOCK_MAX
                        ? seg->head.size: Y_BLOCK_MAX;
                seg->head.version = 0;
                size -= seg->head.size;
                offset += seg->head.size;

                YASSERT(seg->head.size + seg->head.offset <= chk_max);

                mbuffer_init(&seg->buf, 0);

                ret = mbuffer_pop(buf, &seg->buf, seg->head.size);
                if (ret)
                        GOTO(err_ret, ret);

                YASSERT(seg->buf.len == seg->head.size);
        }

        YASSERT(buf->len == 0);

        *count = i;

        YASSERT(*count);

        return 0;
err_ret:
        return ret;
}

int sdfs_write(const fileid_t *fileid, const buffer_t *_buf, uint32_t size, uint64_t offset)
{
        int ret, retry = 0;
        fileinfo_t _md;
        fileinfo_t *md = &_md;
        ec_t ec;
        wseg_t seg_array[YFS_WRITE_SEG_MAX], *seg;
        int i, seg_count;
        buffer_t newbuf;

        ANALYSIS_BEGIN(0);
        
        YASSERT(_buf->len == size);

        DBUG("write "CHKID_FORMAT"\n", CHKID_ARG(fileid));

        mbuffer_init(&newbuf, 0);
        mbuffer_reference(&newbuf, _buf);
        
retry:
        ret = md_getattr((void *)md, fileid);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        if (!S_ISREG(md->at_mode)) {
                if (S_ISDIR(md->at_mode))
                        ret = EISDIR;
                else
                        ret = EINVAL;    
                GOTO(err_ret, ret);
        }

        YASSERT(md->split);
        YASSERT(md->fileid.id);
        YASSERT(md->fileid.volid);

        ret = __sdfs_write_split(seg_array, &seg_count, &newbuf, size,
                                 offset, md->split, &md->fileid);
        if (ret)
                GOTO(err_ret, ret);

        ec.plugin = md->plugin;
        ec.tech = md->tech;
        ec.m = md->m;
        ec.k = md->k;

        for (i = 0; i < seg_count; i++) {
                seg = &seg_array[i];
                ret = sdfs_chunk_write(md, &seg->head.chkid, &seg->buf,
                                       seg->head.size, seg->head.offset, &ec);
                if (ret) {
                        GOTO(err_free, ret);
                }
        }

        for (i = 0; i < seg_count; i++) {
                seg = &seg_array[i];
                mbuffer_free(&seg->buf);
        }
        
retry1:
        ret = md_extend(fileid, size + offset);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry1, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        mbuffer_free(&newbuf);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);

        ret = io_analysis(ANALYSIS_WRITE, size);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_free:
        for (i = 0; i < seg_count; i++) {
                seg = &seg_array[i];
                mbuffer_free(&seg->buf);
        }
err_ret:
        mbuffer_free(&newbuf);
        return ret;
}

static void __sdfs_write_async(void *_arg)
{
        int ret;
        sdfs_write_ctx_t *ctx = _arg;

        ret = sdfs_write(&ctx->fileid, ctx->buf, ctx->size, ctx->offset);
        if (ret)
                GOTO(err_ret, ret);

        ctx->callback(ctx->arg, ctx->buf->len);
        yfree((void **)&ctx);

        return;
err_ret:
        ret = -ret;
        ctx->callback(ctx->arg, ret);
        yfree((void **)&ctx);
        return;
        
}

int sdfs_write_async(const fileid_t *fileid, const buffer_t *buf, uint32_t size,
                  uint64_t off, int (*callback)(void *, int), void *obj)
{
        int ret;
        sdfs_write_ctx_t *ctx;

        YASSERT(fileid->id);
        YASSERT(fileid->volid);

        DBUG("write off %llu size %u\n", (LLU)off, size);

        ret = ymalloc((void **)&ctx, sizeof(*ctx));
        if (ret)
                GOTO(err_ret, ret);

        ctx->fileid = *fileid;
        ctx->buf = buf;
        ctx->size = size;
        ctx->offset = off;
        ctx->callback = callback;
        ctx->arg = obj;

        ret = main_loop_request(__sdfs_write_async, ctx, "sdfs_write_async");
        if (ret)
                GOTO(err_free, ret);

        return 0;
err_free:
        yfree((void **)&ctx);
err_ret:
        return ret;
}

int sdfs_write_sync(fileid_t *fileid, const buffer_t *buf, uint32_t size, uint64_t off)
{
        int ret;
        __block_t blk;

        sem_init(&blk.sem, 0, 0);
        blk.ret = 0;

        ret = sdfs_write_async(fileid, buf, size, off,
                              __resume_sem, &blk);
        if (ret)
                GOTO(err_ret, ret);

        ret = _sem_wait(&blk.sem);
        if (ret) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        ret = blk.ret;
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        return blk.ret;
err_ret:
        return -ret;
}

int sdfs_truncate(const fileid_t *fileid, uint64_t length)
{
        int ret, retry = 0;

#if ENABLE_WORM
        worm_status_t worm_status;

        worm_status = worm_get_status(fileid);
        if (WORM_IN_PROTECT == worm_status)
        {
                ret = EACCES;
                goto err_ret;
        }
#endif

retry:
        ret = md_truncate(fileid, length);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int sdfs_getxattr(const fileid_t *fileid, const char *name, void *value, size_t *size)
{
        return md_getxattr(fileid, name, value, size);
}

int sdfs_removexattr(const fileid_t *fileid, const char *name)
{
        return md_removexattr(fileid, name);
}

int sdfs_listxattr(const fileid_t *fileid, char *list, size_t *size)
{
        return md_listxattr(fileid, list, size);
}


inline static int __chunk_setxattr(const fileinfo_t *md, const char *key, const char *value)
{
        int ret, set, status, i;
        char buf[MAX_BUF_LEN];
        objinfo_t *objinfo;
        chkid_t chkid;

        if (strcmp(key, ATTR_PREALLOC) == 0) {
                status = __S_PREALLOC;
        } else if (strcmp(key, ATTR_WRITEBACK) == 0) {
                status = __S_WRITEBACK;
        } else
                goto out;

        if (strcmp(value, ATTR_TRUE) == 0)
                set = 1;
        else if (strcmp(value, ATTR_FALSE) == 0)
                set = 0;
        else {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        objinfo = (void *)buf;
        for (i = 0; i < (int)md->chknum; i++) {
                fid2cid(&chkid, &md->fileid, i);
                ret = md_objset(objinfo, &chkid, set, status);
                if (ret) {
                        if (ret == ENOENT)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }
        }

out:
        return 0;
err_ret:
        return ret;
}

int sdfs_setxattr(const fileid_t *fileid, const char *name, const void *value,
                 size_t size, int flags)
{
        int ret;
        md_proto_t *md;
        char buf[MAX_BUF_LEN];

        md = (void *)buf;

        ret = md_getattr(md, fileid);
        if (ret)
                GOTO(err_ret, ret);

        if ((!S_ISREG(md->at_mode)) && (!S_ISDIR(md->at_mode))) {
                ret = EOPNOTSUPP;
                GOTO(err_ret, ret);
        }

        mode_t new_mode;
        if (0 == strcmp(name, ACL_EA_ACCESS)) {
                new_mode = md->at_mode;
                if (value) {
                        ret = posix_acl_equiv_mode(value, size, &new_mode);
                        if (ret < 0) {
                                ret = EINVAL;
                                GOTO(err_ret, ret);
                        }

                        if (ret == 0) {
                	        value = NULL;
                	        size = 0;
                	}

                        if (new_mode != md->at_mode) {
                	        ret = sdfs_chmod(fileid, new_mode);
                	        if (ret < 0)
                	                GOTO(err_ret, -ret);
                        }
                } else if (!strcmp(name, ACL_EA_DEFAULT)) {
                        if (value) {
                	        if (!S_ISDIR(md->at_mode)) {
                                        ret = EACCES;
                                        GOTO(err_ret, ret);
                	        }

                                ret = posix_acl_check(value, size);
                                if (ret < 0) {
                                        ret = EINVAL;
                                        GOTO(err_ret, ret);
                                }

                                if (ret == 0) {
                                        value = NULL;
                                        size = 0;
                                }

                        }
                }
        }

        ret = md_setxattr(fileid, name, value, size, flags);
        if (ret)
                GOTO(err_ret, ret);

        UNIMPLEMENTED(__NULL__);
#if 0
        if (S_ISREG(md->at_mode)) {
                ret = __chunk_setxattr((void *)md, name, value);
                if (ret)
                        GOTO(err_ret, ret);
        }
#endif

        return 0;
err_ret:
        return ret;
}


int sdfs_setlock(const fileid_t *fileid, const sdfs_lock_t *lock)
{
        return md_setlock(fileid, lock);
}

int sdfs_getlock(const fileid_t *fileid, sdfs_lock_t *lock)
{
        return md_getlock(fileid, lock);
}

int sdfs_lock_equal(const fileid_t *file1, const sdfs_lock_t *lock1,
                                  const fileid_t *file2, const sdfs_lock_t *lock2)
{
        if (file1 && file2 && chkid_cmp(file1, file2)) {
                DINFO("fileid "CHKID_FORMAT","CHKID_FORMAT"\n",
                      CHKID_ARG(file1), CHKID_ARG(file2));
                return 0;
        }

        DINFO("type %d,%d, sid %d,%d, owner 0x%u,0x%u, start %ju,%ju, end %ju,%ju\n",
              lock1->type, lock2->type,
              lock1->sid, lock2->sid,
              lock1->owner, lock2->owner,
              lock1->start, lock2->start,
              lock1->length, lock2->length);
        
        if (lock1->sid == lock2->sid &&
            lock1->owner == lock2->owner &&
            lock1->start == lock2->start &&
            lock1->length == lock2->length) {
                return 1;
        } else {
                return 0;
        }
}

#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>

#define DBG_SUBSYS S_YFSLIB

#include "sdfs_id.h"
#include "ylib.h"
#include "md_lib.h"
#include "chk_proto.h"
#include "network.h"
#include "net_global.h"
#include "chk_proto.h"
#include "net_global.h"
#include "yfs_file.h"
#include "redis.h"
#include "sdfs_lib.h"
#include "sdfs_chunk.h"
#include "network.h"
#include "yfs_limit.h"
#include "cds_rpc.h"
#include "schedule.h"
#include "xattr.h"
#include "dbg.h"

typedef struct {
        ec_strip_t strips[YFS_CHK_REP_MAX];
        int strip_count;
        int strip_offset;
} ec_arg_t;

typedef struct {
        io_t io;
        const nid_t *nid;
        const buffer_t *buf;
        task_t *task;
        int size;
        int offset;
        int *sub_task;
        int retval;
} chunk_write_ctx_t;

inline static void __chunk_recovery(const chkid_t *chkid)
{
        int ret, retry = 0;

retry:
        ret = sdfs_chunk_recovery(chkid);
        if (unlikely(ret)) {
                DWARN(CHKID_FORMAT" recovery fail\n", CHKID_ARG(chkid));
                if (retry == 0) {
                        retry++;
                        goto retry;
                }
        }
}

static int __chunk_load(const fileinfo_t *md, const chkid_t *chkid,
                        chkinfo_t *chkinfo, int repmin, int *_intect)
{
        int ret, intect = 1, retry = 0;
        nid_t *nid;
        uint32_t i;

        ANALYSIS_BEGIN(0);
        
        (void) retry;
retry:
        ret = md_chunk_load_check(chkid, chkinfo, repmin);
        if (unlikely(ret)) {
                if (ret == ENOENT && md) {
                        ret = md_chunk_create(md, chkid->idx, chkinfo);
                        if (unlikely(ret)) {
                                if (ret == EEXIST) {
                                        goto retry;
                                } else
                                        GOTO(err_ret, ret);
                        }
                } else 
                        GOTO(err_ret, ret);
        }

        for (i = 0; i < chkinfo->repnum; i++) {
                nid = &chkinfo->diskid[i];
                if (nid->status & __S_DIRTY) {
#if 0
                        intect = 0;
                        break;
#else
                        if (retry < 1) {
                                __chunk_recovery(chkid);
                                retry++;
                                goto retry;
                        } else {
                                intect = 0;
                                break;
                        }
#endif
                }
        }

        if (_intect) {
                *_intect = intect;
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

static void __chunk_ec_read_strip(ec_arg_t *ec_arg, int count, int offset, const ec_t *ec)
{
        int i;
        int m, k;
        int row1, row2;
        uint32_t off;

        k = ec->k;
        m = ec->m;

        (void) m;
        /*YASSERT(ec_arg->offset % STRIP_BLOCK == 0);*/
        /*YASSERT(count % STRIP_BLOCK == 0);*/

        //row number start from 0, when size % STRIP_BLOCK * k == 0, row1 is ok,
        //but row2 need row2--, so row2 = (size - 1) / STRIP_BLOCK * k
        off = offset;
        row1 = off / (STRIP_BLOCK * k);
        row2 = (off + count - 1) / (STRIP_BLOCK * k);

        for (i = 0; i < k; i++) {
                ec_arg->strips[i].idx = i; //第几个副本
                ec_arg->strips[i].offset = STRIP_BLOCK * row1;
                ec_arg->strips[i].count = STRIP_BLOCK * (row2-row1+1);
        }

        ec_arg->strip_count = k;
        ec_arg->strip_offset = STRIP_BLOCK * k * row1;
}

static int __chunk_read_ec(const chkid_t *chkid, buffer_t *buf, int count,
                           int offset, const ec_t *ec)
{
        int ret, i, diff, left;
        io_t io;
        ec_arg_t ec_arg;
        ec_strip_t *strip;
        buffer_t tmpbuf, tmpbuf2;
        nid_t *nid;
        chkinfo_t *chkinfo;
        char _chkinfo[CHK_SIZE(YFS_CHK_REP_MAX)];

        DINFO("read "CHKID_FORMAT"\n", CHKID_ARG(chkid));
        
        chkinfo = (void *)_chkinfo;

        ret = __chunk_load(NULL, chkid, chkinfo, ec->k, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        __chunk_ec_read_strip(&ec_arg, count, offset, ec);

        for (i = 0; i < ec_arg.strip_count; i++) {
                strip = &ec_arg.strips[i];
                nid = &chkinfo->diskid[strip->idx];

                io_init(&io, chkid, strip->count, strip->offset, 0);
                mbuffer_init(&strip->buf, 0);
                ret = cds_rpc_read(nid, &io, &strip->buf);
                if (ret) {
                        GOTO(err_ret, ret);
                }
        }

        mbuffer_init(&tmpbuf, 0);
        mbuffer_init(&tmpbuf2, 0);
        left = ec_arg.strips[0].count * ec_arg.strip_count;
        while (left > 0) {
                for (i = 0; i < ec_arg.strip_count; i++) {
                        mbuffer_pop(&ec_arg.strips[i].buf, &tmpbuf, STRIP_BLOCK);
                        left -= STRIP_BLOCK;
                }
                YASSERT(left >= 0);
        }

        diff = offset - ec_arg.strip_offset;
        YASSERT(diff >= 0);
        if (diff > 0) {
                mbuffer_pop(&tmpbuf, &tmpbuf2, diff);
        }

        mbuffer_pop(&tmpbuf, buf, count);
        mbuffer_free(&tmpbuf);
        mbuffer_free(&tmpbuf2);
        for (i = 0; i < ec_arg.strip_count; i++) {
                mbuffer_free(&ec_arg.strips[i].buf);
        }

        YASSERT((int)buf->len == count);
 
        DINFO("read "CHKID_FORMAT" success\n", CHKID_ARG(chkid));
       
        return 0;
err_ret:
        DWARN("read "CHKID_FORMAT" fail\n", CHKID_ARG(chkid));
        return ret;
}

static int __chunk_read(const chkid_t *chkid, buffer_t *buf, int count, int offset)
{
        int ret;
        char _chkinfo[CHK_SIZE(YFS_CHK_REP_MAX)];
        chkinfo_t *chkinfo;
        io_t io;
        nid_t *nid;
        diskid_t array[YFS_CHK_REP_MAX];
        uint32_t i;

        ANALYSIS_BEGIN(0);
        
        chkinfo = (void *)_chkinfo;
        
        ret = __chunk_load(NULL, chkid, chkinfo, 1, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        io_init(&io, chkid, count, offset, 0);

        DBUG("read "CHKID_FORMAT" offset %ju size %u\n",
             CHKID_ARG(&io.id), io.offset, io.size);
        
        memcpy(array, chkinfo->diskid, sizeof(diskid_t) * chkinfo->repnum);

        netable_sort(array, chkinfo->repnum);
        
        for (i = 0; i < chkinfo->repnum; i++) {
                nid = &array[i];
                if (nid->status & __S_DIRTY) {
                        continue;
                }

                ret = cds_rpc_read(nid, &io, buf);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }

                break;
        }

        if (i == chkinfo->repnum) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }

        DBUG("read "CHKID_FORMAT" success\n", CHKID_ARG(chkid));

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        DBUG("read "CHKID_FORMAT" fail\n", CHKID_ARG(chkid));
        return ret;
}

#if 1

STATIC void __chunk_replica_write__(void *arg)
{
        int ret;
        chunk_write_ctx_t *ctx = arg;

        ret = network_connect(ctx->nid, NULL, 1, 0);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        ret = cds_rpc_write(ctx->nid, &ctx->io, ctx->buf);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        ctx->retval = 0;
        *ctx->sub_task = *ctx->sub_task - 1;
        if (*ctx->sub_task == 0)
                schedule_resume(ctx->task, 0, NULL);

        return;
err_ret:
        ctx->retval = ret;
        *ctx->sub_task = *ctx->sub_task - 1;
        if (*ctx->sub_task == 0)
                schedule_resume(ctx->task, 0, NULL);

        return;
}


static int __chunk_write__(const chkinfo_t *chkinfo, const buffer_t *buf, int size, int offset)
{
        int ret, i, success = 0, sub_task = 0, online;
        chunk_write_ctx_t _ctx[YFS_CHK_REP_MAX], *ctx;
        task_t task;
        const nid_t *nid;
        nid_t array[YFS_CHK_REP_MAX];

        ANALYSIS_BEGIN(0);
        
        online = 0;
        for (i = 0; i < (int)chkinfo->repnum; i++) {
                nid = &chkinfo->diskid[i];
                if (nid->status & __S_DIRTY) {
                        continue;
                }

                array[online] = *nid;
                online++;
        }

        task = schedule_task_get();
        sub_task = online;
        for (i = 0; i < online; i++) {
                nid = &array[i];

                ctx = &_ctx[i];
                ctx->buf = buf;
                ctx->nid = nid;
                ctx->task = &task;
                ctx->sub_task = &sub_task;
                io_init(&ctx->io, &chkinfo->chkid, size, offset, 0);
                schedule_task_new("replica_write", __chunk_replica_write__, ctx, -1);
        }

        ret = schedule_yield("replica_wait", NULL, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (i = 0; i < online; i++) {
                ctx = &_ctx[i];
                if (ctx->retval == 0)
                        success++;
        }

        if (unlikely(success != online)) {
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

#else

static int __chunk_write__(const chkinfo_t *chkinfo, const buffer_t *buf, int count, int offset)
{
        int ret;
        uint32_t i;
        io_t io;
        const nid_t *nid;

        ANALYSIS_BEGIN(0);
        
        io_init(&io, &chkinfo->chkid, count, offset, 0);

        for (i = 0; i < chkinfo->repnum; i++) {
                nid = &chkinfo->diskid[i];
                if (nid->status & __S_DIRTY) {
                        continue;
                }

                ret = cds_rpc_write(nid, &io, buf);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        }

        YASSERT(i <= chkinfo->repnum);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

#endif

static int __chunk_write(const fileinfo_t *md, const chkid_t *chkid,
                         const buffer_t *buf, int count, int offset)
{
        int ret, intect;
        char _chkinfo[CHK_SIZE(YFS_CHK_REP_MAX)];
        chkinfo_t *chkinfo;

        ANALYSIS_BEGIN(0);
        
        DBUG("write "CHKID_FORMAT"\n", CHKID_ARG(chkid));
        
        chkinfo = (void *)_chkinfo;

        ret = __chunk_load(md, chkid, chkinfo, 1, &intect);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        if (unlikely(!intect)) {
                ret = klock(NULL, chkid, 10, 1);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }
        
        ret = __chunk_write__(chkinfo, buf, count, offset);
        if (unlikely(ret))
                GOTO(err_lock, ret);

        if (unlikely(!intect)) {
                ret = kunlock(NULL, chkid);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        DBUG("write "CHKID_FORMAT" success\n", CHKID_ARG(chkid));

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_lock:
        if (unlikely(!intect)) {
                kunlock(NULL, chkid);
        }
err_ret:
        DWARN("write "CHKID_FORMAT" fail\n", CHKID_ARG(chkid));
        return ret;
}

static int __chunk_write_ec_strip__(buffer_t *data, uint32_t begin, uint32_t end, int row,
                                    uint32_t count, uint32_t offset, char *buffs,
                                    const nid_t *nid, const chkid_t *chkid)
{
        int ret;
        uint32_t diff;
        uint32_t off;
        buffer_t tmpbuf2;
        buffer_t tmpbuf;
        
        mbuffer_init(&tmpbuf, 0);
        mbuffer_init(&tmpbuf2, 0);

        if (begin >= (uint32_t)offset && end <= (uint32_t)(offset + count)) {
                /*copy*/
                ret = mbuffer_get(data, buffs, STRIP_BLOCK);
                if (ret)
                        GOTO(err_ret, ret);

                /*删除*/
                ret = mbuffer_pop(data, &tmpbuf, STRIP_BLOCK);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                off = STRIP_BLOCK * row;

                io_t io;
                io_init(&io, chkid, STRIP_BLOCK, off, 0);
                ret = cds_rpc_read(nid, &io, &tmpbuf2);
                if (ret) {
                        if (ret == ENOENT) {
                                mbuffer_appendzero(&tmpbuf2, STRIP_BLOCK);
                        } else
                                GOTO(err_ret, ret);
                }

                ret = mbuffer_get(&tmpbuf2, buffs, STRIP_BLOCK);
                if (ret)
                        GOTO(err_ret, ret);

                diff = 0;

                //头
                if (begin <= (uint32_t)offset
                    && end >= ((uint32_t)offset)
                    && end <= (uint32_t)(offset + count)) {
                        off = offset - begin;
                        diff = end - offset;
                }

                //尾
                if (begin >= (uint32_t)offset
                    && begin <= (uint32_t)(offset + count)
                    && end >= (uint32_t)(offset + count)) {
                        off = 0;
                        diff = offset + count - begin;
                }

                //中
                if (begin <= (uint32_t)offset && end >= (uint32_t)(offset + count)) {
                        off = offset - begin;
                        diff = count;
                }

                if (diff) {
                        //copy
                        ret = mbuffer_get(data, buffs + off, diff);
                        if (ret)
                                GOTO(err_ret, ret);

                        //删除
                        ret = mbuffer_pop(data, &tmpbuf, diff);
                        if (ret)
                                GOTO(err_ret, ret);
                }
        }


        mbuffer_free(&tmpbuf);
        mbuffer_free(&tmpbuf2);
        
        return 0;
err_ret:
        mbuffer_free(&tmpbuf);
        mbuffer_free(&tmpbuf2);
        return ret;
}

static int __chunk_ec_write_strip(ec_arg_t *ec_arg, int count, int offset, const ec_t *ec,
                                 const chkinfo_t *chkinfo, buffer_t *data)
{
        int ret, i;
        int m, k, r;
        int row, row1, row2;
        int new, len;
        uint32_t off, begin, end;
        char *buffs[EC_MMAX];
        void *buf;

        k = ec->k;
        m = ec->m;
        r = m - k;

        /*YASSERT(context->offset % STRIP_BLOCK == 0);*/
        /*YASSERT(context->count % STRIP_BLOCK == 0);*/

        for (i = 0; i < EC_MMAX; i++) {
                ret = posix_memalign(&buf, STRIP_ALIGN, STRIP_BLOCK);
                if (ret) {
                        DERROR("alloc error: Fail");
                        GOTO(err_free, ret);
                }

                buffs[i] = buf;
        }

        //row number start from 0, when size % STRIP_BLOCK * k == 0, row1 is ok,
        //but row2 need row2--, so row2 = (size - 1) / STRIP_BLOCK * k
        off = offset;
        row1 = off / (STRIP_BLOCK * k);
        row2 = (off + count - 1) / (STRIP_BLOCK * k);

        for (i = 0; i < k + r; i++) {
                ec_arg->strips[i].idx = i; //第几个副本
                ec_arg->strips[i].offset = STRIP_BLOCK * row1;
                ec_arg->strips[i].count = STRIP_BLOCK * (row2-row1+1);

                mbuffer_init(&ec_arg->strips[i].buf, 0);
        }

        for (row = row1; row <= row2; row++) {
                for (i = 0; i < k; i++) {
                        begin = (STRIP_BLOCK*k)*row + (STRIP_BLOCK*i);
                        end = begin + STRIP_BLOCK;

                        ret = __chunk_write_ec_strip__(data, begin, end, row,
                                                       count, offset,
                                                       buffs[i], &chkinfo->diskid[i],
                                                       &chkinfo->chkid);
                        if (ret)
                                GOTO(err_free, ret);
                                
                }

                //计算后面r个纠删码
                ret = ec_encode(&buffs[0], &buffs[k], STRIP_BLOCK, m, k);
                if (ret)
                        GOTO(err_free, ret);

                for (i = 0; i < k + r; i++) {
                        ret = mbuffer_copy(&ec_arg->strips[i].buf, buffs[i], STRIP_BLOCK);
                        if (ret)
                                GOTO(err_free, ret);
                }
        }

        ec_arg->strip_count = k+r;

        for (i = 0; i < EC_MMAX; i++) {
                if (buffs[i])
                        free(buffs[i]);
        }

        //把每个strip的count切分成小于Y_BLOCK_MAX, 1+1模式会走到下面代码
        new = k+r;
        for (i = 0; i < k + r; i++) {
                if (ec_arg->strips[i].count >  Y_BLOCK_MAX) {
                        YASSERT(ec_arg->strips[i].count <  Y_BLOCK_MAX * 2);
                        mbuffer_init(&ec_arg->strips[new].buf, 0);
                        len = ec_arg->strips[i].count - Y_BLOCK_MAX;
                        mbuffer_pop(&ec_arg->strips[i].buf, &ec_arg->strips[new].buf, len);
                        ec_arg->strips[new].count = len;
                        ec_arg->strips[new].offset = ec_arg->strips[i].offset;

                        ec_arg->strips[i].count -= len;
                        ec_arg->strips[i].offset += len;

                        new++;
                }
        }

        ec_arg->strip_count = new;

        return 0;
err_free:
        for (i = 0; i < EC_MMAX; i++) {
                if (buffs[i])
                        free(buffs[i]);
        }
        return ret;
}

#if 1

static int __chunk_write_ec__(const ec_arg_t *ec_arg, const chkinfo_t *chkinfo)
{
        int ret, i, success = 0, sub_task = 0, online = 0;
        chunk_write_ctx_t _ctx[YFS_CHK_REP_MAX], *ctx;
        const ec_strip_t *strip;
        const nid_t *nid;
        task_t task;
        
        task = schedule_task_get();
        online = ec_arg->strip_count;
        sub_task = online;
        for (i = 0; i < online; i++) {
                strip = &ec_arg->strips[i];
                nid = &chkinfo->diskid[strip->idx];
        
                ctx = &_ctx[i];
                ctx->buf = &strip->buf;
                ctx->nid = nid;
                ctx->task = &task;
                ctx->sub_task = &sub_task;
                io_init(&ctx->io, &chkinfo->chkid, strip->count, strip->offset, 0);
                schedule_task_new("replica_write", __chunk_replica_write__, ctx, -1);
        }

        ret = schedule_yield("replica_wait", NULL, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        for (i = 0; i < online; i++) {
                ctx = &_ctx[i];
                if (ctx->retval == 0)
                        success++;
        }

        if (unlikely(success != online)) {
                ret = EAGAIN;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

#else

static int __chunk_write_ec__(const ec_arg_t *ec_arg, const chkinfo_t *chkinfo)
{
        int ret, i;
        const ec_strip_t *strip;
        const nid_t *nid;
        io_t io;

        for (i = 0; i < ec_arg->strip_count; i++) {
                strip = &ec_arg->strips[i];
                nid = &chkinfo->diskid[strip->idx];

                io_init(&io, &chkinfo->chkid, strip->count, strip->offset, 0);
                YASSERT(strip->count == strip->buf.len);
                ret = cds_rpc_write(nid, &io, &strip->buf);
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}
#endif

static void __chunk_write_ec_free(ec_arg_t *ec_arg)
{
        for (int i = 0; i < ec_arg->strip_count; i++) {
                ec_strip_t *strip = &ec_arg->strips[i];
                mbuffer_free(&strip->buf);
        }
}

static int __chunk_write_ec(const fileinfo_t *md, const chkid_t *chkid,
                            const buffer_t *buf, int count, int offset, const ec_t *ec)
{
        int ret, intect;
        ec_arg_t ec_arg;
        chkinfo_t *chkinfo;
        char _chkinfo[CHK_SIZE(YFS_CHK_REP_MAX)];
        buffer_t newbuf;

        DBUG("write "CHKID_FORMAT"\n", CHKID_ARG(chkid));

        mbuffer_init(&newbuf, 0);
        mbuffer_reference(&newbuf, buf);
        
        chkinfo = (void *)_chkinfo;

        ret = __chunk_load(md, chkid, chkinfo, ec->k, &intect);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (unlikely(!intect)) {
                ret = klock(NULL, chkid, 10, 1);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        YASSERT((int)buf->len == count);
        ret = __chunk_ec_write_strip(&ec_arg, count, offset, ec, chkinfo, &newbuf);
        if (ret)
                GOTO(err_lock, ret);

        ret = __chunk_write_ec__(&ec_arg, chkinfo);
        if (ret)
                GOTO(err_free, ret);

        __chunk_write_ec_free(&ec_arg);
        
        if (unlikely(!intect)) {
                ret = kunlock(NULL, chkid);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        
        DBUG("write "CHKID_FORMAT" success\n", CHKID_ARG(chkid));

        mbuffer_free(&newbuf);
        
        return 0;
err_free:
        __chunk_write_ec_free(&ec_arg);
err_lock:
        if (unlikely(!intect)) {
                kunlock(NULL, chkid);
        }
err_ret:
        DWARN("write "CHKID_FORMAT" fail\n", CHKID_ARG(chkid));
        mbuffer_free(&newbuf);
        return ret;
}

int sdfs_chunk_read(const chkid_t *chkid, buffer_t *buf, int count,
                    int offset, const ec_t *ec)
{
        int ret, retry = 0;

        ANALYSIS_BEGIN(0);
retry:
        if (ec->plugin == PLUGIN_EC_ISA) {
                ret = __chunk_read_ec(chkid, buf, count, offset, ec);
        } else {
                ret = __chunk_read(chkid, buf, count, offset);
        }

        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

int sdfs_chunk_write(const fileinfo_t *md, const chkid_t *chkid,
                     const buffer_t *buf, int count, int offset, const ec_t *ec)
{
        int ret, retry = 0;

        DBUG("sdfs_chunk_write\n");
        
        ANALYSIS_BEGIN(0);
        
        YASSERT((int)buf->len == count);
retry:
        if (ec->plugin == PLUGIN_EC_ISA) {
                ret =  __chunk_write_ec(md, chkid, buf, count, offset, ec);
        } else {
                ret = __chunk_write(md, chkid, buf, count, offset);
        }

        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        USLEEP_RETRY(err_ret, ret, retry, retry, 100, (1000 * 1000));
                } else
                        GOTO(err_ret, ret);
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

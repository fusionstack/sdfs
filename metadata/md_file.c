#include <sys/types.h>
#include <string.h>

#define DBG_SUBSYS S_YFSMDC

#include "sdfs_buffer.h"
#include "file_proto.h"
#include "job_dock.h"
#include "yfs_file.h"
#include "net_global.h"
#include "ynet_rpc.h"
#include "ylib.h"
#include "redis.h"
#include "quota.h"
#include "md_proto.h"
#include "md_lib.h"
#include "md_db.h"
#include "dbg.h"

static inodeop_t *inodeop = &__inodeop__;

static int __md_truncate(const volid_t *volid, md_proto_t *md, uint64_t length)
{
        int ret;
        setattr_t setattr;
        uint64_t size;
        fileid_t *fileid;

        (void) size;
        fileid = &md->fileid;

        if (length > md->at_size) {
                size = length - md->at_size;
                //DBUG("begin quota_space_check_and_inc, size:%llu, quotaid:%llu\n",
                //(LLU)size, (LLU)quotaid);
#if ENABLE_QUOTA
                ret = quota_space_increase(&md->parent, md->at_uid,
                                           md->at_gid, size);
                if (ret)
                        GOTO(err_ret, ret);
#endif
        } else if (length < md->at_size) {
                size = md->at_size - length;

#if ENABLE_QUOTA
                ret = quota_space_decrease(&md->parent, md->at_uid,
                                           md->at_gid, size);
                if (ret)
                        GOTO(err_ret, ret);
#endif
        }

        setattr_init(&setattr, -1, -1, NULL, -1, -1, length);
        ret = inodeop->setattr(volid, fileid, &setattr, 1);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_truncate(const volid_t *volid, const fileid_t *fileid, uint64_t length)
{
        int ret;
        md_proto_t *md;
        char buf[MAX_BUF_LEN] = {0};

        md = (md_proto_t *)buf;
        ret = inodeop->getattr(volid, fileid, md);
        if (ret)
                GOTO(err_ret, ret);

        ret = __md_truncate(volid, md, length);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_extend(const volid_t *volid, const fileid_t *fileid, size_t size)
{
        int ret;

        ANALYSIS_BEGIN(0);
        
        ret = inodeop->extend(volid, fileid, size);
        if (ret)
                GOTO(err_ret, ret);

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

static int __md_lock_collision__(const sdfs_lock_t *lock1, const sdfs_lock_t *lock2)
{
        uint64_t begin1, end1, begin2, end2;

        begin1 = lock1->start;
        begin2 = lock2->start;

        end1 = (lock1->length == 0) ? UINT64_MAX : (lock1->start + lock1->length);
        end2 = (lock2->length == 0) ? UINT64_MAX : (lock2->start + lock2->length);

        if (end1 <= begin2 || end2 <= begin1) {
                return 0;
        }

        if (lock1->type != SDFS_WRLOCK && lock2->type != SDFS_WRLOCK) {
                return 0;
        }

        
        DINFO("type %d,%d, sid %d,%d, owner 0x%u,0x%u, start %ju,%ju, end %ju,%ju\n",
              lock1->type, lock2->type,
              lock1->sid, lock2->sid,
              lock1->owner, lock2->owner,
              lock1->start, lock2->start,
              lock1->length, lock2->length);
        
        return 1;
}

static int __md_lock_collision(const sdfs_lock_t *lock, const char *buf, size_t size)
{
        uint64_t left;
        const sdfs_lock_t *pos;

        left = size;
        pos = (void *)buf;
        while (left) {
                YASSERT(left >= SDFS_LOCK_SIZE(pos));

                if (__md_lock_collision__(lock, pos)) {
                        return 1;
                }

                left -= SDFS_LOCK_SIZE(pos);
                pos = (void *)pos + SDFS_LOCK_SIZE(pos);
        }

        return 0;
}

static int __md_lock(const volid_t *volid, const fileid_t *fileid, const sdfs_lock_t *lock)
{
        int ret;
        char buf[MAX_BUF_LEN] = {0};
        size_t size = MAX_BUF_LEN;

        ret = klock(volid, fileid, 10, 1);
        if (ret)
                GOTO(err_ret, ret);

        ret = inodeop->getlock(volid, fileid, buf, &size);
        if (ret) {
                if (ret == ENOENT) {
                        ret = inodeop->setlock(volid, fileid, lock, SDFS_LOCK_SIZE(lock), O_CREAT);
                        if (ret)
                                GOTO(err_lock, ret);

                        goto out;
                } else
                        GOTO(err_lock, ret);
        }

        
        if (__md_lock_collision(lock, buf, size)) {
                ret = EWOULDBLOCK;
                GOTO(err_lock, ret);
        }

        if (size + SDFS_LOCK_SIZE(lock) > MAX_BUF_LEN) {
                ret = EFBIG;
                GOTO(err_lock, ret);
        }

        memcpy(buf + size, lock, SDFS_LOCK_SIZE(lock));

        ret = inodeop->setlock(volid, fileid, buf, size + SDFS_LOCK_SIZE(lock), 0);
        if (ret)
                GOTO(err_lock, ret);
        
out:
        kunlock(volid, fileid);

        return 0;
err_lock:
        kunlock(volid, fileid);
err_ret:
        return ret;
}

static sdfs_lock_t *__md_lock_find(const sdfs_lock_t *lock, char *buf, size_t size)
{
        uint64_t left;
        sdfs_lock_t *pos;

        left = size;
        pos = (void *)buf;
        while (left) {
                YASSERT(left >= SDFS_LOCK_SIZE(pos));

                if (sdfs_lock_equal(NULL, lock, NULL, pos)) {
                        return pos;
                }

                left -= SDFS_LOCK_SIZE(pos);
                pos = (void *)pos + SDFS_LOCK_SIZE(pos);
        }

        return NULL;
}

static int __md_unlock(const volid_t *volid, const fileid_t *fileid, const sdfs_lock_t *lock)
{
        int ret;
        char buf[MAX_BUF_LEN] = {0};
        size_t size = MAX_BUF_LEN;
        sdfs_lock_t *pos;

        ret = klock(volid, fileid, 10, 1);
        if (ret)
                GOTO(err_ret, ret);

        ret = inodeop->getlock(volid, fileid, buf, &size);
        if (ret) {
                GOTO(err_lock, ret);
        }

        pos = __md_lock_find(lock, buf, size);
        if (pos == NULL) {
                ret = ENOENT;
                GOTO(err_lock, ret);
        }
        
        YASSERT(lock->opaquelen == pos->opaquelen);

        //ARRAY_POP(*pos, SDFS_LOCK_SIZE(pos), size - (pos - buf));
        memmove(pos, (void *)pos + SDFS_LOCK_SIZE(pos),
                size - ((void *)pos - (void *)buf) - SDFS_LOCK_SIZE(pos));

        ret = inodeop->setlock(volid, fileid, buf, size - SDFS_LOCK_SIZE(lock), 0);
        if (ret)
                GOTO(err_lock, ret);
        
        kunlock(volid, fileid);

        return 0;
err_lock:
        kunlock(volid, fileid);
err_ret:
        return ret;
}

int md_setlock(const volid_t *volid, const fileid_t *fileid, const sdfs_lock_t *lock)
{
        if (lock->type == SDFS_UNLOCK) {
                return __md_unlock(volid, fileid, lock);
        } else {
                return __md_lock(volid, fileid, lock);
        }
}

static sdfs_lock_t *__md_lock_find_collision(const sdfs_lock_t *lock, char *buf, size_t size)
{
        uint64_t left;
        sdfs_lock_t *pos;

        left = size;
        pos = (void *)buf;
        while (left) {
                YASSERT(left == SDFS_LOCK_SIZE(pos));

                if (__md_lock_collision__(lock, pos)) {
                        return pos;
                }

                left -= SDFS_LOCK_SIZE(pos);
                pos = (void *)pos + SDFS_LOCK_SIZE(pos);
        }

        return NULL;
}


int md_getlock(const volid_t *volid, const fileid_t *fileid, sdfs_lock_t *lock)
{
        int ret;
        char buf[MAX_BUF_LEN] = {0};
        size_t size = MAX_BUF_LEN;
        sdfs_lock_t *pos;

        ret = klock(volid, fileid, 10, 1);
        if (ret)
                GOTO(err_ret, ret);

        ret = inodeop->getlock(volid, fileid, buf, &size);
        if (ret) {
                GOTO(err_lock, ret);
        }

        pos = __md_lock_find_collision(lock, buf, size);
        if (pos == NULL) {
                ret = ENOENT;
                GOTO(err_lock, ret);
        }

        memcpy(lock, pos, SDFS_LOCK_SIZE(pos));

        kunlock(volid, fileid);

        return 0;
err_lock:
        kunlock(volid, fileid);
err_ret:
        return ret;
}

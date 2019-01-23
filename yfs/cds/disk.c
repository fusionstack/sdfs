#include <sys/types.h>
#include <sys/stat.h>
#include <sys/poll.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <attr/attributes.h>
#include <ctype.h>
#include <fcntl.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSCDS

#include "shm.h"
#include "yfs_conf.h"
#include "yfscds_conf.h"
#include "chk_meta.h"
#include "cds.h"
#include "cds_volume.h"
#include "disk.h"
#include "md_proto.h"
#include "ylib.h"
#include "ynet_rpc.h"
#include "yfs_chunk.h"
#include "sdfs_lib.h"
#include "cd_proto.h"
#include "job_dock.h"
#include "job_tracker.h"
#include "md_lib.h"
#include "bh.h"
#include "dpool.h"
#include "cds_hb.h"
#include "net_global.h"
#include "chkinfo.h"
#include "mond_rpc.h"
#include "dbg.h"
//#include "leveldb_util.h"
//#include "cds_leveldb.h"
#include "adt.h"

/*#define USE_AIO*/

typedef struct {
        chkid_t id;
        int level;
} entry_t;

typedef struct {
        struct iocb iocb;
        diskio_t *diskio;
        cache_entry_t *cent;
        int fd;
} iocb_t;

typedef struct {
        int level;
        int count;
        int idx;
        uint64_t dfree;
        uint64_t dsize;
        chklist_t *chklist;
} level_t;

typedef struct {
        io_context_t ctx[DISK_WORKER_MAX];
        sy_spinlock_t big_lock[DISK_WORKER_MAX];

        sy_spinlock_t level_lock;
        int chklist_count;
        chklist_t *chklist;
        int level_count;
        level_t level[0];
} disk_t;

static disk_t *disk;
static cache_t *cache;
extern int nofile_max;
static dpool_t *dpool_64;

#define TMPFILE_MAX (128)  /*persistence data, never alter it*/
#define TMPFILE_BLOCK (sizeof(chkop_t) + Y_BLOCK_MAX) /*persistence data, never alter it*/
#define DISK_ANALYSIS_INTERVAL (60 * 60)
#define DISK_64M (64 * 1024 * 1024 + 4096 * 2)

static inline void __disk_build_chkpath(char *path, const chkid_t *chkid, int level)
{
        char cpath[MAX_PATH_LEN];

        (void) cascade_id2path(cpath, MAX_PATH_LEN, chkid->id);

        (void) snprintf(path, MAX_PATH_LEN, "%s/disk/%u/%s_v%llu/%u",
                        ng.home, level, cpath, (LLU)chkid->volid, chkid->idx);

}


inline  void __disk_build_deleted_path(char *dpath, const chkid_t *chkid, int level)
{
        int ret;
        char cpath[MAX_PATH_LEN];

        (void) cascade_id2path(cpath, MAX_PATH_LEN, chkid->id);

        (void) snprintf(dpath, MAX_PATH_LEN, "%s/disk/%u/deleted/%s_v%llu/%u",
                        ng.home, level, cpath, (LLU)chkid->volid, chkid->idx);

        ret = path_validate(dpath, 0, 1);
        if (ret) {
                DERROR("error %s\n", dpath);

                UNIMPLEMENTED(__DUMP__);
        }
}

inline int __disk_build_jnlpath(char *dpath, const chkid_t *chkid, uint64_t version)
{
        char path[MAX_PATH_LEN];
        uint32_t seq = version / TMPFILE_MAX;

        snprintf(path, MAX_PATH_LEN, "%s/tmp/%llu_v%llu.%u",
                        ng.home, (LLU)chkid->id, (LLU)chkid->volid, chkid->idx);

        (void) snprintf(dpath, MAX_PATH_LEN, "%s/jnl/%u", path, seq);

        return 0;
}

static inline int __disk_build_tmp(char *path, const chkid_t *chkid, int level)
{
        int ret;

        snprintf(path, MAX_PATH_LEN, "%s/disk/%u/tmp/%llu_v%llu.%u",
                        ng.home, level, (LLU)chkid->id, (LLU)chkid->volid, chkid->idx);

        ret = path_validate(path, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (ret) {
                DERROR("error %s\n", path);
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __disk_getlevel(const chkid_t *id)
{
        int ret, i;
        char path[MAX_PATH_LEN];
        struct stat stbuf;

        for (i = disk->level_count - 1; i >= 0; i--) {
                __disk_build_chkpath(path, id, i);

                ret = stat(path, &stbuf);
                if (ret < 0) {
                        ret = errno;
                        if (ret == ENOENT)
                                continue;
                }

                return i;
        }

        return -ENOENT;
}

static int __disk_infodelchk(const objid_t *chkid, int increase)
{
        int ret;

        DBUG("increae %d\n", increase);

        ret = cds_volume_update(chkid->volid, 0 - increase);
        if (ret)
                GOTO(err_ret, ret);

        ret = chkinfo_add(chkid, 0 - increase);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __disk_infoaddchk(const objid_t *id, int increase)
{
        int ret;

        DBUG("rept "OBJID_FORMAT"\n", OBJID_ARG(id));

        ret = cds_volume_update(id->volid, increase);
        if (ret)
                GOTO(err_ret, ret);

        ret = chkinfo_add(id, increase);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int disk_unlink(const chkid_t *chkid, int size)
{
        int ret, level;
        char dpath[MAX_PATH_LEN] = {0}, newpath[MAX_PATH_LEN];

        ret = cache_drop(cache, chkid);
        if (ret)
                GOTO(err_ret, ret);

        level = __disk_getlevel(chkid);
        if (level < 0) {
                ret = -level;
                GOTO(err_ret, ret);
        }

        __disk_build_chkpath(dpath, chkid, level);

        ret = path_validate(dpath, YLIB_NOTDIR, YLIB_DIRNOCREATE);
        if (ret) {
                GOTO(err_ret, ret);
        }

        ret = __disk_infodelchk(chkid, size);
        if (ret)
                GOTO(err_ret, ret);

#if 1
        (void) newpath;
        unlink(dpath);
#else

        if (size == DISK_64M) {
                DBUG("unlink %s %u\n", dpath, size);

                ret = dpool_put(dpool_64, dpath, level);
                if (ret)
                        GOTO(err_ret, ret);
        } else if (cdsconf.unlink_async) {
                __disk_build_deleted_path(newpath, chkid, level);

                ret = rename(dpath, newpath);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                ret = unlink(dpath);
                if (ret == -1) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }
#endif

        return 0;
err_ret:
        return ret;
}

int disk_sha1(const chkid_t *chkid, char *md)
{
        int ret, level;
        char path[MAX_PATH_LEN];
        unsigned char buf[MAX_BUF_LEN];

        level = __disk_getlevel(chkid);
        if (level < 0) {
                ret = -level;
                GOTO(err_ret, ret);
        }

        __disk_build_chkpath(path, chkid, level);

        UNIMPLEMENTED(__DUMP__);
#if 0
        ret = _sha1_file(path, 0, buf);
        if (ret)
                GOTO(err_ret, ret);
#endif

        _sha1_print(md, buf);

        return 0;
err_ret:
        return ret;
}

#if 1
int disk_join(const diskid_t *diskid, struct statvfs *fsbuf)
{
        diskinfo_stat_t stat;
        FSTAT2DISKSTAT(fsbuf, &stat);
        return mond_rpc_diskjoin(diskid, cds_info.tier, (const uuid_t *)&ng.nodeid, &stat);
}
#else
int disk_join(const diskid_t *diskid, struct statvfs *fsbuf)
{
        int ret;
        mdp_diskjoin_req_t _req;
        mdp_diskjoin_req_t *req = &_req;
        uint32_t reqlen;
        nid_t peer;

        req->op = MDP_DISKJOIN;
        req->diskid = *diskid;
        req->tier = cds_info.tier;
        uuid_copy(req->nodeid, ng.nodeid);
        FSTAT2DISKSTAT(fsbuf, &req->stat);

        reqlen = sizeof(mdp_diskjoin_req_t);
        peer = ng.mds_nh.u.nid;
        ret = rpc_request_wait1("mdc_diskjoin", &peer,
                        req, reqlen, NULL, MSG_MDP,
                        NIO_NORMAL, _get_timeout());
        if (ret) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}
#endif

static int __cmp(const void *s1, const void *s2)
{
        const chkid_t *chkid = s1;
        const entry_t *ent = s2;

#if 0
        DBUG("cmp %llu_v%u : %llu_v%u\n",(LLU)chkid->id, chkid->version,
                        (LLU)ent->chkid.id, ent->chkid.version);
#endif

        return !chkid_cmp(chkid, &ent->id);
}

static uint32_t __hash(const void *key)
{
        return ((chkid_t *)key)->id;
}

static int __drop(void *value, cache_entry_t *cent)
{
        entry_t *ent;

        (void) cent;
        ent = (entry_t *)value;
        yfree((void **)&ent);

        return 0;
}

static int __entry_load(const chkid_t *chkid, entry_t **_ent, int *_size)
{
        int ret, size, level;
        entry_t *ent;

        level = __disk_getlevel(chkid);
        if (level < 0) {
                ret = -level;
                if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        size = sizeof(entry_t);
        ret = ymalloc((void **)&ent, size);
        if (ret)
                GOTO(err_ret, ret);

        ent->id = *chkid;
        ent->level = level;

        *_ent = ent;
        *_size = size;

        return 0;
err_ret:
        return ret;
}

static int __disk_iterator(const char *parent,const char *name, void *_max)
{
        int ret, idx, *max;
        char path[MAX_PATH_LEN];

        (void) parent;
        max = _max;

        if (isdigit(name[0])) {
                snprintf(path, MAX_PATH_LEN, "%s/%s", parent, name);

                if (atoi(name) != 0) {
                        if (!sy_is_mountpoint(path, EXT3_SUPER_MAGIC)
                                        && !sy_is_mountpoint(path, EXT4_SUPER_MAGIC)) {
                                DWARN("%s is not mount point\n", path);
                                ret = ENOENT;
                                goto err_ret;
                        }
                }

                idx = atoi(name);

                *max = *max < idx ? idx : *max;
        } else {
                ret = EIO;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __disk_createroot()
{
        int ret;
        char path[MAX_PATH_LEN];

        snprintf(path, MAX_PATH_LEN, "%s/disk/0", ng.home);

        ret = path_validate(path, 1, 1);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int disk_analysis()
{
        return ENOSYS;
}

int disk_init(const char *home, uint64_t _max_object)
{
        int ret, max, i, size, ent_size;
        char path[MAX_PATH_LEN], subpath[MAX_PATH_LEN];
        struct stat stbuf;
        uint64_t max_object;

        ret = __disk_createroot();
        if (ret)
                GOTO(err_ret, ret);

        (void)_max_object;
        (void) home;

        snprintf(path, MAX_PATH_LEN, "%s/disk", ng.home);
        max = -1;
        ret = _dir_iterator(path, __disk_iterator, &max);
        if (ret)
                GOTO(err_ret, ret);

        if (max == -1) {
                ret = EIO;
                GOTO(err_ret, ret);
        }

        for (i = 0; i < max; i++) {
                snprintf(subpath, MAX_PATH_LEN, "%s/%u", path, i);

                ret = stat(path, &stbuf);
                if (ret) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                if (!S_ISDIR(stbuf.st_mode)) {
                        ret = EIO;
                        GOTO(err_ret, ret);
                }
        }

        size = sizeof(disk_t) + sizeof(level_t) * (max + 1);
        ret = ymalloc((void **)&disk, size);
        if (ret)
                GOTO(err_ret, ret);

        memset(disk, 0x0, size);

        disk->chklist = NULL;
        disk->chklist_count = 0;
        disk->level_count = max + 1;

        ret = sy_spin_init(&disk->level_lock);
        if (ret)
                GOTO(err_ret, ret);

        for (i = 0; i < gloconf.disk_worker; i++) {
                ret = sy_spin_init(&disk->big_lock[i]);
                if (ret)
                        GOTO(err_ret, ret);

                ret = io_setup(MAX_SUBMIT, &disk->ctx[i]);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }
        }

        ent_size = sizeof(cache_entry_t) + sizeof(entry_t);

        /*if (gloconf.cache_size / sizeof(entry_t) < _max_object) {*/
                /*DWARN("need more mem for cds\n");*/
                /*max_object = gloconf.cache_size / ent_size;*/
        /*} else*/
                /*max_object = _max_object;*/

        max_object = gloconf.chunk_entry_max;

        ret = cache_init(&cache, max_object, max_object * ent_size,
                        __cmp, __hash, __drop, 100, "disk");
        if (ret)
                GOTO(err_ret, ret);

        ret = chkinfo_init();
        if (ret)
                GOTO(err_ret, ret);

#if 0
        ret = bh_register("disk_analysis", __disk_analysis, NULL, (60 * 60 * 24 * 7)); /*a week*/
        if (ret)
                GOTO(err_ret, ret);
#endif

        ret = dpool_init(&dpool_64, DISK_64M, cdsconf.prealloc_max, disk->level_count);
        if (ret)
                GOTO(err_ret, ret);

        cdsconf.queue_depth = cdsconf.queue_depth < MAX_SUBMIT ? cdsconf.queue_depth : MAX_SUBMIT;

        return 0;
err_ret:
        return ret;
}

static void __disk_release(cache_entry_t *cent)
{
        cache_release(cent);
}

static int __disk_get(const chkid_t *chkid, cache_entry_t **_cent)
{
        int ret, size;
        entry_t *ent = NULL;
        cache_entry_t *cent;

        DBUG("get "CHKID_FORMAT"\n", CHKID_ARG(chkid));

retry:
        ret = cache_get(cache, chkid, &cent);
        if (ret) {
                if (ret == ENOENT) {
                        ret = __entry_load(chkid, &ent, &size);
                        if (ret) {
                                if (ret == ENOENT)
                                        goto err_ret;
                                else
                                        GOTO(err_ret, ret);
                        }

                        ret = cache_create_lock(cache, chkid, &cent);
                        if (ret) {
                                __drop(ent, NULL);

                                if (ret == EEXIST)
                                        goto retry;
                                else
                                        GOTO(err_ret, ret);
                        }

                        cent->value = ent;

                        cache_increase(cache, cent, size);

                        cache_unlock(cent);
                } else
                        GOTO(err_ret, ret);
        }

        *_cent = cent;

        return 0;
err_ret:
        return ret;
}

int disk_get_syncfd(const chkid_t *chkid, int level, int *_fd)
{
        int ret, fd;
        char path[MAX_PATH_LEN];

        __disk_build_chkpath(path, chkid, level);
        fd = open(path, O_RDWR | O_DSYNC);
        if (-1 == fd) {
                DERROR("open %s failed\n", path);
                ret = errno;
                GOTO(err_ret, ret);
        }

        *_fd = fd;

        return 0;
err_ret:
        return ret;
}

static int __disk_get_fd(const chkid_t *chkid, uint32_t offset, uint32_t count, int level, int *_fd)
{
        int ret, fd;
        char path[MAX_PATH_LEN];

        __disk_build_chkpath(path, chkid, level);
        if (cdsconf.io_sync) {
                if (offset % PAGE_SIZE_4K || count % PAGE_SIZE_4K) {
                        DBUG("dsync offset %u len %d\n", offset, count);
                        fd = open(path, O_RDWR | O_DSYNC);
                } else {
                        DBUG("direct doffset %u len %d\n", offset, count);
                        fd = open(path, O_RDWR | O_DIRECT);
                }
        } else {
                fd = open(path, O_RDWR);
        }

        DBUG("open %s fd %d offset %llu len %llu\n", path, fd, (LLU)offset, (LLU)count);
        if (-1 == fd) {
                DERROR("open %s failed\n", path);
                ret = errno;
                GOTO(err_ret, ret);
        }

        *_fd = fd;
        return 0;
err_ret:
        return ret;
}

static int __disk_entry_create(const chkid_t *chkid, int chk_size, entry_t **_ent, int *_size)
{
        int ret, fd = -1, size, level, i;
        entry_t *ent;
        char path[MAX_PATH_LEN], tmp[MAX_PATH_LEN];

        level = -1;
        for (i = disk->level_count - 1; i >= 0; i--) {
                __disk_build_chkpath(path, chkid, i);

                if (chk_size == DISK_64M) {
                        ret = path_validate(path, YLIB_NOTDIR, YLIB_DIRCREATE);
                        if (ret)
                                GOTO(err_ret, ret);

                        ret = dpool_get(dpool_64, tmp, i);
                        if (ret) {
                                if (ret == ENOSPC)
                                        continue;
                                else
                                        GOTO(err_ret, ret);
                        }

                        ret = rename(tmp, path);
                        if (ret) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }
                } else {
                        fd = _open(path, O_CREAT | O_EXCL | O_WRONLY | DISK_O_FLAG, 0644);
                        if (fd < 0) {
                                ret = -fd;
                                DERROR("create(%s, ...)\n", path);
                                if (ret == ENOENT)
                                        goto err_ret;
                                else
                                        GOTO(err_ret, ret);
                        }

                        if (chk_size) {
                                ret = _fallocate(fd, chk_size);
                                if (ret)
                                        GOTO(err_fd, ret);
                        }
                        close(fd);
                }

                level = i;
                break;
        }

        if (level == -1) {
                ret = ENOSPC;
                GOTO(err_fd, ret);
        }

        size = sizeof(entry_t);

        ret = ymalloc((void **)&ent, size);
        if (ret)
                GOTO(err_fd, ret);

        ent->id = *chkid;
        ent->level = level;

        *_ent = ent;
        *_size = size;

        ret = __disk_infoaddchk(chkid, chk_size);
        if (ret)
                GOTO(err_free, ret);

        return 0;
err_free:
        yfree((void **)&ent);
err_fd:
        close(fd);
        unlink(path);
err_ret:
        return ret;
}

static int __disk_create(cache_entry_t **_cent, const chkid_t *chkid, int chk_size)
{
        int ret, size;
        entry_t *ent = NULL;
        cache_entry_t *cent;

retry:
        ret = __disk_get(chkid, &cent);
        if (ret) {
                if (ret == ENOENT) {
                        ret = cache_create_lock(cache, chkid, &cent);
                        if (ret) {
                                if (ret == EEXIST)
                                        goto retry;
                                else
                                        GOTO(err_ret, ret);
                        }

                        DBUG("chk "CHKID_FORMAT"\n", CHKID_ARG(chkid));

                        ret = __disk_entry_create(chkid, chk_size, &ent, &size);
                        if (ret) {
                                UNIMPLEMENTED(__DUMP__);
                        }

                        cent->value = ent;

                        cache_increase(cache, cent, size);

                        cache_unlock(cent);
                } else
                        GOTO(err_ret, ret);
        }

        *_cent = cent;

        return 0;
err_ret:
        return ret;
}

int disk_create(const chkid_t *chkid, int size)
{
        int ret;
        cache_entry_t *cent;

        ret = __disk_create(&cent, chkid, size);
        if (ret)
                GOTO(err_ret, ret);

        __disk_release(cent);

        return 0;
err_ret:
        return ret;
}

static int __disk_io_trans(struct iocb **ioarray, iocb_t *_iocb, int left, diskio_t *diskio)
{
        int ret, retval, i, fd;
        char path[MAX_PATH_LEN];
        cache_entry_t *cent;
        entry_t *ent;
        struct iovec *iov, *iovs;
        iocb_t *iocb;
        uint64_t offset, count;

#ifdef __CENTOS5__
        if (left < diskio->count)
                return 0;
#else
        (void) offset;
        (void) iovs;
        (void) fd;
        (void) path;
#endif

        ret = __disk_get(&diskio->id, &cent);
        if (ret)
                GOTO(err_ret, ret);

        ret = cache_rdlock(cent);
        if (ret)
                GOTO(err_release, ret);

        ent = cent->value;

        cache_unlock(cent);

        DBUG("offset %llu + %llu count %u\n", (LLU)diskio->offset, (LLU)diskio->offset, diskio->count);

#ifdef __CENTOS5__
        DBUG("job[%u] %s status %u\n", diskio->job->jobid.idx, diskio->job->name, diskio->job->status);

        diskio->ref = diskio->count;
        iovs = diskio->buf;

        offset = 0;
        for (i = 0; i < diskio->count; i++) {
                iov = &iovs[i];
                iocb = &_iocb[i];

                /*YASSERT(iov->iov_len % SDFS_BLOCK_SIZE == 0);*/
                YASSERT(iov->iov_len <= PAGE_SIZE);

                ret = __disk_get_fd(&diskio->id, diskio->offset+offset, iov->iov_len, ent->level, &fd);
                if (ret) {
                        ret = errno;
                        GOTO(err_lock, ret);
                }

                if (diskio->op == DISKIO_OP_READ) {
                        io_prep_pread(&iocb->iocb, fd, iov->iov_base, iov->iov_len,
                                        diskio->offset + offset);
                } else if (diskio->op == DISKIO_OP_WRITE) {
                        io_prep_pwrite(&iocb->iocb, fd, iov->iov_base, iov->iov_len,
                                        diskio->offset + offset);
                } else {
                        ret = EINVAL;
                        DERROR("got invald op %u\n", diskio->op);
                        close(fd);
                        GOTO(err_lock, ret);
                }

                iocb->cent = cent;
                iocb->diskio = diskio;
                iocb->fd = fd;
                iocb->iocb.data = (void *)iocb;
                ioarray[i] = &iocb->iocb;
                offset+= iov->iov_len;
        }

        YASSERT(diskio->count);

        retval = diskio->count;
#else
        (void) iov;
        (void) left;
        (void) i;
        diskio->ref = 1;
        iocb = _iocb;
        count = 0;

        for (i = 0; i < diskio->count; i++) {
                iovs = diskio->buf;
                iov = &iovs[i];
                count += iov->iov_len;
        }

        DBUG("op %d "OBJID_FORMAT" count %lu offset %d\n", diskio->op, OBJID_ARG(&diskio->id), count, diskio->offset);
        ret = __disk_get_fd(&diskio->id, diskio->offset, count, ent->level, &fd);
        if (ret) {
                ret = errno;
                GOTO(err_lock, ret);
        }

        if (diskio->op == DISKIO_OP_READ) {
                io_prep_preadv(&iocb->iocb, fd, diskio->buf, diskio->count, diskio->offset);
        } else if (diskio->op == DISKIO_OP_WRITE) {
                io_prep_pwritev(&iocb->iocb, fd, diskio->buf, diskio->count, diskio->offset);
        } else {
                ret = EINVAL;
                DERROR("got invald op %u\n", diskio->op);
                close(fd);
                GOTO(err_lock, ret);
        }

        iocb->iocb.aio_reqprio = 0;
        iocb->cent = cent;
        iocb->diskio = diskio;
        iocb->fd = fd;
        iocb->iocb.data = (void *)iocb;
        ioarray[0] = &iocb->iocb;

        retval = 1;
#endif

        return retval;
err_lock:
        cache_unlock(cent);
err_release:
        __disk_release(cent);
err_ret:
        return -ret;
}

static int __disk_io_release(const iocb_t *iocb)
{
        /*cache_unlock(iocb->cent);*/
        __disk_release(iocb->cent);

        DBUG("ref %u\n", iocb->cent->ref);

        return 0;
}

#ifndef __CENTOS5__
static void __disk_iov_clear(void *_buf, int _count, int _size, int _offset)
{
        struct iovec *iov, *iovs, *_iovs;
        int begin, i, count, offset, size, cp;

        begin = _offset / PAGE_SIZE;
        YASSERT(begin < _count);
        _iovs = _buf;
        iovs = &_iovs[begin];
        count = _count - begin;
        offset = _offset % PAGE_SIZE;
        size = _size;

        for (i = 0; i < count; i++) {
                iov = &iovs[i];

                /*YASSERT(iov->iov_len <= PAGE_SIZE && iov->iov_len % SDFS_BLOCK_SIZE == 0);*/
                YASSERT(iov->iov_len <= PAGE_SIZE);

                cp = PAGE_SIZE - offset;
                cp = cp < size ? cp : size;

                memset(iov->iov_base + offset, 0x0, cp);

                DBUG("clear offset %u len %u begin %u\n", offset, cp, begin);

                //DINFO("len %llu\n", (LLU)*(uint64_t*)((seg_t *)buf.list.next)->ptr);

                size -= cp;
                offset = 0;
        }
}
#endif

inline static int __disk_io_submit__(const diskio_t **diskios, int _count, int hash)
{
        int ret, count, i, t, r, j, left, got, skip, commit, fail;
        struct io_event events[MAX_SUBMIT], *ev;
        struct iocb *ioarray[MAX_SUBMIT];
        iocb_t queue[MAX_SUBMIT], *iocb;
        diskio_t *diskio;
        struct timespec tmo;

        count = _count < cdsconf.queue_depth ? _count : cdsconf.queue_depth;

        got = 0;
        skip = 0;
        fail = 0;
        commit = 0;
        for (i = 0; i < count; i++) {
                diskio = (diskio_t *)diskios[i];

                if (diskio->status & __DISKIO_SKIP__) {
                        DBUG("skip chk "CHKID_FORMAT" idx %u, count %u got %u\n",
                                        (LLU)CHKID_ARG(&diskio->id), i, count, got);
                        skip++;
                        continue;
                }

                ret = __disk_io_trans(&ioarray[got], &queue[got], MAX_SUBMIT - got, diskio);
                if (ret < 0) {
                        ret = -ret;
                        if (ret == ENOENT) {
                                DWARN("chk "CHKID_FORMAT" not exist\n", CHKID_ARG(&diskio->id));

                                YASSERT(diskio->job);
                                job_resume1(diskio->job, ENOENT, diskio->idx, 0);
                                fail++;

                                continue;
                        } else
                                GOTO(err_ret, ret);
                }

                if (ret == 0)
                        break;

                got += ret;
                commit++;
        }

        if (got == 0) {
                goto out;
        }

        YASSERT(ioarray[0]->data);

        ret = io_submit(disk->ctx[hash], got, ioarray);
        if (ret != got) {
                ret = -ret;
                YASSERT(0);
                GOTO(err_ret, ret);
        }

        t = time(NULL);
        if (t == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        tmo.tv_sec = t + DISK_TIMEOUT;
        tmo.tv_nsec = 0;

        left = got;
        while (left > 0) {
                r = io_getevents(disk->ctx[hash], 1, left > (MAX_SUBMIT)
                                ? (MAX_SUBMIT) : (long) left, events, &tmo);
                if (r > 0) {
                        for (j = 0; j < r; j++) {
                                ev = &events[j];
                                iocb = ev->data;

                                YASSERT(iocb);

                                diskio = iocb->diskio;

                                close(iocb->fd);
                                YASSERT(diskio);
                                YASSERT(diskio->ref > 0);
                                YASSERT(diskio->job);

                                if ((long long)ev->res >= 0) {
#ifndef __CENTOS5__
                                        if ((int)ev->res != diskio->size) {
                                                YASSERT((int)ev->res < diskio->size);

                                                if (diskio->op != DISKIO_OP_READ) {
                                                        DERROR("res %llu size %u offset %llu op %u\n",
                                                                        (LLU)ev->res, diskio->size,
                                                                        (LLU)diskio->offset, diskio->op);

                                                        //YASSERT(0);
                                                } else {
                                                        __disk_iov_clear(diskio->buf, diskio->count,
                                                                        diskio->size - ev->res, ev->res);
                                                }
                                        }
#endif

                                        DBUG("res %llu\n", (LLU)ev->res);

                                        diskio->ref--;

                                        if (diskio->ref == 0) {
                                                DBUG("resume job[%u] %p\n", diskio->job->jobid.idx, diskio->job);

                                                YASSERT(diskio->job);
                                                job_resume1(diskio->job, 0, diskio->idx, 0);

                                                ret = __disk_io_release(iocb);
                                                if (ret)
                                                        GOTO(err_ret, ret);
                                        }
                                } else {
                                        ret = -ev->res;
                                        DERROR("submit ret %d %s\n", ret, strerror(ret));
                                        //YASSERT(0);
                                        EXIT(ret);
                                }
                        }

                        left -= r;
                } else {
                        ret = -r;

                        if (ret == EINTR)
                                continue;
                        DERROR("submit ret %d %s\n", ret, strerror(ret));
                        EXIT(ret);
                }
        }

out:
        return commit + skip + fail;
err_ret:
        return -ret;
}

int __disk_io_submit(const diskio_t **diskios, int count, int hash)
{
        int ret, idx;

        ret = sy_spin_lock(&disk->big_lock[hash]);
        if (ret)
                GOTO(err_ret, ret);

        ANALYSIS_BEGIN(0);

        idx = 0;
retry:

#if 1
        ret = __disk_io_submit__(&diskios[idx], count, hash);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_lock, ret);
        }
#else
        ret = __disk_io_submit_sync(&diskios[idx], count);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_lock, ret);
        }
#endif

        YASSERT(ret > 0 && ret <= count);

        count -= ret;
        idx += ret;

        if (count > 0)
                goto retry;

        ANALYSIS_QUEUE(0, 1000*1000, "disk_io_submit")

        sy_spin_unlock(&disk->big_lock[hash]);

        return 0;
err_lock:
        sy_spin_unlock(&disk->big_lock[hash]);
err_ret:
        return ret;
}

int disk_read_raw(const chkid_t *id, char *buf, int len, int off)
{
        int ret, fd;
        cache_entry_t *cent;
        entry_t *ent;
        char path[MAX_PATH_LEN];

        ret = __disk_get(id, &cent);
        if (ret) {
                if (ret == ENOENT) {
                        goto err_ret;
                } else
                        GOTO(err_ret, ret);
        }

        ent = cent->value;

        __disk_build_chkpath(path, id, ent->level);

        fd = open(path, O_RDONLY);
        if (fd < 0) {
                ret = errno;
                if (ret == ENOENT)
                        GOTO(err_release, ret);
                else
                        EXIT(ret);
        }

        ret = _pread(fd, buf, len, off);
        if (ret < 0) {
                ret = -ret;
                if (ret == EIO)
                        EXIT(ret);
                else
                        GOTO(err_fd, ret);
        }

        close(fd);
        __disk_release(cent);

        return ret;
err_fd:
        close(fd);
err_release:
        __disk_release(cent);
err_ret:
        return -ret;
}

int disk_write_raw(const chkid_t *id, const char *buf, int len, int off)
{
        int ret, fd;
        cache_entry_t *cent;
        entry_t *ent;
        char path[MAX_PATH_LEN];

        ret = __disk_get(id, &cent);
        if (ret)
                GOTO(err_ret, ret);

        ent = cent->value;

        __disk_build_chkpath(path, id, ent->level);

        fd = open(path, O_RDWR);
        if (fd < 0) {
                ret = errno;
                if (ret == ENOENT)
                        GOTO(err_release, ret);
                else
                        EXIT(ret);
        }

        ret = _pwrite(fd, buf, len, off);
        if (ret < 0) {
                ret = -ret;
                if (ret == EROFS || ret == EIO)
                        EXIT(ret);
                else
                        GOTO(err_fd, ret);
        }

        ret = fsync(fd);
        if (ret < 0) {
                ret = errno;
                GOTO(err_fd, ret);
        }

        close(fd);
        __disk_release(cent);

        return ret;
err_fd:
        close(fd);
err_release:
        __disk_release(cent);
err_ret:
        return -ret;
}

int disk_write_raw1(const chkid_t *id, const buffer_t *buf, int len, int off)
{
        int ret, fd;
        cache_entry_t *cent;
        entry_t *ent;
        char path[MAX_PATH_LEN];

        ret = __disk_get(id, &cent);
        if (ret) {
                DWARN("chk" OBJID_FORMAT " %u %s\n", OBJID_ARG(id),
                                ret, strerror(ret));
                goto err_ret;
        }

        ent = cent->value;

        __disk_build_chkpath(path, id, ent->level);

        fd = open(path, O_RDWR);
        if (fd < 0) {
                ret = errno;
                if (ret == ENOENT)
                        GOTO(err_release, ret);
                else
                        EXIT(ret);
        }

        ret = mbuffer_writefile(buf, fd, off, len);
        if (ret) {
                if (ret == EROFS || ret == EIO)
                        EXIT(ret);
                else
                        GOTO(err_fd, ret);
        }

        ret = fsync(fd);
        if (ret < 0) {
                ret = errno;
                GOTO(err_fd, ret);
        }

        close(fd);
        __disk_release(cent);

        return len;
err_fd:
        close(fd);
err_release:
        __disk_release(cent);
err_ret:
        return -ret;
}

int disk_replace(const chkid_t *chkid, const char *_path)
{
        int ret, slevel, max, hash;
        cache_entry_t *cent;
        char path[MAX_PATH_LEN];

        ret = disk_getlevel(chkid, &slevel, &max);
        if (ret)
                GOTO(err_ret, ret);

        __disk_build_chkpath(path, chkid, slevel);

        DBUG("got  from %s, will remove %s\n", _path, path);

        hash = (chkid->id + chkid->idx) % gloconf.disk_worker;
        ret = sy_spin_lock(&disk->big_lock[hash]);
        if (ret)
                GOTO(err_ret, ret);

        ret = __disk_get(chkid, &cent);
        if (ret)
                GOTO(err_lock, ret);

        ret = cache_wrlock(cent);
        if (ret)
                GOTO(err_release, ret);

        unlink(path);

        ret = rename(_path, path);
        if (ret < 0) {
                ret = errno;
                DERROR("rename %s to %s fail ret %u\n", _path, path, ret);
                EXIT(ret);
        }

        cache_unlock(cent);
        __disk_release(cent);
        sy_spin_unlock(&disk->big_lock[hash]);

        return 0;
err_release:
        __disk_release(cent);
err_lock:
        sy_spin_unlock(&disk->big_lock[hash]);
err_ret:
        return ret;
}

int disk_getsize(const chkid_t *id, int *size)
{
        int ret, fd;
        cache_entry_t *cent;
        entry_t *ent;
        struct stat stbuf;

        ret = __disk_get(id, &cent);
        if (ret) {
                if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        DBUG("offset 0 count 0\n");
        ent = cent->value;
        ret = __disk_get_fd(id, 0, 0, ent->level, &fd);
        if (ret)
                GOTO(err_release, ret);

        ret = fstat(fd, &stbuf);
        if (ret)
                GOTO(err_fd, ret);

        if (gloconf.check_version) {
                *size = stbuf.st_size - CHUNK_OFFSET;
        } else {
                *size = stbuf.st_size;
        }

        close(fd);
        __disk_release(cent);

        return 0;
err_fd:
        close(fd);
err_release:
        __disk_release(cent);
err_ret:
        return ret;
}

static int __disk_setvalue(const chkid_t *id, const char *key, const char *value)
{
        int ret, level;
        char path[MAX_PATH_LEN];

        level = __disk_getlevel(id);
        if (level < 0) {
                ret = -level;
                if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        __disk_build_chkpath(path, id, level);

        ret = attr_set(path, key, value, strlen(value), 0);
        if (ret < 0) {
                ret = errno;
                if (ret == EOPNOTSUPP) {
                        DWARN("set key %s at "CHKID_FORMAT" fail\n",
                                        key, CHKID_ARG(id));
                        goto err_ret;
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int disk_setvalue(const chkid_t *id, const char *key, const char *value)
{
        int ret, fd;
        cache_entry_t *cent;
        entry_t *ent;

        ret = cache_get(cache, id, &cent);
        if (ret) {
                if (ret == ENOENT) {
                        return __disk_setvalue(id, key, value);
                } else
                        GOTO(err_ret, ret);
        }

        ret = cache_wrlock(cent);
        if (ret)
                GOTO(err_release, ret);

        DBUG("offset 0 count 0\n");
        ent = cent->value;
        ret = __disk_get_fd(id, 0, 0, ent->level, &fd);
        if (ret)
                GOTO(err_lock, ret);

        ret = attr_setf(fd, key, value, strlen(value), 0);
        if (ret < 0) {
                ret = errno;
                if (ret == EOPNOTSUPP) {
                        DWARN("set key %s at "CHKID_FORMAT" fail\n",
                                        key, CHKID_ARG(id));
                        goto err_fd;
                } else
                        GOTO(err_fd, ret);
        }

        close(fd);
        cache_unlock(cent);
        __disk_release(cent);

        return 0;
err_fd:
        close(fd);
err_lock:
        cache_unlock(cent);
err_release:
        __disk_release(cent);
err_ret:
        return ret;
}

static int __disk_getvalue(const chkid_t *id, const char *key, char *value)
{
        int ret, level, buflen;
        char path[MAX_PATH_LEN];

        level = __disk_getlevel(id);
        if (level < 0) {
                ret = -level;
                DWARN("chk" OBJID_FORMAT " %u %s\n", OBJID_ARG(id), ret, strerror(ret));
                goto err_ret;
        }

        __disk_build_chkpath(path, id, level);

        buflen = MAX_BUF_LEN;
        ret = attr_get(path, key, value, &buflen, 0);
        if (ret < 0) {
                ret = errno;
                if (ret == EOPNOTSUPP) {
                        DWARN("get key %s value %s at %s fail\n",
                                        key, value, path);
                        goto err_ret;
                } else if (ret == ENODATA) {
                        goto err_ret;
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int disk_getvalue(const chkid_t *id, const char *key, char *value)
{
        int ret, buflen, fd;
        cache_entry_t *cent;
        entry_t *ent;

        ret = cache_get(cache, id, &cent);
        if (ret) {
                if (ret == ENOENT) {
                        return __disk_getvalue(id, key, value);
                } else
                        GOTO(err_ret, ret);
        }

        ret = cache_wrlock(cent);
        if (ret)
                GOTO(err_release, ret);

        ent = cent->value;
        DBUG("offset 0 count 0\n");
        ret = __disk_get_fd(id, 0, 0, ent->level, &fd);
        if (ret)
                GOTO(err_lock, ret);

        buflen = MAX_BUF_LEN;
        ret = attr_getf(fd, key, value, &buflen, 0);
        if (ret < 0) {
                ret = errno;
                if (ret == EOPNOTSUPP) {
                        DBUG("get key %s value %s at "CHKID_FORMAT" fail\n",
                                        key, value, CHKID_ARG(id));
                        goto err_fd;
                } else if (ret == ENODATA) {
                        goto err_fd;
                } else
                        GOTO(err_fd, ret);
        }

        close(fd);
        cache_unlock(cent);
        __disk_release(cent);

        return 0;
err_fd:
        close(fd);
err_lock:
        cache_unlock(cent);
err_release:
        __disk_release(cent);
err_ret:
        return ret;
}

int disk_rebalance(int count)
{
        (void)count;
        return ENOSYS;
}

int disk_getlevel(const chkid_t *id, int *level, int *max)
{
        int ret;
        cache_entry_t *cent;
        entry_t *ent;

        ret = __disk_get(id, &cent);
        if (ret) {
                if (ret == ENOENT) {
                        goto err_ret;
                } else
                        GOTO(err_ret, ret);
        }

        ent = cent->value;
        *level = ent->level;
        *max = disk->level_count - 1;

        __disk_release(cent);

        return 0;
err_ret:
        return ret;
}

int disk_truncate(const chkid_t *id, int size)
{
        int ret, fd;
        cache_entry_t *cent;
        entry_t *ent;
        struct stat stbuf;

        ret = __disk_get(id, &cent);
        if (ret) {
                if (ret == ENOENT) {
                        goto err_ret;
                } else
                        GOTO(err_ret, ret);
        }

        DBUG("offset 0 count 0\n");
        ent = cent->value;
        ret = __disk_get_fd(id, 0, 0, ent->level, &fd);
        if (ret)
                GOTO(err_release, ret);

        ret = fstat(fd, &stbuf);
        if (ret < 0) {
                ret = errno;
                GOTO(err_fd, ret);
        }

        if (stbuf.st_size != 0) {
                ret = EINVAL;
                GOTO(err_fd, ret);
        }

        ret = ftruncate(fd, size);
        if (ret < 0) {
                ret = errno;
                GOTO(err_fd, ret);
        }

        close(fd);
        __disk_release(cent);

        return 0;
err_fd:
        close(fd);
err_release:
        __disk_release(cent);
err_ret:
        return ret;
}

int disk_levelcount()
{
        return disk->level_count;
}

int disk_statvfs(struct statvfs *_stbuf)
{
        int ret, i;
        struct statvfs stbuf;
        char path[MAX_PATH_LEN];
        //uint64_t dsize;

        memset(_stbuf, 0x0, sizeof(*_stbuf));

        for (i = 0; i < disk->level_count; i++) {
                snprintf(path, MAX_PATH_LEN, "%s/disk/%u/", ng.home, i);

                ret = statvfs(path, &stbuf);
                if (ret == -1) {
                        ret = errno;
                        DERROR("statvfs(%s, ...) ret (%d) %s\n", path,
                                        ret, strerror(ret));
                        GOTO(err_ret, ret);
                }

                //dsize = dpool_size(dpool_64, i);

                _stbuf->f_bsize = stbuf.f_bsize;
                _stbuf->f_frsize = stbuf.f_frsize;
                _stbuf->f_blocks += stbuf.f_blocks;
                //_stbuf->f_bfree += (stbuf.f_bfree + dsize / stbuf.f_bsize);
                _stbuf->f_bfree += stbuf.f_bfree;
                _stbuf->f_bavail += stbuf.f_bavail;
                _stbuf->f_files += stbuf.f_files;
                _stbuf->f_ffree += stbuf.f_ffree;
                _stbuf->f_favail += stbuf.f_favail;
                _stbuf->f_fsid= stbuf.f_fsid;
                _stbuf->f_flag= stbuf.f_flag;
                _stbuf->f_namemax = stbuf.f_namemax;
        }

        return 0;
err_ret:
        return ret;
}

static void __dumpref(void *arg, void *ent)
{
        cache_entry_t *cent;

        (void) arg;
        cent = ent;

        if (cent->ref)
                DINFO("ref %u\n", cent->ref);
}

void disk_dumpref()
{
        cache_iterator(cache, __dumpref, NULL);
}

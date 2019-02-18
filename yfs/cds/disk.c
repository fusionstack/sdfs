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
#include "sdfs_aio.h"
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
#include "adt.h"

/*#define USE_AIO*/

typedef struct {
        int level;
        int count;
        int idx;
        uint64_t dfree;
        uint64_t dsize;
        chklist_t *chklist;
} level_t;

typedef struct {
        aio_context_t ctx[DISK_WORKER_MAX];
        sy_spinlock_t big_lock[DISK_WORKER_MAX];

        sy_spinlock_t level_lock;
        int chklist_count;
        chklist_t *chklist;
        int level_count;
        level_t level[0];
} disk_t;

static disk_t *disk;
extern int nofile_max;
static dpool_t *dpool_64;

#define TMPFILE_MAX (128)  /*persistence data, never alter it*/
#define TMPFILE_BLOCK (sizeof(chkop_t) + Y_BLOCK_MAX) /*persistence data, never alter it*/
#define DISK_ANALYSIS_INTERVAL (60 * 60)
#define DISK_64M (64 * 1024 * 1024 + 4096 * 2)

int disk_join(const diskid_t *diskid, struct statvfs *fsbuf)
{
        diskinfo_stat_t stat;
        FSTAT2DISKSTAT(fsbuf, &stat);
        return mond_rpc_diskjoin(diskid, cds_info.tier, (const uuid_t *)&ng.nodeid, &stat);
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

int disk_init(const char *home, uint64_t _max_object)
{
        int ret, max, i, size;
        char path[MAX_PATH_LEN], subpath[MAX_PATH_LEN];
        struct stat stbuf;

        ret = __disk_createroot();
        if (ret)
                GOTO(err_ret, ret);

        (void) _max_object;
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

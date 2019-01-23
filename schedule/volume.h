#ifndef __VOLUME_H__
#define __VOLUME_H__

#include <asm-generic/errno-base.h>
#include <asm-generic/errno.h>

#include "ylib.h"
#include "sysutil.h"
#include "fileinfo.h"
#include "sdfs_buffer.h"
#include "job_dock.h"

#define FILEINFO_SIZE(__oinfo__) (sizeof(fileinfo_t))

int volume_init();
int volume_truncate(const char *pool, const fileid_t *id, uint64_t size);
int volume_pwrite(const char *pool, const fileid_t *id, const char *_buf, size_t size, off_t offset);
int volume_pread(const char *pool, const fileid_t *oid, char *_buf, int size, off_t offset);
int volume_write(const char *pool, const io_t *io, const buffer_t *buf, nid_t *nid);
int volume_read(const char *pool, const io_t *io, buffer_t *buf, nid_t *nid);
int volume_unmap(const char *pool, const io_t *io);
void volume_write_async(const char *pool, const fileid_t *id, const buffer_t *buf, size_t size,
                      off_t offset, func1_t func, void *arg);
void volume_read_async(const char *pool, const fileid_t *id, buffer_t *buf, size_t size,
                     off_t offset, func1_t func, void *arg);

int volume_cleanup_bh(const chkid_t *id, const char *name);
int volume_rmsnap_bh(const chkid_t *id, const char *name);
int volume_rollback_bh(const chkid_t *id, const char *name);
int volume_flat_bh(const chkid_t *id, const char *name);

int volume_snapshot_read(const char *pool, const fileid_t *parent, const fileid_t *fileid,
                         buffer_t *_buf, size_t _size, off_t offset, BOOL fillzero);
int volume_snapshot_flat(const char *pool, const fileid_t *id, int idx, int force);
int volume_snapshot_diff(const char *pool, const fileid_t *parent, const fileid_t *fileid,
                         const fileid_t *snapdst, buffer_t *_buf, size_t _size, off_t offset);

#endif

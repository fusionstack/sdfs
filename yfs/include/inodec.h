#ifndef __INODEC_H__
#define __INODEC_H__

#include <stdint.h>

#include "skiplist.h"
#include "file_proto.h"
#include "md_proto.h"
#include "cache.h"
#include "yfs_md.h"
#include "file_proto.h"
#include "chk_proto.h"

int lookup_insert(const fileid_t *parent, const char *name, fileid_t *fileid);
int lookup_lookup(const fileid_t *parent, const char *name, fileid_t *fileid);
int lookup_delete(const fileid_t *parent, const char *name);
int lookup_init();
void preload_queue(const fileid_t *fileid);
int preload_init();

int inodec_getattr(md_proto_t *md, const fileid_t *fileid);
int inodec_load(md_proto_t *md);
int inodec_set_worm_fid(md_proto_t *_md, const fileid_t *fileid, uint64_t fid,
                uint32_t atime);
int inodec_set_quotaid(md_proto_t *_md, const fileid_t *fileid,
                uint64_t quotaid, uint32_t atime);
int inodec_symlink(symlink_md_t *md, const fileid_t *parent, const char *link_name,
                   uint32_t mode, uint32_t uid, uint32_t gid, uint32_t atime,
                   const char *link_target);
int inodec_link(fileinfo_t *md, const fileid_t *fileid,
                const fileid_t *parent, const char *name);
int inodec_fsync(fileinfo_t *_md, const fileid_t *fileid, uint32_t count,
                 uint64_t offset, uint32_t mtime);
int inodec_update(const fileinfo_t *md);
int inodec_init();


#endif /* __CHUNK_H__ */

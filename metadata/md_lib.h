#ifndef __MDC_LIB_H__
#define __MDC_LIB_H__

#include <sys/types.h>
#include <sys/statvfs.h>
#include <sys/stat.h>
#include <unistd.h>

#include "chk_proto.h"
#include "net_global.h"
#include "yfs_chunk.h"
#include "md_root.h"
#include "md.h"
#include "yfs_file.h"
#include "sdfs_lib.h"

/* dir.c */
extern int md_readdir(const fileid_t *fileid, off_t offset, void **de,
                       int *delen);
extern int md_readdirplus(const fileid_t*, off_t offset, void **de,
                          int *delen);
extern int md_readdirplus_count(const fileid_t*, file_statis_t *);
extern int md_readdirplus_with_filter(const fileid_t *fileid, off_t offset,
                                      void **de, int *delen, const filter_t *filter);

/* node.c */
int md_mkdir(const fileid_t *parent, const char *name, const setattr_t *setattr, fileid_t *fileid);
int md_utime(const fileid_t *fileid, const struct timespec *atime,
             const struct timespec *mtime, const struct timespec *ctime);
int md_chmod(const fileid_t *fileid, mode_t mode);
int md_setattr(const fileid_t *fileid, const setattr_t *setattr, int force);
int md_set_wormid(const fileid_t *fileid, uint64_t fid);
int md_list_worm(worm_t *wormlist, uint32_t max_size, int *count, const nid_t *_peer);
int md_chown(const fileid_t *fileid, uid_t uid, gid_t gid);
int md_unlink(const fileid_t *parent, const char *name, md_proto_t *md);
int md_rename(const fileid_t *fparent,
               const char *fname, const fileid_t *tparent, const char *toname);
int md_remove(const fileid_t *fileid);

int md_rmdir(const fileid_t *parent, const char *name);
int md_link2node(const fileid_t *fileid, const fileid_t *parent,
                 const char *name);
int md_setxattr(const fileid_t *fileid, const char *name, const void *value,
                size_t size, int flags);
int md_getxattr(const fileid_t *fileid, const char *name, void *value, size_t *size);
int md_removexattr(const fileid_t *fileid, const char *name);
int md_listxattr(const fileid_t *fileid, char *list, size_t *size);
int md_childcount(const fileid_t *fileid, uint64_t *count);

int md_set_quotaid(const fileid_t *fileid, const fileid_t *quotaid);
// int md_list_quota(const quota_t *quota_owner, quota_type_t quota_type,
                  // quota_t **quota, int *);


int md_set_shareinfo(share_protocol_t prot, const void *req_buf, const int req_buflen);
int md_get_shareinfo(share_protocol_t prot, const void *req_buf,
                      const int req_buflen, char *rep_buf, uint32_t rep_buflen);
int md_list_shareinfo(share_protocol_t prot, const share_key_t *offset_key,
                       void **rep_buf, uint32_t *rep_buflen);
int md_remove_shareinfo(share_protocol_t prot,
                         const void *req_buf, const int req_buflen);

/* chunk.c */
int md_chunk_update(const chkinfo_t *chkinfo);//need lock
int md_chunk_newdisk(const chkid_t *chkid, chkinfo_t *chkinfo, int repmin, int flag);//need lock
int md_chunk_create(const fileinfo_t *md, uint64_t idx, chkinfo_t *chkinfo);
int md_chunk_load(const chkid_t *chkid, chkinfo_t *chkinfo);
int md_chunk_load_check(const chkid_t *chkid, chkinfo_t *chkinfo, int repmin);

int md_chkload(chkinfo_t *chk, const chkid_t *chkid, const nid_t *nid);
int md_chkupdate(const chkinfo_t *chkinfo, const nid_t *nid);

/* file.c */
int md_create(const fileid_t *parent, const char *name, const setattr_t *setattr,
              fileid_t *fileid);
int md_extend(const fileid_t *fileid, size_t size);
int md_id2name(const fileid_t *parent, uint64_t id, char *name, int size);
int md_truncate(const fileid_t *fileid, uint64_t length);
int md_symlink(const fileid_t *parent, const char *link_name, const char *link_target,
               uint32_t mode, uint32_t uid, uint32_t gid);
int md_readlink(const fileid_t *fileid, char *_buf);
int md_lookup(fileid_t *fileid, const fileid_t *parent, const char *name);
int md_getattr(const fileid_t *fileid, md_proto_t *md);
int md_mkvol(const char *name, const setattr_t *setattr, fileid_t *_fileid);
int md_rmvol(const char *name);
int md_dirlist(const dirid_t *dirid, uint32_t count, uint64_t offset, dirlist_t **dirlist);
int md_lookupvol(const char *name, fileid_t *fileid);
int md_initroot();
int md_system_volid(uint64_t *id);
int md_getlock(const fileid_t *fileid, sdfs_lock_t *lock);
int md_setlock(const fileid_t *fileid, const sdfs_lock_t *lock);

/*quota.c*/
extern int md_create_quota(quota_t *quota);
extern int md_get_quota(const fileid_t *quotaid, quota_t *quota, quota_type_t quota_type);
extern int md_modify_quota(const fileid_t *quotaid, INOUT quota_t *quota, const uint32_t modify_mask);
extern int md_remove_quota(const fileid_t *quotaid, const quota_t *quota);
extern int md_update_quota(const quota_t *quota);
extern int quota_remove_lvm(const fileid_t *dirid, int quota_type);
extern int quota_should_be_remove(const fileid_t *quotaid,
                                  const fileid_t *fileid, quota_t *_quota);

/* user.c */
extern int md_set_user(const user_t *user);
extern int md_get_user(const char *user_name, user_t *user);
extern int md_remove_user(const char *user_name);
extern int md_list_user(user_t **user, int *count);

/* group.c */
extern int md_set_groupinfo(const group_t *group);
extern int md_get_groupinfo(const char *group_name, group_t *group);
extern int md_remove_groupinfo(const char *group_name);
extern int md_list_groupinfo(group_t **_group, int *count);
extern int md_get_group_byid(gid_t gid, group_t *group);

int md_share_list_byprotocal(share_protocol_t protocol, shareinfo_t **_shareinfo, int *count);
int md_share_get_byname(const char *name, share_user_type_t type, shareinfo_t *shareinfo);
int md_share_get(const char *key, shareinfo_t *shareinfo);
int md_share_set(const char *key, const shareinfo_t *shareinfo);
/*redis.c*/
//extern int kunlock(const fileid_t *fileid);
//extern int klock(const fileid_t *fileid, int ttl);

/* file lock operation */
extern int md_flock_op(const fileid_t *fileid,
                       uss_flock_op_t flock_op,
                       uss_flock_t *flock);
#endif

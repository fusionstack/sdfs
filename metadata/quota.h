#ifndef _QUOTA_H_
#define _QUOTA_H_

#include "sdfs_quota.h"
#include "md_attr.h"

// functions
#if 0
extern int quota_inode_check_and_inc(const fileid_t *quotaid, const uid_t uid, const gid_t gid, const fileid_t *fileid);
extern int quota_space_check_and_inc(const fileid_t *quotaid, const uid_t uid, const gid_t gid, const fileid_t *fileid, const uint64_t space);
extern int quota_inode_dec(const fileid_t *quotaid, const uid_t uid, const gid_t gid, const fileid_t *lvmid);
extern int quota_space_dec(const fileid_t *quotaid, const uid_t uid, const gid_t gid, const fileid_t *lvmid, const uint64_t space);

extern int quota_chown(const fileid_t *fileid, uid_t new_uid, gid_t new_gid);
extern void quota_removeall(const fileid_t *dirid, const fileid_t *quotaid);
#endif

extern int quota_check_dec(const fileid_t *fileid);
extern int quota_inode_increase(const fileid_t *fileid, const setattr_t *setattr);
extern int quota_inode_decrease(const fileid_t *fileid, const setattr_t *setattr);
extern int quota_space_increase(const fileid_t *fileid, uid_t uid, gid_t gid, uint64_t space);
extern int quota_space_decrease(const fileid_t *fileid, uid_t uid, gid_t gid, uint64_t space);


#endif

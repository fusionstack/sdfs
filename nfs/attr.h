#ifndef _YNFS_ATTR_H__
#define _YNFS_ATTR_H__

#include <sys/types.h>
#include <sys/stat.h>
#include <rpc/rpc.h>
#include <unistd.h>

#include "nfs3.h"
#include "file_proto.h"

extern int get_postopattr(uint32_t dev, uint64_t ino, const char *path,
                          post_op_attr *attr);
extern int get_preopattr(uint32_t dev, uint64_t ino, mode_t mode,
                         preop_attr *attr);
extern int sattr_tomode(mode_t *, sattr *);
extern int get_postopattr_stat(post_op_attr *, struct stat *);

int sattr_utime(const fileid_t *fileid, int at, int mt, int ct);
int sattr_set(const fileid_t *fileid, const sattr *attr, const nfs3_time *ctime);
void get_preopattr1(const fileid_t *fileid, preop_attr *attr);
void get_postopattr1(const fileid_t *fileid, post_op_attr *attr);

#endif

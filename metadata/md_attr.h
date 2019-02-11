#ifndef __MD_ATTR_H__
#define __MD_ATTR_H__

#include "sdfs_lib.h"
#include "yfs_md.h"

#if 0
#define MODE_MAX 01777
#else
#define MODE_MAX 07777
#endif


typedef enum {
        idtype_nid = 0,
        idtype_fileid = 1,
        idtype_max = 3,
} fidtype_t;

void setattr_init(setattr_t *setattr, uint32_t mode,
                  int replica, const ec_t *ec, int uid, int gid, size_t size);
void setattr_update_time(setattr_t *setattr,
                         __time_how ahow,
                         const struct timespec *atime,
                         __time_how mhow,
                         const struct timespec *mtime,
                         __time_how chow,
                         const struct timespec *ctime);

void md_attr_inherit(md_proto_t *md, const md_proto_t *parent, const ec_t *ec, uint32_t mode);
int md_attr_getid(fileid_t *fileid, const fileid_t *parent, uint32_t type, const volid_t *volid);
int md_attr_init(md_proto_t *md, const setattr_t *setattr, uint32_t type,
                 const md_proto_t *parent, const fileid_t *fileid);
void md_attr_update(md_proto_t *md, const setattr_t *setattr);


#endif

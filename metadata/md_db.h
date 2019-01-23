#ifndef __MD_DB__
#define __MD_DB__

#include "md_root.h"
#include "md_attr.h"
#include "redis_util.h"
#include "sdfs_lib.h"

#define SDFS_MD "__system_md__"
#define SDFS_LOCK "__system_lock__"
#define SDFS_MD_SYSTEM "__system"


typedef struct {
        // 设置元数据
        int (*newrec)(const fileid_t *parent, const char *name,
                      const fileid_t *fileid, uint32_t type, int flag);
        
        int (*unlink)(const fileid_t *parent, const char *name);
        
        int (*lookup)(const fileid_t *parent, const char *name, fileid_t *fileid,
                      uint32_t *type);
        
        // 一次最多读多少？
        int (*readdir)(const fileid_t *parent, void *buf, int *buflen, uint64_t offset);
        
        int (*readdirplus)(const fileid_t *parent, void *buf, int *buflen, uint64_t offset);
#if 1
        int (*readdirplus_count)(const fileid_t *parent, file_statis_t *file_statis);
#endif
        int (*readdirplus_filter)(const fileid_t *parent, void *buf, int *buflen,
                                  uint64_t offset, const filter_t *filter);

        //int (*child_id2name)(const fileid_t *parent, uint64_t id, char *name);
        int (*dirlist)(const dirid_t *dirid, uint32_t count, uint64_t offset, dirlist_t **dirlist);
} dirop_t;

typedef struct {
        int (*init)();
        int (*create)(const fileid_t *parent, const setattr_t *setattr, int mode,
                      fileid_t *_fileid);
        //int (*del)(const fileid_t *fileid);
        int (*getattr)(const fileid_t *fileid, md_proto_t *md);
        int (*setattr)(const fileid_t *fileid, const setattr_t *setattr, int force);
        int (*extend)(const fileid_t *fileid, size_t size);
        int (*setxattr)(const fileid_t *id, const char *key, const char *value, size_t size, int flag);
        int (*getxattr)(const fileid_t *id, const char *key, char *value, size_t *value_len);
        int (*listxattr)(const fileid_t *id, char *list, size_t *size);
        int (*removexattr)(const fileid_t *id, const char *key);
        int (*childcount)(const fileid_t *parent, uint64_t *count);
        int (*unlink)(const fileid_t *fileid, md_proto_t *md);
        int (*remove)(const fileid_t *fileid, md_proto_t *md);
        int (*link)(const fileid_t *fileid);
        int (*symlink)(const fileid_t *fileid, const char *link_target);
        int (*readlink)(const fileid_t *fileid, char *link_target);
        int (*mkvol)(const setattr_t *setattr, const uint64_t *volid, fileid_t *_fileid);
        int (*setlock)(const fileid_t *fileid, const void *opaque, size_t len, int flag);
        int (*getlock)(const fileid_t *fileid, void *opaque, size_t *len);
} inodeop_t;

typedef struct {
        int (*update)(const chkinfo_t *chkinfo);
        int (*load)(const chkid_t *chkid, chkinfo_t *chkinfo);
        int (*create)(const chkinfo_t *chkinfo);
} chunkop_t;

typedef struct {
        int (*update)(root_type_t type, const char *key, const void *value, size_t len);
        int (*get)(root_type_t type, const char *key, void *value, size_t *len);
        int (*create)(root_type_t type, const char *key, const void *value, size_t len);
        int (*remove)(root_type_t type, const char *key);
        redisReply* (*scan)(root_type_t type, const char *match, uint64_t offset);
        int (*iter)(root_type_t type, const char *match, func2_t func, void *ctx);
        int (*lock)(root_type_t type);
        int (*unlock)(root_type_t type);
} kvop_t;


extern dirop_t __dirop__;
extern inodeop_t __inodeop__;
extern chunkop_t __chunkop__;
extern kvop_t __kvop__;

#endif

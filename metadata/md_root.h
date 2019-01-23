#ifndef __MD_ROOT_H__
#define __MD_ROOT_H__


typedef enum {
        roottype_user,
        roottype_group,
        roottype_share,
        roottype_quota,
        roottype_max,
} root_type_t;


const fileid_t *md_root_getid(root_type_t type);
#if 0
int md_root_isroot(const fileid_t *fileid);
#endif
int md_root_init();
int md_root_create(uint64_t volid);

#endif

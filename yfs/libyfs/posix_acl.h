#ifndef CEPH_POSIX_ACL
#define CEPH_POSIX_ACL

#define ACL_EA_VERSION          0x0002

#define ACL_USER_OBJ            0x01
#define ACL_USER                0x02
#define ACL_GROUP_OBJ           0x04
#define ACL_GROUP               0x08
#define ACL_MASK                0x10
#define ACL_OTHER               0x20

#define ACL_DEFAULT_EA_ENTRY_COUNT   6

#define ACL_EA_ACCESS  "system.posix_acl_access"
#define ACL_EA_DEFAULT "system.posix_acl_default"

typedef struct {
        uint16_t e_tag;
        uint16_t e_perm;
        uint32_t e_id;
}acl_ea_entry;

typedef struct {
        uint32_t      a_version;
        acl_ea_entry  a_entries[0];
} acl_ea_header;

extern int posix_acl_check(const void *xattr, size_t size);
extern int posix_acl_equiv_mode(const void *xattr, size_t size, mode_t *mode_p);
extern int posix_acl_default_get(void *acl_buf, size_t acl_buf_size, mode_t mode);
extern size_t posix_acl_ea_size(int count);

#endif

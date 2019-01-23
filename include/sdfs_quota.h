#ifndef _SDFS_QUOTA_H_
#define _SDFS_QUOTA_H_

#define QUOTA_NULL 0
#define QUOTA_DIR_PREFIX "dir"
#define QUOTA_GROUP_PREFIX "group"
#define QUOTA_USER_PREFIX "user"

#define MAX_SPACE_LIMIT (uint64_t)5764607523034234880 //5EB 5 *1024^6
#define MAX_INODE_LIMIT (uint64_t)5497558138880 //5T 5 * 1024^4

#define SPACE_HARD_BIT  0x00000001
#define SPACE_SOFT_BIT  0x00000002
#define SPACE_USED_BIT  0x00000004
#define INODE_HARD_BIT  0x00000008
#define INODE_SOFT_BIT  0x00000010
#define INODE_USED_BIT  0x00000020
#define SPACE_GRACE_BIT 0x00000040
#define INODE_GRACE_BIT 0x00000080
#define QUOTA_DELETE_BIT 0x0000100

#define QUOTA_MAX_ENTRY 10000
#define QUOTA_MAX_SIZE  1200000
#define QUOTA_MAX_COUNT 8192
#define QUOTA_MAX_LEVEL 8
#define QUOTA_PREFIX_LEN 256

typedef struct {
        uint64_t space_hard;
        uint64_t space_soft; //support future
        uint64_t space_used;
        uint64_t inode_hard;
        uint64_t inode_soft; //support future
        uint64_t inode_used;
        time_t space_grace;  //support future
        time_t inode_grace;  //support future
        fileid_t quotaid;
        fileid_t pquotaid;
        int quota_type;
        uid_t uid;
        gid_t gid;
        fileid_t dirid;
} quota_t;

typedef enum {
        QUOTA_DIR = 0,
        QUOTA_USER,
        QUOTA_GROUP,
        QUOTA_INVALID_TYPE,
}quota_type_t;

#endif

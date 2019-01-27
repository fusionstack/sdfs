#ifndef __SDFS_SHARE_H__
#define __SDFS_SHARE_H__

typedef enum {
        SHARE_RO, //read-only
        SHARE_RW, //read-write
        SHARE_INVALID_MOD,
}share_mode_t;

#define share_user_type_t uint32_t
#define SHARE_USER               0x000001
#define SHARE_GROUP              0x000002
#define SHARE_HOST               0x000004
#define SHARE_INVALID_USER       (SHARE_USER | SHARE_GROUP | SHARE_HOST)

#define SHARE_CIFS               0x000001
#define SHARE_FTP                0x000002
#define SHARE_NFS                0x000004
#define SHARE_INVALID_PROT       (SHARE_CIFS | SHARE_FTP | SHARE_NFS)

#define share_protocol_t uint32_t

typedef struct {
        fileid_t dirid;
        uid_t uid;
        gid_t gid;
        char uname[MAX_NAME_LEN]; //user name
        char gname[MAX_NAME_LEN]; //group name
        char hname[MAX_NAME_LEN]; //hostname
        char share_name[MAX_NAME_LEN]; //share name
        share_user_type_t usertype;
        share_protocol_t protocol;
        share_mode_t mode;
        char path[MAX_PATH_LEN];
} shareinfo_t;

#define share_cifs_t shareinfo_t
#define share_ftp_t shareinfo_t
#define share_nfs_t shareinfo_t

typedef struct {
        fileid_t dirid;
        share_user_type_t usertype;
        char name[MAX_NAME_LEN]; //user group or host name
        char share_name[MAX_NAME_LEN]; //share name
} share_key_t;

#endif

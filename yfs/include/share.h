#ifndef _SHARE_H_
#define _SHARE_H_

typedef enum {
        SHARE_RO, //read-only
        SHARE_RW, //read-write
        SHARE_INVALID_MOD,
}share_mode_t;

#if 0
typedef enum {
        SHARE_CIFS=0,
        SHARE_FTP,
        SHARE_NFS,
        SHARE_INVALID_PROT,
}share_protocol_t;

#endif

#if 1
#define share_user_type_t uint32_t
#define SHARE_USER               0x000001
#define SHARE_GROUP              0x000002
#define SHARE_HOST               0x000004
#define SHARE_INVALID_USER       (SHARE_USER | SHARE_GROUP | SHARE_HOST)

#else

typedef enum {
        SHARE_USER,
        SHARE_GROUP,
        SHARE_HOST,
        SHARE_INVALID_USER,
}share_user_type_t;

#endif


#define SHARE_CIFS               0x000001
#define SHARE_FTP                0x000002
#define SHARE_NFS                0x000004
#define SHARE_INVALID_PROT       (SHARE_CIFS | SHARE_FTP | SHARE_NFS)

#define share_protocol_t uint32_t


typedef struct {
        fileid_t dirid;
        uid_t uid;
        gid_t gid;
        char uname[MAX_NAME_LEN]; //user or group name
        char gname[MAX_NAME_LEN]; //user or group name
        char hname[MAX_NAME_LEN]; //user or group name
        char share_name[MAX_NAME_LEN]; //share name
        share_user_type_t usertype;
        share_protocol_t protocol;
        share_mode_t mode;
        char path[MAX_PATH_LEN];
} shareinfo_t;

#define share_cifs_t shareinfo_t
#define share_ftp_t shareinfo_t
#define share_nfs_t shareinfo_t

#if 0
typedef struct {
        fileid_t dirid;
        uid_t uid;
        gid_t gid;
        char name[MAX_NAME_LEN]; //user or group name
        char share_name[MAX_NAME_LEN]; //share name
        share_user_type_t usertype;
        share_mode_t mode;
        char path[MAX_PATH_LEN];
}share_cifs_t;

typedef struct {
        fileid_t dirid;
        uid_t uid;
        char name[MAX_NAME_LEN]; //user name
        share_user_type_t usertype;
        share_mode_t mode;
        char path[MAX_PATH_LEN];
}share_ftp_t;

typedef struct {
        fileid_t dirid;
        gid_t gid;
        char name[MAX_NAME_LEN]; //host or group name
        share_user_type_t usertype;
        share_mode_t mode;
        char path[MAX_PATH_LEN];
}share_nfs_t;
#endif

//for get the key of share info in leveldb,
//delete or get the operation need this key
typedef struct {
        fileid_t dirid;
        share_user_type_t usertype;
        char name[MAX_NAME_LEN]; //user group or host name
        char share_name[MAX_NAME_LEN]; //share name
}share_key_t;

#if 0
extern int share_mds_set(IN share_protocol_t prot, IN const char *buf, IN int buflen);
extern int share_mds_get(IN share_protocol_t prot, IN const share_key_t *key,
                         OUT char *rep_buf, OUT uint32_t *rep_buflen);
extern int share_mds_list(IN share_protocol_t prot, IN const share_key_t *offset_key,
                          OUT char *rep_buf, OUT uint32_t *rep_buflen);
extern int share_mds_remove(IN share_protocol_t prot, IN const share_key_t *key);
extern void share_get_key(IN const fileid_t *dir_id, IN const char *name,
                          IN const char *share_name, IN share_user_type_t user_type,
                          OUT share_key_t *share_key);
extern bool dir_is_shared(IN const fileid_t * dirid);
#endif

#endif

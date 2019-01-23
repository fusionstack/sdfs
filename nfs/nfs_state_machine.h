#ifndef __NFS_STATUS_MACHINE_H__
#define __NFS_STATUS_MACHINE_H__

#include <rpc/rpc.h>
#include <stdint.h>

#include "job.h"
#include "sdfs_conf.h"

#define NFS_MAXDATA_UDP 32768
#define NFS_MAX_UDP_PACKET (NFS_MAXDATA_UDP + 4096) /* The extra 4096 bytes are for the RPC header */

#define UNIX_PATH_MAX 108

#define NFS_REMOVED ".nfs3removed"

#define NFS_SERVICE_DEF "2049"
#define NFS_TCPDATA_MAX 1048576
#define NFS_RTMULT 2
#define NFS_NAMLEN_MAX 255

#define NFSMODE_FMT 0170000
#define NFSMODE_DIR 0040000
#define NFSMODE_CHR 0020000
#define NFSMODE_BLK 0060000
#define NFSMODE_REG 0100000
#define NFSMODE_LNK 0120000
#define NFSMODE_SOCK 0140000
#define NFSMODE_FIFO 0010000

#define NFS3_FHSIZE 64
#define NFS3_COOKIEVERFSIZE 8
#define NFS3_CREATEVERFSIZE 8
#define NFS3_WRITEVERFSIZE 8

typedef struct {
        int null;
        int getattr;
        int setattr;
        int lookup;
        int access;
        int readlink;
        int read;
        int write;
        int create;
        int mkdir;
        int symlink;
        int mknod;
        int remove;
        int rmdir;
        int rename;
        int link;
        int readdir;
        int readdirplus;
        int fsstat;
        int fsinfo;
        int pathconf;
        int commit;
        time_t last_output;
} nfs_analysis_t;

typedef enum {
        NFS3_OK = 0,
        NFS3_EPERM = 1,
        NFS3_ENOENT = 2,
        NFS3_EIO = 5,
        NFS3_ENXIO = 6,
        NFS3_EACCES = 13,
        NFS3_EEXIST = 17,
        NFS3_EXDEV = 18,
        NFS3_ENODEV = 19,
        NFS3_ENOTDIR = 20,
        NFS3_EISDIR = 21,
        NFS3_EINVAL = 22,
        NFS3_EFBIG = 27,
        NFS3_ENOSPC = 28,
        NFS3_EROFS = 30,
        NFS3_EMLINK = 31,
        NFS3_ENAMETOOLONG = 63,
        NFS3_ENOTEMPTY = 66,
        NFS3_EDQUOT = 69,
        NFS3_ESTALE = 70,
        NFS3_EREMOTE = 71,
        NFS3_EBADHANDLE = 10001,
        NFS3_ENOTSYNC = 10002,
        NFS3_EBADCOOKIE = 10003,
        NFS3_ENOTSUPP = 10004,
        NFS3_ETOOSMALL = 10005,
        NFS3_ESERVERFAULT = 10006,
        NFS3_EBADTYPE = 10007,
        NFS3_EJUKEBOX = 10008,
} nfs3_stat;

/**
 * \sa RFC1813
 */

/* Basic Data Types */
typedef char *nfspath3;

typedef enum {
        DONT_CHANGE = 0,
        SET_TO_SERVER_TIME,
        SET_TO_CLIENT_TIME,
} time_how;

typedef struct {
        bool_t set_it;
        uint32_t mode;
} set_mode;

typedef struct {
        bool_t set_it;
        uint32_t uid;
} set_uid;

typedef struct {
        bool_t set_it;
        uint32_t gid;
} set_gid;

typedef struct {
        bool_t set_it;
        uint64_t size;
} set_size;

typedef struct {
        uint32_t seconds;
        uint32_t nseconds;
} nfs3_time;

typedef struct {
        time_how set_it;
        nfs3_time time;
} set_time;

typedef struct {
        set_mode mode;
        set_uid uid;
        set_gid gid;
        set_size size;
        set_time atime;
        set_time mtime;
} sattr;

typedef enum {
        NFS3_REG = 1,
        NFS3_DIR,
        NFS3_BLK,
        NFS3_CHR,
        NFS3_LNK,       /* 5 */
        NFS3_SOCK,
        NFS3_FIFO,
} nfs3_ftype;

typedef struct {
        uint32_t data1;
        uint32_t data2;
} nfs3_specdata;

typedef struct {
        uint32_t len;
        char *val;
} nfs_fh3;

typedef struct {
        nfs3_ftype type;
        uint32_t mode;
        uint32_t nlink;
        uint32_t uid;
        uint32_t gid;
        uint64_t size;
        uint64_t used;
        nfs3_specdata rdev;
        uint64_t fsid;
        uint64_t fileid;
        nfs3_time atime;
        nfs3_time mtime;
        nfs3_time ctime;
} nfs3_fattr;

typedef struct {
        bool_t attr_follow;
        nfs3_fattr attr;
} post_op_attr;

typedef struct {
        uint64_t size;
        nfs3_time mtime;
        nfs3_time ctime;
} wcc_attr;

typedef struct {
        bool_t attr_follow;
        wcc_attr attr;
} preop_attr;

typedef struct {
        preop_attr before;
        post_op_attr after;
} wcc_data;

typedef struct {
        bool_t handle_follows;
        nfs_fh3 handle;
} postop_fh;

enum {
        NFS3_NULL = 0,
        NFS3_GETATTR,
        NFS3_SETATTR,
        NFS3_LOOKUP,
        NFS3_ACCESS,
        NFS3_READLINK,  /* 5 */
        NFS3_READ,
        NFS3_WRITE,
        NFS3_CREATE,
        NFS3_MKDIR,
        NFS3_SYMLINK,   /* 10 */
        NFS3_MKNOD,
        NFS3_REMOVE,
        NFS3_RMDIR,
        NFS3_RENAME,
        NFS3_LINK,      /* 15 */
        NFS3_READDIR,
        NFS3_READDIRPLUS,
        NFS3_FSSTAT,
        NFS3_FSINFO,
        NFS3_PATHCONF,  /* 20 */
        NFS3_COMMIT,
};

extern void regenerate_write_verifier(void);

typedef struct {
        nfs3_stat status;
        nfs3_fattr attr;
} getattr_ret;

typedef struct {
        nfs_fh3 obj;
} getattr_args;

typedef struct {
        bool_t check;
        union {
                nfs3_time obj_ctime;
        } sattrguard3_u;
} sattrguard3;

typedef struct {
        nfs_fh3 obj;
        sattr new_attributes;
        sattrguard3 guard;
} setattr3_args;

typedef struct {
        wcc_data obj_wcc;
} setattr3_resok;

typedef struct {
        wcc_data obj_wcc;
} setattr3_resfail;

typedef struct {
        nfs3_stat status;
        union {
                setattr3_resok resok;
                setattr3_resfail resfail;
        } setattr3_res_u;
} setattr3_res;

typedef struct {
        post_op_attr dir_attr;
        nfs_fh3 obj;
        post_op_attr obj_attr;
} lookup_retok;

typedef struct {
        post_op_attr dir_attr;
} lookup_retfail;

typedef struct {
        nfs3_stat status;
        union {
                lookup_retok ok;
                lookup_retfail fail;
        } u;
} lookup_ret;

typedef struct {
        nfs_fh3 dir;
        char *name;
} diropargs3;

typedef diropargs3 lookup_args;

typedef struct {
        diropargs3 from;
        diropargs3 to;
} rename_args;

typedef struct {
        wcc_data from;
        wcc_data to;
} rename_retok;

typedef struct {
        wcc_data from;
        wcc_data to;
} rename_retfail;

typedef struct {
        nfs3_stat status;
        union {
                rename_retok ok;
                rename_retfail fail;
        } u;
} rename_ret;

typedef struct {
        nfs_fh3 file;
        diropargs3 link;
} LINK3args;

typedef struct {
        post_op_attr file_attributes;
        wcc_data linkdir_wcc;
} LINK3resok;

typedef struct {
        post_op_attr file_attributes;
        wcc_data linkdir_wcc;
} LINK3resfail;

typedef struct {
        nfs3_stat status;
        union {
                LINK3resok ok;
                LINK3resfail fail;
        } u;
} LINK3res;

typedef struct {
        post_op_attr obj_attr;
        uint32_t access;
} access_retok;

typedef struct {
        post_op_attr obj_attr;
} access_retfail;

typedef struct {
        nfs3_stat status;
        union {
                access_retok ok;
                access_retfail fail;
        } u;
} access_ret;

#define ACCESS_READ    0x0001
#define ACCESS_LOOKUP  0x0002
#define ACCESS_MODIFY  0x0004
#define ACCESS_EXTEND  0x0008
#define ACCESS_DELETE  0x0010
#define ACCESS_EXECUTE 0x0020

typedef struct {
        nfs_fh3 obj;
        uint32_t access;
} access_args;

typedef struct {
        post_op_attr attr;
        uint32_t count;
        bool_t eof;
        struct {
                uint32_t len;
                char *val;
        } data;
} read_retok;

typedef struct {
        post_op_attr attr;
} read_retfail;

typedef struct {
        nfs3_stat status;
        union {
                read_retok ok;
                read_retfail fail;
        } u;
} read_ret;

typedef struct {
        nfs_fh3 file;
        uint64_t offset;
        uint32_t count;
} read_args;

typedef enum {
        UNSTABLE = 0,
        DATA_SYNC,
        FILE_SYNC,
} stable_how;

typedef struct {
        wcc_data file_wcc;
        uint32_t count;
        stable_how committed;
        char verf[NFS3_WRITEVERFSIZE];
} write_retok;

typedef struct {
        wcc_data file_wcc;
} write_retfail;

typedef struct {
        nfs3_stat status;
        union {
                write_retok ok;
                write_retfail fail;
        } u;
} write_ret;

typedef struct {
        nfs_fh3 file;
        uint64_t offset;
        uint32_t count;
        stable_how stable;
        struct {
                uint32_t len;
                char *val;
        } data;
} write_args;

typedef struct {
        wcc_data dir_wcc;
        postop_fh obj;
        post_op_attr obj_attr;
} create_retok;

typedef struct {
        wcc_data dir_wcc;
} create_retfail;

typedef struct {
        nfs3_stat status;
        union {
                create_retok ok;
                create_retfail fail;
        } u;
} create_ret;

typedef enum {
        UNCHECKED = 0,
        GUARDED,
        EXCLUSIVE,
} create_mode;

typedef struct {
        create_mode mode;
        sattr attr;
        char verf[NFS3_CREATEVERFSIZE];
} create_how;

typedef struct {
        diropargs3 where;
        create_how how;
} create_args;

typedef struct {
        wcc_data dir_wcc;
        postop_fh obj;
        post_op_attr obj_attr;
} mkdir_retok;

typedef struct {
        wcc_data dir_wcc;
} mkdir_retfail;

typedef struct {
        nfs3_stat status;
        union {
                mkdir_retok ok;
                mkdir_retfail fail;
        } u;
} mkdir_ret;

typedef struct {
        diropargs3 where;
        sattr attr;
} mkdir_args;

typedef struct {
        nfs3_stat status;
        wcc_data dir_wcc;
} remove_ret;

typedef struct {
        diropargs3 obj;
} remove_args;

typedef remove_ret rmdir_ret;

typedef remove_args rmdir_args;

/* readdir */
typedef struct {
        nfs_fh3 dir;
        uint64_t cookie;
        char cookieverf[NFS3_COOKIEVERFSIZE];
        uint32_t count;
} readdir_args;

typedef struct _entry {
        uint64_t  fileid;
        char *name;
        uint64_t cookie;
        struct _entry *next;
} entry;

typedef struct {
        entry *entries;
        bool_t eof;
} dirlist;

typedef struct {
        post_op_attr dir_attr;
        char cookieverf[NFS3_COOKIEVERFSIZE];
        dirlist reply;
} readdir_retok;

typedef struct {
        post_op_attr dir_attr;
} readdir_retfail;

typedef struct {
        nfs3_stat status;
        union {
                readdir_retok ok;
                readdir_retfail fail;
        } u;
} readdir_ret;

/* readdirplus */
typedef struct {
        nfs_fh3  dir;
        uint64_t cookie;
        char     cookieverf[NFS3_COOKIEVERFSIZE];
        uint32_t dircount;
        uint32_t maxcount;
} readdirplus_args;

typedef struct _entryplus {
        uint64_t           fileid;
        char              *name;
        uint64_t           cookie;
        post_op_attr       attr;
        postop_fh          fh;
        struct _entryplus *next;
} entryplus;

typedef struct {
        entryplus *entries;
        bool_t     eof;
} dirlistplus;

typedef struct {
        post_op_attr dir_attr;
        char         cookieverf[NFS3_COOKIEVERFSIZE];
        dirlistplus  reply;
} readdirplus_retok;

typedef struct {
        post_op_attr dir_attr;
} readdirplus_retfail;

typedef struct {
        nfs3_stat status;
        union {
                readdirplus_retok   ok;
                readdirplus_retfail fail;
        } u;
} readdirplus_ret;

/* fsstat */
typedef struct {
        post_op_attr attr;
        uint64_t tbytes;
        uint64_t fbytes;
        uint64_t abytes;
        uint64_t tfiles;
        uint64_t ffiles;
        uint64_t afiles;
        uint32_t invarsec;
} fsstat_retok;

typedef struct {
        post_op_attr attr;
} fsstat_retfail;

typedef struct {
        nfs3_stat status;
        union {
                fsstat_retok ok;
                fsstat_retfail fail;
        } u;
} fsstat_ret;

typedef struct {
        nfs_fh3 fsroot;
} fsstat_args;

#define NFSINFO_LINK        0x0001
#define NFSINFO_SYMLINK     0x0002
#define NFSINFO_HOMOGENEOUS 0x0008
#define NFSINFO_CANSETTIME  0x0010

typedef struct {
        post_op_attr obj_attr;
        uint32_t rtmax;
        uint32_t rtpref;
        uint32_t rtmult;
        uint32_t wtmax;
        uint32_t wtpref;
        uint32_t wtmult;
        uint32_t dtpref;
        uint64_t maxfilesize;
        nfs3_time time_delta;
        uint32_t properties;
} fsinfo_retok;

typedef struct {
        post_op_attr obj_attr;
} fsinfo_retfail;

typedef struct {
        nfs3_stat status;
        union {
                fsinfo_retok ok;
                fsinfo_retfail fail;
        } u;
} fsinfo_ret;

typedef struct {
        nfs_fh3 fsroot;
} fsinfo_args;

typedef struct {
        nfs_fh3 object;
} pathconf3_args;

typedef struct {
        post_op_attr obj_attributes;
        uint32_t linkmax;
        uint32_t name_max;
        bool_t no_trunc;
        bool_t chown_restricted;
        bool_t case_insensitive;
        bool_t case_preserving;
} pathconf3_resok;

typedef struct {
        post_op_attr obj_attributes;
} pathconf3_resfail;

typedef struct {
        nfs3_stat status;
        union {
                pathconf3_resok resok;
                pathconf3_resfail resfail;
        } pathconf3_res_u;
} pathconf3_res;

/* commit */
typedef struct {
        wcc_data file_wcc;
        char verf[NFS3_WRITEVERFSIZE];
} commit_retok;

typedef struct {
        wcc_data file_wcc;
} commit_retfail;

typedef struct {
        nfs3_stat status;
        union {
                commit_retok ok;
                commit_retfail fail;
        } u;
} commit_ret;

typedef struct {
        nfs_fh3 file;
        uint64_t offset;
        uint32_t count;
} commit_args;

/* symlink */
typedef struct {
        sattr    symlink_attributes;
        nfspath3 symlink_data;
} symlinkdata3;

typedef struct {
        diropargs3   where;
        symlinkdata3 symlink;
} symlink_args;

typedef struct {
        postop_fh   obj;
        post_op_attr obj_attributes;
        wcc_data    dir_wcc;
} symlink_retok;

typedef struct {
        wcc_data dir_wcc;
} symlink_retfail;

typedef struct {
        nfs3_stat status;
        union {
                symlink_retok   ok;
                symlink_retfail fail;
        } u;
} symlink_res;

/* mknod */
typedef struct {
      sattr dev_attributes;
      nfs3_specdata spec;
}devicedata3;

typedef struct {
        nfs3_ftype type;
        union {
                devicedata3  device;
                sattr pipe_attributes;
        }mknoddata3_u;
}mknoddata3;

typedef struct {
       diropargs3 where;
       mknoddata3 what;
}mknod_args;

typedef struct {
        postop_fh obj;
        post_op_attr obj_attributes;
        wcc_data dir_wcc;
}mknod_resok;

typedef struct {
        wcc_data dir_wcc;
}mknod_resfail;

typedef struct {
        nfs3_stat status;
        union {
                mknod_resok ok;
                mknod_resfail fail;
        } u;
}mknod_res;


/* readlink */
typedef struct {
        nfs_fh3 symlink;
} readlink_args;

typedef struct {
        post_op_attr symlink_attributes;
        nfspath3    data;
} readlink_retok;

typedef struct {
        post_op_attr symlink_attributes;
} readlink_retfail;

typedef struct {
        nfs3_stat status;
        union {
                readlink_retok   ok;
                readlink_retfail fail;
        } u;
} readlink_res;

typedef enum {
        NFS_READ_BEGIN,
        NFS_READ_WAIT_PATH,
        NFS_READ_WAIT_READ,
        NFS_READ_OK,
} nfs_read_status;

typedef enum {
        NFS_GETATTR_BEGIN,
        NFS_GETATTR_WAIT_PATH
} nfs_stat_status;

typedef enum {
        NFS_SETATTR_BEGIN,
        NFS_SETATTR_WAIT_PATH
} nfs_setattr_status;

typedef enum {
        NFS_ACCESS_BEGIN,
        NFS_ACCESS_WAIT_PATH
} nfs_access_status;

typedef enum {
        NFS_MKDIR_BEGIN,
        NFS_MKDIR_WAIT_PATH
} nfs_mkdir_status;

typedef enum {
        NFS_CREATE_BEGIN,
        NFS_CREATE_DISPATCH,
        NFS_CREATE_WAIT_PATH
} nfs_create_status;

typedef enum {
        NFS_WRITE_BEGIN,
        NFS_WRITE_DISPATCH,
        NFS_WRITE_WAIT_PATH,
        NFS_WRITE_FINISH
} nfs_write_status;

typedef enum {
        NFS_LOOKUP_BEGIN,
        NFS_LOOKUP_WAIT_PATH
} nfs_lookup_status;


#define MNTPATH_LEN 1024
#define MNTNAME_LEN 255
#define FH_SIZE 64

typedef struct {
        uint32_t len;
        char *val;
} fhandle;

typedef enum {
        MNT_OK = 0,
        MNT_EPERM = 1,
        MNT_ENOENT = 2,
        MNT_EIO = 5,
        MNT_EACCES = 13,
        MNT_ENOTDIR = 20,
        MNT_EINVAL = 22,
        MNT_ENAMETOOLONG = 63,
        MNT_ENOTSUPP = 10004,
        MNT_ESERVERFAULT = 10006,
} mount_stat;

#pragma pack(1)
typedef struct {
        uint32_t state;
        uint32_t length;
        char buf[0];
} mount_retok_t;
#pragma pack()

typedef struct {
        fhandle fhandle;

        struct {
                uint32_t len;
                int *val;
        } auth_flavors;
} mount_retok;

typedef struct {
        mount_stat fhs_status;

        union {
                mount_retok mountinfo;
        } u;
} mount_ret;

#define MOUNTPROG 100005
#define MOUNTVERS1 1
#define MOUNTVERS3 3

enum {
        MNT_NULL = 0,
        MNT_MNT,
        MNT_DUMP,
        MNT_UMNT,
        MNT_UMNTALL,
        MNT_EXPORT,     /* 5 */
};

enum {
        ACL_NULL = 0,
};

typedef struct {
        uint32_t len;
        char path[0];
} dir_t;

typedef char *dirpath;

typedef char *name;

typedef struct groupnode *groups;

struct groupnode {
	name gr_name;
	groups gr_next;
};
typedef struct groupnode groupnode;

typedef struct exportnode *exports;

struct exportnode {
	dirpath ex_dir;
	groups ex_groups;
	exports ex_next;
};

typedef struct mountbody *mountlist;

struct mountbody {
        name       ml_hostname;
        dirpath    ml_directory;
        mountlist ml_next;
};

typedef struct exportnode exportnode;

int nfs_analysis_init();
int mountlist_init(void);

#endif

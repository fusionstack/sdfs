#ifndef __LIBYFS_H__
#define __LIBYFS_H__

#include <sys/types.h>
#include <sys/statvfs.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

#include "configure.h"
#include "sdfs_buffer.h"
#include "sdfs_api_old.h"
#include "sdfs_id.h"
#include "sdfs_ec.h"
#include "sdfs_quota.h"
#include "sdfs_conf.h"
#include "sdfs_share.h"
#include "sdfs_worm.h"

typedef struct {
        fileid_t fileid;
        uint16_t d_type;
        char name[256];
} __dirlist_t;

typedef struct {
        uint64_t offset;
        uint8_t count;
        uint8_t cursor;
        uint16_t __pad__;
        __dirlist_t array[0];
} dirlist_t;

#define DIRLIST_SIZE(__count__) (sizeof(dirlist_t) + sizeof(__dirlist_t) * (__count__))

#define dir_for_each(buf, buflen, de, off)                      \
        for (de = (void *)(buf);                                \
             (void *)de < (void *)(buf) + buflen ;              \
             off = de->d_off, de = (void *)de + de->d_reclen)

/* return negative number on error */

typedef enum {
        __NOT_SET_SIZE = 0,
        __SET_TRUNCATE,
        __SET_EXTERN,
} __size_how;

typedef struct {
        // = 0x01: set physical size
        // = 0x02: set logical size
        __size_how set_it;
        uint64_t size;
} __set_size;

typedef enum {
        __DONT_CHANGE = 0,
        __SET_TO_SERVER_TIME,
        __SET_TO_CLIENT_TIME,
} __time_how;

typedef struct timespec __time;

typedef struct {
        __time_how set_it;
        __time time;
} __set_time;

typedef struct {
        int set_it;
        uint32_t val;
} __set_u32;

typedef struct {
        int set_it;
        uint64_t val;
} __set_u64;


typedef __set_u32 __set_mode;
typedef __set_u32 __set_uid;
typedef __set_u32 __set_gid;
typedef __set_u32 __set_replica;
typedef __set_u32 __set_clone;
typedef __set_u32 __set_protect;
typedef __set_u32 __set_priority;
typedef __set_u32 __set_writeback;
typedef __set_u32 __set_readraw;
typedef __set_u32 __set_del;
typedef __set_u64 __set_wormid;


typedef struct {
        int set_it;
        ec_t ec;
} __set_ec;

typedef struct {
        int set_it;
        fileid_t val;
} __set_quotaid;


typedef struct {
        int set_it;
        uint32_t nlink;
} __set_nlink;

typedef struct {
        __set_mode mode;
        __set_uid uid;
        __set_gid gid;
        __set_size size;
        __set_nlink nlink;
        __set_time atime;
        __set_time btime;
        __set_time ctime;
        __set_time mtime;
        __set_replica replica;
        __set_ec ec;
        __set_quotaid quotaid;
        __set_wormid wormid;
} setattr_t;

typedef struct {
        uint32_t roff;//redis offset
        uint32_t cursor;
} diroff_t;

typedef struct {
        dirid_t dirid;
        diroff_t diroff;
        struct dirent de;
        dirlist_t *dirlist;
} dirhandler_t;

typedef enum {
        SDFS_RDLOCK = 10,
        SDFS_WRLOCK,
        SDFS_UNLOCK,
} sdfs_lock_type_t;

typedef struct {
        uint8_t type; // shared(readlock), exclusive(writelock), remove(unlock)
        uint64_t sid; // which server (nfs, samba...) requests/holds the lock
        uint64_t owner; // who requests/holds the lock
        uint64_t start; // initial location to lock
        uint64_t length; // num bytes to lock from start
        int16_t opaquelen;
        char opaque[0];
} sdfs_lock_t;

#define SDFS_LOCK_SIZE(__lock__) (sizeof(*__lock__) + __lock__->opaquelen)

typedef struct {
        char vol[MAX_NAME_LEN];
        fileid_t rootid;
        uint64_t snapvers;
        int running;
} sdfs_ctx_t;

/* yfs_lib.c */
int sdfs_init_verbose(const char *name, int redis_conn);
int sdfs_init(const char *name);
int sdfs_connect(const char *vol, sdfs_ctx_t **ctx);
void sdfs_disconnect(sdfs_ctx_t *ctx);
int sdfs_mkvol(const char *name, const ec_t *ec, mode_t mode, fileid_t *_fileid);

//dir
void sdfs_closedir(sdfs_ctx_t *ctx, dirhandler_t *dirhandler);
int sdfs_opendir(sdfs_ctx_t *ctx, const dirid_t *dirid, dirhandler_t **_dirhandler);
int sdfs_readdir(sdfs_ctx_t *ctx, dirhandler_t *dirhandler, struct dirent **_de, fileid_t *fileid);
long sdfs_telldir(sdfs_ctx_t *ctx, dirhandler_t *dirhandler);
void sdfs_rewinddir(sdfs_ctx_t *ctx, dirhandler_t *dirhandler);
void sdfs_seekdir(sdfs_ctx_t *ctx, dirhandler_t *dirhandler, long loc);

int sdfs_readdir1(sdfs_ctx_t *ctx, const fileid_t *fileid, off_t offset, void **de, int *delen);
int sdfs_readdirplus(sdfs_ctx_t *ctx, const fileid_t *fileid, off_t offset, void **de, int *delen);
int sdfs_dirlist(sdfs_ctx_t *ctx, const dirid_t *dirid, uint32_t count, uint64_t offset, dirlist_t **dirlist);
int sdfs_childcount(sdfs_ctx_t *ctx, const fileid_t *fileid, uint64_t *count);
int sdfs_readdirplus_with_filter(sdfs_ctx_t *ctx, const fileid_t *fileid, off_t offset,
                                 void **de, int *delen, const filter_t *filter);
int sdfs_readdirplus(sdfs_ctx_t *ctx, const fileid_t *fileid, off_t offset, void **de, int *delen);
int sdfs_lookup_recurive(const char *path, fileid_t *fileid);
int sdfs_lookupvol(const char *name, fileid_t *fileid);
int sdfs_splitpath(const char *path, fileid_t *parent, char *basename);
int sdfs_lookup(sdfs_ctx_t *ctx, const fileid_t *parent, const char *name, fileid_t *fileid);

int sdfs_create(sdfs_ctx_t *ctx, const fileid_t *parent, const char *name,
                fileid_t *fileid, uint32_t mode, uint32_t uid, uint32_t gid);
int sdfs_mkdir(sdfs_ctx_t *ctx, const fileid_t *parent, const char *name, const ec_t *ec,
               fileid_t *fileid, uint32_t mode, uint32_t uid, uint32_t gid);
int sdfs_rmdir(sdfs_ctx_t *ctx, const fileid_t *parent, const char *name);


//file
int sdfs_read(sdfs_ctx_t *ctx, const fileid_t *fileid, buffer_t *_buf, uint32_t size, uint64_t offset);//coroutine
int sdfs_read_async(sdfs_ctx_t *ctx, const fileid_t *fileid, buffer_t *buf, uint32_t size,
                    uint64_t off, int (*callback)(void *, int), void *obj); // async io
int sdfs_read_sync(sdfs_ctx_t *ctx, const fileid_t *fileid, buffer_t *buf, uint32_t size, uint64_t off); //sync io

int sdfs_write(sdfs_ctx_t *ctx, const fileid_t *fileid, const buffer_t *_buf, uint32_t size, uint64_t offset);//coroutine
int sdfs_write_async(sdfs_ctx_t *ctx, const fileid_t *fileid, const buffer_t *buf, uint32_t size,
                     uint64_t off, int (*callback)(void *, int), void *obj);//async io
int sdfs_write_sync(sdfs_ctx_t *ctx, const fileid_t *fileid, const buffer_t *buf, uint32_t size,
                    uint64_t off);// sync io
int sdfs_truncate(sdfs_ctx_t *ctx, const fileid_t *fileid, uint64_t length);
int sdfs_link2node(sdfs_ctx_t *ctx, const fileid_t *old, const fileid_t *, const char *);
int sdfs_unlink(sdfs_ctx_t *ctx, const fileid_t *parent, const char *name);
int sdfs_setlock(sdfs_ctx_t *ctx, const fileid_t *fileid, const sdfs_lock_t *lock);
int sdfs_getlock(sdfs_ctx_t *ctx, const fileid_t *fileid, sdfs_lock_t *lock);


//node
int sdfs_getattr(sdfs_ctx_t *ctx, const fileid_t *fileid, struct stat *stbuf);
int sdfs_setattr(sdfs_ctx_t *ctx, const fileid_t *fileid, const setattr_t *setattr, int force);
int sdfs_chmod(sdfs_ctx_t *ctx, const fileid_t *fileid, mode_t mode);
int sdfs_chown(sdfs_ctx_t *ctx, const fileid_t *fileid, uid_t uid, gid_t gid);
int sdfs_utime(sdfs_ctx_t *ctx, const fileid_t *fileid, const struct timespec *atime,
               const struct timespec *mtime, const struct timespec *ctime);

//xattr
int sdfs_getxattr(sdfs_ctx_t *ctx, const fileid_t *fileid, const char *name, void *value, size_t *size);
int sdfs_removexattr(sdfs_ctx_t *ctx, const fileid_t *fileid, const char *name);
int sdfs_listxattr(sdfs_ctx_t *ctx, const fileid_t *fileid, char *list, size_t *size);
int sdfs_setxattr(sdfs_ctx_t *ctx, const fileid_t *fileid, const char *name, const void *value,
                  size_t size, int flags);

//misc
int sdfs_statvfs(sdfs_ctx_t *ctx, const fileid_t *, struct statvfs *);
int sdfs_rename(sdfs_ctx_t *ctx, const fileid_t *from, const char *, const fileid_t *to, const char *);
int sdfs_symlink(sdfs_ctx_t *ctx, const fileid_t *parent, const char *link_name,
                 const char *link_target, uint32_t mode, uid_t uid, gid_t gid);
int sdfs_readlink(sdfs_ctx_t *ctx, const fileid_t *from, char *, uint32_t *buflen);
char *sdfs_realpath(const char *path, char *resolved_path);


int sdfs_share_list(int prot, shareinfo_t **shareinfo, int *count);
int sdfs_share_get(const char *key, shareinfo_t *shareinfo);
int sdfs_share_set(const char *key, const shareinfo_t *shareinfo);

//utils
int sdfs_lock_equal(const fileid_t *file1, const sdfs_lock_t *lock1,
                    const fileid_t *file2, const sdfs_lock_t *lock2);

#endif

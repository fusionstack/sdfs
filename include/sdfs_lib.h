#ifndef __LIBYFS_H__
#define __LIBYFS_H__

#ifndef __CYGWIN__
#include <aio.h>
#endif
#include <sys/types.h>
#include <sys/statvfs.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

#include "configure.h"
#include "sdfs_buffer.h"
#include "sdfs_id.h"
#include "sdfs_ec.h"
#include "sdfs_quota.h"
#include "sdfs_conf.h"
#include "sdfs_worm.h"

#ifndef USS_SETATTR_MODE
#define USS_SETATTR_MODE   (1 << 0)
#define USS_SETATTR_UID    (1 << 1)
#define USS_SETATTR_GID    (1 << 2)
#define USS_SETATTR_MTIME  (1 << 3)
#define USS_SETATTR_ATIME  (1 << 4)
#define USS_SETATTR_SIZE   (1 << 5)
#define USS_SETATTR_CTIME  (1 << 6)
#endif


//#define LY_BASE                     0x00000000
#define LY_IO                       0x00000001
//#define LY_NO_MASTER                0x00000002
#define LY_NO_MDC                   0x00000004
#define LY_NO_FILES                 0x00000008
#define LY_LEASE                    0x00000010

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

/* readdirplus count */
typedef struct {
        uint64_t total;
        uint64_t dir_count;
        uint64_t file_count;
} file_statis_t;

/* readdir with filter*/

typedef enum {
        TRAVEL_INVALID_OP = 0,
        FORWARD_LOOKUP,     //num >= 0  forward lookup
        BACKWARD_LOOKUP,    //num < 0   backward lookup
} travel_op_t;

typedef struct {
        char pattern[MAX_NAME_LEN];
        uint32_t from_time;
        uint32_t to_time;

        uint64_t count;
        uint64_t offset;
#if 0
        travel_op_t travel_type;
        uint64_t travel_num;
        uint64_t travel_from;
        uint64_t travel_to;
#endif
}filter_t;

#define dir_for_each(buf, buflen, de, off)                      \
        for (de = (void *)(buf);                                \
             (void *)de < (void *)(buf) + buflen ;              \
             off = de->d_off, de = (void *)de + de->d_reclen)

/* return negative number on error */

/* yfs_lib.c */
extern int ly_prep(int daemon, const char *name, int64_t maxopenfile);
extern int ly_init(int daemon, const char *name, int64_t maxopenfile);
extern int ly_run(char *home, int (*server)(void *args), void *args);
extern int ly_update_status(const char *status, int step);
extern int ly_destroy(void);
extern int ly_init_simple(const char *name);
extern int ly_init_simple2(const char *name);
extern void ly_set_daemon();

/* dir.c */
extern int ly_readdir(const char *path, off_t offset, void **de, int *delen, int prog_type);
extern int ly_readdirplus(const char *path, off_t offset, void **de, int *delen, int prog_type);

extern int ly_readdirplus_count(const char *path, file_statis_t *);
extern int ly_readdirplus_with_filter(const char *path, off_t offset, void **de, int *delen,
                                      const filter_t *filter);


/* node.c */
extern int ly_getattr(const char *path, struct stat *);
extern int ly_mkdir(const char *path, const ec_t *, mode_t);
extern int ly_chmod(const char *path, mode_t);
extern int ly_chown(const char *path, uid_t uid, gid_t gid);
extern int ly_fidchmod(const fileid_t *fid,  mode_t mode);
extern int ly_fidchown(const fileid_t *, uid_t uid, gid_t gid);
extern int ly_unlink(const char *path);
extern int ly_rename(const char *from, const char *to);
extern int ly_link(const char *target, const char *link);
extern int ly_rmdir(const char *path);
extern int ly_statfs(const char *path, struct statvfs *vfs);
extern int ly_opendir(const char *path);
extern int ly_utime(const char *path, uint32_t atime, uint32_t mtime);

extern int ly_link2node(const char *path, fileid_t *fileid);

extern int ly_setrepnum(const char *path, int repnum);
/* return replica num (return nagetive on error) */
extern int ly_getrepnum(const char *path);
extern int ly_setchklen(const char *path, int chklen);
/* return chuck length (return nagetive on error) */
extern int ly_getchklen(const char *path);
extern int ly_getxattr(const char *path, const char *name, void *value, size_t *size);
extern int ly_setxattr(const char *path, const char *name, const void *value, size_t size, int flags);
extern int ly_removexattr(const char *path, const char *name);
extern int ly_listxattr(const char *path, char *list, size_t *size);

extern int ly_statvfs(const char *path, struct statvfs *);

/* file.c */
/* return fd (return nagetive on error) */
extern int ly_open(const char *path);
extern int ly_pread(int fd, char *buf, size_t, yfs_off_t);
extern int ly_read(const char *path, char *buf, size_t size, yfs_off_t offset);
/* return fd (return nagetive on error) */
extern int ly_create(const char *path, mode_t);
extern int ly_pwrite(int fd, const char *buf, size_t, yfs_off_t);
extern int ly_write(const char *path, const char *buf, size_t size, yfs_off_t offset);
extern int ly_release(int fd);

extern int ly_truncate(const char *path, off_t length);
extern int ly_symlink(const char *link_target, const char *link_name);
extern int ly_readlink(const char *link, char *buf, size_t *buflen);

extern int ly_set_default_acl(const char *path, mode_t mode);

/*raw*/
extern int raw_readdirplus_count(const fileid_t *fileid, file_statis_t *);

extern int raw_set_wormid(const fileid_t *fileid);
extern int raw_create_worm(const fileid_t *fileid, const worm_t *worm);
extern int raw_modify_worm(const fileid_t *fileid, const worm_t *worm,
                           const char *username, const char *passwd);
extern int raw_list_worm(worm_t *worm, size_t size, int *count);
extern int raw_unlink_with_worm(const fileid_t *parent, const char *name,
                                const char *username, const char *passwd);
extern int raw_rmdir_with_worm(const fileid_t *, const char *name,
                               const char *username, const char *passwd);
extern int raw_symlink_plus(const char *link_target, const fileid_t *parent,
                       const char *link_name, uint32_t mode, fileid_t *fileid,
                       struct stat *st, const uid_t uid, const gid_t gid);

extern int raw_printfile(fileid_t *fid, uint32_t _chkno);
extern int raw_printfile1(fileid_t *fileid);
extern int raw_is_dir(const fileid_t *fileid, int *is_dir);
extern int raw_is_directory_empty(const fileid_t *fileid, int *is_empty);
extern int raw_create_quota(quota_t *quota);
extern int raw_get_quota(const fileid_t *quotaid, quota_t *quota);
extern int raw_set_quotaid(const fileid_t *fileid, uint64_t quotaid);
// extern int raw_list_quota(const quota_t *quota_owner,
                          // quota_type_t quota_type,
                          // quota_t **quota,
                          // int *);
extern int raw_remove_quota(const fileid_t *quotaid, const quota_t *quota);
extern int raw_modify_quota(const fileid_t *quotaid, quota_t *quota, uint32_t modify_mask);

#if 0
extern int raw_flock_op(const fileid_t *fileid,
                        uss_flock_op_t flock_op,
                        struct flock *flock,
                        const uint64_t owne);
#endif


typedef enum {
        __NOT_SET_SIZE = 0,
        __SET_PHYSICAL_SIZE,
        __SET_LOGICAL_SIZE,
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
        SDFS_RDLOCK,
        SDFS_WRLOCK,
        SDFS_UNLOCK,
} sdfs_lock_type_t;

typedef struct {
        uint8_t type; // shared(readlock), exclusive(writelock), remove(unlock)
        uint64_t sid; // which server (nfs, samba...) requests/holds the lock
        uint64_t owner; // who requests/holds the lock
        uint64_t start; // initial location to lock
        uint64_t length; // num bytes to lock from start
        int16_t len;
        char opaque[0];
} sdfs_lock_t;

#define SDFS_LOCK_SIZE(__lock__) (sizeof(*__lock__) + __lock__->len)

static inline int sdfs_lock_equal(const fileid_t *file1, const sdfs_lock_t *lock1,
                                  const fileid_t *file2, const sdfs_lock_t *lock2)
{
        if (file1 && file2 && chkid_cmp(file1, file2))
                return 0;

        if (lock1->type == lock2->type &&
            lock1->sid == lock2->sid &&
            lock1->owner == lock2->owner &&
            lock1->start == lock2->start &&
            lock1->length == lock2->length)
                return 1;
        else
                return 0;
}


int sdfs_init(const char *name);

//dir
void sdfs_closedir(dirhandler_t *dirhandler);
int sdfs_opendir(const dirid_t *dirid, dirhandler_t **_dirhandler);
int sdfs_readdir(dirhandler_t *dirhandler, struct dirent **_de, fileid_t *fileid);
long sdfs_telldir(dirhandler_t *dirhandler);
void sdfs_rewinddir(dirhandler_t *dirhandler);
void sdfs_seekdir(dirhandler_t *dirhandler, long loc);

int sdfs_readdir1(const fileid_t *fileid, off_t offset, void **de, int *delen);
int sdfs_readdirplus(const fileid_t *fileid, off_t offset, void **de, int *delen);
int sdfs_dirlist(const dirid_t *dirid, uint32_t count, uint64_t offset, dirlist_t **dirlist);
int sdfs_childcount(const fileid_t *fileid, uint64_t *count);
int sdfs_readdirplus_with_filter(const fileid_t *fileid, off_t offset,
                                 void **de, int *delen, const filter_t *filter);
int sdfs_readdirplus(const fileid_t *fileid, off_t offset, void **de, int *delen);
int sdfs_lookup_recurive(const char *path, fileid_t *fileid);
int sdfs_lookupvol(const char *name, fileid_t *fileid);
int sdfs_splitpath(const char *path, fileid_t *parent, char *basename);
int sdfs_lookup(const fileid_t *parent, const char *name, fileid_t *fileid);

int sdfs_create(const fileid_t *parent, const char *name,
                fileid_t *fileid, uint32_t mode, uint32_t uid, uint32_t gid);
int sdfs_mkdir(const fileid_t *parent, const char *name, const ec_t *ec,
               fileid_t *fileid, uint32_t mode, uint32_t uid, uint32_t gid);
int sdfs_mkdir_recurive(const char *path, const ec_t *ec, mode_t mode, fileid_t *_fileid);
int sdfs_rmdir(const fileid_t *parent, const char *name);


//file
int sdfs_read(const fileid_t *fileid, buffer_t *_buf, uint32_t size, uint64_t offset);//coroutine
int sdfs_read_async(const fileid_t *fileid, buffer_t *buf, uint32_t size,
                    uint64_t off, int (*callback)(void *, int), void *obj); // async io
int sdfs_read_sync(fileid_t *fileid, buffer_t *buf, uint32_t size, uint64_t off); //sync io

int sdfs_write(const fileid_t *fileid, const buffer_t *_buf, uint32_t size, uint64_t offset);//coroutine
int sdfs_write_async(const fileid_t *fileid, const buffer_t *buf, uint32_t size,
                     uint64_t off, int (*callback)(void *, int), void *obj);//async io
int sdfs_write_sync(fileid_t *fileid, const buffer_t *buf, uint32_t size, uint64_t off);// sync io
int sdfs_truncate(const fileid_t *fileid, uint64_t length);
int sdfs_link2node(const fileid_t *old, const fileid_t *, const char *);
int sdfs_unlink(const fileid_t *parent, const char *name);
int sdfs_setlock(const fileid_t *fileid, const sdfs_lock_t *lock);
int sdfs_getlock(const fileid_t *fileid, sdfs_lock_t *lock);


//node
int sdfs_getattr(const fileid_t *fileid, struct stat *stbuf);
int sdfs_setattr(const fileid_t *fileid, const setattr_t *setattr, int force);
int sdfs_chmod(const fileid_t *fileid, mode_t mode);
int sdfs_chown(const fileid_t *fileid, uid_t uid, gid_t gid);
int sdfs_utime(const fileid_t *fileid, const struct timespec *atime,
               const struct timespec *mtime, const struct timespec *ctime);

//xattr
int sdfs_getxattr(const fileid_t *fileid, const char *name, void *value, size_t *size);
int sdfs_removexattr(const fileid_t *fileid, const char *name);
int sdfs_listxattr(const fileid_t *fileid, char *list, size_t *size);
int sdfs_setxattr(const fileid_t *fileid, const char *name, const void *value,
                  size_t size, int flags);

//misc
int sdfs_statvfs(const fileid_t *, struct statvfs *);
int sdfs_rename(const fileid_t *from, const char *, const fileid_t *to, const char *);
int sdfs_symlink(const fileid_t *parent, const char *link_name,
                 const char *link_target, uint32_t mode, uid_t uid, gid_t gid);
int sdfs_readlink(const fileid_t *from, char *, uint32_t *buflen);
char *sdfs_realpath(const char *path, char *resolved_path);


#endif

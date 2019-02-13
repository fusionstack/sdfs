#ifndef __SDFS_API_OLD_H__
#define __SDFS_API_OLD_H__


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
#include "sdfs_share.h"
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

extern int ly_prep(int daemon, const char *name, int64_t maxopenfile);
extern int ly_init(int daemon, const char *name, int64_t maxopenfile);
extern int ly_run(char *home, int (*server)(void *args), void *args);
extern int ly_update_status(const char *status, int step);
extern int ly_destroy(void);
extern int ly_init_simple(const char *name);
extern int ly_init_simple2(const char *name);
extern void ly_set_daemon();

#if 1
extern int ly_readdir(const char *path, off_t offset, void **de, int *delen, int prog_type);
extern int ly_readdirplus(const char *path, off_t offset, void **de, int *delen, int prog_type);

extern int ly_readdirplus_with_filter(const char *path, off_t offset, void **de, int *delen,
                                      const filter_t *filter);


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
extern int ly_getrepnum(const char *path);
extern int ly_setchklen(const char *path, int chklen);
extern int ly_getchklen(const char *path);
extern int ly_getxattr(const char *path, const char *name, void *value, size_t *size);
extern int ly_setxattr(const char *path, const char *name, const void *value, size_t size, int flags);
extern int ly_removexattr(const char *path, const char *name);
extern int ly_listxattr(const char *path, char *list, size_t *size);

extern int ly_statvfs(const char *path, struct statvfs *);

extern int ly_open(const char *path);
extern int ly_pread(int fd, char *buf, size_t, yfs_off_t);
extern int ly_read(const char *path, char *buf, size_t size, yfs_off_t offset);
extern int ly_create(const char *path, mode_t);
extern int ly_pwrite(int fd, const char *buf, size_t, yfs_off_t);
extern int ly_write(const char *path, const char *buf, size_t size, yfs_off_t offset);
extern int ly_release(int fd);

extern int ly_truncate(const char *path, off_t length);
extern int ly_symlink(const char *link_target, const char *link_name);
extern int ly_readlink(const char *link, char *buf, size_t *buflen);

extern int ly_set_default_acl(const char *path, mode_t mode);
#endif


/*raw*/
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

#endif

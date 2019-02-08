
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>

#define DBG_SUBSYS S_YFSLIB

#include "sdfs_id.h"

#include "md_lib.h"
#include "chk_proto.h"
#include "network.h"
#include "net_global.h"
#include "chk_proto.h"
#include "file_table.h"
#include "job_dock.h"
#include "ylib.h"
#include "net_global.h"
#include "yfs_file.h"
#include "cache.h"
#include "sdfs_lib.h"
#include "network.h"
#include "yfs_limit.h"
#include "dbg.h"
#include "worm_cli_lib.h"
#include "mond_rpc.h"
#include "main_loop.h"
#include "posix_acl.h"
#include "flock.h"
#include "xattr.h"

int normalize_path(const char *path, char *path2) {
        int i, len, begin, off;

        len = strlen(path);

        off = 0;
        begin = -1;
        path2[off++] = '/';
        for(i = 0; i < len; ++i) {
                if (path[i] == '/') {
                        if (begin == -1) {
                                continue;
                        }

                        strncpy(path2 + off, path + begin, i - begin);
                        off += i - begin;
                        // stop a segment
                        begin = -1;
                        path2[off++] = '/';
                } else {
                        if (begin == -1) {
                                // start a new segment
                                begin = i;
                        }
                }
        }

        if (begin != -1 && begin < i) {
                strncpy(path2 + off, path + begin, i - begin);
                off += i - begin;
        }

        path2[off] = '\0';
        return 0;
}

int raw_readdirplus_count(const fileid_t *fileid, file_statis_t *file_statis)
{
        return md_readdirplus_count(fileid, file_statis);
}

#if ENABLE_WORM
int raw_unlink_with_worm(const fileid_t *parent, const char *name,
                const char *username, const char *password)
{
        int ret;
        fileid_t fileid;
        fileinfo_t md;
        char buf[MAX_BUF_LEN];

        UNIMPLEMENTED(__WARN__);

        (void) username;
        (void) password;
        
        if(worm_auth_valid(username, password) == 0) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }

        memset(&fileid, 0, sizeof(fileid));
        ret = md_lookup(&fileid, parent, name);
        if (ret) {
                if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        ret = worm_remove_file_attr(&fileid);
        if(ret)
                GOTO(err_ret, ret);

        md = (void *)buf;
        ret = md_unlink(&fileid, md);
        if (ret) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}
#endif

#if ENABLE_WORM
int raw_rmdir_with_worm(const fileid_t *parent, const char *name,
                const char *username, const char *password)
{
        int ret;
        fileid_t fileid;
        
        if(worm_auth_valid(username, password) == 0) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }

        memset(&fileid, 0, sizeof(fileid));
        ret = sdfs_lookup(parent, name, &fileid);
        if (ERROR_SUCCESS != ret) {
                goto err_ret;
        }

        ret = worm_remove_attr(&fileid);
        if(ret)
                GOTO(err_ret, ret);

        ret = md_rmdir(parent, name);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}
#endif

int __chk_locate(char *loc, const chkid_t *chkid, const diskid_t *nid)
{
        int ret, seq;
        char cpath[MAX_PATH_LEN], ip[MAX_NAME_LEN];
        const char *rname = NULL;

        rname = netable_rname_nid(nid);
        if (rname == NULL  || strlen(rname) == 0)
                return ENOENT;

        ret = sscanf(rname, "%[^:]:cds/%d", ip, &seq);
        YASSERT(ret == 2);

        (void) cascade_id2path(cpath, MAX_PATH_LEN, chkid->id);

        snprintf(loc, MAX_PATH_LEN, "%s: %s/cds/%u/disk/*%s_v%llu/%u", ip, SDFS_HOME,
                 seq, cpath, (LLU)chkid->volid, chkid->idx);

        return 0;
}

int raw_printfile(fileid_t *fileid, uint32_t _chkno)
{
        int ret;
        uint32_t i, chkno, chknum;
        fileinfo_t *md;
        chkid_t chkid;
        chkinfo_t *chkinfo;
        char buf[MAX_BUF_LEN], buf1[MAX_BUF_LEN], loc[MAX_BUF_LEN];
        char value[MAX_BUF_LEN], key[MAX_BUF_LEN];
        fileid_t volume_dir_id;
        size_t size;

        md = (void *)buf;
        chkinfo = (void *)buf1;

        ret = md_getattr((void *)md, fileid);
        if (ret)
                GOTO(err_ret, ret);

        snprintf(key, MAX_BUF_LEN, USS_SYSTEM_ATTR_ENGINE);
        id2vid(fileid->volid, &volume_dir_id);
        size = sizeof(value);
        ret = sdfs_getxattr(&volume_dir_id, key, value, &size);
        if (0 == ret) {
        }

        if (_chkno == (uint32_t)-1) {
                printf("file "FID_FORMAT" mdsize %llu chklen %u"
                       " chkrep %u \n", FID_ARG(fileid),
                       (LLU)md->md_size,
                       md->split, md->repnum);

                chknum = md->chknum;

                for (chkno = 0; chkno < chknum; chkno++) {
                        fid2cid(&chkid, &md->fileid, chkno);

                        ret = md_chkload(chkinfo, &chkid, NULL);
                        if (ret) {
                                continue;
                        }

                        printf("    chk[%u] rep %u status %x, master %u\n",
                               chkid.idx, chkinfo->repnum, chkinfo->status, chkinfo->master);

                        for (i = 0; i < chkinfo->repnum; i++) {
                                if (__chk_locate(loc, &chkid, &chkinfo->diskid[i]) != 0) {
                                        printf("        net[%u] nid("DISKID_FORMAT"): offline\n",
                                               i, DISKID_ARG(&chkinfo->diskid[i]));
                                        continue;
                                } else {
                                        printf("        net[%u] nid("DISKID_FORMAT"): %s %s\n",
                                               i,
                                               DISKID_ARG(&chkinfo->diskid[i]),
                                               loc,
                                               (chkinfo->diskid[i].status & __S_DIRTY) ?
                                               "dirty" : "available");
                                }

                        }
                }
        } else {
                fid2cid(&chkid, &md->fileid, _chkno);

                ret = md_chkload(chkinfo, &chkid, NULL);
                if (ret) {
                        DWARN("chk[%d] not exist\n", _chkno);
                        GOTO(err_ret, ret);
                }

                printf("    chk[%u] rep %u status %x, master %u\n",
                       chkid.idx, chkinfo->repnum, chkinfo->status, chkinfo->master);

                for (i = 0; i < chkinfo->repnum; i++) {
                        if (__chk_locate(loc, &chkid, &chkinfo->diskid[i]) != 0) {
                                printf("        net[%u] nid("DISKID_FORMAT"): offline\n", i,
                                       DISKID_ARG(&chkinfo->diskid[i]));
                                continue;
                        } else {
                                printf("        net[%u] nid("DISKID_FORMAT"): %s, %s\n",
                                       i,
                                       DISKID_ARG(&chkinfo->diskid[i]),
                                       loc,
                                       (chkinfo->diskid[i].status & __S_DIRTY) ? "dirty" : "available" );
                        }
                }
        }

        return 0;
err_ret:
        return ret;
}

int raw_printfile1(fileid_t *fileid)
{
        int ret;
        uint32_t i, chkno, chknum;
        fileinfo_t *md;
        chkid_t chkid;
        chkinfo_t *chkinfo;
        char buf[MAX_BUF_LEN], buf1[MAX_BUF_LEN], loc[MAX_BUF_LEN];

        md = (void *)buf;
        chkinfo = (void *)buf1;

        ret = md_getattr((void *)md, fileid);
        if (ret)
                GOTO(err_ret, ret);


        printf("file "FID_FORMAT" mdsize %llu chklen %u"
               " chkrep %u \n", FID_ARG(fileid),
               (LLU)md->md_size,
               md->split, md->repnum);

        chknum = md->chknum;

        for (chkno = 0; chkno < chknum; chkno++) {
                fid2cid(&chkid, &md->fileid, chkno);

                ret = md_chkload(chkinfo, &chkid, NULL);
                if (ret) {
                        continue;
                }

                printf("    chk[%u]  "CHKID_FORMAT"  repnum %u  status %x, master %u\n",
                       chkid.idx, CHKID_ARG(&chkinfo->chkid), chkinfo->repnum, chkinfo->status, chkinfo->master);

                for (i = 0; i < chkinfo->repnum; i++) {
                        if (__chk_locate(loc, &chkid, &chkinfo->diskid[i]) != 0) {
                                printf("        net[%u] nid( "DISKID_FORMAT" ): offline\n", i,
                                       DISKID_ARG(&chkinfo->diskid[i]));
                                continue;
                        } else {
                                printf("        net[%u] nid("DISKID_FORMAT"): %s %s\n", i,
                                       DISKID_ARG(&chkinfo->diskid[i]),
                                       loc,
                                       (chkinfo->diskid[i].status & __S_DIRTY) ?
                                       "dirty" : "available");
                        }
                }
        }


        return 0;
err_ret:
        return ret;
}

int raw_is_dir(const fileid_t *fileid, int *is_dir)
{
        int ret;
        struct stat stbuf;

        ret = sdfs_getattr(fileid, &stbuf);
        if (ret)
                GOTO(err_ret, ret);

        if(S_ISDIR(stbuf.st_mode)) {
                *is_dir = 1;
        } else {
                *is_dir = 0;
        }

        return 0;
err_ret:
        return ret;
}

int raw_is_directory_empty(const fileid_t *fileid, int *is_empty)
{
        int ret;
        uint64_t count;

        if (!S_ISDIR(stype(fileid->type))) {
                ret = ENOTDIR;
                GOTO(err_ret, ret);
        }

        ret = md_childcount(fileid, &count);
        if (ret)
                GOTO(err_ret, ret);

        *is_empty = !count;
        
        return 0;
err_ret:
        return ret;
}

int raw_create_quota(quota_t *quota)
{
        return md_create_quota(quota);
}

int raw_get_quota(const fileid_t *quotaid, quota_t *quota)
{
        return md_get_quota(quotaid, quota, quota->quota_type);
}

/* int raw_list_quota(const quota_t *quota_owner, */
/* quota_type_t quota_type, */
/* quota_t **quota, int *len) */
/* { */
/* return md_list_quota(quota_owner, quota_type, quota, len); */
/* } */

int raw_remove_quota(const fileid_t *quotaid, const quota_t *quota)
{
        return md_remove_quota(quotaid, quota);
}

int raw_modify_quota(const fileid_t *quotaid, quota_t *quota, uint32_t modify_mask)
{
        return md_modify_quota(quotaid, quota, modify_mask);
}

#if ENABLE_WORM
int raw_set_wormid(const fileid_t *fileid)
{
        int ret;

        ret = md_set_wormid(fileid, fileid->id);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int raw_create_worm(const fileid_t *fileid, const worm_t *worm)
{
        int ret = 0;

        ret = sdfs_setxattr(fileid, WORM_ATTR_KEY, (void*)worm,
                        sizeof(worm_t), USS_XATTR_CREATE);
        if(ret)
                GOTO(err_ret, ret);

        ret = raw_set_wormid(fileid);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int raw_modify_worm(const fileid_t *fileid, const worm_t *worm, const char *username, const char *password)
{
        int ret = 0;

        if(worm_auth_valid(username, password) == 0) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }

        ret = sdfs_setxattr(fileid, WORM_ATTR_KEY, (void*)worm,
                        sizeof(worm_t), USS_XATTR_DEFAULT);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int raw_list_worm(worm_t *worm, size_t size, int *count)
{
        int ret = 0;

        ret = md_list_worm(worm, size, count, NULL);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}
#endif

/*
*Date   : 2017.08.14
*Author : JiangYang
*raw_realpath : only string operation, not expands symbolic link, not getwd
*    and not stat the directory, resolves references to /./, /../ and extra '/'
*    characters in the null-terminated string named by path to produce a
*    canonicalized absolute pathname. The resulting pathname  is stored as
*    a null-terminated string, up to a maximum of MAX_PATH_LEN  bytes,
*    in the buffer pointed to by resolved_path.
*    The resulting path will have no symbolic link, /./ or /../ components.
*/
char *sdfs_realpath(const char *path, char *resolved_path)
{

        char copy_path[MAX_PATH_LEN];
        char *new_path = resolved_path;
        char *max_path;

        YASSERT(NULL != path);
        YASSERT(NULL != resolved_path);

        /* Make a copy of the source path since we may need to modify it. */
        if (strlen(path) >= MAX_PATH_LEN) {
                errno = ENAMETOOLONG;
                return NULL;
        }

        strcpy(copy_path, path);
        path = copy_path;
        max_path = copy_path + MAX_PATH_LEN - 2;

        /* If it's a relative pathname use '/' for starters. */
        if (*path != '/')
                *new_path++ = '/';
        else{
                *new_path++ = '/';
                path++;
        }

        /* Expand each slash-separated pathname component. */
        while (*path != '\0') {

                /* Ignore stray "/". */
                if (*path == '/') {
                        path++;
                        continue;
                }
                if (*path == '.') {
                        /* Ignore ".". */
                        if (path[1] == '\0' || path[1] == '/') {
                                path++;
                                continue;
                        }

                        if (path[1] == '.') {
                                if (path[2] == '\0' || path[2] == '/') {
                                        path += 2;
                                        /* Ignore ".." at root. */
                                        if (new_path == resolved_path + 1)
                                                continue;
                                        /* Handle ".." by backing up. */
                                        while ((--new_path)[-1] != '/')
                                                ;
                                                continue;
                                }

                        }

                }

                /* Safely copy the next pathname component. */
                while (*path != '\0' && *path != '/') {
                        if (path > max_path) {
                                errno = ENAMETOOLONG;
                                return NULL;
                        }
                        *new_path++ = *path++;
                }
                *new_path++ = '/';
        }

        /* Delete trailing slash but don't whomp a lone slash. */
        if (new_path != resolved_path + 1 && new_path[-1] == '/')
                new_path--;

        /* Make sure it's null terminated. */
        *new_path = '\0';

        return resolved_path;
}

#if 0
int raw_flock_op(const fileid_t *fileid,
                 uss_flock_op_t flock_op,
                 struct flock *flock,
                 const uint64_t owner)
{
        int ret = 0, retry = 0, retry_max = 30;
        uss_flock_t uss_flock;

        uss_flock.sid = net_getnodeid();
        uss_flock.owner = owner;
        flock_to_ussflock(flock, &uss_flock);

retry:
        ret = md_flock_op(fileid, flock_op, &uss_flock);
        if (0 == ret && USS_GETFLOCK == flock_op)
                ussflock_to_flock(&uss_flock, flock);

        if NEED_EAGAIN(ret) {
                SLEEP_RETRY3(err_ret, ret, retry, retry, retry_max);
        }

err_ret:
        return ret;
}
#endif


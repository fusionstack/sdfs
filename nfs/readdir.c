

#include <sys/types.h>
#include <stdint.h>
#include <dirent.h>

#define DBG_SUBSYS S_YNFS

#include "adt.h"
#include "error.h"
#include "readdir.h"
#include "nfs_conf.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "network.h"
#include "yfs_md.h"
#include "dbg.h"

/*
 * maximum number of entries in readdir results
 *
 * this is 4096 / 28 (the minimum size of an entry)
 */
/*
 * static readdir_retok size with XDR overhead
 *
 * 88 bytes attributes, 8 bytes verifier, 4 bytes value_follows first entry,
 * 4 bytes eof flag
 */
#define RETOK_SIZE 104

/*
 * static entry size with XDR overhead
 *
 * 8 bytes fileid, 4 bytes name length, 8 bytes cookie, 4 byte value_follows
 */
#define ENTRY_SIZE 24

/*
 * size of a name with XDR overhead
 *
 * XDR pads to multiple of 4 bytes
 */
#define NAME_SIZE(x) (((strlen((x)) + 3) / 4) * 4)

/*
 * perform a READDIR operation
 *
 * fd_decomp must be called directly before to fill the stat cache
 */
//int read_dir(const char *path, uint64_t offset, char *verf,
//                      uint32_t count)

typedef struct {
        //uint16_t fid;
        uint8_t id;
        uint8_t cur;
        uint16_t __pad__;
        uint32_t roff;
} cookie_t;

static int __readdir_save(cookie_t *_cookie, const dirlist_t *dirlist)
{
        int ret;
        cookie_t cookie = *_cookie;
        char path[MAX_PATH_LEN];
        uint8_t rand, i;

        if (cookie.id == 0 && cookie.cur == 0) {
                rand = _random_range(1, UINT8_MAX);

                for (i = 0; i < UINT8_MAX; i++) {
                        cookie.id = (rand + i) % UINT8_MAX;
                        snprintf(path, MAX_PATH_LEN, "/dev/shm/sdfs/nfs/readdir/%u", cookie.id);

                        ret = _set_value(path, (void *)dirlist, DIRLIST_SIZE(dirlist->count),
                                         O_CREAT | O_EXCL);
                        if (ret) {
                                if (ret == EEXIST) {
                                        //DWARN("save dir %s fail\n", path);
                                        continue;
                                }

                                GOTO(err_ret, ret);
                        }

                        DBUG("save %s\n", path);
                        break;
                }

                *_cookie = cookie;
        } else {
                snprintf(path, MAX_PATH_LEN, "/dev/shm/sdfs/nfs/readdir/%u", cookie.id);
                
                ret = _set_value(path, (void *)dirlist, DIRLIST_SIZE(dirlist->count), O_CREAT);
                if (ret) {
                        GOTO(err_ret, ret);
                }

                DBUG("save %s\n", path);
        }

        return 0;
err_ret:
        return ret;
}

static int __readdir_load(const cookie_t *cookie, dirlist_t **_dirlist)
{
        int ret;
        char path[MAX_PATH_LEN];
        dirlist_t *dirlist;
        struct stat stbuf;

        snprintf(path, MAX_PATH_LEN, "/dev/shm/sdfs/nfs/readdir/%u", cookie->id);

        ret = stat(path, &stbuf);
        if (ret < 0) {
                ret = errno;
                DBUG("%s not found\n", path);
                GOTO(err_ret, ret);
        }
                 
        ret = ymalloc((void**)&dirlist, stbuf.st_size);
        if (ret) {
                UNIMPLEMENTED(__DUMP__);
        }
       
        ret = _get_value(path, (void *)dirlist, stbuf.st_size);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        YASSERT(ret == stbuf.st_size);

        DBUG("load %s\n", path);

        *_dirlist = dirlist;

        return 0;
err_ret:
        return ret;
}

static void __readdir_remove(const cookie_t *cookie)
{
        char path[MAX_PATH_LEN];

        if (cookie->id) {
                snprintf(path, MAX_PATH_LEN, "/dev/shm/sdfs/nfs/readdir/%u", cookie->id);
                unlink(path);
        }
}

static int __readdir_getlist(sdfs_ctx_t *ctx, const dirid_t *dirid, cookie_t *_cookie, dirlist_t **_dirlist)
{
        int ret;
        dirlist_t *dirlist;
        cookie_t cookie;

        cookie = *_cookie;
        if (cookie.id == 0 && cookie.cur == 0) {
                ret = sdfs_dirlist(ctx, dirid, UINT8_MAX / 2, 0, &dirlist);
                if (ret)
                        GOTO(err_ret, ret);

                ret = __readdir_save(&cookie, dirlist);
                if (ret)
                        GOTO(err_ret, ret);

                DBUG("cookie %u\n", cookie.id);
        } else {
                ret = __readdir_load(&cookie, &dirlist);
                if (ret)
                        GOTO(err_ret, ret);
        }

        cookie.roff = dirlist->offset;
        *_dirlist = dirlist;
        *_cookie = cookie;
        DBUG("cookie %u\n", _cookie->id);

        return 0;
err_ret:
        return ret;
}

static int __readdir_putlist(sdfs_ctx_t *ctx, const dirid_t *dirid, const cookie_t *cookie, dirlist_t *dirlist, int *_eof)
{
        int ret, eof;
        uint64_t offset;

        if (dirlist->cursor == dirlist->count) {//finished list
                if ((cookie->id == 0 && cookie->cur == 0) || dirlist->offset == 0) {
                        yfree((void **)&dirlist);
                        __readdir_remove(cookie);
                        eof = TRUE;
                        goto out;
                } else {
                        offset = dirlist->offset;
                        yfree((void **)&dirlist);
                        
                        ret = sdfs_dirlist(ctx, dirid, UINT8_MAX / 2, offset, &dirlist);
                        if (ret) {
#if 0
                                ret = __readdir_save(&cookie, dirlist);
                                if (ret)
                                        UNIMPLEMENTED(__DUMP__);
#endif
                                
                                GOTO(err_ret, ret);
                        }
                }
        }

        cookie_t tmp = *cookie;
        ret = __readdir_save(&tmp, dirlist);
        if (ret)
                GOTO(err_ret, ret);

        eof = FALSE;

out:
        if (_eof)
                *_eof = eof;

        DBUG("eof %d\n", eof);
        
        return 0;
err_ret:
        return ret;
}

static int __readirplus_entry(entryplus *entryplus, char *name, fileid_t *fileid,
                              const __dirlist_t *node, const cookie_t *cookie)
{
        int ret;
        struct stat stbuf;

        ret = sdfs_getattr(NULL, &node->fileid, &stbuf);
        if (ret) {
                if (ret == ENOENT) {
                        memset(&stbuf, 0x0, sizeof(stbuf));
                        stbuf.st_ino = fileid->id;
                } else
                        GOTO(err_ret, ret);
        }
        
        _strcpy(name, node->name);
        *fileid = node->fileid;
        
        YASSERT(fileid->id);
        YASSERT(fileid->volid);

        YASSERT(sizeof(*cookie) == sizeof(entryplus->cookie));
        
        entryplus->fileid = fileid->id;
        entryplus->name = name;
        memcpy(&entryplus->cookie, cookie, sizeof(entryplus->cookie));
        entryplus->fh.handle_follows = 1;
        entryplus->fh.handle.val = (void *)fileid;
        entryplus->fh.handle.len = sizeof(fileid_t);
        get_postopattr_stat(&entryplus->attr, &stbuf);
        entryplus->next = NULL;
 
        DBUG("fileid "CHKID_FORMAT" name %s mode %o cookie %u,%u\n", CHKID_ARG(&node->fileid),
              node->name, stbuf.st_mode, cookie->id, cookie->cur);

        return 0;
err_ret:
        return ret;
}

int readdirplus(sdfs_ctx_t *ctx, const fileid_t *fileid, uint64_t _cookie, char *verf,
                uint32_t count, readdirplus_ret *res, entryplus *_entryplus,
                char *obj, fileid_t *fharray)
{
        int ret;
        uint32_t i, real_count;
        readdirplus_retok *resok;
        //uint64_t cookie = _offset;
        dirlist_t *dirlist;
        cookie_t cookie;
        __dirlist_t *node;

        YASSERT(sizeof(cookie) == sizeof(_cookie));
        memcpy(&cookie, &_cookie, sizeof(cookie));

        /* we refuse to return more than 4K from READDIRPLUS */
        if (count > 4096)
                count = 4096;

        /* account for size of information heading retok structure */
        real_count = RETOK_SIZE;

        /*
         * we are always returning zero as a cookie verifier. one reason for
         * this is that stat() on Windows seems to return cached st_mtime
         * values, which gives spurious NFS3_EBADCOOKIE. btw, here's what
         * Peter Staubach has to say about cookie verifires:

         *
         * "From my viewpoint, the cookieverifier was a failed experiment in
         * NFS v3. the semantics were never well understood nor supported by
         * many local file systems. the Solaris NFs server always returns zeros
         * in the cookieverifier field."
         */
        _memset(verf, 0x0, NFS3_COOKIEVERFSIZE);

        DBUG("readdir "CHKID_FORMAT" count %u, cookie %u,%u\n",
              CHKID_ARG(fileid), count, cookie.id, cookie.cur);

        ret = __readdir_getlist(ctx, fileid, &cookie, &dirlist);
        if (ret) {
                if (ret == ENOENT) {
                        resok = &res->u.ok;
                        resok->reply.eof = 1;
                        _entryplus[0].name = NULL;
                        goto out;
                } else 
                        GOTO(err_ret, ret);
        }

        if (_cookie != 0 && cookie.cur + 1 != dirlist->cursor) {
                DWARN("readdir "FID_FORMAT" count %u, cooke.id %u cur %u -> %u\n",
                      FID_ARG(fileid), count, cookie.id, cookie.cur, dirlist->cursor);
                //dirlist->cursor = cookie.cur;
        }

        YASSERT(dirlist->count >= dirlist->cursor);

        i = 0;
        _entryplus[0].name = NULL;
        while (dirlist->cursor < dirlist->count) {
                YASSERT(real_count < count);
                YASSERT(i + 1 < MAX_DIRPLUS_ENTRIES);

                node = &dirlist->array[dirlist->cursor];
                if (strcmp(node->name, NFS_REMOVED) == 0) {
                        dirlist->cursor++;
                        continue;
                }

                cookie.cur = dirlist->cursor;
                ret = __readirplus_entry(&_entryplus[i], &obj[i * MAX_NAME_LEN],
                                         &fharray[i], node, &cookie);
                if (ret)
                        GOTO(err_free, ret);

                if (i > 0)
                        _entryplus[i - 1].next = &_entryplus[i];
                
                /* account for entry size */
                real_count += (ENTRY_SIZE + NAME_SIZE(_entryplus[i].name));

                i++;
                dirlist->cursor++;
                
                if ((real_count + ENTRY_SIZE + NAME_SIZE(dirlist->array[dirlist->cursor].name)
                     >= count && i > 0)
                    || (i > MAX_DIRPLUS_ENTRIES)) {
                        //_entryplus[i].next = NULL;
                        DBUG("overflowed %u\n", i);
                        break;
                }
                
        }

        resok = &res->u.ok;
        ret = __readdir_putlist(NULL, fileid, &cookie, dirlist, &resok->reply.eof);
        if (ret) {
                GOTO(err_ret, ret);
        }

out:
        if (_entryplus[0].name)
                resok->reply.entries = (void *)&_entryplus[0];
        else {
                resok->reply.entries = NULL;
        }

        _memcpy(resok->cookieverf, verf, NFS3_COOKIEVERFSIZE);

        res->status = NFS3_OK;

        return 0;
err_free:
        __readdir_putlist(ctx, fileid, &cookie, dirlist, NULL);
err_ret:
        res->status = readdir_err(ret);
        return ret;
}

static int __readir_entry(entry *entry, char *name,
                              const __dirlist_t *node, const cookie_t *cookie)
{
        int ret;
        struct stat stbuf;

        ret = sdfs_getattr(NULL, &node->fileid, &stbuf);
        if (ret)
                GOTO(err_ret, ret);
        
        _strcpy(name, node->name);

        YASSERT(sizeof(*cookie) == sizeof(entry->cookie));
        
        entry->fileid = node->fileid.id;
        entry->name = name;
        memcpy(&entry->cookie, cookie, sizeof(entry->cookie));

#if 0
        entry->fh.handle_follows = 1;
        entry->fh.handle.val = (void *)fileid;
        entry->fh.handle.len = sizeof(fileid_t);
        get_postopattr_stat(&entry->attr, &stbuf);
#endif
        
        entry->next = NULL;
 
        DBUG("fileid "CHKID_FORMAT" name %s mode %o cookie %ju\n", CHKID_ARG(&node->fileid),
             node->name, stbuf.st_mode, cookie);

        return 0;
err_ret:
        return ret;
}


int read_dir(sdfs_ctx_t *ctx, const fileid_t *fileid, uint64_t _cookie, char *verf,
             uint32_t count, readdir_ret *res, entry *_entry,
             char *obj)
{
        int ret;
        uint32_t i, real_count;
        readdir_retok *resok;
        dirlist_t *dirlist;
        cookie_t cookie;
        __dirlist_t *node;

        YASSERT(sizeof(cookie) == sizeof(_cookie));
        memcpy(&cookie, &_cookie, sizeof(cookie));
        
        /*UNIMPLEMENTED(__WARN__);*/

        /* we refuse to return more than 4K from READDIR */
        if (count > 4096)
                count = 4096;

        /* account for size of information heading retok structure */
        real_count = RETOK_SIZE;

        /*
         * we are always returning zero as a cookie verifier. one reason for
         * this is that stat() on Windows seems to return cached st_mtime
         * values, which gives spurious NFS3_EBADCOOKIE. btw, here's what
         * Peter Staubach has to say about cookie verifires:
         *
         * "From my viewpoint, the cookieverifier was a failed experiment in
         * NFS v3. the semantics were never well understood nor supported by
         * many local file systems. the Solaris NFs server always returns zeros
         * in the cookieverifier field."
         */
        _memset(verf, 0x0, NFS3_COOKIEVERFSIZE);

        DBUG("readdir "FID_FORMAT" count %u\n", FID_ARG(fileid), count);

        ret = __readdir_getlist(ctx, fileid, &cookie, &dirlist);
        if (ret) {
                if (ret == ENOENT) {
                        resok = &res->u.ok;
                        
                        resok->reply.eof = 1;
                        _entry[0].name = NULL;
                        goto out;
                } else 
                        GOTO(err_ret, ret);
        }

        if (cookie.id != 0 && cookie.cur + 1 != dirlist->cursor) {
                DWARN("readdir "FID_FORMAT" count %u, cur %u -> %u\n",
                      FID_ARG(fileid), count, dirlist->cursor, cookie.cur);
                //dirlist->cursor = cookie.cur;
        }

        YASSERT(dirlist->count >= dirlist->cursor);
        
        i = 0;
        _entry[0].name = NULL;

        while (dirlist->cursor < dirlist->count) {
                YASSERT(real_count < count);
                YASSERT(i + 1 < MAX_DIRPLUS_ENTRIES);

                node = &dirlist->array[dirlist->cursor];
                if (strcmp(node->name, NFS_REMOVED) == 0) {
                        dirlist->cursor++;
                        continue;
                }
                
                cookie.cur = dirlist->cursor;
                ret = __readir_entry(&_entry[i], &obj[i * NFS_PATHLEN_MAX],
                                         node, &cookie);
                if (ret)
                        GOTO(err_free, ret);

                if (i > 0)
                        _entry[i - 1].next = &_entry[i];
                
                /* account for entry size */
                real_count += (ENTRY_SIZE + NAME_SIZE(_entry[i].name));

                i++;
                dirlist->cursor++;
                
                if ((real_count + ENTRY_SIZE + NAME_SIZE(dirlist->array[dirlist->cursor].name)
                     >= count && i > 0)
                    || (i > MAX_DIRPLUS_ENTRIES)) {
                        //_entry[i - 1].next = NULL;
                        DBUG("overflowed %u\n", i);
                        break;
                }
        }
        
        DBUG("ly_opendir "FID_FORMAT" end.\n", FID_ARG(fileid));

        resok = &res->u.ok;
        ret = __readdir_putlist(ctx, fileid, &cookie, dirlist, &resok->reply.eof);
        if (ret) {
                GOTO(err_ret, ret);
        }

out:
        if (_entry[0].name)
                resok->reply.entries = &_entry[0];
        else
                resok->reply.entries = NULL;

        _memcpy(resok->cookieverf, verf, NFS3_COOKIEVERFSIZE);

        res->status = NFS3_OK;

        return 0;
err_free:
        __readdir_putlist(ctx, fileid, &cookie, dirlist, NULL);
err_ret:
        return ret;
}

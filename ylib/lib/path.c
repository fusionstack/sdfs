

#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>
#include <dirent.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "sysutil.h"
#include "adt.h"
#include "ylib.h"
#include "ypath.h"
#include "dbg.h"

void rorate_id2path(char *path, uint32_t pathlen, uint32_t pathlevel,
                    const char *id)
{
        int j, k;
        char chk[9], *p;
        uint32_t i, len, hash;

        j = 0;
        chk[8] = '\0';
        p = path;
        len = pathlen;

        for (i = 0; i < pathlevel; i++) {
                for (k = 0; k < 8; k++) {
                        if (id[j] == '\0')
                                j = 0;

                        chk[k] = id[j++];
                }

                hash = hash_str(chk);

                DBUG("chk[%d] %s hash (%u)\n", i, chk, hash);

                hash %= DIR_SUB_MAX;

                snprintf(p, len, "/%u", hash);

                while (*p != '\0') {
                        p++;
                        len--;
                }
        }

        snprintf(p, len, "/%s", id);
}

void cascade_id2path(char *path, uint32_t pathlen, uint64_t _id)
{
        uint64_t id, dirid;
        char dpath[MAX_PATH_LEN], cpath[MAX_PATH_LEN];

        id = _id;

        cpath[0] = '\0';

        while (id != 0) {
                dirid = id % DIR_SUB_MAX;
                id /= DIR_SUB_MAX;

                snprintf(dpath, MAX_PATH_LEN, "%llu%s",
                         (LLU)dirid, cpath);

                snprintf(cpath, MAX_PATH_LEN, "/%s", dpath);

                DBUG("id = %llu\n", (LLU)id);
                DBUG("dirid = %llu\n", (LLU)dirid);
        }

        snprintf(path, pathlen, "%s", cpath);

#if 0

        if (cpath[0] == '\0')
                snprintf(path, pathlen, "/0/%llu", (LLU)id);
        else
                snprintf(path, pathlen, "%s/%llu", cpath,
                         (LLU)id);
#endif
}

int cascade_path2idver(const char *_path, uint64_t *id, uint32_t *version)
{
        int ret;
        const char *path;
        char *head, *tail;
        uint64_t _id, i = 1;
        
        ret = ymalloc((void **)&head, MAX_PATH_LEN);
        if (ret) 
                GOTO(err_ret, ret);

        path = _path;
        _id = 0;

        while(*++path);

        //get version
        tail = head + MAX_PATH_LEN - 2;
        while (srv_running) {
                *--tail = *path--;
                
                if (*path == 'v') {
                        path -= 2;
                        break;
                }

                if (path == _path) {
                        ret = EINVAL;
                        GOTO(err_free, ret);
                }
        }

        *version = strtoul(tail, NULL, 0);

        //get id
        while (srv_running) {
                _memset((void *)head, 0x0, MAX_PATH_LEN);
                tail = head + MAX_PATH_LEN - 2;

                while(*path != '/') 
                        *--tail = *path--;

                _id += strtoul(tail, NULL, 0) * i;
                i *= 1024;

                if (path-- == _path)
                        break;
        } 

        *id = _id;
        yfree((void **)&head);

        return 0;
err_free:
        yfree((void **)&head);
err_ret:
        return ret;
}

int cascade_iterator(char *path, void *args, int (*callback)(const char *path, void *args))
{
        int ret;
        struct stat statbuf;
        struct dirent *dirp;
        char childpath[MAX_PATH_LEN], *name;
        DIR *dp;
        uint64_t id;
        uint32_t version;

        ret = lstat(path, &statbuf);
        if (ret < 0) {
                ret = -ret;
                goto err_ret;
        }

        name = strrchr(path, '/');

        if (name == NULL)
                return 0;

        DBUG("name %s\n", name);

#ifdef __x86_64__
        ret = sscanf(name, "/%lu_v%u", &id, &version);
#else
        ret = sscanf(name, "/%llu_v%u", &id, &version);
#endif
        if (ret == 2) {
                ret = callback(path, args);
                if (ret)
                        GOTO(err_ret, ret);

                return 0;
        }

        DBUG("Enter dir: %s\n", path);
        dp = opendir(path);
        if (dp == NULL) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        while ((dirp = readdir(dp)) != NULL) {
                if (strcmp(dirp->d_name, ".") == 0 ||
                    strcmp(dirp->d_name, "..") == 0)
                        continue;

                snprintf(childpath, MAX_PATH_LEN,
                                "%s/%s", path, dirp->d_name);

                ret = cascade_iterator(childpath, args, callback);
                if (ret)
                        GOTO(err_dir, ret);
        }

        closedir(dp);

        return 0;
err_dir:
        closedir(dp);
err_ret:
        return ret;
}

int path_build(char *dpath)
{
        int ret;
        char *sla;

        sla = strchr(dpath + 1, '/');

        while (sla) {
                *sla = '\0';

                ret = access(dpath, F_OK);
                if (ret == -1) {
                        ret = errno;
                        if (ret == ENOENT) {
                                ret = mkdir(dpath, 0755);
                                if (ret == -1) {
                                        ret = errno;
                                        if (ret != EEXIST) {
                                                DERROR("mkdir(%s, ...) ret (%d) %s\n",
                                                       dpath, ret, strerror(ret));
                                                GOTO(err_ret, ret);
                                        }
                                }
                        } else
                                GOTO(err_ret, ret);
                }

                *sla = '/';
                sla = strchr(sla + 1, '/');
        }

        ret = access(dpath, F_OK);
        if (ret == -1) {
                ret = errno;
                if (ret == ENOENT) {
                        ret = mkdir(dpath, 0755);
                        if (ret == -1) {
                                ret = errno;
                                goto err_ret;
                        }
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int path_build_for_samba(char *dpath)
{
        int ret;
        char *sla;

        sla = strchr(dpath + 1, '/');

        while (sla) {
                *sla = '\0';

                ret = access(dpath, F_OK);
                if (ret == -1) {
                        ret = errno;
                        if (ret == ENOENT) {
                                ret = mkdir(dpath, 0777);
                                if (ret == -1) {
                                        ret = errno;
                                        if (ret != EEXIST) {
                                                DERROR("mkdir(%s, ...) ret (%d) %s\n",
                                                       dpath, ret, strerror(ret));
                                                GOTO(err_ret, ret);
                                        }
                                }
                                chmod(dpath, 0777);
                        } else
                                GOTO(err_ret, ret);
                }

                *sla = '/';
                sla = strchr(sla + 1, '/');
        }

        ret = access(dpath, F_OK);
        if (ret == -1) {
                ret = errno;
                if (ret == ENOENT) {
                        ret = mkdir(dpath, 0777);
                        if (ret == -1) {
                                ret = errno;
                                goto err_ret;
                        }
                        chmod(dpath, 0777);
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int path_validate(const char *path, int isdir, int dircreate)
{
        int ret, len, i;
        char dpath[MAX_PATH_LEN];
        char *end, *sla, *str;

        if (path == NULL
            || (path[0] != '/' /* [1] */)) {
                ret = EFAULT;
                GOTO(err_ret, ret);
        }

        len = _strlen(path) + 1;
        end = (char *)path + len;    /* *end == '\0' */

        if (!isdir && path[len - 2] == '/') {    /* "/file_name/" */
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        sla = (char *)path;
        while (sla < end && sla[1] == '/')   /* remove leading redundant '/' */
                sla++;

        if (sla == end)         /* all '/', -> "/////" */
                goto out;

        /* non-NULL for above [1] */
        str = strrchr(path, '/');
        while (str > sla && str[-1] == '/')
                str--;

        if (sla == str && !isdir)       /* "/file_name" */
                goto out;

        if (isdir) {    /* *end == '\0' */
                ;
        } else {        /* *end == '/' */
                end = str;
        }

        i = 0;
        while (1) {
                if (i == MAX_PATH_LEN) {
                        ret = ENAMETOOLONG;
                        GOTO(err_ret, ret);
                }

                while (sla < end && sla[0] == '/' && sla[1] == '/')
                        sla++;
                if (sla == end)
                        break;

                dpath[i] = *sla;
                i++;
                sla++;
        }
        dpath[i] = '\0';

        ret = access((const char *)dpath, F_OK);
        if (ret == -1) {
                ret = errno;

                if (ret == ENOENT && dircreate)
                        ret = path_build(dpath);

                if (ret) {
                        DBUG("validate(%s(%s), %s dir, %s create) (%d) %s\n",
                               path, dpath, isdir ? "is" : "not",
                               dircreate ? "" : "no", ret, strerror(ret));

                        if (ret != EEXIST) {
                                DWARN("path %s\n", path);
                                GOTO(err_ret, ret);
                        }
                }
        }

out:
        return 0;
err_ret:
        return ret;
}

int path_validate_for_samba(const char *path, int isdir, int dircreate)
{
        int ret, len, i;
        char dpath[MAX_PATH_LEN];
        char *end, *sla, *str;

        if (path == NULL
            || (path[0] != '/' /* [1] */)) {
                ret = EFAULT;
                GOTO(err_ret, ret);
        }

        len = _strlen(path) + 1;
        end = (char *)path + len;    /* *end == '\0' */

        if (!isdir && path[len - 2] == '/') {    /* "/file_name/" */
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        sla = (char *)path;
        while (sla < end && sla[1] == '/')   /* remove leading redundant '/' */
                sla++;

        if (sla == end)         /* all '/', -> "/////" */
                goto out;

        /* non-NULL for above [1] */
        str = strrchr(path, '/');
        while (str > sla && str[-1] == '/')
                str--;

        if (sla == str && !isdir)       /* "/file_name" */
                goto out;

        if (isdir) {    /* *end == '\0' */
                ;
        } else {        /* *end == '/' */
                end = str;
        }

        i = 0;
        while (1) {
                if (i == MAX_PATH_LEN) {
                        ret = ENAMETOOLONG;
                        GOTO(err_ret, ret);
                }

                while (sla < end && sla[0] == '/' && sla[1] == '/')
                        sla++;
                if (sla == end)
                        break;

                dpath[i] = *sla;
                i++;
                sla++;
        }
        dpath[i] = '\0';

        ret = access((const char *)dpath, F_OK);
        if (ret == -1) {
                ret = errno;

                if (ret == ENOENT && dircreate)
                        ret = path_build_for_samba(dpath);

                if (ret) {
                        DBUG("validate(%s(%s), %s dir, %s create) (%d) %s\n",
                               path, dpath, isdir ? "is" : "not",
                               dircreate ? "" : "no", ret, strerror(ret));

                        if (ret != EEXIST) {
                                DWARN("path %s\n", path);
                                GOTO(err_ret, ret);
                        }
                }
        }

out:
        return 0;
err_ret:
        return ret;
}

/* deepth first */
int scan_one_dir_df(char *d_name, off_t d_off, void (*func)(void *),
                    df_dir_list_t *dflist)
{
        int ret;
        DIR *dp;
        struct dirent *de;
        char dpath[MAX_PATH_LEN];
        struct stat stbuf;
        uint32_t len;
        df_dir_list_t *nextde, *deepde;

        dp = opendir(d_name);
        if (dp == NULL) {
                ret = errno;
                if (ret == ENOENT)
                        goto out0;
                else {
                        DWARN("opendir(%s, ...) ret (%d) %s\n", d_name, ret,
                              strerror(ret));
                        GOTO(err_ret, ret);
                }
        }

        seekdir(dp, d_off);

        while ((de = readdir(dp)) != NULL) {
                if (_strcmp(".", de->d_name) == 0
                    || _strcmp("..", de->d_name) == 0)
                        continue;

                snprintf(dpath, MAX_PATH_LEN, "%s/%s", d_name, de->d_name);

                ret = stat(dpath, &stbuf);
                if (ret == -1) {
                        ret = errno;
                        DWARN("stat(%s, ...) ret (%d) %s\n", dpath,
                                ret, strerror(ret));
                        GOTO(err_dp, ret);
                }

                func(dpath);

                if (!S_ISDIR(stbuf.st_mode))
                        continue;
                else
                        break;
        }

        if (de == NULL)
                goto out;

        len = sizeof(df_dir_list_t) + _strlen(d_name) + 1;

        ret = ymalloc((void **)&nextde, len);
        if (ret) {
                DWARN("ymalloc ret (%d) %s\n", ret, strerror(ret));
                GOTO(err_dp, ret);
        }

        INIT_LIST_HEAD(&nextde->list);
        nextde->d_off = de->d_off;
        snprintf(nextde->d_name, _strlen(d_name) + 1, "%s", d_name);

        list_add(&nextde->list, &dflist->list);

        len = sizeof(df_dir_list_t) + _strlen(dpath) + 1;

        ret = ymalloc((void **)&deepde, len);
        if (ret) {
                DWARN("ymalloc ret (%d) %s\n", ret, strerror(ret));
                GOTO(err_de, ret);
        }

        INIT_LIST_HEAD(&deepde->list);
        deepde->d_off = 0;
        snprintf(deepde->d_name, _strlen(dpath) + 1, "%s", dpath);

        list_add(&deepde->list, &dflist->list);

out:
        ret = closedir(dp);
        if (ret == -1) {
                ret = errno;
                DWARN("closedir(%s) ret (%d) %s\n", d_name, ret,
                      strerror(ret));
                GOTO(err_ret, ret);
        }
out0:

        return 0;
err_de:
        (void) yfree((void **)&nextde);
err_dp:
        (void) closedir(dp);
err_ret:
        return ret;
}

/*
 * deepth first
 *
 * basedir will not be processed by func
 */
int df_iterate_tree(const char *basedir, df_dir_list_t *dflist,
                    void (*func)(void *))
{
        int ret;
        char d_name[MAX_PATH_LEN];
        off_t d_off;
        df_dir_list_t *nextde;

        snprintf(d_name, MAX_PATH_LEN, "%s", basedir);
        d_off = 0;

        INIT_LIST_HEAD(&dflist->list);

        while (srv_running) {
                ret = scan_one_dir_df(d_name, d_off, func, dflist);
                if (ret) {
                        DWARN("ret (%d) %s\n", ret, strerror(ret));
                        GOTO(err_ret, ret);
                }

                //func(d_name);

                if (list_empty(&dflist->list))
                        break;

                nextde = list_entry(dflist->list.next, df_dir_list_t, list);

                list_del_init(&nextde->list);

                snprintf(d_name, MAX_PATH_LEN, "%s", nextde->d_name);
                d_off = nextde->d_off;
        }

        return 0;
err_ret:
        return ret;
}

/**
 * @param is_vol -1:0:1
 * @note /./aa
 */
int path_getvolume(char *path, int *is_vol, char *vol_name)
{
        int i, len, begin, end;
        enum { VOL_NULL, VOL_BEGIN, VOL_END, VOL_NEXT } state;

        /* @init */
        *is_vol = - 1;
        vol_name[0] = '\0';

        if (path == NULL)
                goto out;

        DBUG("path %s\n", path);

        len = strlen(path);

        state = VOL_NULL;
        begin = end = -1;
        for (i = 0; i <  len && state < VOL_NEXT; ++i) {
                switch (state) {
                case VOL_NULL:
                        if (path[i] == '/' || 
                           (path[i] == '.' && i < len - 1 && path[i+1] == '/'))
                                continue;

                        *is_vol = 1;
                        begin = i;
                        state = VOL_BEGIN;
                        break;
                case VOL_BEGIN:
                        if (path[i] == '/') {
                                end = i - 1;
                                state = VOL_END;
                        } else if (i == len -1) {
                                end = i;
                                state = VOL_NEXT;
                        }
                        break;
                case VOL_END:
                        if (path[i] != '/') {
                                *is_vol = 0;
                                state = VOL_NEXT;
                        }
                        break;
                case VOL_NEXT:
                        break;
                }
        }

        if (begin == -1 || end == -1)
                goto out;

        len = end - begin + 1;
        strncpy(vol_name, path+begin, len);
        vol_name[len] = '\0';

out:
        return 0;
}

int path_droptail(char *path, int len)
{
        char sep = '/';
        
        while (len > 1) {
                if (path[len - 1] == sep) {
                        path[len - 1] = '\0';
                        len--;
                } else
                        break;
        }

        return len;
}

int path_drophead(char *path)
{
        int i = 0, len;

        while (path[i] != '\0') {
                if (path[i] != '/')
                        break;

                i++;
        }

        len = strlen(path);

        if (i) {
                ARRAY_POP(path, i, len - i);
        }

        return len - i;
}

void __rmrf(void *arg)
{
        int ret;
        struct stat stbuf;
        char *path = arg;

        DBUG("%s\n", path);

        ret = stat(path, &stbuf);
        if (ret < 0) {
                ret = errno;
                if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        if (S_ISREG(stbuf.st_mode)) {
                DBUG("remove %s\n", path)
                ret = unlink(path);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        } else if (S_ISDIR(stbuf.st_mode)) {
                DBUG("keep %s\n", path)
        }

        return ;
err_ret:
        return ;
}

int rmrf(const char *path)
{
        int ret;
        df_dir_list_t bflist;

        ret = df_iterate_tree(path, &bflist, __rmrf);
        if (ret) {
                fprintf(stderr, "ret (%d) %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}


int get_dir_level(const char *abs_dir, int *level)
{
        int ret;
        char dir[512] = {0};
        char *ptr = NULL;
        uint32_t count = 0;

        memcpy(dir, abs_dir, strlen(abs_dir));
        ptr = dir;

        if(*ptr != '/') {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        while(*ptr != '\0') {
                if(*ptr == '/' && *(ptr+1) != '/') {
                        count++;
                }
                ptr++;
        }

        *level = count;

        return 0;
err_ret:
        return ret;
}

/*
*Date   : 2017.04.19
*Author : Yang
*path_normalize : remove the extra '/' in src_path, then save to dst_path.
*                       example src_path=///test//dd// then dst_path=/test/dd
*/
int path_normalize(IN const char *src_path, OUT char *dst_path, IN size_t dst_size)
{
        int i, j;
        int src_len;
        int dst_len;
        int ret = 0;

        YASSERT(NULL != src_path);
        YASSERT(NULL != dst_path);
        YASSERT(0 < dst_size);

        src_len = strlen(src_path);
        dst_len = dst_size - 1;

        if (dst_len == 0 && src_len > 0) {
                ret = ENAMETOOLONG;
                goto err_ret;
        }

        //remove the extra '/'
        dst_path[0] = src_path[0];
        for (i = 1, j = 1; i < src_len; i++) {
                if (j >= dst_len && src_path[i] != '/') {
                        ret = ENAMETOOLONG;
                        GOTO(err_ret, ret);
                }

                if (src_path[i] == '/') {
                        if (dst_path[j - 1] != '/') {
                                dst_path[j] = src_path[i];
                                j++;
                        }
                } else {
                        dst_path[j] = src_path[i];
                        j++;
                }
        }

        //if the end char of dst_path is '/', then removed this char
        if (j > 1) {
                i = j - 1;
                if (dst_path[i] == '/')
                        dst_path[i] = '\0';
        }

        //the dst_path ending with '\0'.
        dst_path[j] = '\0';

        return 0;
err_ret:
        return ret;
}

int path_access(const char *path)
{
        int ret;
        ret = access(path, F_OK);
        if (ret < 0) {
                ret = errno;
                DBUG("%s access ret %d\n", path, ret);
                if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

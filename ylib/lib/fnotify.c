#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/inotify.h>

#include "configure.h"
#include "net_global.h"
#include "sdfs_conf.h"
#include "ylib.h"
#include "hash_table.h"
#include "sysutil.h"
#include "fnotify.h"
#include "dbg.h"

typedef struct fnotify_node {
        fnotify_callback mod_callback;
        fnotify_callback del_callback;
        void *context;
        uint32_t fd;
        char path[MAX_PATH_LEN];        /* path of the file which to be monitored */
} entry_t;

typedef struct {
        sy_spinlock_t lock;
        hashtable_t table;
        int inotify_fd;
} fnotify_t;

typedef struct {
        int (*callback)(const char *buf, uint32_t flag);
        //int (*remove)(void *context, uint32_t mask);
        //int (*modify)(void *context, uint32_t mask);
        uint32_t flag;
        char value[MAX_NAME_LEN];
        char path[MAX_PATH_LEN];
} args_t;

static fnotify_t fnotify;

#define FNOTIFY_HTABLE_INIT_SIZE        20

static uint32_t __key (const void *_key)
{
        return *(uint32_t *)_key;
}

static int __cmp(const void *_v1, const void *_v2)
{
        const entry_t *ent = (entry_t *)_v1;
        int v1, v2;

        v1 = ent->fd;
        v2 = *(uint32_t *)_v2;

        //DINFO("%d --> %d\n", v1, v2);

        return v1 - v2;
}


/* end of hash table functions */

inline static int __build_path(const char *path, const char *buf)
{
        int ret, fd, count;

        ret = path_validate(path, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }   

        count = _write(fd, buf, strlen(buf));
        if (count < 0) {
                ret = -count;
                GOTO(err_ret, ret);
        }   

        close(fd);

        return 0;
err_ret:
        return ret;
}

static void *__fnotify_thr_fn(void *arg)
{
        int ret, wd;
        char buf[1024];
        uint32_t len, idx;
        struct inotify_event *event;
        entry_t *ent;
        struct stat st;

        (void) arg;

        while (srv_running) {
                _memset(buf, 0x0, sizeof(buf));

                len = sizeof(buf);
                ret = _read(fnotify.inotify_fd, buf, len);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                //DINFO("read %u\n", ret);

                len = ret;
                for (idx = 0; idx < len; idx += sizeof(struct inotify_event) + event->len) {
                        event = (struct inotify_event *)(buf + idx);

                        ret = sy_spin_lock(&fnotify.lock);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);

                        ent = hash_table_find(fnotify.table, (void *)&event->wd);
                        if (ent == NULL) {
                                if ((event->mask & IN_IGNORED) || (event->mask & IN_DELETE_SELF)) {
                                        sy_spin_unlock(&fnotify.lock);
                                        continue;
                                }

                                YASSERT(0);
                        }

                        sy_spin_unlock(&fnotify.lock);

                        (void) wd;
                        stat(ent->path, &st);

                        if (S_ISREG(st.st_mode)) {
                                if ((event->mask &  IN_DELETE_SELF) || (event->mask & IN_MOVE_SELF)) {
                                        ret = sy_spin_lock(&fnotify.lock);
                                        if (unlikely(ret))
                                                GOTO(err_ret, ret);

                                        ret = hash_table_remove(fnotify.table, &event->wd, (void **)&ent);
                                        if (unlikely(ret))
                                                UNIMPLEMENTED(__DUMP__);

                                        sy_spin_unlock(&fnotify.lock);

                                        if (!ent->del_callback) {
                                                DERROR("%s removed\n", ent->path);
                                                EXIT(EIO);
                                        } else {
                                                DINFO("path %s del %p wd %d mask %u name %s (%d)\n",
                                                      ent->path, ent->del_callback,
                                                      event->wd, event->mask, event->name, event->len);
                                                ent->del_callback(ent->context, event->mask);
                                        }

                                        yfree((void **)&ent);
                                }

                                if (event->mask & IN_MODIFY) {
                                        if (!ent->mod_callback) {
                                                YASSERT(0);
                                        } else {
                                                DINFO("path %s mod %p wd %d mask %u name %s (%d)\n",
                                                      ent->path, ent->mod_callback, event->wd, event->mask, event->name, event->len);
                                                ent->mod_callback(ent->context, event->mask);
                                        }
                                }
                        } else if (S_ISDIR(st.st_mode)) {
                                if (event->mask & IN_DELETE) {
                                        if (!ent->del_callback) {
                                                DERROR("%s removed\n", ent->path);
                                                EXIT(EIO);
                                        } else {
                                                DINFO("path %s del %p wd %d mask %u name %s (%d)\n",
                                                      ent->path, ent->del_callback, event->wd, event->mask, event->name, event->len);
                                                ent->del_callback(ent->context, event->mask);
                                        }
                                }

                                if (event->mask & IN_CREATE) {
                                        if (!ent->mod_callback) {
                                                YASSERT(0);
                                        } else {
                                                DINFO("path %s mod %p wd %d mask %u name %s (%d)\n",
                                                      ent->path, ent->mod_callback, event->wd, event->mask, event->name, event->len);
                                                ent->mod_callback(ent->context, event->mask);
                                        }
                                }
                        }
                }
        }

        return NULL;
err_ret:
        YASSERT(0);
        return NULL;
}

/* fnotify functions :
 * fnotify_init
 * fnotify_destroy
 * fnotify
 */
int fnotify_init()
{
        int ret;
        pthread_t th;
        pthread_attr_t ta;

        ret = sy_spin_init(&fnotify.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        fnotify.table = hash_create_table(__cmp, __key, "fnotify");
        if (fnotify.table == NULL) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        fnotify.inotify_fd = inotify_init();
        if (fnotify.inotify_fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        pthread_attr_init(&ta);
        pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        ret = pthread_create(&th, &ta, __fnotify_thr_fn, NULL);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int fnotify_register(const char *path, fnotify_callback mod_callback,
                     fnotify_callback del_callback, void *context)
{
        int ret, wd;
        entry_t *ent;

        ret = ymalloc((void **)&ent, sizeof(*ent) + strlen(path) + 1);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ent->mod_callback = mod_callback;
        ent->del_callback = del_callback;
        ent->context = context;
        _strncpy(ent->path, path, sizeof(ent->path) + 1);
        //_strncpy(ent->buf, buf, sizeof(ent->buf)  1);

        wd = inotify_add_watch(fnotify.inotify_fd, path, IN_MODIFY | IN_DELETE_SELF
                               | IN_MOVE_SELF | IN_CREATE | IN_DELETE | IN_MOVE);
        if (wd == -1) {
                ret = errno;
                DWARN("notify %s\n", path);
                GOTO(err_ret, ret);
        }

        ent->fd = wd;

        ret = sy_spin_lock(&fnotify.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = hash_table_insert(fnotify.table, (void *)ent, &ent->fd, 0);
        if (unlikely(ret))
                GOTO(err_lock, ret);

        sy_spin_unlock(&fnotify.lock);

        DBUG("path %s mod %p del %p context %p fd %d\n",
              path, mod_callback, del_callback, context, ent->fd);

        return 0;
err_lock:
        sy_spin_unlock(&fnotify.lock);
err_ret:
        return ret;
}

static int __dmsg_mod(void *context, uint32_t mask)
{
        int ret;
        char buf[MAX_BUF_LEN];
        args_t *args;

        (void) mask;

        args = context;
        ret = _get_text(args->path, buf, sizeof(buf));
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        ret = args->callback(buf, args->flag);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __dmsg_remove(void *context, uint32_t mask)
{
        int ret;
        args_t *args;

        (void) mask;
        args = context;

        ret = _set_text_direct(args->path, args->value, strlen(args->value) + 1,
                        O_CREAT | O_EXCL | O_SYNC);
        if (ret && ret != EEXIST)
                GOTO(err_ret, ret);

        ret = args->callback(args->value, args->flag);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = fnotify_register(args->path, __dmsg_mod, __dmsg_remove, args);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int fnotify_create(const char *path, const char *value,
                       int (*callback)(const char *buf, uint32_t flag),
                       uint32_t flag)
{
        int ret;
        char buf[MAX_BUF_LEN];
        args_t *args;

        //ret = path_validate(path, 0, YLIB_DIRCREATE);
        ret = path_validate(path, 0, ng.daemon ? YLIB_DIRCREATE : 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = _set_text_direct(path, value, strlen(value) + 1,
                               O_CREAT | O_SYNC | O_EXCL);
        if (ret && ret != EEXIST)
                GOTO(err_ret, ret);

        ret = _get_text(path, buf, sizeof(buf));
        if (ret < 0) {
                ret = -ret;
                if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        ret = callback(buf, flag);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (!ng.daemon)
                goto out;

        ret = ymalloc((void **)&args, sizeof(*args));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        strcpy(args->path, path);
        strcpy(args->value, value);
        args->callback = callback;
        args->flag = flag;

        ret = fnotify_register(path, __dmsg_mod, __dmsg_remove, args);
        if (unlikely(ret))
                GOTO(err_ret, ret);

out:
        return 0;
err_ret:
        return ret;
}

int fnotify_unregister(const char *path)
{
        int ret, wd;
        entry_t *ent;

        wd = inotify_add_watch(fnotify.inotify_fd, path, IN_MODIFY | IN_DELETE_SELF
                               | IN_MOVE_SELF | IN_CREATE | IN_DELETE | IN_MOVE);
        if (wd == -1) {
                goto out;
        }

        ret = sy_spin_lock(&fnotify.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ent = hash_table_find(fnotify.table, (void *) &wd);
        if (ent) {
                ret = hash_table_remove(fnotify.table, &ent->fd, (void **) &ent);
                if (unlikely(ret)) {
                    DERROR("ret (%u) %s\n", ret, strerror(ret));
                    GOTO(err_lock, ret);
                }

                yfree((void **) &ent);
        }

        sy_spin_unlock(&fnotify.lock);

        ret = inotify_rm_watch(fnotify.inotify_fd, wd);
        if (ret < 0) {
                ret = errno;
                DERROR("ret (%u) %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

out:
        return 0;
err_lock:
        sy_spin_unlock(&fnotify.lock);
err_ret:
        return ret;
}

int quorum_fnotify_register(const char *path, fnotify_callback mod_callback,
                            fnotify_callback del_callback, void *context)
{
        int ret, wd;
        entry_t *ent;

        ret = ymalloc((void **) &ent, sizeof(*ent) + strlen(path) + 1);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ent->mod_callback = mod_callback;
        ent->del_callback = del_callback;
        ent->context = context;
        _strncpy(ent->path, path, sizeof(ent->path) + 1);

        wd = inotify_add_watch(fnotify.inotify_fd, path, IN_MODIFY | IN_DELETE_SELF | IN_MOVE_SELF);
        if (wd == -1) {
                ret = errno;
                DWARN("notify %s\n", path);
                GOTO(err_ret, ret);
        }

        ent->fd = wd;

        ret = sy_spin_lock(&fnotify.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = hash_table_insert(fnotify.table, (void *) ent, &ent->fd, 0);
        if (unlikely(ret))
                GOTO(err_lock, ret);

        sy_spin_unlock(&fnotify.lock);
        //DINFO("registered id %d\n", ent->fd);

        return wd;
err_lock:
        sy_spin_unlock(&fnotify.lock);
err_ret: 
        return ret;
}

int quorum_fnotify_unregister(int wd)
{
        int ret;
        entry_t *ent;

        ret = inotify_rm_watch(fnotify.inotify_fd, wd);
        if (ret < 0) {
                ret = errno;
                DERROR("ret (%u) %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        ret = sy_spin_lock(&fnotify.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ent = hash_table_find(fnotify.table, (void *) &wd);
        if (ent == NULL) {
                ret = errno;
                DERROR("ret (%u) %s\n", ret, strerror(ret));
                GOTO(err_lock, ret);
        }

        ret = hash_table_remove(fnotify.table, &ent->fd, (void **) &ent);
        if (unlikely(ret)) {
            DERROR("ret (%u) %s\n", ret, strerror(ret));
            GOTO(err_lock, ret);
        }

        sy_spin_unlock(&fnotify.lock);
        yfree((void **) &ent);

        return 0;
err_lock:
        sy_spin_unlock(&fnotify.lock);
err_ret: 
        return ret;
}
/* end of fnotify funtions */

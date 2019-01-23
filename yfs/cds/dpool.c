

#include <sys/types.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSCDS

#include "sysutil.h"
#include "ylib.h"
#include "hash_table.h"
#include "configure.h"
#include "disk.h"
#include "dpool.h"
#include "net_global.h"
#include "job_dock.h"
#include "dbg.h"

typedef struct {
        struct list_head hook;
        char path[MAX_PATH_LEN];
} entry_t;

static void __dpool_newname(char *name, const char *home)
{
        uuid_t uuid;
        char id[MAX_PATH_LEN];

        uuid_generate(uuid);
        uuid_unparse(uuid, id);

        snprintf(name, MAX_PATH_LEN, "%s/%s", home, id);
}

static int __dpool_newfile(char *name, const char *home, int size)
{
        int ret, fd;
        job_t *job;

        (void)size;
        __dpool_newname(name, home);

        fd = _open(name, O_CREAT | O_EXCL | O_RDWR | O_DIRECT, 0644);
        if (fd < 0) {
                ret = errno;
                DERROR("create(%s, ...)\n", name);
                if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        ret = job_create(&job, NULL, "dpool_alloc");
        if (ret)
                GOTO(err_fd, ret);

        job->update_load = 1;

        job_destroy(job);

        close(fd);

        return 0;
err_fd:
        close(fd);
err_ret:
        return ret;
}

static int __dpool_alloc(dpool_level_t *level, int max, int size)
{
        int ret;
        entry_t *ent;
        uint64_t dsize, dfree;

        while (level->len < max) {
                ret = _disk_dfree(level->home, &dsize, &dfree);
                if (ret)
                        GOTO(err_ret, ret);

                if (dfree < (unsigned)size * 2) {
                        goto out;
                }

                ret = ymalloc((void **)&ent, sizeof(*ent));
                if (ret)
                        GOTO(err_ret, ret);

                ret = __dpool_newfile(ent->path, level->home, size);
                if (ret)
                        GOTO(err_free, ret);

                ret = sy_spin_lock(&level->lock);
                if (ret)
                        GOTO(err_free, ret);

                list_add_tail(&ent->hook, &level->list);

                level->len++;

                sy_spin_unlock(&level->lock);
        }

out:
        return 0;
err_free:
        yfree((void **)&ent);
err_ret:
        return ret;
}

static void *__worker(void *_args)
{
        int ret, i;
        dpool_t *dpool;

        dpool = _args;

        while (1) {
                sleep(1);

                for (i = dpool->level_count - 1; i >= 0; i--) {
                        ret = __dpool_alloc(&dpool->array[i], dpool->max, dpool->size);
                        if (ret)
                                GOTO(err_ret, ret);

                }
        }

        return NULL;
err_ret:
        UNIMPLEMENTED(__DUMP__);
        return NULL;
}

static int __disk_iterator(const char *parent, const char *name, void *_args)
{
        int ret;
        dpool_level_t *level;
        entry_t *ent;

        (void) parent;
        level = _args;

        if (strcmp(name, ".") == 0
            || strcmp(name, "..") == 0)
                return 0;

        ret = ymalloc((void **)&ent, sizeof(*ent));
        if (ret)
                GOTO(err_ret, ret);

        snprintf(ent->path, MAX_PATH_LEN, "%s/%s", parent, name);

        DBUG("load %s\n", ent->path);

        level->len++;

        list_add_tail(&ent->hook, &level->list);

        return 0;
err_ret:
        return ret;
}

int dpool_init(dpool_t **_dpool, size_t size, int max, int level_count)
{
        int ret, i;
        pthread_t th;
        pthread_attr_t ta;
        dpool_t *dpool;
        dpool_level_t *level;

        ret = ymalloc((void **)&dpool, sizeof(*dpool) + sizeof(dpool_level_t) * level_count);
        if (ret)
                GOTO(err_ret, ret);

        YASSERT(dpool->inited == 0);
        dpool->max = max;
        dpool->inited = 1;
        dpool->size = size;
        dpool->level_count = level_count;

        for (i = 0; i < level_count; i++) {
                level = &dpool->array[i];
                snprintf(level->home, MAX_PATH_LEN, "%s/disk/%u/dpool/%llu", ng.home, i, (LLU)size);

                ret = sy_spin_init(&level->lock);
                if (ret)
                        GOTO(err_ret, ret);

                INIT_LIST_HEAD(&level->list);
                level->len = 0;

                ret = path_validate(level->home, 1, 1);
                if (ret)
                        GOTO(err_ret, ret);

                ret = _dir_iterator(level->home, __disk_iterator, level);
                if (ret)
                        GOTO(err_ret, ret);

                DINFO("load level %u count %u\n", i, level->len);
        }

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        ret = pthread_create(&th, &ta, __worker, dpool);
        if (ret)
                GOTO(err_ret, ret);

        *_dpool = dpool;

        return 0;
err_ret:
        return ret;
}

int dpool_get(dpool_t *dpool, char *path, int levelid)
{
        int ret;
        struct list_head *pos;
        entry_t *ent;
        dpool_level_t *level;
        uint64_t dsize, dfree;

        YASSERT(dpool->inited);

        level = &dpool->array[levelid];

        ret = sy_spin_lock(&level->lock);
        if (ret)
                GOTO(err_ret, ret);

        if (level->len == 0) {
                sy_spin_unlock(&level->lock);

                ret = _disk_dfree(level->home, &dsize, &dfree);
                if (ret)
                        GOTO(err_ret, ret);

                if (dfree < (unsigned)dpool->size * 2) {
                        ret = ENOSPC;
                        DINFO("dfree=%lu,  dpool->size=%lu\n", dfree, dpool->size);
                        goto err_ret;
                }

                DINFO("dpool level %u %llu no ent\n", levelid, (LLU)dpool->size);

                ret = __dpool_newfile(path, level->home, dpool->size);
                if (ret)
                        GOTO(err_ret, ret);

                goto out;
        } else {
                YASSERT(list_empty(&level->list) == 0);

                pos = level->list.next;
                list_del(pos);
                level->len--;
                ent = (void *)pos;
                strcpy(path, ent->path);
                yfree((void **)&ent);
        }

        sy_spin_unlock(&level->lock);

out:
        return 0;
err_ret:
        return ret;
}

int dpool_put(dpool_t *dpool, const char *path, int levelid)
{
        int ret;
        entry_t *ent;
        dpool_level_t *level;

        YASSERT(dpool->inited);

        level = &dpool->array[levelid];

        ret = sy_spin_lock(&level->lock);
        if (ret)
                GOTO(err_ret, ret);

        if (level->len == dpool->max * 7) {
                DBUG("max recycle, max %u\n", dpool->max * 7);
                unlink(path);
        } else {
                ret = ymalloc((void **)&ent, sizeof(*ent));
                if (ret)
                        GOTO(err_ret, ret);

                __dpool_newname(ent->path, level->home);

                ret = rename(path, ent->path);
                if (ret)
                        GOTO(err_ret, ret);

                list_add_tail(&ent->hook, &level->list);
                level->len++;
        }

        sy_spin_unlock(&level->lock);

        return 0;
err_ret:
        return ret;
}

uint64_t dpool_size(dpool_t *dpool, int levelid)
{
        dpool_level_t *level;
        uint64_t size;

        YASSERT(dpool->inited);

        return 0;

        level = &dpool->array[levelid];

        size =  (uint64_t)dpool->size * level->len;
        
        DBUG("level %u size %llu\n", levelid, (LLU)size);

        return size;
}

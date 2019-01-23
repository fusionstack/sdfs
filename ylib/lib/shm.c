

#include <string.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define DBG_SUBSYS S_LIBYLIB

#include "configure.h"
#include "shm.h"
#include "ylock.h"
#include "ylib.h"
#include "dbg.h"

typedef struct {
        int fd;
        int ref;
        void *addr;
        uint32_t size;
        uint32_t offset;
        sy_spinlock_t lock;
} entry_t;

typedef struct {
        char name[MAX_NAME_LEN];
        int current;
        sy_rwlock_t lock;
        entry_t array[MAX_OPEN_FILE];
} shm_t;

static shm_t *shm;

int __shm_free(int fd, void *addr, int size)
{
        int ret;
        
        ret = close(fd);
        if (ret < 0) {
                ret = errno;
                DWARN("close %d fail %d\n", fd, ret);
                GOTO(err_ret, ret);
        }

        if (addr != MAP_FAILED) {
                ret = munmap(addr, size);
                if (ret < 0) {
                        ret = errno;
                        DWARN("munmap %d fail %d\n", fd, ret);
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

int shm_init(const char *name)
{
        int ret, i;
        entry_t *entry;
        char path[MAX_PATH_LEN];

        sprintf(path, "/dev/shm/uss/tmp/%s", name);

        ret = path_validate(path, 1, 1);
        if (ret)
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&shm, sizeof(shm_t));
        if (ret)
                GOTO(err_ret, ret);

        strcpy(shm->name, name);

        for (i = 0; i < MAX_OPEN_FILE; i++) {
                entry = &shm->array[i];

                ret = sy_spin_init(&entry->lock);
                if (ret)
                        GOTO(err_ret, ret);

                entry->fd = -1;
                entry->ref = 0;
                entry->addr = MAP_FAILED;
        }

        ret = sy_rwlock_init(&shm->lock, NULL);
        if (ret)
                GOTO(err_ret, ret);

	return 0;
err_ret:
        return ret;
}

static int __shm_new_entry()
{
        int ret, fd;
        char path[MAX_PATH_LEN];
        entry_t *ent;
        void *addr;
        int size = SHM_MAX;

        sprintf(path, "/dev/shm/uss/tmp/%s/l-XXXXXX", shm->name);

        DBUG("new shm %s\n", path);

        ret = sy_rwlock_wrlock(&shm->lock);
        if (ret)
                GOTO(err_ret, ret);

        ent = &shm->array[shm->current];

        if (ent->fd != -1 && ent->offset < ent->size) {
                goto out;
        }

        fd = mkstemp(path);
        if (fd < 0) {
                ret = errno;
                GOTO(err_lock, ret);
        }

        unlink(path);

        ret = ftruncate(fd, size);
        if (ret < 0) {
                ret = errno;
                GOTO(err_fd, ret);
        }

        addr = mmap(0, size, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_LOCKED, fd, 0);
        if (addr == MAP_FAILED) {
                ret = errno;
                GOTO(err_fd, ret);
        }

        ent = &shm->array[fd];

        ret = sy_spin_lock(&ent->lock);
        if (ret)
                GOTO(err_map, ret);

        ent->size = size;
        ent->addr = addr;
        ent->offset = 0;

        YASSERT(ent->fd == -1);
        YASSERT(ent->ref == 0);

        ent->fd = fd;
        ent->ref = 1; /*release when full*/

        sy_spin_unlock(&ent->lock);

        shm->current = fd;
out:
        sy_rwlock_unlock(&shm->lock);

        return 0;
err_map:
        munmap(addr, size);
err_fd:
        close(fd);
err_lock:
        sy_rwlock_unlock(&shm->lock);
err_ret:
        return ret;
}

int shm_ref(int fd)
{
        int ret;
        entry_t *entry;

        entry = &shm->array[fd];

        YASSERT(entry->ref > 0);
        YASSERT(entry->fd >= 0);

        ret = sy_spin_lock(&entry->lock);
        if (ret)
                GOTO(err_ret, ret);

        entry->ref ++;

        sy_spin_unlock(&entry->lock);

        return 0;
err_ret:
        return ret;
}

int shm_unref(int fd)
{
        int ret;
        entry_t *entry;

        entry = &shm->array[fd];

        YASSERT(entry->ref > 0);
        YASSERT(entry->fd >= 0);

        ret = sy_spin_lock(&entry->lock);
        if (ret)
                GOTO(err_ret, ret);

        entry->ref --;

        DBUG("fd %u ref %u\n", entry->fd, entry->ref);

        if (entry->ref == 0) {
                ret =  __shm_free(entry->fd, entry->addr, entry->size);
                if (ret)
                        GOTO(err_ret, ret);

                entry->fd = -1;
                entry->addr = MAP_FAILED;
        }

        sy_spin_unlock(&entry->lock);

        return 0;
err_ret:
        return ret;
}

static int __shm_new_buffer(int *_fd, off_t *offset, void **ptr, uint32_t _size, entry_t *ent)
{
        int ret, size;

        if (_size % PAGE_SIZE)
                size = (_size / PAGE_SIZE + 1) * PAGE_SIZE;
        else
                size = _size;

        ret = sy_spin_lock(&ent->lock);
        if (ret)
                GOTO(err_ret, ret);

        if (ent->fd == -1 || ent->size == ent->offset) {
                DBUG("fd %d size %d offset %d\n", ent->fd, ent->size, ent->offset);
                ret = ENOENT;
                goto err_lock;
        }

        if (ent->offset + size > ent->size) {
                DBUG("fd %d size %d offset %d size %u\n", ent->fd, ent->size, ent->offset, size);
                ent->offset = ent->size;

                ent->ref --;

                DBUG("fd %u ref %u\n", ent->fd, ent->ref);

                if (ent->ref == 0) {
                        ret =  __shm_free(ent->fd, ent->addr, ent->size);
                        if (ret)
                                GOTO(err_ret, ret);

                        ent->fd = -1;
                        ent->addr = MAP_FAILED;
                }

                ret = ENOENT;
                goto err_lock;
        }

        DBUG("fd %d size %d offset %d size %u\n", ent->fd, ent->size, ent->offset, size);

        *_fd = ent->fd;
        *ptr = ent->addr + ent->offset;;
        *offset = ent->offset;
        ent->offset += size;
        ent->ref ++;

        sy_spin_unlock(&ent->lock);

        return 0;
err_lock:
        sy_spin_unlock(&ent->lock);
err_ret:
        return ret;
}

int shm_new(int *_fd, off_t *offset,  void **ptr, uint32_t size)
{
        int ret;
        entry_t *ent;

retry:
        ret = sy_rwlock_rdlock(&shm->lock);
        if (ret)
                GOTO(err_ret, ret);

        ent = &shm->array[shm->current];

        ret = __shm_new_buffer(_fd, offset, ptr, size, ent);
        if (ret) {
                if (ret == ENOENT) {
                        sy_rwlock_unlock(&shm->lock);

                        __shm_new_entry();
                        goto retry;
                } else
                        GOTO(err_lock, ret);
        }

        sy_rwlock_unlock(&shm->lock);

        return 0;
err_lock:
        sy_spin_unlock(&ent->lock);
err_ret:
        return ret;
}

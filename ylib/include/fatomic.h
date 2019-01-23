#ifndef __FATOMIC_H
#define __FATOMIC_H

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

#include "ylock.h"
#include "dbg.h"

typedef uint64_t fatomic_value;

typedef struct {
        sy_rwlock_t    lock;
        int            fd;
        void          *mmap;
        fatomic_value  value;
} fatomic_t;

static inline int fatomic_init(fatomic_t *atom, const char *path, 
                fatomic_value init_value, fatomic_value *value)
{
        int ret, fd;
        void *ptr;

        fd = open(path, O_RDWR|O_CREAT);
        if (fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ptr = mmap(NULL, sizeof(fatomic_value), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
                ret = errno;
                GOTO(err_fd, ret);
        }

        sy_rwlock_init(&atom->lock, NULL);
        atom->value = init_value;
        atom->fd = fd;
        atom->mmap = ptr;

        *value = atom->value;

        return 0;
err_fd:
        close(fd);
err_ret:
        return 0;
}

static inline int fatomic_destroy(fatomic_t *atom)
{
        if (atom) {
                sy_rwlock_destroy(&atom->lock);

                if (atom->mmap)
                        munmap(atom->mmap, sizeof(fatomic_t));

                if (atom->fd)
                        close(atom->fd);

                atom->value = 0;
        }

        return 0;
}

static inline int fatomic_get(fatomic_t *atom, fatomic_value *value)
{
        int ret;

        *value = 0;

        ret = sy_rwlock_rdlock(&atom->lock);
        if (ret)
                GOTO(err_ret, ret);

        *value = *(fatomic_value *)atom->mmap;

        sy_rwlock_unlock(&atom->lock);

        return 0;
err_ret:
        return ret;
}

static inline int fatomic_get_and_inc(fatomic_t *atom, fatomic_value *value)
{
        int ret;

        *value = 0;

        ret = sy_rwlock_wrlock(&atom->lock);
        if (ret)
                GOTO(err_ret, ret);

        if (value) *value = *(fatomic_value *)atom->mmap;
        (*(fatomic_value *)atom->mmap)++;

        sy_rwlock_unlock(&atom->lock);
        return 0;
err_ret:
        return ret;
}

#endif

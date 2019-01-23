#ifndef __YATOMIC_H
#define __YATOMIC_H

#include "ylock.h"
#include "dbg.h"

#define __USE_SPINLOCK

#ifdef __USE_SPINLOCK

#define yatomic_lock_t       sy_spinlock_t
#define yatomic_lock_init    sy_spin_init
#define yatomic_lock_destroy sy_spin_destroy
#define yatomic_lock_rdlock  sy_spin_lock
#define yatomic_lock_wrlock  sy_spin_lock
#define yatomic_lock_unlock  sy_spin_unlock

#else

#define yatomic_lock_t       sy_rwlock_t
#define yatomic_lock_init    sy_rwlock_init
#define yatomic_lock_destroy sy_rwlock_destroy
#define yatomic_lock_rdlock  sy_rwlock_rdlock
#define yatomic_lock_wrlock  sy_rwlock_wrlock
#define yatomic_lock_unlock  sy_rwlock_unlock

#endif

typedef uint64_t yatomic_value;

typedef struct {
        yatomic_lock_t lock;
        yatomic_value  value;
} yatomic_t;

static inline int yatomic_init(yatomic_t *atom, yatomic_value init_value)
{
        yatomic_lock_init(&atom->lock);
        atom->value = init_value;

        return 0;
}

static inline int yatomic_destroy(yatomic_t *atom)
{
        if (atom) {
                yatomic_lock_destroy(&atom->lock);
        }

        return 0;
}

static inline int yatomic_get(yatomic_t *atom, yatomic_value *value)
{
        int ret;

        *value = 0;

        ret = yatomic_lock_rdlock(&atom->lock);
        if (ret)
                GOTO(err_ret, ret);

        *value = atom->value;

        yatomic_lock_unlock(&atom->lock);

        return 0;
err_ret:
        return ret;
}

static inline int yatomic_get_and_inc(yatomic_t *atom, yatomic_value *value)
{
        int ret;

        if (value) *value = 0;

        ret = yatomic_lock_wrlock(&atom->lock);
        if (ret)
                GOTO(err_ret, ret);

        if (value) *value = atom->value;
        atom->value++;

        yatomic_lock_unlock(&atom->lock);
        return 0;
err_ret:
        return ret;
}

static inline int yatomic_add_and_get(yatomic_t *atom, yatomic_value part, yatomic_value *value)
{
        int ret;

        if (value) *value = 0;

        ret = yatomic_lock_wrlock(&atom->lock);
        if (ret)
                GOTO(err_ret, ret);

        atom->value += part;
        if (value) *value = atom->value;

        yatomic_lock_unlock(&atom->lock);
        return 0;
err_ret:
        return ret;
}

static inline int yatomic_get_and_dec(yatomic_t *atom, yatomic_value *value)
{
        int ret;

        if (value) *value = 0;

        ret = yatomic_lock_wrlock(&atom->lock);
        if (ret)
                GOTO(err_ret, ret);

        if (value) *value = atom->value;
        if (atom->value > 0) atom->value--;

        yatomic_lock_unlock(&atom->lock);
        return 0;
err_ret:
        return ret;
}

static inline int yatomic_dec_and_get(yatomic_t *atom, yatomic_value *value)
{
        int ret;

        if (value) *value = 0;

        ret = yatomic_lock_wrlock(&atom->lock);
        if (ret)
                GOTO(err_ret, ret);

        if (atom->value > 0) atom->value--;
        if (value) *value = atom->value;

        yatomic_lock_unlock(&atom->lock);
        return 0;
err_ret:
        return ret;
}

static inline int yatomic_set(yatomic_t *atom, yatomic_value value)
{
        int ret;

        ret = yatomic_lock_wrlock(&atom->lock);
        if (ret)
                GOTO(err_ret, ret);

        atom->value = value;

        yatomic_lock_unlock(&atom->lock);
        return 0;
err_ret:
        return ret;
}

#endif

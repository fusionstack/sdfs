

#include <string.h>
#include <pthread.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "array_table.h"
#include "ylib.h"
#include "dbg.h"

atable_t array_create_table(int (*compare_func)(void *, void *), int max_array)
{
        int ret, i;
        uint32_t len;
        void *ptr;
        atable_t t;

        len = sizeof(struct atable)
              + sizeof(struct atable_entry) * max_array;

        ret = ymalloc(&ptr, len);
        if (ret)
                GOTO(err_ret, ret);

        t = (atable_t)ptr;

        for (i = 0; i < max_array; i++) {
                ret = sy_rwlock_init(&t->array[i].rwlock, NULL);
                if (ret)
                        GOTO(err_table, ret);

                t->array[i].value = NULL;
        }

        t->nr_array = 0;
        t->max_array = max_array;
        t->array_no = 0;
        t->compare_func = compare_func;

        return t;
err_table:
        for (i--; i >= 0; i--)
                (void) sy_rwlock_destroy(&t->array[i].rwlock);
err_ret:
        return NULL;
}

int array_table_get_nr(atable_t t)
{
        return t->nr_array;
}

int array_table_get_max(atable_t t)
{
        return t->max_array;
}

/* return -ENOENT, if there are no more free table */
int array_table_insert_empty(atable_t t, void *value)
{
        int ret, curno, no;

        if (t->array_no == t->max_array)
                t->array_no = 0;

        curno = t->array_no++;
        no = curno;
        while (1) {
                ret = sy_rwlock_trywrlock(&t->array[no].rwlock);
                if (ret != 0) {
                        no = (no + 1) % t->max_array;
                        if (no == curno)  /* not found */
                                break;

                        continue;
                } else  /* free table */
                        goto found;
        }
        return -ENOENT;

found:
        UNIMPLEMENTED(__NULL__); /*need lock*/
        t->nr_array++;
        t->array[no].value = value;

        return no;
}

void *array_table_find(atable_t t, int no, void *comparator)
{
        if (t->compare_func
            && (*t->compare_func)(t->array[no].value, comparator))
                return NULL;
        else
                return t->array[no].value;
}

int array_table_remove(atable_t t, int no, void *comparator, void **value)
{
        int ret;

        ret = sy_rwlock_trywrlock(&t->array[no].rwlock);
        if (ret == 0) {
                (void) sy_rwlock_unlock(&t->array[no].rwlock);

                ret = EINVAL;
                DERROR("table %d un-alloc'ed\n", no);
                GOTO(err_ret, ret);
        }

        if (t->compare_func && comparator
            && (*t->compare_func)(t->array[no].value, comparator)) {
                DERROR("not equal, it's very bad\n");
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        if (value)
                *value = t->array[no].value;

        t->array[no].value = NULL;
        t->nr_array--;

        sy_rwlock_unlock(&t->array[no].rwlock);

        return 0;
err_ret:
        return ret;
}

void array_table_destroy(atable_t t)
{
        int i;

        for (i = t->max_array; i >= 0; i--) {
                if (t->array[i].value)
                        yfree((void **)&t->array[i].value);
                (void) sy_rwlock_destroy(&t->array[i].rwlock);
        }

        yfree((void **)&t);
}

#ifndef __ARRAY_TABLE_H__
#define __ARRAY_TABLE_H__

#include "ylock.h"

struct atable_entry {
        sy_rwlock_t rwlock;
        void *value;
};

typedef struct atable {
        int nr_array;
        int max_array;

        int array_no;
        int (*compare_func)(void *, void *);
        struct atable_entry array[0];
} *atable_t;

#endif

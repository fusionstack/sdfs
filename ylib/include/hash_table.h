#ifndef __HASH_TABLE_H__
#define __HASH_TABLE_H__

/* shamlessly stolean from http://www.sf.net/projects/sandiaportals/ */

#include <stdint.h>

#include "sdfs_conf.h"

typedef struct hashtable_entry {
        uint32_t key;
        void *value;
        struct hashtable_entry *next;
} *hashtable_entry_t;

typedef struct hashtable {
        char name[MAX_NAME_LEN];
        unsigned int size;
        unsigned int num_of_entries;
        int (*compare_func)(const void *, const void *);
        uint32_t (*key_func)(const void *);
        hashtable_entry_t *entries;
} *hashtable_t;

#endif

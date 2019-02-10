#ifndef __MEM_CACHE_H__
#define __MEM_CACHE_H__

#include <stdint.h>
#include <sys/types.h>
#include <stdint.h>
#include <semaphore.h>
#include <unistd.h>
#include <stdlib.h>

#define ENABLE_MEM_CACHE 1

#include "sdfs_conf.h"
#include "sdfs_list.h"

#define MEM_CACHE_SIZE64 64
#define MEM_CACHE_SIZE128 128
#define MEM_CACHE_SIZE4K 4096
#define MEM_CACHE_SIZE8K 8192

typedef struct {
        char buf[MEM_CACHE_SIZE64];
} mem_cache64_t;

typedef struct {
        char buf[MEM_CACHE_SIZE128];
} mem_cache128_t;

typedef struct {
        char buf[MEM_CACHE_SIZE4K];
} mem_cache4k_t;

typedef struct {
        char buf[MEM_CACHE_SIZE8K];
} mem_cache8k_t;


typedef enum {
        MEM_CACHE_64,
        MEM_CACHE_128,
        MEM_CACHE_4K,
        MEM_CACHE_8K,
        MEM_CACHE_NR,
} mem_cache_type_t;

static inline int mem_cache_size(mem_cache_type_t type)
{
        uint64_t array[] = {MEM_CACHE_SIZE64, MEM_CACHE_SIZE128, MEM_CACHE_SIZE4K, MEM_CACHE_SIZE8K};
        return array[type];
}

typedef struct {
        char *name;                     /* Name */
        time_t last_expand;

        uint32_t base_nr;                    /* The base number of current pool */
        uint32_t max_nr;                     /* The maximum number of current pool */
        uint32_t idx;                        /* Index of next free slot */
        uint32_t thread;
        mem_cache_type_t type;

        int _private;
        uint32_t align;                      /* Align flag */

        uint32_t unit_size;                  /* Unit size of pool object */
        uint32_t real_size;                  /* Real size of pool object */

        void **pool;                    /* Array of pointers that point to units */

        pthread_spinlock_t lock;        /* Lock of this memory cache */

        pthread_spinlock_t cross_lock;
        struct list_head cross_list;
        int cross_free;
} mem_cache_t;

/*
 * Alloc can't fail. If there is no memory, function will block and wait for
 * memory alloc success.
 */
#define __MC_FLAG_NOFAIL__  0x01

int mem_cache_init();
int mem_cache_private_init();
int mem_cache_private_destroy();

int mem_cache_inited();

void *mem_cache_calloc(mem_cache_type_t type, uint8_t flag);
void *mem_cache_calloc1(mem_cache_type_t type, int size);
void mem_cache_free(mem_cache_type_t, void *);

#endif /* MEM_CACHE_H */

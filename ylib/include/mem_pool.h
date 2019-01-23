/*
 * =====================================================================================
 *
 *       Filename:  mem_pool.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  03/14/2011 03:05:56 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */

#ifndef MEM_POOL_H_
#define MEM_POOL_H_

#include <stdint.h>
#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <pthread.h>
#include "../include/ylock.h"
#include "../include/dbg.h"
#define ALLOC_ALGIN_SIZE sizeof(void*)
#define MIN_ALLOC_SIZE  2*(sizeof(void*) + sizeof(size_t))
#define MAX_MEM_SUPPORT ((1024-2) * ALLOC_ALGIN_SIZE)
#define MEMSIZE_ALGIN( size )\
        (size & (~(ALLOC_ALGIN_SIZE-1))) + ALLOC_ALGIN_SIZE
#define MEM_POOL 0
#if 0
#define GOTO(lab, ret)\
	goto lab;
#endif
extern size_t mem_pool_real_alloc;
extern size_t mem_pool_real_free;
extern size_t mem_pool_total_alloc;
extern size_t mem_pool_total_free;
extern size_t mem_pool_huge_alloc;
extern size_t mem_pool_huge_free;
extern size_t mem_pool_free_num;
extern size_t mem_pool_alloc_num;
extern size_t mem_pool_static_num;

typedef struct mem_ent_s {
        /*be sure sizeof(struct mem_ent_s) == sizeof(void *)
         */
        struct mem_ent_s *next;
        char data[0];
} mem_ent_t;

/*mem_internal format
 *point
 *size
 *mem 
 */

typedef struct mem_blk_s {
	int mem_len;
	int free_nums;
	int tota_nums;
	void *mm;
	struct mem_blk_s *next;
	struct mem_blk_s *prev;
	char data[0];
	/*   the data contains bitsmap, mem_hent_s *next, and others
	 */
}mem_blk_t;
typedef struct mem_hent_s {
	int    mem_len;
	int    tota_nums;
} mem_hent_t;

typedef struct mem_pool_s {

        struct mem_ent_s * array[1024];
	struct mem_blk_s * huge_blk[1024];
        struct mem_blk_s * null_blk[1024];

	struct mem_hent_s  huge_statics[1024];

        sy_spinlock_t lock[1024];
        sy_spinlock_t glock;

        size_t status[1024];
	size_t status_free[1024];

	size_t total_alloc_size;
	size_t total_free_size;

	size_t real_alloc_size;
	size_t real_free_size;

	size_t total_free_hold;
	size_t total_pool_memusage;

        size_t huge_mem_alloc;
        size_t huge_mem_free;
        size_t huge_mem_hold_ent;
        size_t huge_mem_alloc_hold;
        int    inuse;
        size_t miss;
        size_t hint;
	int    pagesize;
} mem_pool_t;


extern mem_pool_t global_mem_pool;
extern int        global_mem_pool_inuse;
extern int mem_pool_init(mem_pool_t *pool, int pagesize);
extern int mem_pool_free(mem_pool_t *pool,void *ptr, size_t size);
extern int mem_pool_alloc(mem_pool_t *pool,void **ptr,size_t size);
extern void mem_pool_dump(mem_pool_t *pool);
extern int mem_pool_bigmem_alloc(struct mem_pool_s *pool, void**ptr,size_t size);
extern int mem_pool_bigmem_free(struct mem_pool_s *pool, void**ptr);


#endif


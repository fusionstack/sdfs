/*
 * =====================================================================================
 *
 *       Filename:  mem_pool.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  03/14/2011 03:05:33 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */
#include "../include/mem_pool.h"
#include <assert.h>

mem_pool_t global_mem_pool; 
int        global_mem_pool_inuse = 0;
size_t mem_pool_static_num = 0;
size_t mem_pool_decrease_num = 0;
size_t mem_pool_decrease_need = 0;

typedef struct mem_status_s {
        size_t num;
        int len;
} mem_status_t;
static int mem_pool_decrease(mem_pool_t *pool);

int mem_status_cmp(const void *d1, const void *d2)
{
        mem_status_t *v1, *v2;
        v1 = (mem_status_t*)d1;
        v2 = (mem_status_t*)d2;
        if ((v1->len * v1->num) > (v2->len * v2->num))
                return -1;
        if ((v1->len * v1->num) == (v2->len * v2->num))
                return 0;
        if ((v1->len * v1->num) < (v2->len * v2->num))
                return 1;
        return 0;
}
int sizet_cmp(const void *d1, const void *d2)
{
        size_t v1,v2;
        v1 = *((size_t*)d1);
        v2 = *((size_t*)d2);
        if (v1 > v2) 
                return -1;
        if (v1 == v2)
                return 0;
        if (v1 < v2)
                return 1;
        return 0;
}
/*
 * the mem_len is alloc_size + sizeof(void*)
 */
int get_blk_ents_num(int page_size, int head_len, int mem_len)
{
	int alloc_num;
	int guest_page_num;
	int bitsize;
        uint32_t size;
	/*
	 * be sure that alloc_num * mem_len + (alloc_num/8)+1 <= page_size*n
	 */
	if ( (page_size -head_len)/ mem_len >= 64 ) {
		guest_page_num =  1;
	} else if ((64*mem_len)&(page_size-1)){
		guest_page_num = ((64* mem_len) / page_size);
	} else {
		guest_page_num = ((64* mem_len) / page_size)+1;
	}
	alloc_num = (guest_page_num * page_size - head_len) / mem_len;
	while(alloc_num) {
		bitsize = alloc_num / 8;
		if (alloc_num & 7) {
			bitsize++;
		}
                size = alloc_num*mem_len+head_len+bitsize;
                if (size & 7) {
                        size = (size &(~7))+8;
                }
		if ((size) >= (uint32_t)(guest_page_num * page_size)) {
			alloc_num--;
			continue;
		} 
		break;
	}
	return alloc_num;
}
int get_blk_size(int page_size, int head_len, int mem_len)
{
	int alloc_num;
	uint32_t size;
	uint32_t _page_size;

	_page_size = page_size;
	alloc_num = get_blk_ents_num(page_size, head_len, mem_len);

	size = alloc_num*mem_len + head_len + (alloc_num / 8);
	if (alloc_num & 7)
		size++;
        if (size & 7) {
                size = (size &(~7))+8;
        }
	if (size & (_page_size-1)) {
		size = (size & (~(_page_size-1))) + _page_size;
	}
	return size;
}

/*
 * bits lib
 * 1 to n
 */
void bits_set(unsigned char *bits, uint32_t nbit)
{
	uint32_t i,j;
	i = nbit / 8;
        if (i && !(nbit&7))
                i=i-1;
        if ((nbit & 7)) {
                j = (nbit & 7) -1;
        } else {
                j = 7;
        }
        bits[i] = bits[i] | (0x80 >> j);
}

void bits_clr(unsigned char *bits, uint32_t nbit)
{
        uint32_t i, j;
        i = nbit / 8;
        if (i && !(nbit & 7))
                i = i -1;
        if ((nbit & 7)) {
                j = (nbit & 7) -1;
        } else {
                j = 7;
        }
        bits[i] = bits[i] & (~(0x80 >> j));
}
int bits_first_null(char *bits, uint32_t nbit, uint32_t *retval) 
{
	int  num,nbits,i,j,ret;
	unsigned char bvalue;


	nbits = nbit / 8;
	
	for (i=0; i <nbits; i++) {
		bvalue = bits[i];
		if (bvalue != 0xFF)
			break;
	}
	if (bvalue == 0xFF) {
		num = nbits * 8;
		bvalue = bits[nbits];
                /*should nbit */
		j = nbit & 7;
	} else {
		num = (i) * 8; 
		j = 8;
	}
	for (i = 0; i < j; i++) {
		if (!(bvalue & (0x80 >> i)))
			break;
	}
	if (i == j) {
		ret = ENOENT;
		goto err_ret;
	}
	num = num + i+1;
	*retval = num;
	return 0;
err_ret:
        printf("bits i %d,j %d\n", i, j);
	return ret;
}
int get_mem_from_blk(struct mem_pool_s *pool, struct mem_blk_s *mem_blk, void **mem)
{
	uint32_t nbits,num;
	int ret;
        size_t key;

	num = mem_blk->tota_nums;
	if (bits_first_null((char*)mem_blk->data, num, &nbits)) {
                printf("tota_nums %d, nbit %d,free %d\n",mem_blk->tota_nums,num ,mem_blk->free_nums);
		ret = ENOMEM;
		goto err_ret;
	}
	bits_set((unsigned char*)mem_blk->data,nbits);
	mem_blk->free_nums--;
	(*mem) =(char*)mem_blk->mm + (nbits -1) * mem_blk->mem_len;

        /*if the blk become the null, we put it into the null list
         */
        if (mem_blk->free_nums == 0) {
                key = (mem_blk->mem_len - sizeof(size_t) - sizeof(void*)) / sizeof(void*);
                /*1. delete it from new list
                 * there is a trick that now, mem_blk is the first blk in huge_blk[key]
                 */
                if (pool->huge_blk[key]->next) {
                        pool->huge_blk[key]->next->prev = pool->huge_blk[key]->prev;
                        pool->huge_blk[key] = pool->huge_blk[key]->next;
                } else 
                        pool->huge_blk[key] = NULL;
                /*2. insert it into new list
                 */
                if (NULL == pool->null_blk[key]) {
                        pool->null_blk[key] = mem_blk;
                        mem_blk->next = NULL;
                        mem_blk->prev = mem_blk;
                } else {
                        mem_blk->prev = pool->null_blk[key]->prev;
                        pool->null_blk[key]->prev->next = mem_blk;
                        pool->null_blk[key]->prev = mem_blk;
                        mem_blk->next = NULL;
                }
        }
	return 0;
err_ret:
	return ret;
}
void new_mem_blk(int tota_nums, int mem_len, void *buf,
                struct mem_blk_s **mem_blk)
{
        uint32_t size;
        assert(mem_len != 0);
        assert(tota_nums != 0);
	(*mem_blk) = buf;
	(*mem_blk)->tota_nums = tota_nums;
	(*mem_blk)->free_nums = tota_nums;
	(*mem_blk)->mem_len   = mem_len;
        size = (tota_nums / 8);
        if (tota_nums & 7) {
                size = size+1;
        size = (size &(~7)) + 8;
        }

	(*mem_blk)->mm        = (*mem_blk)->data + size;
	memset((*mem_blk)->data, 0x00, (tota_nums / 8) + 1);

}
int mem_pool_bigmem_alloc(struct mem_pool_s *pool, void**ptr,size_t size)
{
        int ret;
        mem_ent_t *p,*n;

        DBUG("in big_mem  alloc size %zu\n", size);

        sy_spin_lock(&pool->lock[0]);
        p = pool->array[0];
        if (p) {
                if ((*((size_t*)p -1) >= size) &&  
                                (*((size_t*)p -1) < 1.3*size)) {
                        pool->array[0] = p->next;
                        *ptr = p;
                        goto free_hold;
                }
                n = p->next;
                while(n) {
                        if ((*((size_t*)n -1) >= size) &&  
                                        (*((size_t*)n -1) < 1.3*size)) {
                                p->next = n->next;
                                *ptr = n;
                                goto free_hold;
                        }
                        p = n;
                        n = n->next;
                }
        } 

        *ptr = calloc(1,size + sizeof(size_t)+sizeof(void*)); 
        if (!(*ptr)) {
                ret = ENOMEM;
                GOTO(err_max_nomem, ret);
        }

        sy_spin_lock(&pool->glock);
        pool->total_pool_memusage+=(size+sizeof(size_t)+sizeof(void*));
        pool->huge_mem_alloc+= (size+sizeof(size_t)+sizeof(void*));
        pool->real_alloc_size+=(size+sizeof(size_t)+sizeof(void*));
        sy_spin_unlock(&pool->glock);

        *((size_t*)((void**)(*ptr)+1)) = size;
        *ptr = (char*)(*ptr)+sizeof(size_t)+sizeof(void*);
        goto out;
free_hold:
        sy_spin_unlock(&pool->lock[0]);
        sy_spin_lock(&pool->glock);
        pool->huge_mem_hold_ent--;
        pool->huge_mem_alloc_hold -=(*((size_t*)(*ptr)-1) + sizeof(size_t) + sizeof(void*));
        pool->total_free_hold-=(*((size_t*)(*ptr)-1) + sizeof(size_t)+sizeof(void*));
        pool->total_alloc_size+=(*((size_t*)(*ptr)-1) + sizeof(size_t)+sizeof(void*));
        sy_spin_unlock(&pool->glock);
        return 0;
out:
        sy_spin_unlock(&pool->lock[0]);
        sy_spin_lock(&pool->glock); 
        pool->total_alloc_size+=(size + sizeof(size_t)+sizeof(void*));
        sy_spin_unlock(&pool->glock); 
        return 0;
err_max_nomem:
        return ret;
}
int mem_pool_bigmem_free(struct mem_pool_s *pool, void**ptr)
{
        mem_ent_t *p;
        p = *ptr;
        size_t i;

        DBUG("in big_mem free size %zu\n", *((size_t*)(*ptr)-1));

        sy_spin_lock(&pool->lock[0]);
        p->next = pool->array[0];
        sy_spin_lock(&pool->glock); 
        pool->total_free_size += *((size_t*)p-1)+sizeof(size_t)+sizeof(void*);
        pool->total_free_hold += *((size_t*)p-1) + sizeof(size_t) + sizeof(void*);
        pool->huge_mem_hold_ent++;
        pool->huge_mem_alloc_hold +=(*((size_t*)p-1) + sizeof(size_t) + sizeof(void*));
        sy_spin_unlock(&pool->glock); 
        pool->array[0] = p;

        i = pool->huge_mem_hold_ent;
        if ((pool->huge_mem_alloc_hold > 320*1024*1024)&& i>16) {
                 //DWARN("mem_pool bigmem is free ing hd %zu ha %zu he %zu\n",
                 //       pool->huge_mem_alloc_hold,
                 //       pool->huge_mem_alloc,
                 //       pool->huge_mem_hold_ent
                 //);
                 if (i < 1024)i=i>>1;
                 while(p&&(i--)&&(pool->huge_mem_alloc_hold>256*1024*1024)) {
                        pool->array[0] = p->next;
                        sy_spin_lock(&pool->glock); 

                        pool->huge_mem_alloc_hold -= (*((size_t*)p-1) + sizeof(void*) + sizeof(size_t));
                        pool->total_free_hold -= (*((size_t*)p -1)+sizeof(void*)+sizeof(size_t));
                        pool->real_free_size+= (*((size_t*)p -1) + sizeof(void*) + sizeof(size_t));
                        pool->total_pool_memusage-= (*((size_t*)p -1) + sizeof(void*) + sizeof(size_t));
                        pool->huge_mem_alloc -= (*((size_t*)p-1) + sizeof(void*) + sizeof(size_t));
                        pool->huge_mem_hold_ent--;
                        sy_spin_unlock(&pool->glock); 

                        free(*((void**)((size_t*)p -1)-1));
                        //DWARN("bigmem really free %zu\n", *((size_t*)p-1));
                        p = pool->array[0];
                 }
        }
        sy_spin_unlock(&pool->lock[0]);
        return 0;
}
int mem_pool_blk_append(struct mem_pool_s *pool, struct mem_blk_s *mem_blk)
{
	int key __attribute__((unused));
	(void)pool;
	key = (mem_blk->mem_len - sizeof(void*) - sizeof(size_t)) / sizeof(void*);
	/* uimplement
	 * */
	return 0;
}

void *mem_watch_dog(void *pool)
{
        mem_pool_t *_pool __attribute__((unused));
        _pool = pool;

        while(1) {
                sleep(1);
        }

}
int mem_watch_init(mem_pool_t *pool)
{
        pthread_t td;
        pthread_create(&td, NULL,mem_watch_dog , pool); 
        return 0;
}

void mem_pool_dump(mem_pool_t *pool)
{
        size_t total_alloc __attribute__((unused))=0, pool_alloc=0, pool_free=0,size=0;
        mem_status_t status_alloc[1024];
        mem_status_t status_free[1024];
        int i;
        memset(status_alloc, 0x00, sizeof(mem_status_t) * 1024);
        memset(status_free, 0x00, sizeof(mem_status_t) * 1024);

        sy_spin_lock(&pool->glock);
        size =  pool->total_alloc_size - pool->total_free_size; 
        /*
         * for overflow..
         */
        if (pool->total_alloc_size > 10000000000){
                pool->total_alloc_size = size;
                pool->total_free_size = 0;
        }
        total_alloc = pool->real_alloc_size - pool->real_free_size;

        for (i = 1; i < 1024; i++) {
                if (pool->status_free[i])
                        pool_free = pool_free + (pool->status_free[i] * ((i << 3)+sizeof(size_t)+sizeof(void*)));
                if (pool->status[i])
                        pool_alloc = pool_alloc + (pool->status[i] * ((i << 3)+ sizeof(size_t)+sizeof(void*)));
                status_alloc[i].num = pool->status[i];
                status_alloc[i].len = (i << 3)+ sizeof(size_t)+sizeof(void*);
                status_free[i].num = pool->status_free[i];
                status_free[i].len = ( i<<3 )+ sizeof(size_t)+sizeof(void*);
        }
        sy_spin_unlock(&pool->glock);

        
        if (size > 64*1024*1024) {
       
                if ( (pool_free > 64*1024*1024) ) {
                        i = 0;
                        size = pool_free;
                        qsort(&status_free[1], 1023, sizeof(mem_status_t), mem_status_cmp);
                        while(size > (pool_free >> 4)) {
                                size = size - (status_free[i].num * status_free[i].len);
                                i++;
                        }
                }
                if ( (pool_alloc > 10 * 1024*1024) ) {
                        i = 1;
                        size = pool_alloc;
                        qsort(&status_alloc[1], 1023, sizeof(mem_status_t), mem_status_cmp);
                        while(size > (pool_alloc >> 4)) {
                                size = size - (status_alloc[i].num * status_alloc[i].len);
                                i++;
                        }
                }
        }
}
int mem_pool_init(mem_pool_t *pool, int pagesize)
{
        int i;

        for (i=1; i < 1024; i++) {
                sy_spin_init(&pool->lock[i]);
                pool->status[i] = 0;
                pool->status_free[i]=0;
        }
        pool->pagesize = pagesize;
        sy_spin_init(&pool->glock);
        sy_spin_init(&pool->lock[0]);

        pool->status[0] = 0;
        pool->status_free[0]=0;
        memset(pool->array, 0x00, sizeof(struct mem_ent_s *)* 1024);
        memset(pool->huge_blk, 0x00, sizeof(struct mem_blk_s*) * 1024);
        memset(pool->null_blk, 0x00, sizeof(struct mem_blk_s*)*1024);

        pool->inuse = 1;
        pool->total_pool_memusage = 0;

        pool->total_free_size = 0;
        pool->total_free_hold = 0;
        pool->real_alloc_size = 0;
        pool->real_free_size  = 0;


        pool->huge_mem_hold_ent = 0;
        pool->huge_mem_alloc_hold = 0;

        global_mem_pool_inuse = 1;
        return 0;
}


static int mem_pool_newblk(mem_pool_t *pool, int key, mem_blk_t **blkent)
{
        int ret,mem_len, blk_size;
        void *_ptr;
        mem_len  = (key * sizeof(void*))+sizeof(void*) + sizeof(size_t);
        blk_size = get_blk_size(pool->pagesize, sizeof(mem_blk_t),mem_len);
        assert(blk_size > 4000);
        _ptr = calloc(1, blk_size);
        if (!_ptr) {
                assert(0);
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }
        new_mem_blk(get_blk_ents_num(pool->pagesize,sizeof(mem_blk_t),mem_len), 
                        mem_len, _ptr, blkent);
        if (pool->huge_blk[key]) {
                pool->huge_blk[key]->prev->next = (*blkent);
                (*blkent)->prev = pool->huge_blk[key]->prev;
                pool->huge_blk[key]->prev = (*blkent);
        } else {
                pool->huge_blk[key] = (*blkent);
                (*blkent)->prev = (*blkent);
        }
        (*blkent)->next = NULL;
        sy_spin_lock(&pool->glock);
        pool->total_free_hold += ((*blkent)->free_nums * mem_len);
        pool->real_alloc_size+=blk_size;
        pool->total_pool_memusage += blk_size;
        sy_spin_unlock(&pool->glock);
        return 0;
err_ret:
        return ret;
}

/*ymalloc(size)
 * mem_pool_alloc(size
 * 
 */
int mem_pool_alloc(mem_pool_t *pool,void **ptr,size_t size)
{
        int key;
        void *_ptr = NULL;
        mem_ent_t *mem;
        struct mem_blk_s *blkent;
        int ret;
        int mem_len;

        key = (size) / (sizeof(void *));
        mem_len = size + sizeof(size_t) + sizeof(void*);

        sy_spin_lock(&global_mem_pool.lock[key]);
        pool->status[key]++;
        if (pool->array[key]) {
                *ptr = pool->array[key];
                mem  = pool->array[key];
                pool->array[key] = mem->next;
                pool->status_free[key]--;

                sy_spin_lock(&pool->glock);
                pool->total_free_hold -=mem_len;
                pool->total_alloc_size +=mem_len;
                sy_spin_unlock(&pool->glock);

                sy_spin_unlock(&global_mem_pool.lock[key]);
        } else {
                if (pool->huge_blk[key]) {
                        /*
                         * alloc mem from need no resort
                         * */
#if 1
                        /*
                         * for optm. we put the null blk into pool->null_blk
                         * so, the while is useless
                        */
                        blkent = pool->huge_blk[key];
#else
                        /*
                         * 
                        */
                        blkent = pool->huge_blk[key]; 
                        while(blkent&&!blkent->free_nums)
                                blkent = blkent->next;
                        if (blkent) {
                                 /*alloc
                                 */
                        } else {
                                 /*calloc
                                 */
                                ret = mem_pool_newblk(pool, key, &blkent);
                                if (ret)
                                        GOTO(err_ret, ret);
                        }
#endif
                } else {
                        /*key * sizeof(void*) mem is alligne d by sizeof(void*)
                         *sizeof(void*)  is pointed to the blk
                         *sizeof(size_t) is the len of the buf. because we let the malloc to 
                         *manage the bigbuffer
                         */
                        ret = mem_pool_newblk(pool, key, &blkent);
                        if (ret)
                                GOTO(err_ret, ret);
                }
                assert(0==get_mem_from_blk(pool,blkent, &_ptr));

                sy_spin_lock(&pool->glock);
                pool->total_free_hold -= mem_len;
                pool->total_alloc_size +=mem_len;
                sy_spin_unlock(&pool->glock);

                sy_spin_unlock(&global_mem_pool.lock[key]);
                *((void**)_ptr) = blkent;
                *((size_t*)((void**)_ptr +1)) = size;
                *ptr = (char*)_ptr + sizeof(void*) + sizeof(size_t);
        }
        memset(*ptr, 0x00, size);
        return 0;
err_ret:
        sy_spin_unlock(&global_mem_pool.lock[key]);
        return ret;
}
int mem_pool_free(mem_pool_t *pool,void *ptr, size_t size)
{
        size_t key;
        mem_ent_t *mem;
        key = (size) / (sizeof(void*));
        mem = ptr; 
        sy_spin_lock(&pool->lock[key]);
        sy_spin_lock(&pool->glock);
        pool->total_free_size += (size + sizeof(size_t) + sizeof(void*));
        pool->total_free_hold += (size + sizeof(size_t) + sizeof(void*));
        sy_spin_unlock(&pool->glock);
        pool->status_free[key]++;
        pool->status[key]--;

        if (pool->array[key]) {
                mem->next = pool->array[key];
                pool->array[key] = mem;
        } else {
                mem->next = NULL;          
                pool->array[key] = mem;
        }
        sy_spin_unlock(&pool->lock[key]);
        if (pool->total_free_hold > (pool->huge_mem_alloc_hold + 32*1024*1024)) {
                mem_pool_decrease_num++;
                if ( mem_pool_decrease_num  >= mem_pool_decrease_need ) {
                        mem_pool_decrease(pool);
                        mem_pool_decrease_num = 0;
                }
                if (mem_pool_decrease_need > 1024*8) {
                        mem_pool_decrease_need = 1;
                }
        }
        return 0;
}

static void mem_pool_free2blk(mem_pool_t *pool, void *ptr)
{
        int key,i;
        int nbits;
        mem_blk_t *blk,*pos;
        key = (*((size_t*)ptr-1)) / sizeof(void*);
        blk = *((void**)((size_t*)ptr-1) -1);
        size_t blk_size;
        /*
         *free bitmaps
         */
        nbits = (((char*)ptr - sizeof(size_t) - sizeof(void*) -  (char*)blk->mm) / blk->mem_len)+1;
        bits_clr((unsigned char*)blk->data, nbits);
        blk->free_nums++;

        /*
         *if full, free all blk
         */
        for (i=0; i<1024;i++)
                if (pool->huge_blk[key])
                        assert(pool->huge_blk[key]->prev->next == NULL);
        if (blk->free_nums == 1) {
                /*1. delete it from NULL_list
                 * key = (mem_blk->mem_len - sizeof(size_t) - sizeof(void*)) / sizeof(void*);
                 */
                if (blk == pool->null_blk[key]) {
                        pool->null_blk[key] = blk->next;
                        if(blk->next)
                                blk->next->prev = blk->prev;
                } else if (NULL != blk->next){
                        blk->prev->next = blk->next;
                        blk->next->prev = blk->prev;
                } else {
                        pool->null_blk[key]->prev = blk->prev;
                        blk->prev->next = NULL;
                }
                /*2. insert it into huge_blk list
                 */
                if (pool->huge_blk[key]) {
                        blk->prev = pool->huge_blk[key]->prev;
                        blk->next = pool->huge_blk[key];
                        pool->huge_blk[key]->prev = blk;
                        pool->huge_blk[key] = blk;
                } else  {
                        pool->huge_blk[key] = blk;;
                        blk->prev = blk;
                        blk->next = NULL;
                }
                return ;
        }

        if (blk->free_nums == blk->tota_nums) {
#if 1
                blk_size = get_blk_size(pool->pagesize, sizeof(mem_blk_t),blk->mem_len);
                if(blk == pool->huge_blk[key]) {
                        assert(blk->prev->next == NULL);
                        pool->huge_blk[key] = blk->next;
                        if (blk->next) {
                                blk->next->prev = blk->prev;
                        }
                } else if (blk->next == NULL){
                        pool->huge_blk[key]->prev = blk->prev;
                        blk->prev->next = NULL;
                        assert(pool->huge_blk[key]->prev->next ==NULL);
                        
                } else {
                        blk->prev->next = blk->next;
                        blk->next->prev = blk->prev;
                        assert(pool->huge_blk[key]->prev->next ==NULL);
                }

                sy_spin_lock(&pool->glock);
                pool->total_free_hold -= (blk->tota_nums *blk->mem_len) ;
                pool->real_free_size += blk_size;
                pool->total_pool_memusage -= (blk->tota_nums*blk->mem_len);
                sy_spin_unlock(&pool->glock);
                free(blk);
                return;
#endif
        }

        /* 
         *resort the list 
         */
#if 1
        if (blk->next) {
                if (blk->free_nums > blk->next->free_nums) {
                        if (blk == pool->huge_blk[key]) {
                                if (blk->next->next) {
                                        /*
                                         * check this logic
                                         */
                                        blk->next->prev = pool->huge_blk[key]->prev;
                                        pool->huge_blk[key] = blk->next;
                                        blk->next->next->prev = blk;
                                        blk->next = blk->next->next;
                                        blk->prev = pool->huge_blk[key];
                                        pool->huge_blk[key]->next = blk;
                                } else {
                                        pool->huge_blk[key] = blk->next;
                                        pool->huge_blk[key]->prev= blk;

                                        blk->next->next = blk;
                                        blk->prev = blk->next;

                                        blk->next = NULL;
                                        return;
                                }
                        }
                        /*
                         */
                        pos = blk->next;
                        while(pos&&(blk->free_nums > pos->free_nums)) {
                                blk->prev->next = pos; 
                                pos->prev = blk->prev;

                                blk->prev = pos;
                                blk->next = pos->next;

                                if (pos->next) {
                                        pos->next->prev = blk;
                                } else {
                                        pool->huge_blk[key]->prev = blk;          
                                }
                                pos->next = blk;
                                pos = blk->next;
                        }
                        return;
                }
        }
#endif
}

int mem_pool_decrease(mem_pool_t *pool)
{
        mem_status_t status_free[1024];
        size_t free_mem=0,num = 0,tmp=0;
        int i=0,len;
        mem_ent_t *mem;
        memset(status_free, 0x00, sizeof(mem_status_t)*1024);
        memset(status_free, 0x00, sizeof(mem_status_t) * 1024);
        for (i = 1; i < 1024; i++) {
                status_free[i].num = pool->status_free[i];
                status_free[i].len = i;
        }
        qsort(&status_free[1], 1023, sizeof(mem_status_t), mem_status_cmp);
        i = 1;
        num = status_free[i].num;
        len = status_free[i].len;
        if ( num > 8196 ) {
                while( len && (i<1024) && (num > (status_free[1].num >>1))) {
                        sy_spin_lock(&global_mem_pool.lock[len]);
                        mem = pool->array[len];
                        if (mem) {
                                num = pool->status_free[len];
                                tmp = num;
                                //mem = pool->array[(len - sizeof(size_t))>> 3];
                                while((tmp) && mem) {
                                        pool->array[len]=mem->next;
                                        mem_pool_free2blk(pool, mem);
                                        free_mem = free_mem+(len<<3)+sizeof(size_t);
                                        mem = pool->array[len];
                                        pool->status_free[len]--;
                                        tmp --;
                                }
                        }
                        i++;
                        num = status_free[i].num;
                        sy_spin_unlock(&global_mem_pool.lock[len]);
                        len = status_free[i].len;
                }
        } else {
                mem_pool_decrease_need+=(num>>3);
        }
        return 0;
}
#if 0
int ymalloc(void **ptr, size_t size)
{
        size_t _size;
        int ret;
        if (size == 0)
                *ptr = NULL;
	
	mem_pool_static_num++;
	if (mem_pool_static_num > 102400) {
		mem_pool_static_num = 0;
                mem_pool_dump(&global_mem_pool);
	}

        if (size > MAX_MEM_SUPPORT) {
                ret = mem_pool_bigmem_alloc(&global_mem_pool, ptr, size);
                goto err_max_nomem;
        }
        if (size < MIN_ALLOC_SIZE) {
                size = MIN_ALLOC_SIZE;
        }
        else if(size & (ALLOC_ALGIN_SIZE -1)) {
                size = MEMSIZE_ALGIN(size) + sizeof(void*);
        }
        _size = size;
        if ( global_mem_pool_inuse == 0 ) {
                sy_spin_lock(&global_mem_pool.glock);
                if (global_mem_pool_inuse == 0) {
                        ret = mem_pool_init(&global_mem_pool,4096);
                        if (ret)
                                printf("error inited\n");
                        global_mem_pool_inuse = 1;
                }
                sy_spin_unlock(&global_mem_pool.glock);
        }
        ret = mem_pool_alloc(&global_mem_pool,ptr,_size);
        if (ret)
                printf("alloc error\n");
        return 0;
err_lock:
        sy_spin_unlock(&global_mem_pool.glock);
err_mem_pool:
err_max_nomem:
        return ret;
}
int yfree(void **ptr)
{
	mem_pool_static_num++;
        if (*ptr != NULL) {
                if (*(((size_t*)(*ptr)) - 1) > MAX_MEM_SUPPORT) {
                        mem_pool_bigmem_free(&global_mem_pool, ptr);
                        *ptr = NULL;
                        goto out;
                }
                mem_pool_free(&global_mem_pool,*ptr,*((((size_t*)(*ptr)) - 1)));
        } else {
        }
 out:
	if (mem_pool_static_num > 102400) {
	}
        return 0;
}

int yrealloc(void **_ptr, size_t size, size_t newsize)
{
        int ret, i;
        void *ptr=NULL;

        if (*_ptr == NULL && size == 0) /*malloc*/ {
                ret = ymalloc(&ptr, newsize);
                if (ret)
                        GOTO(err_ret, ret);

    //            memset(ptr, 0x0, newsize);

                *_ptr = ptr;
                if (*_ptr == 0)
                        printf("anothre error\n");
                return 0;
        }
        if (*_ptr && size == 0);

        if (newsize == 0) {
                yfree(_ptr);
                *_ptr = 0;
                return 0;
        }

        if (newsize <= size) {
                ptr = *_ptr;
                memset(ptr + newsize, 0x0, size - newsize);
        }

        ret = ENOMEM;
        for (i = 0; i < 3; i++) {
                ret =  ymalloc(&ptr, newsize);
                if (ptr != NULL)
                        goto out;
        }

        goto err_ret;
out:
        
        if (newsize >= size) {
                memcpy(ptr, *_ptr, size);
                memset(ptr + size, 0x0, newsize - size);
        }
        else {
                memcpy(ptr, *_ptr, newsize);
        }
        yfree(_ptr);
        *_ptr = ptr;
        return 0;
err_ret:
        return ret;
}
#endif
#ifdef MEM_POOL_TEST
void *thread_fn(void *arg)
{
        int  i, size1,size2;
        void **mem = malloc(100*sizeof(void*));
        while(1) {

        for (i = 0; i <100; i++) {
                size1 = random() % 8169+1;
                size2 = random() % 8169+1;
                ymalloc(mem+i, size1);
                if (*(mem+i)== 0)
                        printf("error\n");

#if 1
                yrealloc(mem+i, size1, size2);
                yrealloc(mem+i, size2, 0);
                if (*(mem+i) != 0)
                        printf("error\n");
                yrealloc(mem+i, 0, size1);
                memset(*(mem+i),0,size1);
                yrealloc(mem+i, size1, 0);
#endif
                yfree(mem+i);
        }
        printf("i stillwork\n");
        }

        return 0;
}
int main(int argc, char *argv[])
{
        int i = atoi(argv[1]);
        int j = atoi(argv[2]);
        int k,l,rand, rand1,rand2;
        void *tmp;
        time_t start,end;
        mem_pool_init(&global_mem_pool,4096);
        pthread_t array[10];
        printf("sizeof(void*) %d\n", sizeof(void*));
        sleep(1);
        for (i = 0 ; i <1; i++)
                pthread_create(&array[i], NULL,thread_fn ,NULL);
        while(i--) {
                i++;
                usleep(100);
        }
        return 0; 
}
#endif

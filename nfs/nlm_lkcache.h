/*
 * =====================================================================================
 *
 *       Filename:  nlm_lkcache.h
 *
 *    Description:  nlm_impl
 *
 *        Version:  1.0
 *        Created:  04/08/2011 10:06:18 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  lvwengjian
 *        Company:  MDS
 *
 * =====================================================================================
 */

#ifndef __NLM_LKCACHE_H_
#define __NLM_LKCACHE_H_
#include "sdfs_lib.h"
#include "ylib.h"
#include "dbg.h"
#include "nlm_state_machine.h"

typedef struct nlmlock_s {
        nfs_fh3                 fh;
        struct ynetobj          owner;
        uint64_t                l_offset;
        uint64_t                l_len;
        bool_t                  exclusive; /*this may be umimplement */
        int32_t                 svid;      /*the process hold the locker*/
        struct nlmlock_s        *n;
        struct nlmlock_s        *p;
} nlmlock_t;

typedef enum {
        NLM_READ_LOCK = 0,
        NLM_WRITE_LOCK,
}nlmlock_type_t;

typedef struct nlmlk_cache_s {
        hashtable_t hashtb;
        pthread_mutex_t lock;
} nlmlk_cache_t;

extern nlmlk_cache_t nlmlk_cache;
int lockargs2nlmlock(nlm_lockargs *lockargs, nlmlock_t *nlmlock, job_t *job);
int testargs2nlmlock(nlm_testargs *testargs,  nlmlock_t *nlmlock, job_t *job);
int nlmlock2testres(nlm_testres *testres, nlmlock_t *nlmlock);
int unlockargs2nlmlock(nlm_unlockargs *unlockargs, nlmlock_t *nlmlock, job_t *job);
int cancargs2nlmlock(nlm_cancargs *cancargs, nlmlock_t *nlmlock, job_t *job);

uint32_t nlm_hash(const void *d1);
int nlmlk_file_compare(const void *d1, const void *d2);

/*if colison, return 1
*there should be a good implement. there is just for simple
*/
int nlmlk_colison_cmp(void *d1, void *d2);
int nlmlk_cmp(void *d1, void*d2);

/* while insert, you should use other mechism to sure is there has the existed lock
*/
int free_nlmlock(nlmlock_t *lock);
int nlmlk_cache_init(nlmlk_cache_t *nlmlk_cache);
int nlmlk_cache_lock(nlmlk_cache_t *nlmlk_cache);
int nlmlk_cache_unlock(nlmlk_cache_t *nlmlk_cache);
int nlmlk_cache_insert(nlmlk_cache_t *nlmlk_cache, nlmlock_t *nlmlock);
int nlmlk_cache_delete(nlmlk_cache_t *nlmlk_cache, nlmlock_t *nlmlock, nlmlock_t **retval);
int nlmlk_cache_clear(nlmlk_cache_t *nlmlk_cache, nlmlock_t *nlmlock, int *num);
int nlmlk_cache_colison(nlmlk_cache_t *nlmlk_cache, nlmlock_t *nlmlock, nlmlock_t **colision);
int nlmlk_cache_find(nlmlk_cache_t *nlmlk_cache, nlmlock_t *nlmlock, nlmlock_t **retval);
int nlmlk_del_host(nlmlk_cache_t *nlmlk_cache, unsigned char *host, int state);
int nlmlk_cache_destory();
#endif

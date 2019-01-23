/*
 * =====================================================================================
 *
 *       Filename:  nlm_lkcache.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  04/08/2011 10:06:02 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *        Company:
 *
 * =====================================================================================
 */
#include "nlm_lkcache.h"
#include "net_table.h"
extern int job2host(job_t *job, char *host, int type);
nlmlk_cache_t nlmlk_cache;

int testargs2nlmlock(nlm_testargs *testargs,  nlmlock_t *nlmlock, job_t *job)
{
	    char host[64];
	    memset(host, 0x0, 64);
        job2host(job, host, REMOTE_HOST);
        nlmlock->fh.len = testargs->alock.fh.len;
        ymalloc((void**)&nlmlock->fh.val, nlmlock->fh.len);
        YASSERT(nlmlock->fh.val != 0);
        memcpy(nlmlock->fh.val, testargs->alock.fh.val, nlmlock->fh.len);
        nlmlock->l_len     = testargs->alock.l_len;
        nlmlock->l_offset  = testargs->alock.l_offset;
        nlmlock->exclusive = testargs->exclusive;

        ymalloc((void**)&nlmlock->owner.data, strlen(host));
        YASSERT(NULL!=nlmlock->owner.data);
        memcpy(nlmlock->owner.data, host,strlen(host));
	    nlmlock->owner.len = strlen(host);

        nlmlock->svid = testargs->alock.svid;
        return 0;
}

int lockargs2nlmlock(nlm_lockargs *lockargs, nlmlock_t *nlmlock, job_t *job)
{
    	char host[64];
    	memset(host, 0x0, 64);
        job2host(job, host, REMOTE_HOST);
        nlmlock->fh.len = lockargs->alock.fh.len;
        ymalloc((void**)&nlmlock->fh.val, nlmlock->fh.len);
        YASSERT(nlmlock->fh.val != 0);
        memcpy(nlmlock->fh.val, lockargs->alock.fh.val, nlmlock->fh.len);
        nlmlock->l_len     = lockargs->alock.l_len;
        nlmlock->l_offset  = lockargs->alock.l_offset;
        nlmlock->exclusive = lockargs->exclusive;
#if 0   /*use ip*/
        ymalloc((void**)&nlmlock->owner.data, strlen(host));
        YASSERT(NULL!=nlmlock->owner.data);
        memcpy(nlmlock->owner.data, host,strlen(host));
	nlmlock->owner.len = strlen(host);
#else   /*use host*/
        ymalloc((void**)&nlmlock->owner.data, lockargs->alock.len);
        YASSERT(NULL!=nlmlock->owner.data);
        memcpy(nlmlock->owner.data, lockargs->alock.caller, lockargs->alock.len);
	    nlmlock->owner.len = lockargs->alock.len;
#endif
        nlmlock->svid = lockargs->alock.svid;
#if 0
	memset(strbuf, 0x0, 1024);
	memcpy(strbuf, lockargs->alock.caller, lockargs->alock.len);
	DINFO("callor %s, len %d\n", strbuf, lockargs->alock.len);
	memset(strbuf, 0x0, 1024);
	memcpy(strbuf, lockargs->cookies.data, lockargs->cookies.len);
	DINFO("netobf %s  len %d\n",  strbuf, lockargs->cookies.len);
#endif
        return 0;
}

int unlockargs2nlmlock(nlm_unlockargs *unlockargs, nlmlock_t *nlmlock, job_t *job)
{
        char host[64];
        char strbuf[1024];

        memset(strbuf, 0x0, 1024);
        memset(host, 0x0, 64);
        job2host(job, host, REMOTE_HOST);
        memcpy(strbuf, unlockargs->alock.caller, unlockargs->alock.len);
        DINFO("callor %s, len %d\n", strbuf, unlockargs->alock.len);


        nlmlock->fh.len = unlockargs->alock.fh.len;
        ymalloc((void**)&nlmlock->fh.val, nlmlock->fh.len);
        YASSERT(nlmlock->fh.val != 0);
        memcpy(nlmlock->fh.val, unlockargs->alock.fh.val, nlmlock->fh.len);
        nlmlock->l_len     = unlockargs->alock.l_len;
        nlmlock->l_offset  = unlockargs->alock.l_offset;
#if 0	/*ip*/
        ymalloc((void**)&nlmlock->owner.data, strlen(host));
        YASSERT(NULL!=nlmlock->owner.data);
        memcpy(nlmlock->owner.data, host,strlen(host));
	nlmlock->owner.len = strlen(host);
#else
        ymalloc((void**)&nlmlock->owner.data, unlockargs->alock.len);
        YASSERT(NULL!=nlmlock->owner.data);
        memcpy(nlmlock->owner.data, unlockargs->alock.caller,unlockargs->alock.len);
	    nlmlock->owner.len = unlockargs->alock.len;
#endif
        nlmlock->svid = unlockargs->alock.svid;
        return 0;
}

int cancargs2nlmlock(nlm_cancargs *cancargs, nlmlock_t *nlmlock, job_t *job)
{
        char host[64];
        nlm_lock *alock = &cancargs->alock;

        memset(host, 0x0, 64);
        job2host(job, host, REMOTE_HOST);
        nlmlock->fh.len = alock->fh.len;
        ymalloc((void **)&nlmlock->fh.val, nlmlock->fh.len);
        YASSERT(NULL != nlmlock->fh.val);
        memcpy(nlmlock->fh.val, alock->fh.val, nlmlock->fh.len);

        nlmlock->owner.len = alock->len;
        ymalloc((void **)&nlmlock->owner.data, nlmlock->owner.len);
        YASSERT(NULL != nlmlock->owner.data);
        memcpy(nlmlock->owner.data, alock->caller, nlmlock->owner.len);

        nlmlock->l_offset = alock->l_offset;
        nlmlock->l_len = alock->l_len;
        nlmlock->svid = alock->svid;
        return 0;
}

int nlmlock2testres(nlm_testres *testres, nlmlock_t *nlmlock)
{
        testres->test_stat.nlm_testrply_u.holder.exclusive = nlmlock->exclusive;
        testres->test_stat.nlm_testrply_u.holder.l_len     = nlmlock->l_len;
        testres->test_stat.nlm_testrply_u.holder.l_offset  = nlmlock->l_offset;
        testres->test_stat.nlm_testrply_u.holder.svid      = 111111; /*unimplement*/
        testres->test_stat.nlm_testrply_u.holder.oh.len    = nlmlock->owner.len;
        ymalloc((void**)&testres->test_stat.nlm_testrply_u.holder.oh.data, nlmlock->owner.len);
        YASSERT(testres->test_stat.nlm_testrply_u.holder.oh.data != NULL);
        memcpy(testres->test_stat.nlm_testrply_u.holder.oh.data, nlmlock->owner.data, nlmlock->owner.len);
        return 0;
}

int nlmlk_file_compare(const void *d1, const void *d2)
{
        nlmlock_t *v1, *v2;
        v1 = (nlmlock_t*)d1;
        v2 = (nlmlock_t*)d2;

        if ( v1->fh.len == v2->fh.len &&
             (0==memcmp(v1->fh.val,v2->fh.val,v1->fh.len))
            )
                return 0;
        return 1;
}

uint32_t nlm_hash(const void *d1)
{
        nlmlock_t *v1;
        v1 = (nlmlock_t*)d1;
        return hash_mem(v1->fh.val, v1->fh.len);
}

int nlmlk_cache_init(nlmlk_cache_t *nlmlk_cache)
{
        nlmlk_cache->hashtb = hash_create_table(nlmlk_file_compare, nlm_hash, "nlm_lock");
        if (!nlmlk_cache->hashtb)
                return ENOMEM;
        pthread_mutex_init(&nlmlk_cache->lock, NULL);
        return 0;
}

/*
 * while insert, you should use other mechism to sure is there has the existed lock
 */
int nlmlk_cache_insert(nlmlk_cache_t *nlmlk_cache, nlmlock_t *nlmlock)
{
        int ret = 0;
        nlmlock_t *_nlmlock;
        _nlmlock = hash_table_find(nlmlk_cache->hashtb, (void*)nlmlock);
        if (_nlmlock) {
                _nlmlock->p->n = nlmlock;
                nlmlock->n     = NULL;
                nlmlock->p     = _nlmlock->p;
                _nlmlock->p    = nlmlock;
        } else {
                nlmlock->p = nlmlock;
                nlmlock->n = NULL;
                ret =  hash_table_insert(nlmlk_cache->hashtb, (void*)nlmlock, (void*)nlmlock, 0);
                if (ret)
                        GOTO(err_ret, ret);
        }
        return 0;
err_ret:
        return ret;
}

/*
* if the range of file lock is overlap return 1, or return 0.
*/
static int __nlmlk_rang_cmp(nlmlock_t *v1, nlmlock_t *v2)
{
        uint64_t start1, end1, start2, end2;

        start1 = v1->l_offset;
        start2 = v2->l_offset;
        end1 = start1 + v1->l_len;
        end2 = start2 + v2->l_len;

        // if l_len = 0, the file lock the file start from l_offset
        //[start1, start2, max], [start2, start1, max]
        if (((start1 == end1) && (start1 <= start2)) ||
            ((start2 == end2) && (start2 <= start1)))
                return 1;

        //no overlap [start1, end1] < [start2, end2] or
        //           [start2, end2] < [start1, end1]
        if ((end1 <= start2) || (start1 >= end2))
                return 0;
        else
                return 1;
}

/*
 *if colision, return 1
 */
int nlmlk_colision_cmp(nlmlock_t *oldlock, nlmlock_t *newlock)
{
        // if no overlap, return no colision
        if (0 == __nlmlk_rang_cmp(oldlock, newlock))
                return 0;

        //at least one write lock return colision
        if (NLM_WRITE_LOCK == oldlock->exclusive ||
            NLM_WRITE_LOCK == newlock->exclusive)
                return 1;

        /* both read lock, svid the same, return colision */
        if (oldlock->svid == newlock->svid)
                return 1;

        return 0;
}

int nlmlk_cmp(void *d1, void*d2)
{
        nlmlock_t *v1, *v2;
        v1 = (nlmlock_t*)d1;
        v2 = (nlmlock_t*)d2;

        if (v1->svid != v2->svid)
                return 1;

        if (v2->l_offset != v1->l_offset)
                return 1;

        if (v2->l_len == v1->l_len)
                return 0;

        if ((0 != v2->l_offset) && (0 == v2->l_len))
                return 0;

        return 1;
}

int nlmlk_cache_delete(nlmlk_cache_t *nlmlk_cache, nlmlock_t *nlmlock, nlmlock_t **retval)
{
        int ret = 0;
        nlmlock_t *_nlmlock,*pos,*next;

        (*retval) = NULL;

        _nlmlock = hash_table_find(nlmlk_cache->hashtb, (void*)nlmlock);
        if (_nlmlock) {
                if (!nlmlk_cmp(_nlmlock, nlmlock)) {

                        ret = hash_table_remove(nlmlk_cache->hashtb, (void*)nlmlock, (void**)retval);
                        if (ret)
                                GOTO(err_ret, ret);
                        YASSERT((*retval)== _nlmlock);

                        pos = _nlmlock->n;
                        if (pos) {
                                pos->p = _nlmlock->p;
                                /* insert it into hashtb
                                */
                                ret =  hash_table_insert(nlmlk_cache->hashtb, (void*)pos, (void*)pos, 0);
                                if (ret)
                                        GOTO(err_ret, ret);
                                goto ok;
                        }
                        goto ok;
                }
                pos  = _nlmlock;
                next = _nlmlock->n;
                while(next) {
                        if (!nlmlk_cmp(next, nlmlock)) {

                                 pos->n = next->n;
                                 if (next->n) {
                                        next->n->p = pos;
                                 } else {
                                         _nlmlock->p = pos;
                                 }
                                 (*retval) = next;
                                 goto ok;
                        }
                        pos = next;
                        next = next->n;
                }
                if (next == NULL) {
                        ret = ENOENT;
                        GOTO(err_ret, ret);
                }
        } else  {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }
        return ENOENT;
ok:
        /*for error check
         */
        (*retval)->n = NULL;
        (*retval)->p = NULL;
        return 0;
err_ret:
        return ret;
}

//the svid same and the lock section have overlap, lock type use the newlock type
static void __update_nlmlk_info(nlmlock_t *oldlock, nlmlock_t *newlock)
{
        uint64_t oldstart = oldlock->l_offset;
        uint64_t newstart = newlock->l_offset;
        uint64_t oldlen = oldlock->l_len;
        uint64_t newlen = newlock->l_len;
        uint64_t oldend = oldstart + oldlen;
        uint64_t newend = newstart + newlen;

        //the same lock type
        if (oldlock->exclusive == newlock->exclusive) {
                if (oldstart > newstart)
                        oldstart = newstart;
                if (oldend < newend)
                        oldend = newend;

                if ((0 == oldlen) || (0 == newlen))
                        oldlen = 0;
                else
                        oldlen = oldend - oldstart;

                oldlock->l_offset = oldstart;
                oldlock->l_len = oldlen;
        }
        //else implement later
}

int nlmlk_cache_colison(nlmlk_cache_t *nlmlk_cache, nlmlock_t *newlock, nlmlock_t **colision)
{
        nlmlock_t *oldlock;

        oldlock = hash_table_find(nlmlk_cache->hashtb, (void*)newlock);
        if (!oldlock)
                goto nocolision;
        while(oldlock) {
                if (nlmlk_colision_cmp(oldlock, newlock))
                        goto colision;
                oldlock = oldlock->n;
        }
nocolision:
        (*colision) = NULL;
        return 0;
colision:
        //the same process, update cach lock info
        if (oldlock->svid == newlock->svid) {
                __update_nlmlk_info(oldlock, newlock);
        }
        (*colision) = oldlock;
        return 0;
}

int nlmlk_cache_rfind(nlmlk_cache_t *nlmlk_cache, nlmlock_t *nlmlock, nlmlock_t **retval)
{
        nlmlock_t *_nlmlock;
        (*retval) = NULL;

        _nlmlock = hash_table_find(nlmlk_cache->hashtb, (void*)nlmlock);
        (*retval) = _nlmlock;
        return 0;
}

int nlmlk_cache_clear(nlmlk_cache_t *nlmlk_cache, nlmlock_t *nlmlock, int *num)
{
        /*while the app of clients exit, it will send unlock by l_len 0, l_offset 0
         * so , we do some clean
         */
        int ret;
        nlmlock_t *head,*n,*next,*retval;

        *num = 0;
        head = hash_table_find(nlmlk_cache->hashtb, (void*)nlmlock);
        if (head) {
                n = head->n;
                while(n) {
                        next = n->n;
                        if ((n->svid == nlmlock->svid) &&
                            (n->l_offset >= nlmlock->l_offset)) {
                                n->p->n = n->n;
                                if (n->n) {
                                        n->n->p = n->p;
                                } else
                                        head->p = n->p;
                                *num = *num+1;
                                free_nlmlock(n);
                        }
                        n = next;
                }
                /*
                 * now process head
                 */
                if (head->svid == nlmlock->svid) {
                        next = head->n;
                        if (next)
                                next->p = head->p;
                        ret = hash_table_remove(nlmlk_cache->hashtb, (void*)head, (void**)&retval);
                        if (ret)
                                GOTO(err_ret, ret);
                        free_nlmlock(head);
                        *num = *num+1;
                        if (next)
                                ret =  hash_table_insert(nlmlk_cache->hashtb, (void*)next, (void*)next, 0);
                        YASSERT(ret == 0);
                }

        } else {

        }
        return 0;
err_ret:
        return ret;
}

int nlmlk_cache_find(nlmlk_cache_t *nlmlk_cache, nlmlock_t *nlmlock, nlmlock_t **retval)
{
        int ret = 0;
        nlmlock_t *_nlmlock;

        (*retval) = NULL;

        _nlmlock = hash_table_find(nlmlk_cache->hashtb, (void*)nlmlock);
        while(_nlmlock) {
                if (!nlmlk_cmp(_nlmlock, nlmlock)) {
                        (*retval) = _nlmlock;
                        goto ok;
                }
		_nlmlock = _nlmlock->n;
        }
        if (_nlmlock == NULL) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }
ok:
        return 0;
err_ret:
        return ret;
}

struct host_arg_s {
        char host[64];
        hashtable_t hashtb;
        nlmlock_t *freelock;
        int state;
};

int free_nlmlock(nlmlock_t *lock)
{
        yfree((void**)&lock->fh.val);
        yfree((void**)&lock->owner.data);
        yfree((void**)&lock);
        return 0;
}

static void nlm_del_host_fn(void *arg, void *_head)
{
        struct host_arg_s *host_arg;
        nlmlock_t *n,*f;
        nlmlock_t *head;
        char owner[64];
        int ret;

        head = _head;
        host_arg = arg;
        n = head->n;
        while(n) {
                f = n->n;
                memset(owner, 0x0, 64);
                memcpy(owner, n->owner.data, n->owner.len);
                DINFO("del_host_fn lock ower %s--while host %s\n",owner, host_arg->host);
                if (0 == memcpy(n->owner.data, host_arg->host, strlen(host_arg->host))) {
                        f = n->n;
                        n->p->n = n->n;
                        if (n->n)
                                n->n->p = n->p;
                        else {
                                head->p = head;
                        }
                        free_nlmlock(n);
                }
                n = f;
        }

        /*
         *process head
         */
	memset(owner, 0x0, 64);
	memcpy(owner, head->owner.data, head->owner.len);
	DINFO("process HEAD owner %s, host %s\n", owner, host_arg->host);
        if ( 0 == memcpy(head->owner.data, host_arg->host, strlen(host_arg->host))) {
                /*  free_nlmlock(f);
                 *  not free here, we put it into the freelock
                 */
		n = head->n;
                if (host_arg->freelock) {
                        host_arg->freelock->p->n = head;
                        head->p = host_arg->freelock->p;
                        host_arg->freelock->p = head;
                        head->n = NULL;
                } else {
                        host_arg->freelock = head;
                        head->n = head;
                        head->p = head;
                }
        }
        if (n) { /*new head*/
		DINFO("insert into hashtb");
                ret =  hash_table_insert(host_arg->hashtb, (void*)n, (void*)n, 0);
                if (ret)
                        YASSERT(0);
        }
}

int nlmlk_del_host(nlmlk_cache_t *nlmlk_cache, unsigned char *host, int state)
{
        struct host_arg_s host_arg;
        host_arg.state = state;
        memset(host_arg.host, 0x0, 64);
        memcpy(host_arg.host, host,strlen((char*)host));
        host_arg.freelock = NULL;
        nlmlock_t *lock;

        host_arg.hashtb = hash_create_table(nlmlk_file_compare, nlm_hash, "nlm_lock");
        YASSERT(host_arg.hashtb);

        hash_iterate_table_entries(nlmlk_cache->hashtb, nlm_del_host_fn, &host_arg);
        hash_destroy_table(nlmlk_cache->hashtb, NULL);
        /*
         * free all lock
         */
        while(host_arg.freelock) {
                lock  = host_arg.freelock;
                host_arg.freelock = host_arg.freelock->n;
                free_nlmlock(lock);
        }
        nlmlk_cache->hashtb = host_arg.hashtb;
        return 0;
}

int nlmlk_cache_lock(nlmlk_cache_t *nlmlk_cache)
{
        pthread_mutex_lock(&nlmlk_cache->lock);
        return 0;
}

int nlmlk_cache_unlock(nlmlk_cache_t *nlmlk_cache)
{
        pthread_mutex_unlock(&nlmlk_cache->lock);
        return 0;
}

int nlmlk_cache_destory()
{
        return 0;
}

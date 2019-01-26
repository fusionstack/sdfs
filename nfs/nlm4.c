/*
 * =====================================================================================
 *
 *       Filename:  nlm_state_machine.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  04/07/2011 10:32:20 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *        Company:
 *
 * =====================================================================================
 */



#include <aio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <rpc/rpc.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>


#define DBG_SUBSYS S_YNFS

#include "yfs_conf.h"
#include "ynfs_conf.h"
#include "aiocb.h"
#include "attr.h"
#include "error.h"
#include "nlm_job_context.h"
#include "nlm_lkcache.h"
#include "net_global.h"
#include "nfs_conf.h"
#include "nlm_events.h"
#include "nlm_state_machine.h"
#include "nlm_nsm.h"
#include "../sock/sock_tcp.h"
#include "sunrpc_proto.h"
#include "sunrpc_reply.h"
#include "sdfs_lib.h"
#include "xdr_nlm.h"
#include "configure.h"
#include "sdfs_conf.h"
#include "nlm_nsm.h"
#include "nlm_async.h"
#include "core.h"
#include "nfs_events.h"
#include "yfs_limit.h"
#include "sdfs_id.h"
#include "dbg.h"

#if 1
static int __grace_period__  = 0;
#endif

#define __FREE_ARGS(__func__, __request__)              \
        do {                                            \
                xdr_t xdr;                              \
                                                        \
                xdr.op = __XDR_FREE;                    \
                xdr.buf = __request__;                  \
                xdr_##__func__##args(&xdr, args);       \
        } while (0)

static int __nlm4_null_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                           uid_t uid, gid_t gid, nfsarg_t *arg, buffer_t *buf)
{
        int ret;

        (void) req;
        (void) uid;
        (void) gid;
        (void) arg;
        (void) buf;

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK, NULL, NULL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

typedef struct {
        int uppid;
        int ownerlen;
        int callerlen;
        char buf[0];
} nlm_ext_t;

static void __nlock2slock(int exclusive, struct nlm_lock *alock, sdfs_lock_t *slock)
{
        nlm_ext_t *ext;
        
        slock->sid = alock->svid;
        slock->owner  = crc32_sum(alock->oh.data, alock->oh.len);
        //slock->owner = 0;
        slock->start = alock->l_offset;
        slock->length = alock->l_len;
        slock->type = exclusive ? SDFS_WRLOCK : SDFS_RDLOCK;

        ext = (void *)slock->opaque;
        memset(ext, 0x0, sizeof(*ext));
        ext->uppid = alock->svid;
        ext->ownerlen = alock->oh.len;
        ext->callerlen = strlen(alock->caller) + 1;
        
        slock->opaquelen = sizeof(*ext) + ext->ownerlen + ext->callerlen;
        memcpy(ext->buf, alock->oh.data, alock->oh.len);
        memcpy(ext->buf + alock->oh.len, alock->caller, ext->callerlen);
}

static void __slock2holder(const sdfs_lock_t *slock, nlm4_holder *holder)
{
        nlm_ext_t *ext;

        ext = (void *)slock->opaque;
        holder->exclusive = (slock->type == SDFS_RDLOCK) ? 1 : 0;
        holder->l_len = slock->length ;
        holder->l_offset = slock->start;
        holder->svid = slock->sid;
        holder->oh.len = ext->ownerlen;
        holder->oh.data = (void *)ext->buf;
}

#define MAX_LOCK_LEN (sizeof(sdfs_lock_t) + sizeof(nlm_ext_t) + 2048)

static int __nlm4_test_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                           uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        nlm_testargs *args = &_arg->testargs;
        nlm_testres res;
        fileid_t *fileid;
        sdfs_lock_t *lock, *owner;
        char cookie[MAX_BUF_LEN];
        char _lock[MAX_LOCK_LEN], _owner[MAX_LOCK_LEN];

        YASSERT(args->cookies.len < MAX_BUF_LEN);

        (void) gid;
        (void) uid;

        fileid = (fileid_t *)args->alock.fh.val;

        lock = (void *)_lock;
        owner = (void *)_owner;

        DINFO("NLM_TEST fileid"FID_FORMAT"offset %llu len %llu exclusive %u svid %u from %s\n",
              FID_ARG(fileid), (LLU)args->alock.l_offset, (LLU)args->alock.l_len,
              args->exclusive, args->alock.svid, args->alock.caller);

        __nlock2slock(args->exclusive, &args->alock, lock);

        memcpy(owner, lock, SDFS_LOCK_SIZE(lock));
        ret = sdfs_getlock(fileid, owner);
        if (ret) {
                if (ret == ENOENT) {
                        res.test_stat.status = NLM4_GRANTED;
                        memset(&res.test_stat.nlm_testrply_u.holder,
                               0x0, sizeof(res.test_stat.nlm_testrply_u.holder));
                        goto out;
                } else
                        GOTO(err_rep, ret);
        }

        if (owner->owner == lock->owner && owner->sid == lock->sid) {
                res.test_stat.status = NLM4_GRANTED;
                __slock2holder(lock, &res.test_stat.nlm_testrply_u.holder);
        } else {
                res.test_stat.status = NLM4_DENIED;
                __slock2holder(owner, &res.test_stat.nlm_testrply_u.holder);
        }

out:
        //cookies
        res.cookies.len = args->cookies.len;
        res.cookies.data = (void *)cookie;
        memcpy(res.cookies.data, args->cookies.data, args->cookies.len);
        
        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_nlm_testres);
        if (ret)
                GOTO(err_ret, ret);
        
        __FREE_ARGS(nlm_test, buf);

        return 0;
err_rep:
        res.test_stat.status = NLM4_DENIED;
        res.cookies.len = args->cookies.len;
        res.cookies.data = (void *)cookie;
        memcpy(res.cookies.data, args->cookies.data, args->cookies.len);
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_nlm_testres);
err_ret:
        __FREE_ARGS(nlm_test, buf);
        return ret;
}

static int __nlm4_lock_grace(const fileid_t *fileid, const sdfs_lock_t *lock)
{
        int ret;
        sdfs_lock_t *owner;
        char _owner[MAX_LOCK_LEN];

        owner = (void *)_owner;
        memcpy(owner, lock, SDFS_LOCK_SIZE(lock));
        ret = sdfs_getlock(fileid, owner);
        if (ret) {
                GOTO(err_ret, ret);
        }

        if (owner->owner != lock->owner || owner->sid != lock->sid) {
                ret = EWOULDBLOCK;
                GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}

static int __nlm4_lock_wait(const fileid_t *fileid, const sdfs_lock_t *lock)
{
        int ret, cancel;
        
        cancel = 0;
        ret = nlm4_async_reg(fileid, lock, &cancel);
        if (ret)
                GOTO(err_ret, ret);

        while (1) {
                DINFO("try lock "FID_FORMAT"\n", FID_ARG(fileid));
                
                if (cancel) {
                        ret = EAGAIN;
                        DINFO("lock "FID_FORMAT" canceled\n", FID_ARG(fileid));
                        GOTO(err_reg, ret);
                }

                ret = sdfs_setlock(fileid, lock);
                if (ret) {
                        if (ret == EWOULDBLOCK) {
                                schedule_sleep("nlm", 100 * 1000);
                                continue;
                        } else {
                                GOTO(err_reg, ret);
                        }
                } else {
                        DINFO("lock "FID_FORMAT" granted\n", FID_ARG(fileid));
                        break;
                }
        }

        ret = nlm4_async_unreg(fileid, lock, &cancel);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_reg:
        nlm4_async_unreg(fileid, lock, &cancel);
err_ret:
        return ret;
}
        

static int __nlm4_lock_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                           uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        nlm_lockargs *args = &_arg->lockargs;
        nlm_res res;
        fileid_t *fileid;
        sdfs_lock_t *lock;
        char _lock[MAX_LOCK_LEN];
        char cookie[MAX_BUF_LEN];

        YASSERT(args->cookies.len < MAX_BUF_LEN);

        (void) gid;
        (void) uid;
        
        fileid = (fileid_t *)args->alock.fh.val;

        DINFO("NLM_LOCK_SVC fileid"FID_FORMAT"offset %llu len %llu exclusive %u svid %u\n",
              FID_ARG(fileid), (LLU)args->alock.l_offset, (LLU)args->alock.l_len,
              args->exclusive, args->alock.svid);

        lock = (void *)_lock;
        __nlock2slock(args->exclusive, &args->alock, lock);

        if (unlikely(__grace_period__)) {
                if (args->reclaim == 0) {
                        ret = EPERM;
                        res.stat = NLM4_DENIED_GRACE_PERIOD;
                        GOTO(err_ret, ret);
                }
                
                ret = __nlm4_lock_grace(fileid, lock);
                if (ret) {
                        res.stat = NLM4_DENIED_GRACE_PERIOD;
                        GOTO(err_rep, ret);
                }
        } else {
                ret = sdfs_setlock(fileid, lock);
                if (ret) {
                        if (ret == EWOULDBLOCK) {
                                DWARN("NLM_LOCK_SVC blocked fileid"FID_FORMAT"offset %llu len %llu exclusive %u svid %u\n",
                                      FID_ARG(fileid), (LLU)args->alock.l_offset, (LLU)args->alock.l_len,
                                      args->exclusive, args->alock.svid);
                                res.stat = NLM4_BLOCKED;
                                res.cookies.len = args->cookies.len;
                                res.cookies.data = (void *)cookie;
                                memcpy(res.cookies.data, args->cookies.data, args->cookies.len);
                                ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                                                   &res, (xdr_ret_t)xdr_nlm_res);
                                if (ret)
                                        GOTO(err_ret, ret);

                                ret = __nlm4_lock_wait(fileid, lock);
                                if (ret)
                                        GOTO(err_ret, ret);
                        } else
                                GOTO(err_ret, ret);

                }
                
                res.stat = NLM4_GRANTED;
        }
        
        /*cookies
         */
        res.cookies.len = args->cookies.len;
        res.cookies.data = (void *)cookie;
        memcpy(res.cookies.data, args->cookies.data, args->cookies.len);
 
        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_nlm_res);
        if (ret)
                GOTO(err_ret, ret);
        
        __FREE_ARGS(nlm_lock, buf);

        return 0;
err_rep:
        res.cookies.len = args->cookies.len;
        res.cookies.data = (void *)cookie;
        memcpy(res.cookies.data, args->cookies.data, args->cookies.len);
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_nlm_res);
err_ret:
        __FREE_ARGS(nlm_lock, buf);
        return ret;
}

static int __nlm4_unlock_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                             uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        nlm_unlockargs *args = &_arg->unlockargs;
        nlm_res res;
        fileid_t *fileid;
        sdfs_lock_t *lock;
        char _lock[MAX_LOCK_LEN];
        char cookie[MAX_BUF_LEN];

        (void) gid;
        (void) uid;
        
        fileid = (fileid_t *)args->alock.fh.val;

        DINFO("NLM_UNLOCK fileid"FID_FORMAT"offset %llu len %llu svid %u\n", FID_ARG(fileid),
	          (LLU)args->alock.l_offset, (LLU)args->alock.l_len, args->alock.svid);

        lock = (void *)_lock;
        __nlock2slock(0, &args->alock, lock);
        lock->type = SDFS_UNLOCK;
        
        ret = sdfs_setlock(fileid, lock);
        if (ret) {
                GOTO(err_rep, ret);
        }

        res.stat = NLM4_GRANTED;
        
        //cookies
        res.cookies.len = args->cookies.len;
        res.cookies.data = (void *)cookie;
        memcpy(res.cookies.data, args->cookies.data, args->cookies.len);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_nlm_res);
        if (ret)
                GOTO(err_ret, ret);
        
        __FREE_ARGS(nlm_unlock, buf);
        
        return 0;
err_rep:
        res.stat = NLM4_GRANTED;
        res.cookies.len = args->cookies.len;
        res.cookies.data = (void *)cookie;
        memcpy(res.cookies.data, args->cookies.data, args->cookies.len);
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_nlm_res);
err_ret:
        __FREE_ARGS(nlm_unlock, buf);
        return ret;
}

static int __nlm4_cancel_svc(const sockid_t *sockid, const sunrpc_request_t *req,
                           uid_t uid, gid_t gid, nfsarg_t *_arg, buffer_t *buf)
{
        int ret;
        nlm_cancargs *args = &_arg->cancargs;
        nlm_res res;
        fileid_t *fileid;
        sdfs_lock_t *lock;
        char _lock[MAX_LOCK_LEN];
        char cookie[MAX_BUF_LEN];

        (void) gid;
        (void) uid;
        
        fileid = (fileid_t *)args->alock.fh.val;

        DINFO("NLM_CANCEL fileid"FID_FORMAT"offset %llu len %llu exclusive %u svid %u\n",
              FID_ARG(fileid), (LLU)args->alock.l_offset, (LLU)args->alock.l_len,
              args->exclusive, args->alock.svid);

        lock = (void *)_lock;
        __nlock2slock(args->exclusive, &args->alock, lock);
        ret = nlm4_async_cancel(fileid, lock);
        if (ret)
                GOTO(err_rep, ret);
        
        res.stat = NLM4_GRANTED;

        //cookies
        res.cookies.len = args->cookies.len;
        res.cookies.data = (void *)cookie;
        memcpy(res.cookies.data, args->cookies.data, args->cookies.len);

        ret = sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                           &res, (xdr_ret_t)xdr_nlm_res);
        if (ret)
                GOTO(err_ret, ret);
        
        __FREE_ARGS(nlm_canc, buf);

        return 0;
err_rep:
        res.stat = NLM4_GRANTED;
        res.cookies.len = args->cookies.len;
        res.cookies.data = (void *)cookie;
        memcpy(res.cookies.data, args->cookies.data, args->cookies.len);
        sunrpc_reply(sockid, req, ACCEPT_STATE_OK,
                     &res, (xdr_ret_t)xdr_nlm_res);
err_ret:
        __FREE_ARGS(nlm_canc, buf);
        return ret;

}

#if 0

/* write verifier */
extern jobtracker_t *jobtracker;
extern int grace_period;

int nlm_job2ip(job_t *job, char *host)
{
	(void)job;
        #if 1
        //sprintf(host, "%s", inet_ntoa(((struct sockaddr_in*)&addr)->sin_addr));
        sprintf(host, "%s", "192.168.1.22");
#endif
	return 0;
}


int job2host(job_t *job, char *host, int type)
{
	int sd, ret;
	unsigned char *p;
	struct sockaddr addr;
	socklen_t addrlen;
	net_handle_t *nh;
	nh = &job->net;
	sd = nh->u.sd.sd;
	if ( type == REMOTE_HOST) {
		ret = getpeername(sd, &addr, &addrlen);
		if (ret)
			DINFO("getpeername %s\n", strerror(errno));
		ret = getpeername(sd, &addr, &addrlen);
		if (ret)
			DINFO("getpeername %s\n", strerror(errno));
	}else {
		ret = getsockname(sd, &addr, &addrlen);
		if (ret)
			DINFO("getsockname %s\n", strerror(errno));
		ret = getsockname(sd, &addr, &addrlen);
		if (ret)
			DINFO("getsockname %s\n", strerror(errno));
	}
	p = (unsigned char*)&(((struct sockaddr_in*)&addr)->sin_addr);
	sprintf(host, "%d.%d.%d.%d", *p, *(p+1), *(p+2), *(p+3));
//	sprintf(host, "%s", inet_ntoa(((struct sockaddr_in*)&addr)->sin_addr));
	DINFO("JOB2host host is %s\n", host);
	return 0;
}

int nlm_unmon_process()
{
        int ret;
        uint32_t port = 0;
        int sd;
        struct sockaddr addr;
        socklen_t addrlen;
        char host[64];
        memset(host, 0x0, 64);
        struct my_id my_id;
        struct sm_stat res;
	    unsigned char *p __attribute__((unused));

        ret = tcp_sock_portlisten(&sd, 0, &port, 4, 0);
        if (ret)
                GOTO(err_ret, ret);

        ret = getsockname(sd, &addr, &addrlen);
        if (ret)
                GOTO(err_ret, ret);

	    p = (unsigned char*)&(((struct sockaddr_in*)&addr)->sin_addr);
#if 0
        sprintf(host, "%d.%d.%d.%d", *p, *(p+1), *(p+2), *(p+3));
#else
        gethostname(host, 64);
#if 0
        sprintf(host, "%s", "127.0.0.1");
#endif
#endif

        DINFO("unmon host is %s\n", host);

        my_id.len = strlen(host);
        ret = ymalloc((void**)&my_id.name, my_id.len);
        if (ret)
                GOTO(err_ret, ret);
        memcpy(my_id.name, host, my_id.len);

        my_id.my_prog = NLM_PROGRAM;
        my_id.my_vers = NLM_VERSION;
        my_id.my_proc = PNLM4_NSM_NOTIFY;
#if 0
        ret = callrpc("127.0.0.1", 100024,
                        1,4 ,
                        (xdrproc_t)xdr_nsm_myid, (char*)&my_id,
                        (xdrproc_t)xdr_nsm_unmonres,  (char*)&res
                     );

        if (ret)
                DINFO("unmont rpc failed %s\n", strerror(ret));
        if (res.state % 2) {
                DINFO("localnsm  is down\n")
        } else {
                DINFO("localnsm  is up\n")
        }
#endif
    	CLIENT *client = clnt_create(host, SM_PROGRAM, SM_VERSION, "tcp");
    	if (client == NULL) {
                ret = errno;
        		DINFO("client create error");
                GOTO(err_free, ret);
    	}
    	struct timeval timeout ={2, 0};
    	enum clnt_stat stat = clnt_call(client, SM_UNMON_ALL,
    			(xdrproc_t)xdr_nsm_myid, (char*)&my_id,
    			(xdrproc_t)xdr_nsm_unmonres,  (char*)&res,
    			timeout
    			);
    	(void)stat;
    	clnt_destroy(client);
        DINFO("unmon clnt_call %s\n", clnt_sperrno(stat));

err_free:
        yfree((void**)&my_id.name);
err_ret:
        return ret;
}


int jobnlm2monid(nlmlock_t *lock, struct mon_id *monid, job_t *job)
{
        int ret;
        char host[64];
        memset(host, 0x0, 64);
        monid->len = lock->owner.len;

        ret = ymalloc((void**)&monid->name,monid->len);
        if (ret)
                GOTO(err_ret, ret);
#if  1
        memcpy(monid->name, lock->owner.data, monid->len);
#endif

#if 0
        nlm_job2ip(job,host);
#endif
    	(void)job;
    	gethostname(host, 64);

        monid->my_id.len = strlen(host);
        ret = ymalloc((void**)&monid->my_id.name, monid->my_id.len);
        if (ret)
                GOTO(err_mem, ret);
        memcpy(monid->my_id.name, host, monid->my_id.len);
	    return 0;
err_mem:
        yfree((void**)&monid->name);
err_ret:
        return ret;
}
int jobnlm2mon(nlmlock_t *lock, struct mon *mon, job_t *job)
{
        int ret;
        ret = jobnlm2monid(lock, &mon->mon_id, job);
        if (ret)
                GOTO(err_ret, ret);
	return 0;
err_ret:
        return ret;
}

int nlm_lock_process(hostcache_t *hostcache, nlmlock_t *nlmlock, job_t *job)
{
        int ret;
        hostentry_t *hostentry, *retval = NULL;
        struct mon mon;
        struct sm_stat_res  res;
	char buf[64];

        ret = ymalloc((void**)&hostentry, sizeof(hostentry_t));
        if (ret)
                GOTO(err_ret, ret);
        hostentry->len = nlmlock->owner.len;


        ret = ymalloc((void**)&hostentry->nm, hostentry->len);
        if (ret)
                GOTO(err_mem, ret);
        memcpy(hostentry->nm, nlmlock->owner.data, nlmlock->owner.len);

    	memset(buf, 0x0, 64);
    	sprintf(buf, hostentry->nm, nlmlock->owner.len);
        DINFO("find the host, %s\n", buf);

        hostcache_find(hostcache, hostentry, &retval);
        if (retval) {
                retval->lkn++;
                yfree((void**)&hostentry->nm);
                yfree((void**)&hostentry);
		        nlmlk_cache_unlock(&nlmlk_cache);
        } else {
		        hostentry->lkn = 1;
                hostcache_insert(hostcache, hostentry);
		        nlmlk_cache_unlock(&nlmlk_cache);
                /*
                 * call the sm_rpc
                 */
                jobnlm2mon(nlmlock, &mon, job);

                mon.mon_id.my_id.my_prog = NLM_PROGRAM;
                mon.mon_id.my_id.my_vers = NLM_VERSION;
                mon.mon_id.my_id.my_proc = PNLM4_NSM_NOTIFY;

        		memset(buf, 0x0, 64);
        		gethostname(buf, 64);

        		CLIENT *client = clnt_create(buf, SM_PROGRAM, SM_VERSION, "tcp");
        		if (client == NULL) {
        			    DINFO("client create error");
		        }

                struct timeval timeout ={2, 0};
        		enum clnt_stat stat = clnt_call(client, SM_MON,
                                (xdrproc_t)xdr_nsm_monargs, (char*)&mon,
                                (xdrproc_t)xdr_nsm_monres,  (char*)&res, timeout);
        		(void)stat;
        		if (res.res_stat == STAT_SUCC) {
        			    DINFO("NSM agree to mon\n");
        		} else {
        			    DINFO("NSM mon failed\n");
        		}
                clnt_destroy(client);
                DINFO("lock clnt_call %s\n", clnt_sperrno(stat));

#if 0
                DINFO("unfind the host, rpc call %s\n", buf);
                ret = callrpc("192.168.1.22", 100024, 1, 3,
                        (xdrproc_t)xdr_nsm_monargs, (char*)&mon,
                        (xdrproc_t)xdr_nsm_monres,  (char*)&res
                        );
                if (ret)
                        DINFO("call rpc error %s\n", strerror(ret));
                if (res.res_stat == STAT_SUCC) {
                        DINFO("mon sucess\n");
                }
                else  {
                        DINFO("mon failed\n");
                }
#endif
        }
        return 0;
err_mem:
        yfree((void**)&hostentry);
err_ret:
	    nlmlk_cache_unlock(&nlmlk_cache);
        return ret;
}

int nlm_unlock_process(hostcache_t *hostcache, nlmlock_t *nlmlock, job_t *job, int num)
{
        int ret;
        hostentry_t *hostentry, *retval = NULL;
        struct mon_id mon_id;
        struct sm_stat res;
	    char buf[64];

        ret = ymalloc((void**)&hostentry, sizeof(hostentry_t));
        if (ret)
                GOTO(err_ret, ret);
        hostentry->len = nlmlock->owner.len;

        ret = ymalloc((void**)&hostentry->nm, hostentry->len);
        if (ret)
                GOTO(err_mem, ret);
        memcpy(hostentry->nm, nlmlock->owner.data, nlmlock->owner.len);
    	memset(buf, 0x0, 64);
    	memcpy(buf, nlmlock->owner.data, nlmlock->owner.len);
    	DINFO("unlock host %s\n", buf);

        hostcache_find(hostcache, hostentry, &retval);
        if (retval) {
                retval->lkn = retval->lkn - num;
                if (retval->lkn == 0) {
                        hostcache_delete(hostcache, hostentry, &retval);
			            nlmlk_cache_unlock(&nlmlk_cache);
                        yfree((void**)&retval->nm);
                        yfree((void**)&retval);
                        /*
                         * rpc call
                         */
                        jobnlm2monid(nlmlock, &mon_id, job);
                        mon_id.my_id.my_prog = NLM_PROGRAM;
                        mon_id.my_id.my_vers = NLM_VERSION;
                        mon_id.my_id.my_proc = PNLM4_NSM_NOTIFY;

            			memset(buf, 0x0, 64);
            			gethostname(buf, 64);
            			CLIENT *client = clnt_create(buf, SM_PROGRAM, SM_VERSION, "tcp");
            			if (client == NULL) {
            				DINFO("client create error");
            			}
            			struct timeval timeout ={2, 0};
            			enum clnt_stat stat = clnt_call(client, SM_UNMON ,
            					(xdrproc_t)xdr_nsm_unmonargs, (char*)&mon_id,
            					(xdrproc_t)xdr_nsm_unmonres,  (char*)&res, timeout);
            			(void)stat;
            			clnt_destroy(client);
            			DINFO("unlock clnt_call %s\n", clnt_sperrno(stat));
#if 0
                        ret = callrpc("127.0.0.1", 100024,
                                        1, 3,
                                        (xdrproc_t)xdr_nsm_unmonargs, (char*)&mon_id,
                                        (xdrproc_t)xdr_nsm_unmonres,  (char*)&res
                               );
                        if (ret)
                                DWARN("call rpc error %s\n", strerror(ret));
#endif
                } else {
        			    nlmlk_cache_unlock(&nlmlk_cache);
        		}
                yfree((void**)&hostentry->nm);
                yfree((void**)&hostentry);
        } else {
		        nlmlk_cache_unlock(&nlmlk_cache);
                DWARN("host cache NULL ,why happen\n");
        }
        return 0;
err_mem:
        yfree((void**)&hostentry);
err_ret:
	    nlmlk_cache_unlock(&nlmlk_cache);
        return ret;
}

static int __unlock_proc(nlmlock_t *nlmlock, job_t *job)
{
        int ret = 0;
        nlmlock_t *retval;
        int num = 0;

        nlmlk_cache_lock(&nlmlk_cache);

        //unlock the section start from the nlomlock->l_offset
        if (0 == nlmlock->l_len) {
                DINFO("unlock the section start from %ld.\n", nlmlock->l_offset);
                ret = nlmlk_cache_clear(&nlmlk_cache, nlmlock, &num);
                if (num)
                        nlm_unlock_process(&hostcache, nlmlock, job, num);
                else
                        nlmlk_cache_unlock(&nlmlk_cache);
        }
        else {
                ret = nlmlk_cache_find(&nlmlk_cache, nlmlock, &retval);
                if (retval) {

                        /*two case
                         *1. not the good svid ; there must be a coding error
                         *2. the good svid
                         */
                        DINFO("unlock in the cache, will unlock\n");
                        ret = nlmlk_cache_delete(&nlmlk_cache, nlmlock, &retval);
                        nlm_unlock_process(&hostcache, nlmlock, job, 1);
                        YASSERT((ret == 0) && (NULL != retval));
                        free_nlmlock(retval);
                }
                else
                     nlmlk_cache_unlock(&nlmlk_cache);

        }

        return ret;
}

int nlm4_notify_svc(job_t *job, char *name)
{
        (void)job;
        (void)name;
        int done = 0;
        nlm_job_context_t *context;
        nlm_notifyargs *args;
	    hostentry_t hostentry,*retval;
        char host[64];

        context = (nlm_job_context_t *)job->context;
        args = &context->arg.notifyargs;

        memset(host, 0x0, 64);
        memcpy(host, args->cookies.data, args->cookies.len);
        DINFO("NLM4_NOTIFY_SVC host %s, state %d\n", host, (int)args->state);
        while (done == 0) {
                switch (job->status) {
                        default:
                        nlmlk_cache_lock(&nlmlk_cache);
                        nlmlk_del_host(&nlmlk_cache, (unsigned char*)host, args->state);
            			ymalloc((void**)&hostentry.nm, args->cookies.len);
            			memcpy(hostentry.nm, args->cookies.data, args->cookies.len);
            			hostentry.len = args->cookies.len;
            			hostcache_delete(&hostcache, &hostentry, &retval);
                        nlmlk_cache_unlock(&nlmlk_cache);

            			if (retval) {
                				DINFO("del the host %s\n", host);
                				yfree((void**)&retval->nm);
            			} else {
            				    DINFO("error , can not find the host %s\n", host);
            			}
            			yfree((void**)&hostentry.nm);
                        //SUNRPC_REPLY_OK(NULL, FREE_JOB, ACCEPT_STATE_OK);
                        FREE_ARGS(nlm_notify);
                        job_destroy(job);
                        done = 1;
                }
        }
        return 0;
#if 0
err_rep:
        SUNRPC_REPLY_ERROR(xdr_nlm_res, FREE_JOB, ACCEPT_STATE_OK);
#endif
}

int nlm4_granted_svc(job_t *job, char *name)
{
        (void)job;
        (void)name;
        return 0;
}

#endif

static int __core_handler(va_list ap)
{
        int ret;
        nfs_handler handler = va_arg(ap, nfs_handler);
        const sockid_t *sockid = va_arg(ap, const sockid_t *);
        const sunrpc_request_t *req = va_arg(ap, const sunrpc_request_t *);
        uid_t uid = va_arg(ap, uid_t);
        gid_t gid = va_arg(ap, gid_t);
        nfsarg_t *nfsarg = va_arg(ap, nfsarg_t *);
        buffer_t *buf = va_arg(ap, buffer_t *);

        va_end(ap);

        ret = handler(sockid, req, uid, gid, nfsarg, buf);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int nfs_nlm4(const sockid_t *sockid, const sunrpc_request_t *req,
            uid_t uid, gid_t gid, buffer_t *buf)
{
        int ret;
        nfs_handler handler = NULL;
        xdr_arg_t xdr_arg = NULL;
        hash_args_t hash_args = NULL;
        nfsarg_t nfsarg;
        xdr_t xdr;
        const char *name;

        switch (req->procedure) {
        case PNLM4_NULL:
                DINFO("NLM4_NULL\n");
                handler = __nlm4_null_svc;
                xdr_arg = NULL;
                name = "nlm_null";
                break;
        case PNLM4_TEST:
                DINFO("NLM4_TEST\n");
                handler = __nlm4_test_svc;
                xdr_arg = (xdr_arg_t)xdr_nlm_testargs;
                hash_args = (hash_args_t)hash_test;
                name = "nlm_test";
                break;
        case PNLM4_LOCK:
                handler = __nlm4_lock_svc;
                xdr_arg = (xdr_arg_t)xdr_nlm_lockargs;
                hash_args = (hash_args_t)hash_lock;
                DINFO("NLM4_LOCK\n");
                name = "nlm_lock";
                break;
        case PNLM4_UNLOCK:
                handler = __nlm4_unlock_svc;
                xdr_arg = (xdr_arg_t)xdr_nlm_unlockargs;
                hash_args = (hash_args_t)hash_unlock;
                DINFO("NLM4_UNLOCK\n");
                name = "nlm_unlock";
                break;
        case PNLM4_CANCEL:
                handler = __nlm4_cancel_svc;
                xdr_arg =(xdr_arg_t)xdr_nlm_cancargs;
                DINFO("NLM4_CANCEL\n");
                name = "nlm_cancel";
                break;
#if 0
        case PNLM4_GRANTED:
                DINFO("NLM4_GRANTED UNIMPLEMENT\n");
                break;
        case PNLM4_TEST_MSG:
                DINFO("NLM4_TEST_MSG UNIMPLEMENT\n");
                break;
        case PNLM4_LOCK_MSG:
                DINFO("NLM4_LOCK_MSG UNIMPLEMENT\n");
                break;
        case PNLM4_CANCEL_MSG:
                DINFO("NLM4_CANCEL_MSG UNIMPLEMENT\n");
                break;
        case PNLM4_UNLOCK_MSG:
                DINFO("NLM4_UNLOCK_MSG UNIMPLEMENT\n");
                break;
        case PNLM4_GRANTED_MSG:
                DINFO("NLM4_GRANTED_MSG UNIMPLEMENT\n");
                break;
        case PNLM4_TEST_RES:
                DINFO("NLM4_TEST_MSG UNIMPLEMENT\n");
                break;
        case PNLM4_LOCK_RES:
                DINFO("NLM4_LOCK_RES UNIMPLEMENT\n");
                break;
        case PNLM4_CANCEL_RES:
                DINFO("NLM4_CANCEL_RES UNIMPLEMENT\n");
                break;
        case PNLM4_UNLOCK_RES:
                DINFO("NLM4_UNLOCK_RES UNIMPLEMNET\n");
                break;
        case PNLM4_GRANTED_RES:
                DINFO("NLM4_GRANTED_RES UNIMPLEMENT\n");
                break;
        case PNLM4_NSM_NOTIFY:
                /*
                 * call
                 */
                handler = nlm4_notify_svc;
                xdr_arg = (xdr_arg_t)xdr_nlm_notifyargs;
		        hash_args = (hash_args_t)hash_notify;
                DINFO("NLM4_NSM_NOTIFY\n");
                break;
        case PNLM4_SHARE:
                DINFO("NLM4_SHARE UNIMPLEMENT\n");
                break;
        case PNLM4_UNSHARE:
                DINFO("NLM4_UNSHARE UNIMPLEMENT\n");
                break;
        case PNLM4_NM_LOCK:
                DINFO("NLM4_NM_LOCK UNIMPLEMENT\n");
                break;
        case PNLM4_FREE_ALL:
                DINFO("NLM4_FREE_ALL UNIMPLEMENT\n");
                break;
#endif
        default:
                DERROR("error procedure %d\n", req->procedure);
        }
        
        if (handler == NULL) {
                ret = EINVAL;
                DERROR("error proc %s\n", name);
                GOTO(err_ret, ret);
        }

        xdr.op = __XDR_DECODE;
        xdr.buf = buf;
        
        if (xdr_arg) {
                if (xdr_arg(&xdr, &nfsarg)) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }

        if (name) {
                schedule_task_setname(name);
        }

        if (hash_args) {
                int hash = hash_args(&nfsarg);

                DBUG("core request %s hash %d\n", name, hash);
                ret = core_request(hash, -1, name, __core_handler, handler,
                                   sockid, req, uid, gid, &nfsarg, buf);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                ret = handler(sockid, req, uid, gid, &nfsarg, buf);
                if (ret)
                        GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}

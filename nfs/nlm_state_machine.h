/*
 * =====================================================================================
 *
 *       Filename:  nlm_state_machine.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  04/07/2011 10:28:47 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *        Company:
 *
 * =====================================================================================
 */
#ifndef __NLM_STATUS_MACHINE_H__
#define __NLM_STATUS_MACHINE_H__
#include "nfs_state_machine.h"
#include "xdr.h"

#include <rpc/rpc.h>
#include <stdint.h>

#include "job.h"
#include "sdfs_conf.h"

#define NFS_MAXDATA_UDP 32768
#define NFS_MAX_UDP_PACKET (NFS_MAXDATA_UDP + 4096) /* The extra 4096 bytes are for the RPC header */

#define UNIX_PATH_MAX 108

#define NFS_TCPDATA_MAX 1048576
#define NFS_RTMULT 2
#define NFS_NAMLEN_MAX 255

#define NFSMODE_FMT 0170000
#define NFSMODE_DIR 0040000
#define NFSMODE_CHR 0020000
#define NFSMODE_BLK 0060000
#define NFSMODE_REG 0100000
#define NFSMODE_LNK 0120000
#define NFSMODE_SOCK 0140000
#define NFSMODE_FIFO 0010000

#define NFS3_FHSIZE 64
#define NFS3_COOKIEVERFSIZE 8
#define NFS3_CREATEVERFSIZE 8
#define NFS3_WRITEVERFSIZE 8

enum {
	REMOTE_HOST,
	LOCAL_HOST,
};

typedef enum {
        /*
         * this implement just for nlm4
         */
        NLM4_GRANTED             = 0,
        NLM4_DENIED              = 1,
        NLM4_DENIED_NOLOCKS      = 2,
        NLM4_BLOCKED             = 3,
        NLM4_DENIED_GRACE_PERIOD = 4,
        NLM4_DEADLCK             = 5,
        NLM4_ROFS                = 6,
        NLM4_STALE_FH            = 7,
        NLM4_FBIG                = 8,
        NLM4_FAILED              = 9
} nlm_stats;

enum {
        /*
         * this implement just for nlm4
         */
        PNLM4_NULL               = 0,
        PNLM4_TEST               = 1,
        PNLM4_LOCK               = 2,
        PNLM4_CANCEL             = 3,
        PNLM4_UNLOCK             = 4,
        PNLM4_GRANTED            = 5,/* 5 */
        PNLM4_TEST_MSG           = 6,
        PNLM4_LOCK_MSG           = 7,
        PNLM4_CANCEL_MSG         = 8,
        PNLM4_UNLOCK_MSG         = 9,
        PNLM4_GRANTED_MSG        = 10,/* 10 */
        PNLM4_TEST_RES           = 11,
        PNLM4_LOCK_RES           = 12,
        PNLM4_CANCEL_RES         = 13,
        PNLM4_UNLOCK_RES         = 14,
        PNLM4_GRANTED_RES        = 15,/* 15 */
        PNLM4_NSM_NOTIFY         = 16,/* statd callback */

        PNLM4_SHARE              = 20,/* 20 */
        PNLM4_UNSHARE            = 21,
        PNLM4_NM_LOCK            = 22,
        PNLM4_FREE_ALL           = 23,
};


#define XDR_MAX_NETOBJ      1024
#if 1
typedef struct ynetobj {
        unsigned int len;
        unsigned char *data;
} ynetobj;
#endif

typedef struct nlm_lock {
        char *                  caller;
        uint32_t                len;
        nfs_fh3                 fh;
        struct ynetobj          oh;
        uint32_t                svid;
        uint64_t                l_offset;
        uint64_t                l_len;
} nlm_lock;

#define NLM_MAXCOOKIELEN 32
struct nlm_cookie {
        unsigned char data[NLM_MAXCOOKIELEN];
        unsigned int len;
};

/*
 * generic lockd result
 */
typedef struct nlm_args {
        struct nlm_cookie  cookie;
        struct nlm_lock    lock;
        uint32_t           block;
        uint32_t           reclaim;
        uint32_t           monitor;
        uint32_t           fsm_access;
        uint32_t           fsm_mode;
} nlm_args;

#define SM_MAXSTRLEN  1024
#define SM_PRIV_SIZE  16
struct nsm_private {
        unsigned char data[SM_PRIV_SIZE];
};
typedef struct nlm_reboot {
        char *mon;
        uint32_t len;
        uint32_t state;
        struct nsm_private priv;
}nlm_reboot;



typedef struct nlm4_holder {
        bool_t exclusive;
        int32_t svid;
        ynetobj oh;
        uint64_t l_offset;
        uint64_t l_len;
} nlm4_holder;

/*
 *
 */

typedef struct nlm4_testargs {
        struct ynetobj cookies;
        bool_t            exclusive;
        struct nlm_lock   alock;
} nlm4_testargs;

typedef struct nlm_stat {
        nlm_stats stat;
}nlm_stat;

typedef struct nlm_res {
        struct ynetobj cookies;
        nlm_stats stat;
}nlm_res;

typedef struct nlm_testrply{
        nlm_stats status;
        union {
                nlm4_holder holder;
        } nlm_testrply_u;
} nlm_testrply;

typedef struct nlm_testres {
        struct ynetobj cookies;
        nlm_testrply test_stat;
} nlm_testres;

typedef struct nlm_lockargs {
        struct ynetobj cookies;
        bool_t block;
        bool_t exclusive;
        struct nlm_lock alock;
        bool_t reclaim;
        int    state;
} nlm_lockargs;

typedef struct nlm_cancargs {
        struct ynetobj cookies;
        bool_t block;
        bool_t exclusive;
        struct nlm_lock alock;
} nlm_cancargs;

typedef struct nlm_testargs {
        struct ynetobj cookies;
        bool_t exclusive;
        struct nlm_lock alock;
} nlm_testargs;

typedef struct nlm_unlockargs {
        struct ynetobj cookies;
        struct nlm_lock alock;
}nlm_unlockargs;

typedef struct nlm_notifyargs {
        struct ynetobj cookies;
        int    state;
        char   priv[16];
}nlm_notifyargs;

#if 0
int nlm_null_svc(job_t *job, char *name);
int nlm4_test_svc(job_t *job, char *name);
int nlm4_lock_svc(job_t *job, char *name);
int nlm4_cancel_svc(job_t *job, char *name);
int nlm4_unlock_svc(job_t *job, char *name);
int nlm4_granted_svc(job_t *job, char *name);
int nlm4_notify_svc(job_t *job, char *name);

int nlm_unmon_process();

int nlm4_testmsg_svc(job_t *job, char *name)
int nlm4_lockmsg_svc(job_t *job, char *name)
int nlm4_unlockmsg_svc(job_t *job, char *name)
int nlm4_grantedmsg_svc(job_t *job, char *name)
int nlm4_testres_svc(job_t *job, char *name)
int nlm4_lockres_svc(job_t *job, char *name)
int nlm4_cancelres_svc(job_t *job, char *name)
int nlm4_unlockres_svc(job_t *job, char *name)
int nlm4_grantedres_svc(job_t *job, char *name)
int nlm4_nsmnotify_svc(job_t *job, char *name)
int nlm4_unshare_svc(job_t *job, char *name)
int nlm4_nmlock_svc(job_t *job, char *name)
int nlm4_freeall_svc(job_t *job, char *name)
#endif
#endif



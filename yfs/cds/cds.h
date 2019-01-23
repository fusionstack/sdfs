#ifndef __CDS_H__
#define __CDS_H__

#include <dirent.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <sys/statvfs.h>
#include <sys/types.h>

#include "disk_proto.h"
#include "sdfs_list.h"
#include "md_proto.h"
#include "ylock.h"
#include "ynet_rpc.h"
#include "yfscds_conf.h"

#define CDS_WRITE_THREADS 7

/**
 * hb service
 */
typedef struct {
        struct list_head *list;
        sy_rwlock_t      *rwlock;

        sem_t             sem;
        int               stop;
        int               running;
        int               inited;

        int               servicenum;
        diskid_t          diskid;
        struct statvfs    fsbuf;
} hb_service_t;

typedef struct {
        struct list_head list;
        chkjnl_t        chkrept;
} hb_chkjnl_t;

/**
 * robot service
 */
typedef struct {
        sem_t sem;
        int   stop;
        int   running;
        int   inited;
} robot_service_t;

/**
 * global cds info
 */
typedef struct {
	enum {
		CDS_TYPE_NODE,
		CDS_TYPE_CACHE,
	} type;
	
	int              diskno;
        uint32_t         tier; /*0:ssd, 1:hdd*/

        hb_service_t     hb_service;
        sem_t            hb_sem;

        int readonly;
        int running;
        robot_service_t  robot_service;
        net_handle_t rjnld;
} cds_info_t;

extern cds_info_t cds_info;
extern int cds_run(void *);

void cds_exit_handler(int sig);
void cds_signal_handler(int sig);
void cds_monitor_handler(int sig);

typedef struct {
        int daemon;
	    int diskno;
} cds_args_t;

#endif

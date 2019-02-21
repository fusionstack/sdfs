#ifndef __NFS_EVENTS_H__
#define __NFS_EVENTS_H__





#include "job.h"
#include "nfs_args.h"
#include "sunrpc_proto.h"

typedef int (*hash_args_t)(void*);

extern int mnt_event_handler(job_t *);
extern int nfs_event_handler(job_t *);
extern int acl_event_handler(job_t *);

typedef int (*nfs_handler)(const sockid_t *sockid, const sunrpc_request_t *req,
                       uid_t uid, gid_t gid, nfsarg_t *arg, buffer_t *buf);

extern int nfs_mount(const sockid_t *sockid, const sunrpc_request_t *req,
                     uid_t uid, gid_t gid, buffer_t *buf);
extern int nfs_ver3(const sockid_t *sockid, const sunrpc_request_t *req,
                    uid_t uid, gid_t gid, buffer_t *buf);
extern int nfs_nlm4(const sockid_t *sockid, const sunrpc_request_t *req,
                    uid_t uid, gid_t gid, buffer_t *buf);


void nfs_newtask(const sockid_t *sockid, const sunrpc_request_t *req,
                  uid_t uid, gid_t gid, buffer_t *buf);

#endif

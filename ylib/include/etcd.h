#ifndef __ECTD_H__
#define __ECTD_H__

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <semaphore.h>
#include <netinet/in.h>

#include "sdfs_list.h"
#include "adt.h"
#include "sdfs_id.h"
#include "etcd-api.h"

#define ETCD_ROOT       "/sdfs"
#define ETCD_STORAGE    "storage"
#define ETCD_NODE       "node"
#define ETCD_CONN       "network/conn"
#define ETCD_TRANS      "trans"
#define ETCD_RECYCLE    "recycle"
#define ETCD_MISC       "misc"
#define ETCD_NID        "network/nid"
#define ETCD_CHAP       "chap"
#define ETCD_INITIATOR  "initiator"
#define ETCD_CLEANUP    "cleanup"
#define ETCD_MIGRATE    "migrate"
#define ETCD_POOLSTATUS "poolstatus"
#define ETCD_POOLSCAN   "poolscan"

#define __ETCD_READ_MULTI__ 0


typedef struct{
        int running;
        int retval;
        int ttl;
        int update;
        uint32_t magic;
        sem_t sem;
        sem_t stoped;
        char hostname[MAX_NAME_LEN];
        char key[MAX_PATH_LEN];
} etcd_lock_t;

int etcd_mkdir(const char *dir, int ttl);
int etcd_readdir(const char *_key, char *buf, int *buflen);

int etcd_list(const char *_key, etcd_node_t **_node);
int etcd_count(int *num_servers, const char *key);
int etcd_del(const char *prefix, const char *_key);
int etcd_del_dir(const char *prefix, const char *_key, int recursive);

int etcd_create_text(const char *prefix, const char *_key, const char *_value, int ttl);
int etcd_get_text (const char *prefix, const char *_key, char *value, int *idx);
int etcd_update_text(const char *prefix, const char *_key, const char *_value, const int  *idx, int ttl);

int etcd_create(const char *prefix, const char *_key, const void *_value, int valuelen, int ttl);
int etcd_get_bin(const char *prefix, const char *_key, void *_value, int *_valuelen, int *idx);
int etcd_update(const char *prefix, const char *_key, const void *_value, int valuelen,
                const int *idx, int ttl);

int etcd_lock_init(etcd_lock_t *lock, const char *prefix, const char *key, int ttl, uint32_t magic, int update);
int etcd_lock(etcd_lock_t *lock);
int etcd_unlock(etcd_lock_t *lock);
int etcd_lock_delete(etcd_lock_t *lock);
int etcd_locker(etcd_lock_t *lock, char *locker, nid_t *nid, uint32_t *_magic, int *idx);
int etcd_lock_watch(etcd_lock_t *lock, char *value, nid_t *nid, uint32_t *magic, int *idx);
int etcd_lock_health(etcd_lock_t *lock);

int etcd_set_with_ttl(const char *prefix, const char *key, const char *val, int ttl);

int etcd_init();

int etcd_cluster_node_count(int *node_count);
int etcd_is_proxy();

#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/poll.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <attr/attributes.h>
#include <ctype.h>
#include <fcntl.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSLIB

#include "yfs_conf.h"
#include "etcd.h"
#include "network.h"
#include "allocator.h"
#include "mond_rpc.h"
#include "sysutil.h"
#include "net_table.h"
#include "ylib.h"
#include "dbg.h"

typedef struct {
        int count;
        int cursor;
        nid_t array[0];
} allocator_node_t;

typedef struct {
        sy_rwlock_t lock;
        int count;
        int cursor;
        allocator_node_t **array;
} allocator_t;

static allocator_t *__allocator__ = NULL; 

static int __allocator_disk(const nid_t *nid)
{
        int ret;

        ret = network_connect(nid, NULL, 1, 0);
        if(ret)
                GOTO(err_ret, ret);

        DINFO("disk %s online\n", network_rname(nid));
        
        return 0;
err_ret:
        return ret;
}

static int __allocator_node(const char *nodeinfo, allocator_node_t **_allocator_node)
{
        int ret, disk_count = 512;
        char *list[512];
        allocator_node_t *allocator_node;
        nid_t nid;

        DINFO("scan %s, disk %s\n", nodeinfo, strchr(nodeinfo, ' ') + 1);

        disk_count = 1024;
        _str_split(strchr(nodeinfo, ' ') + 1, ',', list, &disk_count);

        YASSERT(disk_count);

        ret = ymalloc((void **)&allocator_node, sizeof(*allocator_node) + sizeof(nid_t) * disk_count);
        if (ret)
                GOTO(err_ret, ret);

        allocator_node->count = 0;
        allocator_node->cursor = _random();
        for (int i = 0; i < disk_count; i++) {
                str2nid(&nid, list[i]);

                ret = __allocator_disk(&nid);
                if (ret)
                        continue;
                
                allocator_node->array[allocator_node->count] = nid;
                allocator_node->count++;
        }

        if (allocator_node->count == 0) {
                yfree((void **)&allocator_node);
                ret = ENOSPC;
                GOTO(err_ret, ret);
        }

        *_allocator_node = allocator_node;
        
        return 0;
err_ret:
        return ret;
}

inline static int __allocator_scan(char *value, int *_count, allocator_node_t ***_array)
{
        int ret, node_count, count;
        char *list[1024];
        allocator_node_t **node_array, *allocator_node;

        node_count = 1024;
        _str_split(value, '\n', list, &node_count);

        if (node_count == 0) {
                ret = ENOSPC;
                GOTO(err_ret, ret);
        }

        ret = ymalloc((void **)&node_array, sizeof(allocator_node_t *) * node_count);
        if (ret)
                GOTO(err_ret, ret);

        count = 0;
        for (int i = 0; i < node_count; i++) {
                ret = __allocator_node(list[i], &allocator_node);
                if (ret)
                        continue;

                node_array[count] = allocator_node;
                count++;
        }

        if (count == 0) {
                yfree((void **)&node_array);
                ret = ENOSPC;
                GOTO(err_ret, ret);
        }
        
        *_array = node_array;
        *_count = count;
        
        return 0;
err_ret:
        return ret;
}

inline static int __allocator_replace(int count, allocator_node_t **array)
{
        int ret, old;
        allocator_node_t **old_array;
        
        ret = sy_rwlock_wrlock(&__allocator__->lock);
        if (ret)
                GOTO(err_ret, ret);

        old = __allocator__->count;
        old_array = __allocator__->array;
        __allocator__->count = count;
        __allocator__->array = array;
                
        sy_rwlock_unlock(&__allocator__->lock);

        for (int i = 0; i < old; i++) {
                yfree((void **)&old_array[i]);
        }

        return 0;
err_ret:
        return ret;
}

static int __allocator_apply(char *value)
{
        int ret, count;
        allocator_node_t **node_array;

        ret = __allocator_scan(value, &count, &node_array);
        if (ret) {
                DWARN("scan fail\n");
                return 0;
        }

        ret = __allocator_replace(count, node_array);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

static void *__allocator_worker(void *arg)
{
        int ret, idx = 0;
        etcd_node_t  *node = NULL;
        etcd_session  sess;
        char key[MAX_PATH_LEN], *host;

        (void) arg;

        char *buf;
        ret = ymalloc((void **)&buf, 1024 * 1024);
        if (ret)
                UNIMPLEMENTED(__DUMP__);
        
        while (1) {
                ret = etcd_get_text(ETCD_DISKMAP, "diskmap", buf, NULL);
                if (ret) {
                        DWARN("diskmap not found\n");
                        sleep(1);
                        continue;
                }

                ret = __allocator_apply(buf);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);

                break;
        }

        yfree((void **)&buf);
        
        host = strdup("localhost:2379");
        sess = etcd_open_str(host);
        if (!sess) {
                ret = ENONET;
                GOTO(err_ret, ret);
        }

        snprintf(key, MAX_NAME_LEN, "%s/%s/diskmap", ETCD_ROOT, ETCD_DISKMAP);
        DINFO("watch %s idx %u\n", key, idx);
        while (1) {
                ret = etcd_watch(sess, key, &idx, &node, 0);
                if(ret != ETCD_OK){
                        if (ret == ETCD_ENOENT) {
                                DWARN("%s not exist\n");
                                sleep(1);
                                continue;
                        } else
                                GOTO(err_close, ret);
                }

                DINFO("conn watch node:%s nums:%d\n", node->key, node->num_node);
                idx = node->modifiedIndex + 1;
                ret = __allocator_apply(node->value);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);

                free_etcd_node(node);
        }

        etcd_close_str(sess);
        free(host);
        pthread_exit(NULL);
err_close:
        etcd_close_str(sess);
err_ret:
        free(host);
        UNIMPLEMENTED(__DUMP__);
        pthread_exit(NULL);
}

int allocator_init()
{
        int ret;

        ret = ymalloc((void **)&__allocator__, sizeof(*__allocator__));
        if (ret)
                GOTO(err_ret, ret);

        ret = sy_rwlock_init(&__allocator__->lock, "allocator");
        if (ret)
                GOTO(err_ret, ret);
        
        ret = sy_thread_create2(__allocator_worker, NULL, "allocator");
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

static void __allocator_new_disk(allocator_node_t *node, nid_t *nid)
{
        *nid = node->array[node->cursor % node->count];
        node->cursor++;
}

static int __allocator_new(int repnum, int hardend, int tier, nid_t *disks)
{
        int ret;
        allocator_t *allocator = __allocator__;

#if 0
        return mond_rpc_newdisk(net_getnid(), tier, repnum, hardend, disks);
#endif
        
        (void) hardend;
        (void) tier;
        
#if 1
        if (__allocator__ == NULL || gloconf.solomode) {
                return mond_rpc_newdisk(net_getnid(), tier, repnum, hardend, disks);
        }
#endif

        ret = sy_rwlock_rdlock(&allocator->lock);
        if (ret)
                GOTO(err_ret, ret);

        if (allocator->count < repnum) {
                ret = ENOSPC;
                DWARN("need %u got %u\n", repnum, allocator->count);
                GOTO(err_lock, ret);
        }

        int cur = allocator->cursor;
        for (int i = 0; i < repnum; i++ ) {
                __allocator_new_disk(allocator->array[(i + cur) % allocator->count],
                                     &disks[i]);
        }

        allocator->cursor++;
        
        sy_rwlock_unlock(&allocator->lock);

        return 0;
err_lock:
        sy_rwlock_unlock(&allocator->lock);
err_ret:
        return ret;
}

int allocator_new(int repnum, int hardend, int tier, nid_t *disks)
{
#if ENABLE_ALLOCATE_BALANCE
        int ret;
        nid_t array[16];

        YASSERT(repnum + 1 < 16);

        ret = __allocator_new(repnum + 1, hardend, tier, array);
        if (ret) {
                if (ret == ENOSPC) {
                        return __allocator_new(repnum, hardend, tier, disks);
                } else {
                        GOTO(err_ret, ret);
                }
        }

        netable_sort(array, repnum + 1);
        memcpy(disks, array, sizeof(nid_t) * repnum);
        
        return 0;
err_ret:
        return ret;
#else
        return  __allocator_new(repnum, hardend, tier, disks);
#endif
}

#if 0
int allocator_register()
{
        
}

int allocator_unregister()
{
        
}

#endif

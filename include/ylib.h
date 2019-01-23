#ifndef __SYSY_LIB_H__
#define __SYSY_LIB_H__

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <uuid/uuid.h>

#include "array_table.h"
#include "hash_table.h"
#include "nls.h"
#include "etcd.h"
#include "sdfs_conf.h"
#include "newdef.h"
#include "sdfs_id.h"
#include "ylock.h"
#include "ypath.h"
#include "cache.h"
#include "heap.h"
#include "sysutil.h"
#include "mini_hashtb.h"

typedef struct {
        chkid_t id;
        //vclock_t vclock;
        uint64_t lease;
        union {
                uint64_t offset;
                struct {
                        uint32_t chunk_off:20;  // 1M
                        uint32_t chunk_id:32;
                        uint32_t __pad:12;
                };
        };
        uint32_t size;
        uint32_t flags;
        uint64_t lsn;
} io_t;


typedef struct {
        uint32_t addr;
        uint32_t port;
} addr_t;

/* auth.c */
extern int chech_auth_shadow(const char *user, const char *passwd);

/* array_table.c */
extern atable_t array_create_table(int (*compare_func)(void *, void *),
                                   int max_array);
extern int array_table_get_nr(atable_t);
extern int array_table_get_max(atable_t);
extern int array_table_insert_empty(atable_t, void *value);
extern void *array_table_find(atable_t, int no, void *comparator);
extern int array_table_remove(atable_t, int no, void *comparator, void **value);
extern void array_table_destroy(atable_t);

/* cmp.c */
extern int nid_cmp(const nid_t *key, const nid_t *data);
extern int nid_void_cmp(const void *key, const void *data);
extern int verid64_void_cmp(const void *key, const void *data);

/*md5.c  */
typedef unsigned char *POINTER; /* POINTER defines a generic pointer type */
typedef unsigned short int UINT2; /* UINT2 defines a two byte word */
typedef unsigned long int UINT4; /* UINT4 defines a four byte word */

/* crc32.c */
#define crc32_init(crc) ((crc) = ~0U)
extern int crc32_stream(uint32_t *_crc, const char *buf, uint32_t len);
extern uint32_t crc32_stream_finish(uint32_t crc);
int crc32_md_verify(const void *ptr, uint32_t len);
void crc32_md(void *ptr, uint32_t len);
uint32_t crc32_sum(const void *ptr, uint32_t len);

/* crcrs.c */
extern void crcrs_init(void);

/* daemon.c */
int daemon_update(const char *key, const char *value);
int daemon_lock(const char *key);
extern int daemonlize(int daemon, int maxcore, char *chr_path, int preservefd, int64_t _maxopenfile);
int daemon_pid(const char *path);
extern int lock_pid(const char *lockname, int seq);
extern int set_environment();
int get_nodeid(uuid_t, const char *path);
int server_run(int daemon, const char *lockname, int seq, int (*server)(void *), void *args);

/* hash.c */
extern uint32_t hash_str(const char *str);
extern uint32_t hash_mem(const void *mem, int size);

/* hash_table.c */
extern hashtable_t hash_create_table(int (*compare_func)(const void *, const void *),
                                     uint32_t (*key_func)(const void *), const char *name);
extern void *hash_table_find(hashtable_t, void *comparator);
extern int hash_table_insert(hashtable_t, void *value, void *comparator,
                             int overwrite);
extern int hash_table_remove(hashtable_t, void *comparator, void **value);
extern void hash_iterate_table_entries(hashtable_t,
                                       void (*handler)(void *, void *),
                                       void *arg);
extern void hash_filter_table_entries(hashtable_t, int (*handler)(void *, void *),
                                      void *arg, void (*thunk)(void *));
extern void hash_destroy_table(hashtable_t, void (*thunk)(void *));
int hashtable_resize(hashtable_t t, int size);
int hashtable_size(hashtable_t t);

/* lock.c */
extern int sy_rwlock_init(sy_rwlock_t *rwlock, const char *name);
extern int sy_rwlock_destroy(sy_rwlock_t *rwlock);
extern int sy_rwlock_rdlock(sy_rwlock_t *rwlock);
extern int sy_rwlock_tryrdlock(sy_rwlock_t *rwlock);
extern int sy_rwlock_wrlock(sy_rwlock_t *rwlock);
extern int sy_rwlock_trywrlock(sy_rwlock_t *rwlock);
extern void sy_rwlock_unlock(sy_rwlock_t *rwlock);

/* mem.c */
#define ypadsize(size) \
((size) + ((size) % 8))

#if 1
typedef struct {
        sy_spinlock_t lock;
        struct list_head list;
        int len;
        int inited;
        int max;
        size_t size;
        size_t align;
        sem_t sem;
} mpool_t;

int mpool_init(mpool_t *mpool, size_t align, size_t size, int max);
int mpool_get(mpool_t *mpool, void **ptr);
int mpool_put(mpool_t *mpool, void *ptr);
#endif

extern int ymalloc(void **ptr, size_t size);
int ymalloc2(void **_ptr, size_t size);
extern int ymalign(void **_ptr, size_t align, size_t size);
extern int yrealloc(void **_ptr, size_t size, size_t newsize);
extern int yfree(void **ptr);

/* nls.c */
extern int nls_getable(char *charset, struct nls_table **);

/* path.c */
#define YLIB_ISDIR 1
#define YLIB_NOTDIR 0

#define YLIB_DIRCREATE 1
#define YLIB_DIRNOCREATE 0

int cascade_iterator(char *path, void *args, int (*callback)(const char *path, void *args));
extern void rorate_id2path(char *path, uint32_t pathlen, uint32_t pathlevel,
                           const char *id);
extern void cascade_id2path(char *path, uint32_t pathlen, uint64_t id);
extern int cascade_path2idver(const char *path, uint64_t *id,
                              uint32_t *version);
extern int get_dir_level(const char *abs_dir, int *level);
extern int path_normalize(const char *src_path, char *dst_path, size_t dst_size);
extern int path_validate(const char *path, int isdir, int dircreate);
extern int path_validate_for_samba(const char *path, int isdir, int dircreate);
extern int df_iterate_tree(const char *basedir, df_dir_list_t *,
                           void (*func)(void *));
extern int path_getvolume(char *path, int *is_vol, char *vol_name);
int path_access(const char *path);
int path_droptail(char *path, int len);
int path_drophead(char *path);
int rmrf(const char *path);

/* stat.c */
extern void mode_to_permstr(uint32_t mode, char *perms);
extern void stat_to_datestr(struct stat *, char *date);

/* str.c */
extern int str_replace_char(char *str, char from, char to);
extern int str_replace_str(char *str, char *from, char *to);
extern int str_upper(char *str);
extern unsigned int str_octal_to_uint(const char *str);
extern int str_endwith(const char *string, const char *substr);

/*
 * sends at-most the specified # of byets.
 * @retval Returns the result of calling _send() on right or -errno.
 */
extern int sy_send(int sd, const char *buf, uint32_t buflen);
/*
 * Receives at-most the specified # of bytes.
 * @retval Returns the result of calling _recv() on right or -errno.
 */
extern int sy_recv(int sd, char *buf, uint32_t buflen);
/*
 * Peek to see if any data is available. This call will not remove the data
 * from the underlying socket buffers.
 * @retval Returns # of bytes copied in on right or -errno.
 */
extern int sy_peek(int sd, char *buf, uint32_t buflen);
extern int sy_shutdown(int sd);
extern int sy_close(int fd);
extern void sy_close_failok(int fd);
extern void sy_msleep(uint32_t milisec);
extern int sy_isidle();
extern int sy_is_mountpoint(const char *path, long f_type);

/* var.c */
/* args ends with -1 */
extern int varsum(int first, ...);

/* configure.c */
extern int conf_init(const char *conf_path);
extern int nfs_config_init(const char *conf_path);
extern int yftp_config_init(const char *conf_path);
extern int conf_destroy(void);

/* heap.c */
int heap_init(struct heap_t *heap, gt_func gt, heap_drop_func drop,
                heap_print_func print, uint32_t max_element, void *min);
int heap_insert(struct heap_t *heap, void *data);
int heap_pop(struct heap_t *heap, void **data);
void heap_print(struct heap_t *heap);
uint32_t heap_len(struct heap_t *heap);

/* async.c */

int async_init();
int async_push(void *addr, uint32_t len);
void async_print();

int config_import(addr_t *addr, int *_count, const char *pattern);

/* mini_hashtb.c */
extern void *mini_hashtb_meminit(size_t size);
extern int mini_hashtb_init(mini_hashtb_t **hashtb, int (*cmp)(void *, void *), size_t hash(void *),
                     size_t size,size_t maxent, void *mem);
#endif /* __SYSY_LIB_H__ */


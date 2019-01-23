#ifndef LEVELDB_UTIL_H
#define LEVELDB_UTIL_H
#include <dirent.h>

//#include "leveldb/c.h"
#include "sdfs_conf.h"
#include "sdfs_buffer.h"
#include "hash_table.h"

#define USS_PREFIX_LEN              (8)

#define USS_ID2NAME_NAME        SDFS_HOME"/id2name"
#define USS_LEVELDB_HOME        "/var/lib/leveldb/"
#define USS_VOLUMEDB_NAME       "volumedb"

#define USS_DB_MAX_KEY_LEN (MAX_NAME_LEN+USS_PREFIX_LEN)
#define USS_DB_MAX_VAL_LEN (BUF_SIZE_64K)

#define USS_DB_MAX_TASK (1024)
#define MAX_INCR_STEP (1000)
#define CONTAINER_SELF_NAME "."
#define CONTAINER_FAKE_NAME CONTAINER_SELF_NAME

typedef enum {
        TASK_NULL = 0,
        TASK_GET, //mds
        TASK_PUT,
        TASK_DELETE,

        TASK_CDS_READ, //cds
        TASK_CDS_WRITE,
        TASK_CDS_DELETE,
} uss_leveldb_task_op_t;

typedef struct {
        struct list_head hook;
        uss_leveldb_task_op_t op;
        uint64_t id; //db id
        char key[USS_DB_MAX_KEY_LEN];
        int klen;
        //char *val;
        //size_t vlen;
        //buffer_t *buf;
        uint32_t size;
        uint32_t offset;

        struct iovec *iov;
        int iov_count;

        int idx;
        int ret;
        void (*cb)(void *task_self); //callback
        void *arg;
} uss_leveldb_task_t;

typedef struct uss_leveldb {
        uint64_t id; //volid
        char path[MAX_PATH_LEN];

        uint64_t incr_id; //
        uint64_t incr_mark; //
        sy_spinlock_t incr_lock;

        leveldb_t* db;
        leveldb_cache_t *cache;
        leveldb_filterpolicy_t* policy;
        leveldb_comparator_t* cmp;
        leveldb_options_t* options;          /*for create leveldb*/
        leveldb_readoptions_t* roptions;     /*for read leveldb*/
        leveldb_readoptions_t* roptions_notfillcache;     /*for read leveldb, not fill cache*/
        leveldb_writeoptions_t* woptions;    /*for write leveldb*/
        leveldb_writeoptions_t* woptions_sync;    /*for write leveldb*/
        sy_rwlock_t rwlock;
} uss_leveldb_t;

typedef struct uss_leveldbs {
        hashtable_t dbtable;
        pthread_rwlock_t prwlock;
        sem_t incr_sem;
        char home[MAX_PATH_LEN];
} uss_leveldbs_t;

typedef enum {
        VTYPE_NULL = 0,
        VTYPE_INCR,
        VTYPE_XATTR,
        VTYPE_DENT, //dir_entry_t
        VTYPE_MD,
        VTYPE_CHKINFO,
        VTYPE_USERINFO,
        VTYPE_GROUPINFO,
        VTYPE_CIFSSHARE,
        VTYPE_FTPSHARE,
        VTYPE_NFSSHARE,
        VTYPE_DIRQUOTA,
        VTYPE_USERQUOTA,
        VTYPE_GROUPQUOTA,
        VTYPE_FLOCKINFO,
} value_type_t;

typedef struct {
        value_type_t type;
        //uint32_t crc;
        uint32_t len;
        char buf[0];
} value_t;

typedef enum {
        USER_DB,  //save user and group info
        QUOTA_DB, //save quota info
        SHARE_DB, //save share directory info
        XATTR_DB, //save extra attribute info
        FLOCK_DB, //save file range lock info
        INVALID_DB,
}infodb_type_t;

typedef struct {
        uint64_t id;
        uint64_t volid;
}fileid_keyprefix_t;

int uss_leveldb_create(uss_leveldb_t *db, const char *name,
                       int(*cmp_compare)(void* arg, const char* a, size_t alen,
                                         const char* b, size_t blen));
inline void uss_leveldb_encode64(const uint64_t id, char *key);
inline void uss_leveldb_decode64(const char *key, uint64_t *id);
leveldb_readoptions_t *uss_leveldb_readoptions_create(int needCache);
int uss_volumedb_init(const char *name);
uss_leveldb_t *uss_get_leveldb(uint64_t id);
int uss_volumedb_create(uint64_t id);
int uss_leveldb_close(uint64_t id);
int uss_leveldb_init_incr(uint64_t id, uint64_t incr_id);
int uss_leveldb_get_incr(uint64_t id, uint64_t *incr_id);
void uss_leveldb_encodekey(const uint64_t keyid, const char *name, char *key, int *klen);
void uss_leveldb_decodekey(const char *key, uint64_t *keyid, char *name, int size);
void uss_leveldb_prefix(const uint64_t keyid, char *keyprefix);
int uss_leveldb_destroy(const uint64_t id);
int uss_leveldb_vlen(uint64_t id, const char *key, int klen, size_t *_vlen);
int uss_leveldb_exist(uint64_t id, const char *key, int klen, int *_exist);
int uss_leveldb_get(uint64_t id, const char *key, int klen, char *value, int *vlen, value_type_t vtype);
int uss_leveldb_get2(uss_leveldb_t *uss_db, const char *key, int klen,
                             char *_value, int *_vlen, value_type_t vtype);
int uss_leveldb_get1(uss_leveldb_t *db, const char *key, int klen, char *value,
                    int *vlen, value_type_t vtype);
int uss_leveldb_get3(uss_leveldb_t *uss_db, const char *key, int klen, char *_value,
                    int *_vlen, value_type_t vtype);
int uss_leveldb_repair(char *name);
int uss_leveldb_put(uint64_t id, const char *key, int klen, const char *value, int vlen, value_type_t vtype);
int uss_leveldb_put2(uss_leveldb_t *uss_db, const char *key, int klen,
                             const char *value, int vlen, value_type_t vtype);
int uss_leveldb_put2_sync(uss_leveldb_t *uss_db, const char *key, int klen,
                             const char *value, int vlen, value_type_t vtype);
int uss_leveldb_put_raw(uint64_t id, const char *key, int klen, const char *value, int vlen);
int uss_leveldb_iterator(uint64_t id,  int (*callback)(void *arg, const void *k, size_t klen, const void *v, size_t vlen), void *arg);
int uss_leveldb_delete(uint64_t id, const char *key, int klen);
int uss_leveldb_create_snap(uint64_t id, const leveldb_snapshot_t* snap);
int uss_leveldb_release_snap(uint64_t id, const leveldb_snapshot_t* snap);
int uss_leveldb_prefix_scan(uint64_t id, const char *prefix,
                const char *offset, int offset_len, leveldb_iterator_t **_iter);
int uss_leveldb_prefix_count(uint64_t id, const char *prefix, uint64_t *count);
int uss_leveldb_prefix_count_detail(uint64_t id, const char *prefix, uint64_t *, uint64_t *, uint64_t *);
int uss_leveldb_prefix_empty(uint64_t id, const char *prefix, int *empty, const char *skip);
int uss_leveldb_prefix_iterator(uint64_t id, const char *prefix, int (*callback)(void *arg, const void *k, size_t klen, const void *v, size_t vlen, int *finish), void *arg);

leveldb_writebatch_t *uss_leveldb_writebatch_create();
int uss_leveldb_writebatch_put(leveldb_writebatch_t *wb,
                const char *key, int klen, const char *val, int vlen, value_type_t vtype);
int uss_leveldb_writebatch_delete(leveldb_writebatch_t *wb,
                const char *key, int klen);
int uss_leveldb_write(uint64_t id, leveldb_writebatch_t *wb);

int uss_leveldb_task_read(uss_leveldb_task_t *task);
int uss_leveldb_task_write(uss_leveldb_task_t *task);

int uss_infodb_init(void);
int uss_infodb_exist(infodb_type_t db_type, const char *key, int klen, value_type_t vtype, int *exist);
int uss_infodb_get(infodb_type_t db_type, const char *key, int klen,
                   char *value, int *vlen, value_type_t vtype);
int uss_infodb_put(infodb_type_t db_type, const char *key, int klen,
                   const char *value, int vlen, value_type_t vtype);
int uss_infodb_remove(infodb_type_t db_type, const char *key, int klen);

int uss_infodb_prefix_scan(infodb_type_t db_type, const char *prefix,
                           const char *offset, int offset_len,
                           leveldb_iterator_t **_iter);
void uss_leveldb_get_fileid_prefix(IN const fileid_t *fileid,
                                   OUT char *keyprefix, OUT int *keyprefix_len);
void uss_leveldb_fileid_encodekey(IN const fileid_t *fileid, IN const char *name,
                                  OUT char *key, INOUT int *keylen);
void uss_leveldb_decode_fileid(IN const char *key, OUT fileid_t *fileid);
uss_leveldb_t *uss_leveldb_get_infodb(infodb_type_t db_type);

int uss_leveldb_incr_post();
int uss_leveldb_incr_need_update(uint64_t volid,
                uint64_t incr_id, uint64_t incr_mark);

#endif

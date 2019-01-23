#ifndef __XATTR_H__
#define __XATTR_H__

//set xattr flag
#define USS_XATTR_DEFAULT (0)
#define USS_XATTR_CREATE  (1 << 0) /* set value, fail if attr already exists */
#define USS_XATTR_REPLACE (1 << 1) /* set value, fail if attr does not exist */
#define USS_XATTR_INVALID (1 << 31)

typedef struct {
        int (*set)(const fileid_t *fileid, const char *key, const void *value,
                   size_t size, int flags);
        int (*get)(const fileid_t *fileid, const char *key, void *value, size_t *size);
        int (*remove)(const fileid_t *fileid, const char *key);
        int (*list)(const fileid_t *fileid, void *list, size_t *size);
} xattr_op_t;

typedef struct {
        int (*init)(void);
        int (*set)(const fileid_t *fileid, const char *key, const void *value,
                   size_t size, int flags);
        int (*get)(const fileid_t *fileid, const char *key, void *value, size_t *size);
        int (*remove)(const fileid_t *fileid, const char *key);
} xattr_queue_op_t;

extern void xattr_removeall(IN const fileid_t *fileid);
extern int xattr_queue_removeall(IN const fileid_t *fileid);

#endif

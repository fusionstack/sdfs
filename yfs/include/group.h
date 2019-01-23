/*
 *Date   : 2017.07.29
 *Author : jinxx
 */

#ifndef __GROUP_H__
#define __GROUP_H__

#define GROUP_PREFIX     1
#define MAX_GROUP_KEY_LEN  512

typedef struct {
        char gname[MAX_NAME_LEN];
        gid_t gid;
} group_t;

typedef enum {
        GROUP_SET = 0,
        GROUP_GET,
        GROUP_REMOVE,
        GROUP_LIST,
        GROUP_INVALID_OP,
} group_op_type_t;

extern int group_set(const group_t *group);
extern int group_get(const char *name, group_t *group);
extern int group_remove(const char *name);
extern int group_list(group_t **group, int *len);

#endif


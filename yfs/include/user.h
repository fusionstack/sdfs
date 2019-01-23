/*
 *Date   : 2017.07.29
 *Author : jinxx
 */
#ifndef __USER_H__
#define __USER_H__

#define USER_PREFIX     0
#define MAX_USER_KEY_LEN  4096

typedef struct {
        char name[MAX_NAME_LEN];
        char password[MAX_NAME_LEN];
        uid_t uid;
        gid_t gid;
} user_t;

typedef enum {
        USER_SET = 0,
        USER_GET,
        USER_REMOVE,
        USER_LIST,
        USER_INVALID_OP,
} user_op_type_t;

typedef struct {
        user_t user;
        user_op_type_t opt;
} user_op_t;

extern int user_set(const user_t *user);
extern int user_remove(const char *name);
extern int user_get(const char *name, user_t *user);
extern int user_list(user_t **user, int *count);

#endif


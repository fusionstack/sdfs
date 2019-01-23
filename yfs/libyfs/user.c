#include <sys/types.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSLIB

#include "md_lib.h"
#include "net_global.h"
#include "sdfs_lib.h"
#include "dbg.h"
#include "user.h"

int user_set(const user_t *user)
{
        int ret;

        ret = md_set_user(user);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

#if 0
int user_add(const user_op_t *user)
{
        int ret;

        ret = md_user_set(user);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int user_mod(const user_op_t *user)
{
        int ret;

        ret = md_user_set(user);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}
#endif

int user_remove(const char *user_name)
{
        int ret;

        ret = md_remove_user(user_name);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int user_get(const char *user_name, user_t *user)
{
        int ret;

        ret = md_get_user(user_name, user);
        if (ret)
                GOTO(err_ret, ret);

        DBUG("ret[%d] user[%s] gid[%d] pwd[%s]\n", ret, user->name, user->gid, user->password);

        return 0;
err_ret:
        return ret;
}


int user_list(user_t **user, int *count)
{
        int ret;

        ret = md_list_user(user, count);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

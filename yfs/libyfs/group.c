#define DBG_SUBSYS S_YFSLIB

#include "md_lib.h"
#include "dbg.h"
#include "group.h"

int group_set(const group_t *group)
{
        int ret;

        ret = md_set_groupinfo(group);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int group_get(const char *group_name, group_t *group)
{
        int ret;

        ret = md_get_groupinfo(group_name, group);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int group_remove(const char *group_name)
{
        int ret;

        ret = md_remove_groupinfo(group_name);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int group_list(group_t **group, int *len)
{
        int ret;

        ret = md_list_groupinfo(group, len);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}


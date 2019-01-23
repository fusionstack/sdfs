
#define DBG_SUBSYS S_YFSMDC

#include "job_dock.h"
#include "ylib.h"
#include "md_proto.h"
#include "md_lib.h"
#include "md_db.h"
#include "dbg.h"

static kvop_t *kvop = &__kvop__;

typedef struct {
        gid_t gid;
        group_t group;
        int found;
} arg_t;

static void __redis_iter(void *_key, void *_value, void *ctx)
{
        const char *key = _key;
        const group_t *group = _value;
        arg_t *arg = ctx;

        DINFO("scan %s %d -> %d\n", key, group->gid, arg->gid);
        
        if (arg->found)
                return;

        if (group->gid == arg->gid) {
                arg->group = *group;
                arg->found = 1;
        }

        return;
}

int md_get_group_byid(gid_t gid, group_t *group)
{
        int ret;
        arg_t arg;

        arg.gid = gid;
        arg.found = 0;

        ret = kvop->iter(roottype_group, NULL, __redis_iter, &arg);
        if (ret)
                GOTO(err_ret, ret);

        if (arg.found == 0) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        *group = arg.group;
        
        return 0;
err_ret:
        return ret;
}

int md_set_groupinfo(const group_t *group)
{
        int ret;
        group_t old_group;
        
        if (strlen(group->gname) > MAX_NAME_LEN) {
                ret = ENAMETOOLONG;
                GOTO(err_ret, ret);
        }

        ret = md_get_group_byid(group->gid, &old_group);
        if (ret == 0) {
                if (0 != strcmp(group->gname, old_group.gname)) {
                        ret = EEXIST;
                        DERROR("gid %u is exist, group name is %s\n",
                                old_group.gid, old_group.gname);
                        goto err_ret;
                }
        } else if (ENOENT == ret) {
                ret = kvop->create(roottype_group, group->gname, group,
                                   sizeof(*group));
                if (ret)
                        GOTO(err_ret, ret);
        } else
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_get_groupinfo(const char *group_name, group_t *group)
{
        size_t len = sizeof(*group);
        return kvop->get(roottype_group, group_name, group, &len);
}

int md_remove_groupinfo(const char *group_name)
{
        return kvop->remove(roottype_group, group_name);
}

int md_list_groupinfo(group_t **_group, int *count)
{
        int ret, i;
        redisReply *reply, *e1, *v1;
        group_t *group;
        
        reply = kvop->scan(roottype_group, NULL, 0);
        if (reply == NULL || reply->type != REDIS_REPLY_ARRAY) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        e1 = reply->element[1];

        if (e1->elements == 0) {
                *count = 0;
                *_group = NULL;
                goto out;
        }
        
        ret = ymalloc((void **)&group, sizeof(*group) * e1->elements);
        if (ret)
                GOTO(err_ret, ret);
        
        for (i = 0; i < (int)e1->elements; i += 2) {
                v1 = e1->element[i + 1];

                YASSERT(v1->len == sizeof(*group));
                memcpy(&group[i], v1->str, v1->len);
        }

        *count = e1->elements / 2;
        *_group = group;

out:
        freeReplyObject(reply);
        
        return 0;
//err_free:
        //freeReplyObject(reply);
err_ret:
        return ret;
}

#include <sys/types.h>
#include <string.h>

#define DBG_SUBSYS S_YFSMDC

#include "net_global.h"
#include "job_dock.h"
#include "ynet_rpc.h"
#include "ylib.h"
#include "md_proto.h"
#include "redis_util.h"
#include "md_lib.h"
#include "md_db.h"
#include "dbg.h"


static kvop_t *kvop = &__kvop__;

static int is_exist_group(const gid_t gid)
{
        int ret;
        group_t group;

        ret = md_get_group_byid(gid, &group);
        return !ret;
}

int md_set_user(const user_t *new_user)
{
        int ret;
        user_t old_user;
        size_t klen;

        if (strlen(new_user->name) > MAX_NAME_LEN) {
                ret = ENAMETOOLONG;
                GOTO(err_ret, ret);
        }

        klen = MAX_USER_KEY_LEN;
        ret = kvop->get(roottype_user, new_user->name, &old_user, &klen);
        if (ret == 0) {
                //modify userinfo include  uid gid password
                if (new_user->gid != old_user.gid) {
                        if (!is_exist_group(new_user->gid)) {
                                ret = ENOENT;
                                DERROR("set user's gid but the group is "
                                       "not exist (%u) %s\n", new_user->gid,
                                       strerror(ret));
                                GOTO(err_ret, ret);
                        }
                        old_user.gid = new_user->gid;
                }
                if (new_user->uid != old_user.uid)
                        old_user.uid = new_user->uid;
                if (strcmp(new_user->password, old_user.password))
                        strcpy(old_user.password, new_user->password);

                ret = kvop->update(roottype_user, new_user->name, &old_user,
                                   sizeof(user_t));
                if (ret)
                        GOTO(err_ret, ret);
        } else if (ENOENT == ret) {
                //add a user
                DBUG("new user[%s] uid[%u] gid[%u] password[%s]\n",
                      new_user->name, new_user->uid, new_user->gid, new_user->password);
                if (!is_exist_group(new_user->gid)) {
                        ret = ENOENT;
                        DERROR("set user's gid but the group is not exist (%u) %s\n",
                               new_user->gid, strerror(ret));
                        GOTO(err_ret, ret);
                }

                ret = kvop->create(roottype_user, new_user->name, new_user,
                                   sizeof(user_t));
                if (ret)
                        GOTO(err_ret, ret);
        } else
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_get_user(const char *user_name, user_t *user)
{
        size_t len = sizeof(*user);
        return kvop->get(roottype_user, user_name, user, &len);
}

int md_remove_user(const char *user_name)
{
        return kvop->remove(roottype_user, user_name);
}

int md_list_user(user_t **_user, int *count)
{
        int ret, i;
        redisReply *reply, *e1, *v1;
        user_t *user;
        
        reply = kvop->scan(roottype_user, NULL, 0);
        if (reply == NULL || reply->type != REDIS_REPLY_ARRAY) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        e1 = reply->element[1];

        if (e1->elements == 0) {
                *count = 0;
                *_user = NULL;
                goto out;
        }
        
        ret = ymalloc((void **)&user, sizeof(*user) * e1->elements);
        if (ret)
                GOTO(err_free, ret);

        
        for (i = 0; i < (int)e1->elements; i += 2) {
                v1 = e1->element[i + 1];

                YASSERT(v1->len == sizeof(*user));
                memcpy(&user[i], v1->str, v1->len);
        }

        *count = e1->elements / 2;
        *_user = user;

out:
        freeReplyObject(reply);
        
        return 0;
err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

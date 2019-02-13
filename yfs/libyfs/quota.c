/*
 *Date   : 2017.06.20
 *Author : JinagYang
 *quota.c : implement the quota feature for sdfs,
 *          First implement the hard quota for the first level directory
 *          include directory, user and group quota.
 */
#include <pthread.h>

#define DBG_SUBSYS S_YFSLIB
#include "sdfs_list.h"
#include "sdfs_id.h"
#include "cache.h"
#include "sdfs_lib.h"
#include "sdfs_conf.h"
#include "schedule.h"
#include "md_lib.h"
#include "quota.h"
#include "sdfs_quota.h"

typedef struct {
        struct list_head owner_list;
        int count;
        pthread_mutex_t lock;
}quota_owner_list_t;

typedef struct {
        struct list_head entry;
        quota_t quota;
}quota_owner_node_t;

void volid2lvmid(uint64_t volid, fileid_t *dirid)
{
        dirid->id = volid;
        dirid->idx = 0;
        dirid->volid = volid;
}

//owner list for client update quota info from mds
static quota_owner_list_t g_owner_list;

static int __find_quota(const void *q1, const void *q2)
{
        quota_t *quota1 = (quota_t *)q1;
        quota_t *quota2 = (quota_t *)q2;

        if(fileid_cmp(&quota1->quotaid, &quota2->quotaid) == 0 &&
           quota1->uid == quota2->uid &&
           quota1->gid == quota2->gid) {
                return true;
        } else {
                return false;
        }
}

//add quota to owner list which need to update quota info to db
int __quota_mds_add_owner2list(IN const quota_t *quota)
{
        int ret = 0;
        quota_owner_node_t *owner, *pos, *tmp;
        bool find = false;

        YASSERT(NULL != quota);

        ret = pthread_mutex_lock(&g_owner_list.lock);
        if (ret)
                GOTO(err_ret, ret);

        list_for_each_entry_safe(pos, tmp, &g_owner_list.owner_list, entry) {
                if (__find_quota(&pos->quota, quota))  {
                        pos->quota.space_used += quota->space_used;
                        find = true;
                        break;
                }
        }

        if (false == find) {
                ret = ymalloc((void **)&owner, sizeof(quota_owner_node_t));
                if (ret)
                        GOTO(err_lock, ret);

                owner->quota = *quota;
                list_add_tail(&owner->entry, &g_owner_list.owner_list);
                g_owner_list.count++;
        }

        ret = pthread_mutex_unlock(&g_owner_list.lock);
        if (ret)
                GOTO(err_ret, ret);

        return 0;

err_lock:
        pthread_mutex_unlock(&g_owner_list.lock);
err_ret:
        return ret;
}


//mds update quota info to db ervery 10 seconds
static void *__quota_mds_worker(void *arg)
{
        int ret;
        quota_owner_node_t *pos, *tmp;

        (void)arg;
        while (1) {

                ret = pthread_mutex_lock(&g_owner_list.lock);
                if (ret)
                        GOTO(err_ret, ret);

                if (g_owner_list.count > 0) {
                        list_for_each_entry_safe(pos, tmp, &g_owner_list.owner_list, entry) {
                                md_update_quota(&pos->quota);
                                list_del_init(&pos->entry);
                                yfree((void **)&pos);
                        }
                }

                g_owner_list.count = 0;

                ret = pthread_mutex_unlock(&g_owner_list.lock);
                if (ret)
                        GOTO(err_ret, ret);

                sleep(10);
        }

err_ret:
        YASSERT(0);
        return NULL;
}

static int __quota_mds_worker_init(void)
{
        int ret = 0;
        pthread_t thread_id;
        pthread_attr_t ta;

        INIT_LIST_HEAD(&g_owner_list.owner_list);
        g_owner_list.count = 0;

        ret = pthread_mutex_init(&g_owner_list.lock, NULL);
        if (ret)
                GOTO(err_ret, ret);

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);
        ret = pthread_create(&thread_id, &ta, __quota_mds_worker, NULL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int quota_md_init(void)
{
        int ret;

        ret = __quota_mds_worker_init();
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __quota_set(const quota_t *quota)
{
        if(quota == NULL)
                return false;

        if(quota->inode_used || quota->inode_hard ||
                        quota->space_used || quota->space_hard) {
                return true;
        } else {
                return false;
        }
}


static int __increase_inode_for_directory_quota(const fileid_t *start_quotaid)
{
        int i = 0, ret = 0, level = 1;
        fileid_t init_quotaid = *start_quotaid;
        quota_t tmp_quota;
        quota_t last_quota;

        for(i=QUOTA_MAX_LEVEL; i>0; --i) {
                if(init_quotaid.id != QUOTA_NULL) {
                        ret = md_get_quota(&init_quotaid, &tmp_quota, QUOTA_DIR);
                        if(ret)
                                GOTO(err_ret, ret);

                        if(false == __quota_set(&tmp_quota)) {
                                continue;
                        }

                        if(tmp_quota.inode_used < UINT64_MAX)
                                tmp_quota.inode_used++;
                        if((tmp_quota.inode_hard > 0) &&
                                        (tmp_quota.inode_used > tmp_quota.inode_hard)) {
                                if(level > 1) {
                                        last_quota.inode_used--;
                                        md_update_quota(&last_quota);
                                }
                                ret = EDQUOT;
                                goto err_ret;
                        }

                        ret = md_update_quota(&tmp_quota);
                        if(ret)
                                GOTO(err_ret, ret);

                        level++;
                        memcpy(&last_quota, &tmp_quota, sizeof(quota_t));

                        init_quotaid = tmp_quota.pquotaid;
                } else {
                        break;
                }
        }

        return 0;
err_ret:
        return ret;
}

static int __increase_inode_for_group_or_user_quota(
                const uid_t uid,
                const gid_t gid,
                const fileid_t *fileid,
                int quota_type)
{
        int ret;
        quota_t tmp_quota;

        memset(&tmp_quota, 0, sizeof(quota_t));
        tmp_quota.uid = uid;
        tmp_quota.gid = gid;
        tmp_quota.quota_type = quota_type;
        tmp_quota.dirid = *fileid;

        ret = md_get_quota(NULL, &tmp_quota, tmp_quota.quota_type);
        if(ret)
                GOTO(err_ret, ret);

        if(false == __quota_set(&tmp_quota)) {
                goto out;
        }

        if(tmp_quota.inode_used < tmp_quota.inode_hard) {
                tmp_quota.inode_used++;
        }

        ret = md_update_quota(&tmp_quota);
        if(ret)
                GOTO(err_ret, ret);

out:
        return 0;
err_ret:
        return ret;
}

static int __inode_checked(const quota_t *quota)
{
        if(quota == NULL)
                return false;

        if(quota->inode_hard > 0 &&
                        quota->inode_used >= quota->inode_hard) {
                return false;
        } else {
                return true;
        }
}

static int __space_checked(uint64_t space, const quota_t *quota)
{
        uint64_t space_used_tmp = 0;

        if(quota == NULL)
                return false;

        space_used_tmp = quota->space_used;
        if(space_used_tmp < (UINT64_MAX - space)) {
                space_used_tmp += space;
        } else {
                space_used_tmp = UINT64_MAX;
        }

        if(quota->space_hard > 0 &&
                        space_used_tmp >= quota->space_hard) {
                return false;
        } else {
                return true;
        }
}

static int __get_dir_quota(const fileid_t *quotaid, quota_t *_quota)
{
        int ret;
        quota_t quota;

        if(quotaid == 0)
                return 0;

        ret = md_get_quota(quotaid, &quota, QUOTA_DIR);
        if(ret)
                GOTO(err_ret, ret);

        if(_quota != NULL) {
                memcpy(_quota, &quota, sizeof(quota_t));
        }

        return 0;
err_ret:
        return ret;
}

static int __get_group_quota(const gid_t gid, const fileid_t *fileid, quota_t *_quota)
{
        int ret;
        quota_t quota;

        quota.gid = gid;
        quota.dirid = *fileid;
        quota.quota_type = QUOTA_GROUP;

        ret = md_get_quota(NULL, &quota, QUOTA_GROUP);
        if(ret)
                GOTO(err_ret, ret);

        if(_quota != NULL) {
                memcpy(_quota, &quota, sizeof(quota_t));
        }

        return 0;
err_ret:
        return ret;
}

static int __get_user_quota(const uid_t uid, const fileid_t *fileid, quota_t *_quota)
{
        int ret;
        quota_t quota;

        quota.uid = uid;
        quota.dirid = *fileid;
        quota.quota_type = QUOTA_USER;

        ret = md_get_quota(NULL, &quota, QUOTA_USER);
        if(ret) {
                GOTO(err_ret, ret);
        }

        if(_quota != NULL) {
                memcpy(_quota, &quota, sizeof(quota_t));
        }

        return 0;
err_ret:
        return ret;
}

int quota_inode_check_and_inc(const fileid_t *quotaid,
                const uid_t uid,
                const gid_t gid,
                const fileid_t *fileid)
{
        int ret;
        fileid_t lvmid;
        quota_t dir_quota, group_quota, user_quota;

        volid2lvmid(fileid->volid, &lvmid);

        memset(&dir_quota, 0, sizeof(quota_t));
        memset(&group_quota, 0, sizeof(quota_t));
        memset(&user_quota, 0, sizeof(quota_t));

        ret = __get_dir_quota(quotaid, &dir_quota);
        if(ret && ret != ENOENT)
                GOTO(err_ret, ret);

        ret = __get_group_quota(gid, &lvmid, &group_quota);
        if(ret && ret != ENOENT)
                GOTO(err_ret, ret);

        ret = __get_user_quota(uid, &lvmid, &user_quota);
        if(ret && ret != ENOENT)
                GOTO(err_ret, ret);

        if(__quota_set(&dir_quota)) {
                if(__inode_checked(&dir_quota)) {
                        if(__quota_set(&group_quota)) {
                                if(__inode_checked(&group_quota)) {
                                        if(__quota_set(&user_quota)) {
                                                if(__inode_checked(&user_quota)) {
                                                        ret = __increase_inode_for_directory_quota(quotaid);
                                                        if(ret)
                                                                GOTO(err_ret, ret);

                                                        ret = __increase_inode_for_group_or_user_quota(0, gid, &lvmid, QUOTA_GROUP);
                                                        if(ret)
                                                                GOTO(err_ret, ret);

                                                        ret = __increase_inode_for_group_or_user_quota(uid, 0, &lvmid, QUOTA_USER);
                                                        if(ret)
                                                                GOTO(err_ret, ret);
                                                } else {
                                                        /* 用户配额检查失败 */
                                                        ret = EDQUOT;
                                                        GOTO(err_ret, ret);
                                                }
                                        } else {
                                                ret = __increase_inode_for_directory_quota(quotaid);
                                                if(ret)
                                                        GOTO(err_ret, ret);

                                                /* group */
                                                ret = __increase_inode_for_group_or_user_quota(0, gid, &lvmid, QUOTA_GROUP);
                                                if(ret)
                                                        GOTO(err_ret, ret);
                                        }
                                } else {
                                        /* 用户组配额检查失败 */
                                        ret = EDQUOT;
                                        GOTO(err_ret, ret);
                                }
                        } else {
                                if(__quota_set(&user_quota)) {
                                        if(__inode_checked(&user_quota)) {
                                                ret = __increase_inode_for_directory_quota(quotaid);
                                                if(ret)
                                                        GOTO(err_ret, ret);

                                                /* user */
                                                ret = __increase_inode_for_group_or_user_quota(uid, 0, &lvmid, QUOTA_USER);
                                                if(ret)
                                                        GOTO(err_ret, ret);
                                        } else {
                                                /* 用户配额检查失败 */
                                                ret = EDQUOT;
                                                GOTO(err_ret, ret);
                                        }
                                } else {
                                        ret = __increase_inode_for_directory_quota(quotaid);
                                        if(ret)
                                                GOTO(err_ret, ret);
                                }
                        }
                } else {
                        /* 目录配额检查失败 */
                        ret = EDQUOT;
                        GOTO(err_ret, ret);
                }
        } else {
                if(__quota_set(&group_quota)) {
                        if(__inode_checked(&group_quota)) {
                                if(__quota_set(&user_quota)) {
                                        if(__inode_checked(&user_quota)) {
                                                /* group */
                                                ret = __increase_inode_for_group_or_user_quota(0, gid, &lvmid, QUOTA_GROUP);
                                                if(ret)
                                                        GOTO(err_ret, ret);

                                                /* user */
                                                ret = __increase_inode_for_group_or_user_quota(uid, 0, &lvmid, QUOTA_USER);
                                                if(ret)
                                                        GOTO(err_ret, ret);

                                        } else {
                                                /* 用户配额检查失败 */
                                                ret = EDQUOT;
                                                GOTO(err_ret, ret);
                                        }
                                } else {
                                        /* group */
                                        ret = __increase_inode_for_group_or_user_quota(0, gid, &lvmid, QUOTA_GROUP);
                                        if(ret)
                                                GOTO(err_ret, ret);
                                }
                        } else {
                                /* 用户组检查失败 */
                                ret = EDQUOT;
                                GOTO(err_ret, ret);
                        }
                } else {
                        if(__quota_set(&user_quota)) {
                                if(__inode_checked(&user_quota)) {
                                        /* user */
                                        ret = __increase_inode_for_group_or_user_quota(uid, 0, &lvmid, QUOTA_USER);
                                        if(ret)
                                                GOTO(err_ret, ret);
                                } else {
                                        ret = EDQUOT;
                                        GOTO(err_ret, ret);
                                }
                        } else {
                                //do nothing
                                goto out;
                        }
                }
        }

out:
        return 0;
err_ret:
        return ret;
}

static int __increase_space_for_directory_quota(const fileid_t *start_quotaid,
                const uint64_t space)
{
        int i = 0, ret = 0, level = 1;
        fileid_t init_quotaid = *start_quotaid;
        quota_t tmp_quota, last_quota;
        uint64_t space_used_tmp = 0;

        for(i=QUOTA_MAX_LEVEL; i>0; --i) {
                if(init_quotaid.id != QUOTA_NULL) {
                        //DBUG("next space check quotaid:%llu, i:%d\n", (LLU)init_quotaid, i);
                        ret = md_get_quota(&init_quotaid,
                                        &tmp_quota, QUOTA_DIR);
                        if(ret)
                                GOTO(err_ret, ret);

                        if(false == __quota_set(&tmp_quota)) {
                                //DBUG("quotaid:%llu not set, i:%d\n", (LLU)init_quotaid, i);
                                break;
                                //continue;
                        }

                        space_used_tmp = tmp_quota.space_used;
                        if(space_used_tmp < (UINT64_MAX - space))
                                space_used_tmp += space;
                        else
                                space_used_tmp = UINT64_MAX;

                        //DBUG("quotaid:%llu tmp_quota.space_hard:%llu, used:%llu, i:%d\n",
                        //(LLU)init_quotaid, (LLU)tmp_quota.space_hard, (LLU)space_used_tmp, i);
                        if((tmp_quota.space_hard > 0) &&
                                        (space_used_tmp > tmp_quota.space_hard)) {
                                if(level > 1) {
                                        last_quota.space_used -= space;
                                        ret = md_update_quota(&last_quota);
                                        if (ret)
                                                GOTO(err_ret, ret);
                                }
                                ret = EDQUOT;
                                goto err_ret;
                        }

                        tmp_quota.space_used = space_used_tmp;

                        ret = md_update_quota(&tmp_quota);
                        if(ret)
                                GOTO(err_ret, ret);

                        level++;
                        memcpy(&last_quota, &tmp_quota, sizeof(quota_t));

                        /* ret = md_update_quota(&tmp_quota); */
                        /* if(ret) */
                                /* GOTO(err_ret, ret); */

                        init_quotaid = tmp_quota.pquotaid;
                } else {
                        break;
                }
        }

        return 0;
err_ret:
        return ret;
}

static int __increase_space_for_group_or_user_quota(const uid_t uid,
                const gid_t gid, const fileid_t *fileid,
                const int quota_type, const uint64_t space)
{
        int ret;
        quota_t tmp_quota;
        uint64_t space_used_tmp = 0;

        memset(&tmp_quota, 0, sizeof(quota_t));
        tmp_quota.uid = uid;
        tmp_quota.gid = gid;
        tmp_quota.quota_type = quota_type;
        tmp_quota.dirid = *fileid;

        ret = md_get_quota(QUOTA_NULL, &tmp_quota, tmp_quota.quota_type);
        if(ret)
                GOTO(err_ret, ret);

        if(false == __quota_set(&tmp_quota)) {
                goto out;
        }

        space_used_tmp = tmp_quota.space_used;
        if(space_used_tmp < (UINT64_MAX - space)) {
                space_used_tmp += space;
        } else {
                space_used_tmp = UINT64_MAX;
        }

        tmp_quota.space_used = space_used_tmp;


        ret = md_update_quota(&tmp_quota);
        if(ret)
                GOTO(err_ret, ret);

out:
        return 0;
err_ret:
        return ret;
}

int quota_space_check_and_inc(const fileid_t *quotaid,
                const uid_t uid,
                const gid_t gid,
                const fileid_t *fileid,
                const uint64_t space)
{
        int ret;
        fileid_t lvmid;
        quota_t dir_quota, group_quota, user_quota;

        volid2lvmid(fileid->volid, &lvmid);

        memset(&dir_quota, 0, sizeof(quota_t));
        memset(&group_quota, 0, sizeof(quota_t));
        memset(&user_quota, 0, sizeof(quota_t));

        ret = __get_dir_quota(quotaid, &dir_quota);
        if(ret && ret != ENOENT)
                GOTO(err_ret, ret);

        ret = __get_group_quota(gid, &lvmid, &group_quota);
        if(ret && ret != ENOENT)
                GOTO(err_ret, ret);

        ret = __get_user_quota(uid, &lvmid, &user_quota);
        if(ret && ret != ENOENT)
                GOTO(err_ret, ret);

        if(__quota_set(&dir_quota)) {
                if(__space_checked(space, &dir_quota)) {
                        if(__quota_set(&group_quota)) {
                                if(__space_checked(space, &group_quota)) {
                                        if(__quota_set(&user_quota)) {
                                                if(__space_checked(space, &user_quota)) {
                                                        ret = __increase_space_for_directory_quota(quotaid, space);
                                                        if(ret)
                                                                GOTO(err_ret, ret);

                                                        ret = __increase_space_for_group_or_user_quota(0,
                                                                        gid, &lvmid, QUOTA_GROUP, space);
                                                        if(ret)
                                                                GOTO(err_ret, ret);

                                                        ret = __increase_space_for_group_or_user_quota(uid,
                                                                        0, &lvmid, QUOTA_USER, space);
                                                        if(ret)
                                                                GOTO(err_ret, ret);
                                                } else {
                                                        /* 用户配额检查失败 */
                                                        ret = EDQUOT;
                                                        GOTO(err_ret, ret);
                                                }
                                        } else {
                                                ret = __increase_space_for_directory_quota(quotaid, space);
                                                if(ret)
                                                        GOTO(err_ret, ret);

                                                /* group */
                                                ret = __increase_space_for_group_or_user_quota(0,
                                                                gid, &lvmid, QUOTA_GROUP, space);
                                                if(ret)
                                                        GOTO(err_ret, ret);
                                        }
                                } else {
                                        /* 用户组配额检查失败 */
                                        ret = EDQUOT;
                                        GOTO(err_ret, ret);
                                }
                        } else {
                                if(__quota_set(&user_quota)) {
                                        if(__space_checked(space, &user_quota)) {
                                                ret = __increase_space_for_directory_quota(quotaid, space);
                                                if(ret)
                                                        GOTO(err_ret, ret);

                                                /* user */
                                                ret = __increase_space_for_group_or_user_quota(uid,
                                                                0, &lvmid, QUOTA_USER, space);
                                                if(ret)
                                                        GOTO(err_ret, ret);
                                        } else {
                                                /* 用户配额检查失败 */
                                                ret = EDQUOT;
                                                GOTO(err_ret, ret);
                                        }
                                } else {
                                        ret = __increase_space_for_directory_quota(quotaid, space);
                                        if(ret)
                                                GOTO(err_ret, ret);
                                }
                        }
                } else {
                        /* 目录配额检查失败 */
                        ret = EDQUOT;
                        GOTO(err_ret, ret);
                }
        } else {
                if(__quota_set(&group_quota)) {
                        if(__space_checked(space, &group_quota)) {
                                if(__quota_set(&user_quota)) {
                                        if(__space_checked(space, &user_quota)) {
                                                /* group */
                                                ret = __increase_space_for_group_or_user_quota(0,
                                                                gid, &lvmid, QUOTA_GROUP, space);
                                                if(ret)
                                                        GOTO(err_ret, ret);

                                                /* user */
                                                ret = __increase_space_for_group_or_user_quota(uid,
                                                                0, &lvmid, QUOTA_USER, space);
                                                if(ret)
                                                        GOTO(err_ret, ret);

                                        } else {
                                                /* 用户配额检查失败 */
                                                ret = EDQUOT;
                                                GOTO(err_ret, ret);
                                        }
                                } else {
                                        /* group */
                                        ret = __increase_space_for_group_or_user_quota(0,
                                                        gid, &lvmid, QUOTA_GROUP, space);
                                        if(ret)
                                                GOTO(err_ret, ret);
                                }
                        } else {
                                /* 用户组检查失败 */
                                ret = EDQUOT;
                                GOTO(err_ret, ret);
                        }
                } else {
                        if(__quota_set(&user_quota)) {
                                if(__space_checked(space, &user_quota)) {
                                        /* user */
                                        ret = __increase_space_for_group_or_user_quota(uid,
                                                        0, &lvmid, QUOTA_USER, space);
                                        if(ret)
                                                GOTO(err_ret, ret);
                                } else {
                                        ret = EDQUOT;
                                        GOTO(err_ret, ret);
                                }
                        } else {
                                //do nothing
                                goto out;
                        }
                }
        }

out:
        return 0;
err_ret:
        return ret;
}

//decrease inode_used of quota  for quota_type, not need check
static int __decrease_inode_for_directory_qutoa(const fileid_t *start_quotaid)
{
        int i = 0, ret = 0;
        fileid_t init_quotaid = *start_quotaid;
        quota_t tmp_quota;

        for(i=QUOTA_MAX_LEVEL; i>0; --i) {
                if(init_quotaid.id != QUOTA_NULL) {
                        ret = md_get_quota(&init_quotaid,
                                        &tmp_quota, QUOTA_DIR);
                        if(ret)
                                GOTO(err_ret, ret);

                        if(false == __quota_set(&tmp_quota)) {
                                continue;
                        }

                        if(tmp_quota.inode_used > 0)
                                tmp_quota.inode_used--;

                        ret = md_update_quota(&tmp_quota);
                        if(ret)
                                GOTO(err_ret, ret);

                        init_quotaid = tmp_quota.pquotaid;
                } else {
                        break;
                }
        }

        return 0;
err_ret:
        return ret;
}

static int __decrease_inode_for_user_or_group_quota(const uid_t uid,
                const gid_t gid,
                const fileid_t *lvmid,
                int quota_type)
{
        int ret;
        quota_t tmp_quota;

        memset(&tmp_quota, 0, sizeof(quota_t));
        tmp_quota.uid = uid;
        tmp_quota.gid = gid;
        tmp_quota.dirid = *lvmid;
        tmp_quota.quota_type = quota_type;

        ret = md_get_quota(NULL, &tmp_quota, quota_type);
        if(ret)
                GOTO(err_ret, ret);

        if(false == __quota_set(&tmp_quota)) {
                goto out;
        }

        if(tmp_quota.inode_used > 0) {

                tmp_quota.inode_used--;
                ret = md_update_quota(&tmp_quota);
                if(ret)
                        GOTO(err_ret, ret);
        }

out:
        return 0;
err_ret:
        return ret;
}

//decrease inode_used of quota for all quota_types if need
int quota_inode_dec(const fileid_t *quotaid,
                    const uid_t uid,
                    const gid_t gid,
                    const fileid_t *fileid)
{
        int ret = 0;
        fileid_t lvmid;

        volid2lvmid(fileid->volid, &lvmid);

        ret = __decrease_inode_for_directory_qutoa(quotaid);
        if(ret)
                GOTO(err_ret, ret);

        ret = __decrease_inode_for_user_or_group_quota(uid,
                        gid, &lvmid, QUOTA_GROUP);
        if(ret)
                GOTO(err_ret, ret);

        ret = __decrease_inode_for_user_or_group_quota(uid,
                        gid, &lvmid, QUOTA_USER);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static int __decrease_space_for_directory_quota(const fileid_t *start_quotaid,
                const uint64_t space)
{
        int i = 0, ret = 0;
        fileid_t init_quotaid = *start_quotaid;
        quota_t tmp_quota;
        uint64_t space_used_tmp = 0;

        for(i=QUOTA_MAX_LEVEL; i>0; --i) {
                if(init_quotaid.id != QUOTA_NULL) {
                        ret = md_get_quota(&init_quotaid,
                                           &tmp_quota, QUOTA_DIR);
                        if(ret)
                                GOTO(err_ret, ret);

                        if(false == __quota_set(&tmp_quota)) {
                                continue;
                        }

                        space_used_tmp = tmp_quota.space_used;
                        if(space_used_tmp < space) {
                                space_used_tmp = 0;
                        } else {
                                space_used_tmp -= space;
                        }

                        tmp_quota.space_used = space_used_tmp;

                        ret = md_update_quota(&tmp_quota);
                        if(ret)
                                GOTO(err_ret, ret);

                        init_quotaid = tmp_quota.pquotaid;
                } else {
                        break;
                }
        }

        return 0;
err_ret:
        return ret;
}

//decrease space_used(sapce_used -= space) of quota for quota_type, not need check
static int __decrease_space_for_user_or_group_quota(const uid_t uid,
                const gid_t gid,
                const fileid_t *lvmid,
                const int quota_type,
                const uint64_t space)
{
        int ret = 0;
        quota_t tmp_quota;

        memset(&tmp_quota, 0, sizeof(quota_t));
        tmp_quota.uid = uid;
        tmp_quota.gid = gid;
        tmp_quota.dirid = *lvmid;
        tmp_quota.quota_type = quota_type;

        ret = md_get_quota(NULL, &tmp_quota, quota_type);
        if(ret)
                GOTO(err_ret, ret);

        if(false == __quota_set(&tmp_quota)) {
                goto out;
        }

        if(tmp_quota.space_used < space) {
                tmp_quota.space_used = 0;
        } else {
                tmp_quota.space_used -= space;
        }

        ret = md_update_quota(&tmp_quota);
        if(ret)
                GOTO(err_ret, ret);

out:
        return 0;
err_ret:
        return ret;
}

//decrease space_used of quota for all quota_types if need
int quota_space_dec(const fileid_t *quotaid,
                const uid_t uid,
                const gid_t gid,
                const fileid_t *fileid,
                const uint64_t space)
{
        int ret = 0;
        fileid_t lvmid;

        volid2lvmid(fileid->volid, &lvmid);

        ret = __decrease_space_for_directory_quota(quotaid, space);
        if(ret)
                GOTO(err_ret, ret);

        ret = __decrease_space_for_user_or_group_quota(uid,
                        gid, &lvmid, QUOTA_GROUP, space);
        if(ret && ret != ENOENT)
                GOTO(err_ret, ret);

        ret = __decrease_space_for_user_or_group_quota(uid,
                        gid, &lvmid, QUOTA_USER, space);
        if(ret && ret != ENOENT)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;

}

int quota_chown(const fileid_t *fileid, IN uid_t new_uid, IN gid_t new_gid)
{
        int ret = 0;
        uid_t old_uid = 0;
        gid_t old_gid = 0;
        uint64_t size = 0;
        md_proto_t *md;
        char buf[MAX_BUF_LEN];

        md = (md_proto_t *)buf;

        ret = md_getattr(NULL, fileid, md);
        if (ret)
                GOTO(err_ret, ret);
        
        
        size = md->at_size;
        old_uid = md->at_uid;
        old_gid = md->at_gid;

        //uid == 0 or gid == 0 is directory quota, not need update quota informantion
        if (old_uid != new_uid) {
                if (0 != old_uid) {
                        ret = __decrease_inode_for_user_or_group_quota(old_uid,
                                        0, &md->fileid, QUOTA_USER);
                        if(ret)
                                GOTO(err_ret, ret);

                        ret = __decrease_space_for_user_or_group_quota(old_uid,
                                        0, &md->fileid, QUOTA_USER, size);
                        if(ret)
                                GOTO(err_ret, ret);
                }

                if (0 != new_uid) {
                        ret = __increase_inode_for_group_or_user_quota(new_uid,
                                        0, &md->fileid, QUOTA_USER);
                        if(ret)
                                GOTO(err_ret, ret);

                        ret = __increase_space_for_group_or_user_quota(new_uid,
                                        0, &md->fileid, QUOTA_USER, size);
                        if(ret)
                                GOTO(err_ret, ret);
                }
        }

        if (old_gid != new_gid) {
                if (0 != old_gid) {
                        ret = __decrease_inode_for_user_or_group_quota(0,
                                        old_gid, &md->fileid, QUOTA_GROUP);
                        if(ret)
                                GOTO(err_ret, ret);

                        ret = __decrease_space_for_user_or_group_quota(0,
                                        old_gid, &md->fileid, QUOTA_GROUP, size);
                        if(ret)
                                GOTO(err_ret, ret);
                }

                if (0 != new_gid) {
                        ret = __increase_inode_for_group_or_user_quota(0,
                                        new_gid, &md->fileid, QUOTA_GROUP);
                        if(ret)
                                GOTO(err_ret, ret);

                        ret = __increase_space_for_group_or_user_quota(0,
                                        new_gid, &md->fileid, QUOTA_GROUP, size);
                        if(ret)
                                GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

/*
 *Date : 2017.08.24
 *Author : JiangYang
 *quota_key_compare : quota key compare for key sort save in db
 */

int quota_key_compare(void* arg, const char* a, size_t alen,
                const char* b, size_t blen)
{
        (void)arg;
        int n = (alen < blen) ? alen : blen;
        int r = memcmp(a, b, n);
        if(r == 0) {
                if(alen < blen) r = -1;
                else if(alen > blen) r = +1;
        }

        return r;
}

void quota_removeall(const fileid_t *dirid, const fileid_t *quotaid)
{
        int is_lvm = 0;
        quota_t quota;

        if(dirid->id == dirid->volid) {
                is_lvm = 1;
        }

        memset(&quota, 0, sizeof(quota_t));
        if(is_lvm == 0) {
                /* quota参数对于directory配额无意义, 只是为了实现通用接口 */
                if(quota_should_be_remove(quotaid, dirid, &quota) == 0) {
                        md_remove_quota(quotaid, &quota);
                }
        } else if(is_lvm) {
                quota_remove_lvm(dirid, QUOTA_GROUP);
                quota_remove_lvm(dirid, QUOTA_USER);
        }

        return;
}

int quota_check_dec(const fileid_t *fileid)
{
        int ret;
        md_proto_t *md;
        char buf[MAX_BUF_LEN];

        md = (md_proto_t *)buf;

        ret = md_getattr(NULL, fileid, md);
        if (ret)
                GOTO(err_ret, ret);

        if (md->quotaid.id == QUOTA_NULL) {
                return 0;
        }
        
        if (S_ISREG(md->at_mode) &&  md->at_nlink == 1) {
                quota_space_dec(&md->quotaid, md->at_uid, md->at_gid,
                                &md->fileid, md->at_size);

                quota_inode_dec(&md->quotaid, md->at_uid, md->at_gid, &md->fileid);
        } else if (S_ISDIR(md->at_mode)) {
                quota_inode_dec(&md->quotaid, md->at_uid, md->at_gid, &md->fileid);
        }

        return 0;
err_ret:
        return ret;
}

int quota_inode_increase(const fileid_t *fileid, const setattr_t *setattr)
{
        int ret;
        fileid_t quotaid;
        md_proto_t *md;
        char buf[MAX_BUF_LEN];

        md = (void *)buf;
        ret = md_getattr(NULL, fileid, md);
        if (ret) {
                GOTO(err_ret, ret);
        }

        quotaid = md->quotaid;
        if (quotaid.id != QUOTA_NULL) {
                DINFO("parent:"CHKID_FORMAT", quotaid:" CHKID_FORMAT" \n",
                      CHKID_ARG(fileid), CHKID_ARG(&quotaid));
                ret = quota_inode_check_and_inc(&quotaid, setattr->uid.val,
                                                setattr->gid.val, fileid);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int quota_inode_decrease(const fileid_t *fileid, const setattr_t *setattr)
{
        int ret;
        fileid_t quotaid;
        md_proto_t *md;
        char buf[MAX_BUF_LEN];

        md = (void *)buf;
        ret = md_getattr(NULL, fileid, md);
        if (ret) {
                GOTO(err_ret, ret);
        }

        quotaid = md->quotaid;
        if (quotaid.id != QUOTA_NULL) {
                DINFO("parent:"CHKID_FORMAT", quotaid:" CHKID_FORMAT" \n",
                      CHKID_ARG(fileid), CHKID_ARG(&quotaid));
                ret = quota_inode_dec(&quotaid, setattr->uid.val,
                                      setattr->gid.val, fileid);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int quota_space_increase(const fileid_t *fileid, uid_t uid, gid_t gid, uint64_t space)
{
        int ret;
        fileid_t quotaid;
        md_proto_t *md;
        char buf[MAX_BUF_LEN];

        md = (void *)buf;
        ret = md_getattr(NULL, fileid, md);
        if (ret) {
                GOTO(err_ret, ret);
        }

        quotaid = md->quotaid;
        if (quotaid.id != QUOTA_NULL) {
                DINFO("parent:"CHKID_FORMAT", quotaid:" CHKID_FORMAT" \n",
                      CHKID_ARG(fileid), CHKID_ARG(&quotaid));
                ret = quota_space_check_and_inc(&quotaid, uid, gid, fileid, space);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}


int quota_space_decrease(const fileid_t *fileid, uid_t uid, gid_t gid, uint64_t space)
{
        int ret;
        fileid_t quotaid;
        md_proto_t *md;
        char buf[MAX_BUF_LEN];

        md = (void *)buf;
        ret = md_getattr(NULL, fileid, md);
        if (ret) {
                GOTO(err_ret, ret);
        }

        quotaid = md->quotaid;
        if (quotaid.id != QUOTA_NULL) {
                DINFO("parent:"CHKID_FORMAT", quotaid:" CHKID_FORMAT" \n",
                      CHKID_ARG(fileid), CHKID_ARG(&quotaid));
                ret = quota_space_dec(&quotaid, uid, gid, fileid, space);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

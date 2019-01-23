#include <stdio.h>
#include <string.h>

#include "sdfs_conf.h"
#include "sdfs_id.h"
#include "md.h"
#include "redis.h"
#include "sdfs_quota.h"
#include "md_db.h"
#include "md_lib.h"

#define QUOTA_NEW 1

static kvop_t *kvop = &__kvop__;

#if QUOTA_NEW


static int __md_quota_key(const fileid_t *quotaid, const quota_t *quota, fileid_t *_key, int flag)
{
        int ret;
        char key[MAX_NAME_LEN];
        size_t size = sizeof(*_key);
        fileid_t fileid;
        
        if(quota->quota_type == QUOTA_DIR) {
                /* dir_quotaid */
                *_key = *quotaid;
        } else {
                if(quota->quota_type == QUOTA_GROUP) {
                        /* group_volid_gid */
                        
                        snprintf(key, MAX_NAME_LEN, "%s_%llu_%llu",
                                 QUOTA_GROUP_PREFIX, (LLU)quota->dirid.volid, (LLU)quota->gid);
                } else if (quota->quota_type == QUOTA_USER) {
                        snprintf(key, MAX_NAME_LEN, "%s_%llu_%llu",
                                 QUOTA_USER_PREFIX, (LLU)quota->dirid.volid, (LLU)quota->uid);
                } else {
                        UNIMPLEMENTED(__DUMP__);
                }

        retry:
                ret = kvop->get(roottype_quota, key, (void *)&fileid, &size);
                if(ret) {
                        if (ret == ENOENT && (flag & O_CREAT)) {
                                ret = md_attr_getid(&fileid, NULL, ftype_vol, NULL);
                                if(ret)
                                        GOTO(err_ret, ret);

                                YASSERT(fileid.type);
                                ret = kvop->create(roottype_quota, key,
                                                   (void *)&fileid, sizeof(fileid));
                                if(ret) {
                                        if (ret == EEXIST) {
                                                goto retry;
                                        } else
                                                GOTO(err_ret, ret);
                                } else {
                                        GOTO(err_ret, ret);
                                }

                                goto retry;
                        } else {
                                GOTO(err_ret, ret);
                        }
                }

                YASSERT(fileid.type);
                *_key = fileid;
        }        

        return 0;
err_ret:
        return ret;
}

#else

static void __build_quota_key(const fileid_t *quotaid, const quota_t *quota,
                              char *_key, uint32_t *_keylen)
{
        uint32_t keylen = 0;
        char key[MAX_NAME_LEN] = {0};

        if(quota->quota_type == QUOTA_DIR) {
                /* dir_quotaid */
                snprintf(key, MAX_NAME_LEN, "%s_%llu",
                                QUOTA_DIR_PREFIX, (LLU)quotaid->id);
                keylen = strlen(key) + 1;
        } else if(quota->quota_type == QUOTA_GROUP) {
                /* group_volid_gid */
                snprintf(key, MAX_NAME_LEN, "%s_%llu_%llu",
                                QUOTA_GROUP_PREFIX, (LLU)quota->dirid.volid, (LLU)quota->gid);
                keylen = strlen(key) + 1;
        } else if(quota->quota_type ==  QUOTA_USER) {
                /* user_volid_uid */
                snprintf(key, MAX_NAME_LEN, "%s_%llu_%llu",
                                QUOTA_USER_PREFIX, (LLU)quota->dirid.volid, (LLU)quota->uid);
                keylen = strlen(key) + 1;
        }

        if(_key) {
                memcpy(_key, key, keylen);
                *_keylen = keylen;
        }
}
#endif

static int __create_quota(const fileid_t *quotaid,
                INOUT quota_t *quota)
{
        int ret;

#if QUOTA_NEW
        fileid_t fileid;
        
        ret = __md_quota_key(quotaid, quota, &fileid, O_CREAT);
        if(ret)
                GOTO(err_ret, ret);
        
        ret = kset(&fileid, quota, sizeof(*quota), O_EXCL);
        if (ret)
                GOTO(err_ret, ret);
        
#else
        uint32_t keylen = 0;
        char key[MAX_NAME_LEN] = {0}, value[MAX_BUF_LEN] = {0};
        size_t vlen = sizeof(quota_t);

        __build_quota_key(quotaid, quota, key, &keylen);

        /*exists ?*/
        ret = kvop->get(roottype_quota, key, (void *)&value, &vlen);
        if (ret == 0) {
                ret = EEXIST;
                GOTO(err_ret, ret);
        } else if (ret == ENOENT) {
                ret = kvop->create(roottype_quota, key, (void *)quota, sizeof(*quota));
                if (unlikely(ret)) {
                        DERROR("create quota fail, key:%s, ret:%d\n", key, ret);
                        GOTO(err_ret, ret);
                }
        } else {
                GOTO(err_ret, ret);
        }
#endif

        return 0;
err_ret:
        return ret;
}

int md_create_quota(quota_t *quota)
{
        fileid_t quotaid;
        int ret;

        ret = md_attr_getid(&quota->quotaid, NULL, ftype_vol, NULL);
        if(ret)
                GOTO(err_ret, ret);

        quotaid = quota->quotaid;

        //DBUG("===>generate new quotaid : %llu\n", (LLU)quotaid);

        ret = __create_quota(&quotaid, quota);
        if (unlikely(ret))
                GOTO(err_ret, ret);

	ret = md_set_quotaid(&quota->dirid, &quotaid);
        if (unlikely(ret)) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __get_quota(const fileid_t *quotaid,
                INOUT quota_t *quota)
{
        int ret;

#if QUOTA_NEW
        fileid_t fileid;
        
        ret = __md_quota_key(quotaid, quota, &fileid, 0);
        if(ret)
                GOTO(err_ret, ret);
        
        size_t size = sizeof(*quota);
        ret = kget(&fileid, quota, &size);
        if (ret)
                GOTO(err_ret, ret);
#else        
        
        uint32_t keylen = 0;
        char key[MAX_NAME_LEN] = {0};
        size_t vlen = sizeof(quota_t);

        __build_quota_key(quotaid, quota, key, &keylen);

        ret = kvop->get(roottype_quota, key, (void *)quota, &vlen);
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif

        return 0;
err_ret:
        return ret;
}

int md_get_quota(const fileid_t *quotaid, quota_t *quota, quota_type_t quota_type)
{
        quota->quota_type = quota_type;
        return __get_quota(quotaid, quota);
}

static void __quota_limit_set(OUT quota_t *dst__quota,
                IN const quota_t *src__quota,
                IN const uint32_t set_mask)
{
        if (set_mask & SPACE_HARD_BIT)
                dst__quota->space_hard = src__quota->space_hard;

        if (set_mask & SPACE_SOFT_BIT)
                dst__quota->space_soft = src__quota->space_soft;

        if (set_mask & INODE_HARD_BIT)
                dst__quota->inode_hard = src__quota->inode_hard;

        if (set_mask & INODE_SOFT_BIT)
                dst__quota->inode_soft = src__quota->inode_soft;

        return;
}

int md_modify_quota(const fileid_t *quotaid,
                INOUT quota_t *quota,
                const uint32_t modify_mask)
{
        int ret;
#if QUOTA_NEW
        size_t vlen = sizeof(quota_t);
        quota_t tmp_quota;
        fileid_t fileid;
        
        ret = __md_quota_key(quotaid, quota, &fileid, 0);
        if(ret)
                GOTO(err_ret, ret);
        
        
        ret = kget(&fileid, &tmp_quota, &vlen);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        __quota_limit_set(&tmp_quota, quota, modify_mask);

        ret = kset(&fileid, &tmp_quota, sizeof(*quota), 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        
        memcpy(quota, &tmp_quota, sizeof(quota_t));
#else
        size_t vlen = sizeof(quota_t);
        uint32_t keylen = 0;
        char key[MAX_NAME_LEN] = {0};
        quota_t tmp_quota;

        __build_quota_key(quotaid, quota, key, &keylen);

        ret = kvop->get(roottype_quota, key, (void *)&tmp_quota, &vlen);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        /* 是否需要检查修改条件 */
        __quota_limit_set(&tmp_quota, quota, modify_mask);

        ret = kvop->update(roottype_quota, key, (void*)&tmp_quota,
                           sizeof(quota_t));
        if (ret)
                GOTO(err_ret, ret);

        memcpy(quota, &tmp_quota, sizeof(quota_t));
#endif
        

        return 0;
err_ret:
        return ret;
}

int md_update_quota(const quota_t *quota)
{
        int ret;
#if QUOTA_NEW
        fileid_t fileid;
        
        ret = __md_quota_key(&quota->quotaid, quota, &fileid, 0);
        if(ret)
                GOTO(err_ret, ret);
        
        ret = kset(&fileid, quota, sizeof(*quota), 0);
        if (unlikely(ret))
                GOTO(err_ret, ret);
#else
        uint32_t keylen = 0;
        char key[MAX_NAME_LEN] = {0};


        __build_quota_key(&quota->quotaid, quota, key, &keylen);

        ret = klock(&quota->dirid, 10, 1);
        if (ret)
                GOTO(err_ret, ret);
        
        ret = kvop->update(roottype_quota, key, (void*)quota,
                           sizeof(quota_t));
        if (ret)
                GOTO(err_lock, ret);

        ret = kunlock(&quota->dirid);
        if (ret)
                GOTO(err_ret, ret);
#endif

        return 0;
#if !QUOTA_NEW
err_lock:
        kunlock(&quota->dirid);
#endif
err_ret:
        return ret;
}

int quota_should_be_remove(const fileid_t *quotaid,
                const fileid_t *fileid, quota_t *_quota)
{
        int ret = 0;
        quota_t get_quota;

        memset(&get_quota, 0, sizeof(quota_t));

        ret = __get_quota(quotaid, &get_quota);
        if(ret)
                GOTO(err_ret, ret);

        if(fileid_cmp(&get_quota.dirid, fileid) == 0) {
                if(_quota != NULL) {
                        memcpy(_quota, &get_quota, sizeof(quota_t));
                }
        } else {
                ret = ENOKEY;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __quota_remove(const fileid_t *quotaid, const quota_t *quota)
{
        int ret;

#if QUOTA_NEW
        (void) quota;

        fileid_t fileid;
        
        ret = __md_quota_key(quotaid, quota, &fileid, 0);
        if(ret)
                GOTO(err_ret, ret);
        
        ret = kdel(&fileid);
        if(ret)
                GOTO(err_ret, ret);
        
#else
        uint32_t keylen = 0;
        char key[MAX_NAME_LEN];

        __build_quota_key(quotaid, quota, key, &keylen);

        ret = kvop->remove(roottype_quota, key);
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif
 
        return 0;
err_ret:
        return ret;
}

int md_remove_quota(const fileid_t *quotaid, const quota_t *quota)
{
        int ret = 0, quota_type;
        quota_t out_quota;

        quota_type = quota->quota_type;

        //DBUG("===>remove quotaid : %llu\n", (LLU)quotaid);

        if(quota_type == QUOTA_DIR) {
                ret = quota_should_be_remove(quotaid, &quota->dirid, &out_quota);
                if(unlikely(ret))
                        GOTO(err_ret, ret);
        }

        ret = __quota_remove(quotaid, quota);
        if(unlikely(ret))
                GOTO(err_ret, ret);

        if(quota_type == QUOTA_DIR) {
                ret = md_set_quotaid(&out_quota.dirid, &out_quota.pquotaid);
                if(unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

#if !QUOTA_NEW
static int _get_lvm_prefix(const fileid_t *lvmid, char *_prefix, size_t *prefix_len, int quota_type)
{
        char prefix[MAX_NAME_LEN] = {0};
        uint64_t volid = lvmid->volid;

        if(quota_type == QUOTA_GROUP) {
                snprintf(prefix, MAX_NAME_LEN, "%s_%llu*", QUOTA_GROUP_PREFIX, (LLU)volid);
        } else if(quota_type == QUOTA_USER) {
                snprintf(prefix, MAX_NAME_LEN, "%s_%llu*", QUOTA_USER_PREFIX, (LLU)volid);
        }

        *prefix_len = strlen(prefix);
        memcpy(_prefix, prefix, *prefix_len);

        return 0;
}

static void __remove_quota(void *_key, void *_value, void *ctx)
{
        const char *key = _key;
        int ret;

        (void)_value;
        (void)ctx;

        DINFO("scan remove %s\n", key);
        ret = kvop->remove(roottype_quota, key);
        if (unlikely(ret)) {
                DERROR("remove %s fail, ret:%d\n", key, ret);
                YASSERT(0);
        }

        return;
}
#endif

int quota_remove_lvm(const fileid_t *dirid, int quota_type)
{
#if QUOTA_NEW
        (void) dirid;
        (void) quota_type;
        UNIMPLEMENTED(__DUMP__);
        return 0;
#else
        
        int ret = 0;
        size_t prefix_len = 0;
        char prefix[MAX_NAME_LEN] = {0};

        _get_lvm_prefix(dirid, prefix, &prefix_len, quota_type);

        ret = kvop->iter(roottype_quota, prefix, __remove_quota, NULL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
#endif
}

#if 0
int md_list_quota(const quota_t *quota_owner,
                  quota_type_t quota_type,
                  quota_t **quota,
                  int *len)
{
        int ret, reqlen;
        mdp_quota_req_t _req;
        mdp_quota_req_t *req = &_req;
        buffer_t buf;
        void *ptr = NULL;
        buffer_t *reply = NULL;

        reqlen = sizeof(mdp_quota_req_t);
        req->op = MDP_LISTQUOTA;
        req->quota = *quota_owner;
        req->quota_type = quota_type;

        mbuffer_init(&buf, 0);
        ret = rpc_request_wait2("mdc_list_quota", &ng.mds_nh.u.nid,
                                req, reqlen, &buf, MSG_MDP,
                                NIO_NORMAL, _get_timeout());
        if (ret)
                GOTO(err_free, ret);

        reply = &buf;
        if (reply && reply->len > 0) {
                ret = ymalloc(&ptr, reply->len);
                if (ret)
                        GOTO(err_free, ret);

                mbuffer_get(reply, ptr, reply->len);
                *len = reply->len;
        } else {
                ptr = NULL;
                *len = 0;
        }

        *quota = ptr;

        mbuffer_free(&buf);
        return 0;
err_free:
        mbuffer_free(&buf);
        return ret;
}
#endif



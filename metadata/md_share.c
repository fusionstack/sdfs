
/*
*Date   : 2017.07.31
*Author : Yang
*share.c : implement the share feature for sdfs,
*/

#define DBG_SUBSYS S_YFSLIB
#include "sdfs_list.h"
#include "sdfs_id.h"
#include "sdfs_conf.h"
#include "sdfs_lib.h"
#include "md_db.h"
#include "md_lib.h"
#include "sdfs_conf.h"
#include "md_lib.h"

#define SHARE_USER_CHAR  'U'
#define SHARE_GROUP_CHAR 'G'
#define SHARE_HOST_CHAR  'H'
#define SHARE_NAME_PREFIX_LEN 2 //U_ G_ H_
#define SHARE_MAX_KEY_LEN 768 //MAX_NAME_LEN * 3


static kvop_t *kvop = &__kvop__;

static int __md_share_set(const char *key, const shareinfo_t *_shareinfo)
{
        int ret;
        shareinfo_t shareinfo;
        size_t size = sizeof(shareinfo);

        ret = kvop->get(roottype_share, key, &shareinfo, &size);
        if (ret) {
                if (ret == ENOENT) {
                        DINFO("create %s\n", key);
                        ret = kvop->create(roottype_share, key, _shareinfo,
                                           sizeof(*_shareinfo));
                        if (ret)
                                GOTO(err_ret, ret);
                } else {
                        GOTO(err_ret, ret);
                }
        } else {
                if (_shareinfo->usertype) {
                        if (_shareinfo->usertype & SHARE_USER) {
                                strcpy(shareinfo.uname, _shareinfo->uname);
                        }

                        if (_shareinfo->usertype & SHARE_GROUP) {
                                strcpy(shareinfo.gname, _shareinfo->gname);
                        }

                        if (_shareinfo->usertype & SHARE_HOST) {
                                strcpy(shareinfo.hname, _shareinfo->hname);
                        }

                        DINFO("update %s, usertype %x --> %x \n", key,
                              shareinfo.usertype, shareinfo.usertype | _shareinfo->usertype);

                        shareinfo.usertype |= _shareinfo->usertype;
                }

                if (_shareinfo->protocol) {
                        DINFO("update %s, protocol %x --> %x \n", key,
                              shareinfo.protocol, shareinfo.protocol | _shareinfo->protocol);

                        shareinfo.protocol |= _shareinfo->protocol;
                }

                ret = kvop->update(roottype_share, key, &shareinfo, sizeof(shareinfo));
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int md_share_set(const char *key, const shareinfo_t *shareinfo)
{
        int ret;

        ret = kvop->lock(roottype_share);
        if (ret)
                GOTO(err_ret, ret);
        
        ret = __md_share_set(key, shareinfo);
        if (ret)
                GOTO(err_lock, ret);

        ret = kvop->unlock(roottype_share);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_lock:
        kvop->unlock(roottype_share);
err_ret:
        return ret;
}

static int __md_share_remove(const char *key, share_protocol_t prot, share_user_type_t type)
{
        int ret;
        shareinfo_t shareinfo;
        size_t size = sizeof(shareinfo);

        ret = kvop->get(roottype_share, key, &shareinfo, &size);
        if (ret)
                GOTO(err_ret, ret);

        if (shareinfo.protocol == prot && shareinfo.usertype == type) {
                ret = kvop->remove(roottype_share, key);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                shareinfo.protocol ^= prot;
                shareinfo.usertype ^= type;
                
                ret = kvop->update(roottype_share, key, &shareinfo, sizeof(shareinfo));
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int md_share_remove(const char *key, share_protocol_t prot, share_user_type_t type)
{
        int ret;

        ret = kvop->lock(roottype_share);
        if (ret)
                GOTO(err_ret, ret);
        
        ret = __md_share_remove(key, prot, type);
        if (ret)
                GOTO(err_lock, ret);

        ret = kvop->unlock(roottype_share);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_lock:
        kvop->unlock(roottype_share);
err_ret:
        return ret;
}

int md_share_get(const char *key, shareinfo_t *shareinfo)
{
        int ret;
        size_t size = sizeof(*shareinfo);

        ret = kvop->get(roottype_share, key, shareinfo, &size);
        if (ret) {
                GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}

typedef struct {
        char name[MAX_NAME_LEN];
        shareinfo_t *shareinfo;
        share_user_type_t type;
        int found;
} arg_t;

static void __md_share_get_byuser(void *_key, void *_value, void *ctx)
{
        const char *key = _key;
        const shareinfo_t *shareinfo = _value;
        arg_t *arg = ctx;

        DINFO("scan %s, path %s, protocol 0x%x\n", key, shareinfo->path,
              shareinfo->protocol);
        
        if (arg->found)
                return;

        if ((shareinfo->usertype & arg->type) == 0)
                return;

        if (((arg->type == SHARE_USER) && strcmp(shareinfo->uname, arg->name) == 0)
            || ((arg->type == SHARE_GROUP) && strcmp(shareinfo->gname, arg->name) == 0)
            || ((arg->type == SHARE_HOST) && strcmp(shareinfo->gname, arg->name) == 0)) {
                *arg->shareinfo = *shareinfo;
                arg->found = 1;
        }

        return;
}

int md_share_get_byname(const char *name, share_user_type_t type, shareinfo_t *shareinfo)
{
        int ret;
        arg_t arg;

        strcpy(arg.name, name);
        arg.shareinfo = shareinfo;
        arg.type = type;
        arg.found = 0;

        ret = kvop->iter(roottype_share, NULL, __md_share_get_byuser, &arg);
        if (ret)
                GOTO(err_ret, ret);

        if (arg.found == 0) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

typedef struct {
        shareinfo_t *shareinfo;
        int count;
        int max;
        share_protocol_t protocol;
} arg1_t;

static void __md_share_list_byprotocal(void *_key, void *_value, void *ctx)
{
        int ret;
        const char *key = _key;
        const shareinfo_t *shareinfo = _value;
        arg1_t *arg = ctx;

        DINFO("scan %s protocol %x %x\n", key, arg->protocol, shareinfo->protocol);

        if ((arg->protocol & shareinfo->protocol) == 0) {
                return;
        }

        if (arg->count + 1 > arg->max) {
                ret = yrealloc((void **)&arg->shareinfo,
                               sizeof(*arg->shareinfo) * arg->max,
                               sizeof(*arg->shareinfo) * (arg->max + 64));
                if (ret)
                        UNIMPLEMENTED(__WARN__);

                
                arg->max += 64;
        }

        arg->shareinfo[arg->count] = *shareinfo;
        arg->count++;
        
        return;
}

int md_share_list_byprotocal(share_protocol_t protocol, shareinfo_t **_shareinfo, int *count)
{
        int ret;
        arg1_t arg;

        arg.shareinfo = NULL;
        arg.protocol = protocol;
        arg.max = 0;
        arg.count = 0;

        ret = kvop->iter(roottype_share, NULL, __md_share_list_byprotocal, &arg);
        if (ret)
                GOTO(err_ret, ret);

        *_shareinfo = arg.shareinfo;
        *count = arg.count;

        return 0;
err_ret:
        return ret;
}

int md_set_shareinfo(share_protocol_t prot, const void *req_buf, const int req_buflen)
{
        int ret;
        const shareinfo_t *shareinfo = req_buf;
        shareinfo_t _shareinfo = *shareinfo;

        (void) req_buflen;

        if (strlen(shareinfo->share_name) == 0) {
                printf("share name needed\n");
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        _shareinfo.protocol = prot;

        ret = md_share_set(shareinfo->share_name, &_shareinfo);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_get_shareinfo(share_protocol_t prot, const void *req_buf,
                      const int req_buflen, char *rep_buf, uint32_t rep_buflen)
{
        int ret;
        const share_key_t *share_key = req_buf;
        shareinfo_t _shareinfo;

        (void) req_buflen;
        (void) rep_buflen;
        
        YASSERT(strlen(share_key->share_name));
        ret = md_share_get(share_key->share_name, &_shareinfo);
        if (ret)
                GOTO(err_ret, ret);

        if ((_shareinfo.protocol & prot) == 0) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        memcpy(rep_buf, &_shareinfo, sizeof(_shareinfo));
        
        return 0;
err_ret:
        return ret;
}

int md_remove_shareinfo(share_protocol_t prot, const void *req_buf, const int req_buflen)
{
        const share_key_t *share_key = req_buf;

        (void) req_buflen;

        YASSERT(strlen(share_key->share_name));

        return md_share_remove(share_key->share_name, prot, share_key->usertype);
}

#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSMDC

#include "job_dock.h"
#include "net_global.h"
#include "ylib.h"
#include "md_proto.h"
#include "md_lib.h"
#include "md_root.h"
#include "md_db.h"
#include "schedule.h"
#include "dbg.h"

//static dirop_t *dirop = &__dirop__;
//static inodeop_t *inodeop = &__inodeop__;

#define ROOTID "root"

#if 0
#define ROOT_FS "fs"
#endif
#define ROOT_USER "user"
#define ROOT_GROUP "group"
#define ROOT_QUOTA "quota"

static int __inited__ = 0;
static fileid_t __idarray__[roottype_max];
//static const char *__type__[roottype_max] = {"fs", "user", "group", "share", "quota"};
static const char *__type__[roottype_max] = {"user", "group", "share", "quota"};

static int __md_root_create(const char *type, uint64_t volid)
{
        int ret, valuelen;
        fileid_t fileid;

        valuelen = sizeof(fileid);
        ret = etcd_get_bin(ROOTID, type, &fileid, &valuelen, NULL);
        if (ret) {
                if (ret == ENOKEY) {
                        //pass
                } else
                        GOTO(err_ret, ret);
        } else {
                ret = EEXIST;
                GOTO(err_ret, ret);
        }

        //int t = (strcmp(type, ROOT_FS) == 0) ? ftype_vol : ftype_tab;
        int t = ftype_tab;
        ret = md_attr_getid(&fileid, NULL, t, &volid);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        valuelen = sizeof(fileid);
        ret = etcd_create(ROOTID, type, &fileid, valuelen, -1);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int md_root_create(uint64_t volid)
{
        int ret, i;

        for (i = 0; i < roottype_max; i++) {
                ret = __md_root_create(__type__[i], volid);
                if (unlikely(ret)) {
                        if (ret == EEXIST) {
                                //DINFO("%s/%s exist\n", ROOT, ROOT_FS);
                                continue;
                        } else
                                GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

static int __md_root_load_remote(const char *type, fileid_t *_fileid)
{
        int ret, valuelen;
        fileid_t fileid;
        char buf[MAX_BUF_LEN];

        valuelen = sizeof(fileid);
        ret = etcd_get_bin(ROOTID, type, &fileid, &valuelen, NULL);
        if (ret) {
                GOTO(err_ret, ret);
        }

        base64_encode((void *)&fileid, sizeof(fileid), buf);
        ret = maping_set(ROOTID, type, buf);
        if (ret)
                GOTO(err_ret, ret);

        DINFO("load %s "CHKID_FORMAT" remote\n", type, CHKID_ARG(&fileid));
        
        *_fileid = fileid;
        
        return 0;
err_ret:
        return ret;
}


static int __md_root_load(const char *type, fileid_t *_fileid)
{
        int ret, valuelen;
        fileid_t fileid;
        char buf[MAX_BUF_LEN];

        ret = maping_get(ROOTID, type, buf, NULL);
        if (unlikely(ret)) {
                if (ret == ENOENT) {
                        ret = __md_root_load_remote(type, &fileid);
                        if (unlikely(ret))
                                GOTO(err_ret, ret);
                }
        } else {
                valuelen = sizeof(fileid);
                base64_decode(buf, &valuelen, (void *)&fileid);
                DINFO("load %s "CHKID_FORMAT" local\n", type, CHKID_ARG(&fileid));
        }

        *_fileid = fileid;
        YASSERT(fileid.id > 0);
        
        return 0;
err_ret:
        return ret;
}

int md_root_init()
{
        int ret, i;

        YASSERT(__inited__ == 0);

        for (i = 0; i < roottype_max; i++) {
                ret = __md_root_load(__type__[i], &__idarray__[i]);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        __inited__ = 1;
        
        return 0;
err_ret:
        return ret;
}

const fileid_t *md_root_getid(root_type_t type)
{
        YASSERT(__inited__);
        
        return &__idarray__[type];
}

#if 0
int md_root_isroot(const fileid_t *fileid)
{
        YASSERT(__inited__);

        return (fileid_cmp(&__idarray__[roottype_fs], fileid) == 0);
}
#endif

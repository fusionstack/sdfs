
#include <sys/types.h>
#include <sys/stat.h>
#include <rpc/rpc.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <dirent.h>


#define DBG_SUBSYS S_YNFS

#include "yfs_conf.h"
#include "ynfs_conf.h"
#include "network.h"

#include "attr.h"
#include "error.h"
#include "nfs_events.h"
#include "nfs_job_context.h"
#include "net_global.h"
#include "nfs_conf.h"
#include "nfs_events.h"
#include "nfs_state_machine.h"
#include "readdir.h"
#include "sunrpc_proto.h"
#include "sunrpc_reply.h"
#include "sdfs_lib.h"
#include "xdr_nfs.h"
#include "configure.h"
#include "sdfs_lib.h"
#include "yfs_limit.h"
#include "nfs_proc.h"
#include "dbg.h"

static int __sdfs_dir_itor1(const dirid_t *dirid, func1_t func, void *ctx)
{
        int ret;
        off_t offset = 0;
        void *de0 = NULL;
        int delen = 0;
        struct dirent *de;
        uint64_t count;
        
        while (srv_running) {
                ret = sdfs_readdir1(dirid, offset, &de0, &delen);
                if (ret) {
                        GOTO(err_ret, ret);
                }

                if (delen == 0) {
                        break;
                }
                
                dir_for_each(de0, delen, de, offset) {
                        func(de, ctx);
                        offset = de->d_off;
                        count ++;

                        DBUG("%s offset %ju %p\n", de->d_name, offset, de);
                }

                yfree((void **)&de0);
                
                if (offset == 0) {
                        break;
                }
        }

        return 0;
err_ret:
        return ret;
}

static void __nfs_remove_file(void *_de, void *_arg)
{
        int ret;
        struct dirent *de = _de;
        const dirid_t *parent = _arg;

        DINFO("remove %s @ "CHKID_FORMAT"\n", de->d_name, CHKID_ARG(parent));
        
        ret = sdfs_unlink(parent, de->d_name);
        if (ret) {
                DWARN("remove %s @ "CHKID_FORMAT" fail\n", de->d_name, CHKID_ARG(parent));
        }
}

static void __nfs_remove_bytime(void *_de, void *_arg)
{
        int ret;
        struct dirent *de = _de;
        time_t t, now;
        const dirid_t *parent = _arg;
        dirid_t dirid;

        now = time(NULL);
        t = atoi(de->d_name);
        if (now - t < 3600) {
                DINFO("name %s diff %u, skip it\n", de->d_name, now - t);
                return;
        }

        DINFO("name %s diff %u, remove it\n", de->d_name, now - t);

        ret = sdfs_lookup(parent, de->d_name, &dirid);
        if (ret) {
                DWARN("lookup %s @ "CHKID_FORMAT" fail\n", de->d_name, CHKID_ARG(parent));
                return;
        }

        ret = __sdfs_dir_itor1(&dirid, __nfs_remove_file, &dirid);
        if (ret) {
                DWARN("cleanup %s @ "CHKID_FORMAT" fail\n", de->d_name, CHKID_ARG(parent));
                return;
        }

        ret = sdfs_rmdir(parent, de->d_name);
        if (ret) {
                DWARN("rmdir %s @ "CHKID_FORMAT" fail\n", de->d_name, CHKID_ARG(parent));
                return;
        }
}

static int __nfs_remove_volume(const char *vol)
{
        int ret;
        dirid_t dirid;
        char path[MAX_PATH_LEN];

        snprintf(path, MAX_PATH_LEN, "/%s/%s", vol, NFS_REMOVED);
        ret = sdfs_lookup_recurive(path, &dirid);
        if (ret)
                GOTO(err_ret, ret);

        ret = __sdfs_dir_itor1(&dirid, __nfs_remove_bytime, &dirid);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

static void *__nfs_remove_worker(void *arg)
{
        int ret, i;
        etcd_node_t *array, *node;

        (void) arg;

        while (1) {
                //sleep(60 * 10);
                sleep(60 * 10);

                ret = etcd_list(ETCD_VOLUME, &array);
                if (ret) {
                        DWARN("list volume fail\n");
                        continue;
                }

                for (i = 0; i < array->num_node; i++) {
                        node = array->nodes[i];
                        DINFO("volume %s\n", node->key);

                        __nfs_remove_volume(node->key);
                }

                free_etcd_node(array);
        }

        return NULL;
}

int nfs_remove_init()
{
        int ret;

        ret = sy_thread_create2(__nfs_remove_worker, NULL, "__conn_worker");
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

static  int __nfs_remove_getvolid(const dirid_t *_dirid, volid_t *volid)
{
        int ret;
        dirid_t dirid, parent;

        YASSERT(_dirid->type);
        
        dirid = *_dirid;
        while (1) {
                ret = sdfs_lookup(&dirid, "..", &parent);
                if (ret)
                        GOTO(err_ret, ret);

                DBUG("parent "CHKID_FORMAT" \n", CHKID_ARG(&parent));
                YASSERT(parent.type);
                
                if (parent.type == ftype_vol) {
                        *volid = parent;
                        break;
                } else {
                        dirid = parent;
                }
        }

        return 0;
err_ret:
        return ret;
}

int nfs_remove(const fileid_t *parent, const char *name)
{
        int ret;
        volid_t volid;
        dirid_t removed, workdir;
        time_t now;
        char tname[MAX_NAME_LEN], _uuid[MAX_NAME_LEN];
        uuid_t uuid;

        ret = __nfs_remove_getvolid(parent, &volid);
        if (ret)
                GOTO(err_ret, ret);

retry:
        ret = sdfs_lookup(&volid, NFS_REMOVED, &removed);
        if (ret) {
                if (ret == ENOENT) {
                        ret = sdfs_mkdir(&volid, NFS_REMOVED, NULL, &removed, 0, 0, 0);
                        if (ret) {
                                if (ret == EEXIST) {
                                        goto retry;
                                } else
                                        GOTO(err_ret, ret);
                        }
                } else
                        GOTO(err_ret, ret);
        }

        now = time(NULL);
        snprintf(tname, MAX_NAME_LEN, "%d", ((int)now / 3600) * 3600);

        ret = sdfs_lookup(&removed, tname, &workdir);
        if (ret) {
                if (ret == ENOENT) {
                        ret = sdfs_mkdir(&removed, tname, NULL, &workdir, 0, 0, 0);
                        if (ret) {
                                if (ret == EEXIST) {
                                        goto retry;
                                } else
                                        GOTO(err_ret, ret);
                        }
                } else 
                        GOTO(err_ret, ret);
        }

        uuid_generate(uuid);
        uuid_unparse(uuid, _uuid);

#if ENABLE_MD_POSIX
        fileid_t fileid;
        ret = sdfs_lookup(parent, name, &fileid);
        if (ret)
                GOTO(err_ret, ret);
#endif
        
        
        DBUG("rename to %s\n", _uuid);
        ret = sdfs_rename(parent, name, &workdir, _uuid);
        if (ret) {
                GOTO(err_ret, ret);
        }

#if ENABLE_MD_POSIX
        struct timespec t;
        clock_gettime(CLOCK_REALTIME, &t);
        sdfs_utime(parent, NULL, &t, &t);
        sdfs_utime(&fileid, NULL, NULL, &t);
#endif
        
        return 0;
err_ret:
        return ret;
}



#define DBG_SUBSYS S_YRPC

#include "ynet_net.h"
#include "configure.h"
#include "shm.h"
#include "net_global.h"
#include "job_dock.h"
#include "rpc_proto.h"
#include "rpc_table.h"
#include "main_loop.h"
#include "net_rpc.h"
#include "ynet_rpc.h"
#include "dbg.h"

int rpc_inited = 0;

int rpc_init(net_proto_t *op, const char *name, int seq, const char *path)
{
        int ret;

#if 0
        if (!op->request_handler) {
                DERROR("you need a request handler, halt\n");

                YASSERT(0);
        }
#endif
        ret = net_init(op);
        if (ret)
                GOTO(err_ret, ret);

        if (strlen(name) >= MAX_NAME_LEN) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        if (path) {
                _strcpy(ng.home, path);
        }

        if (seq != -1)
                ng.seq = seq;

        if (ng.daemon) {
                char key[MAX_NAME_LEN], value[MAX_BUF_LEN];
                sprintf(key, "%s/%s/nid", ng.home, YFS_STATUS_PRE);

                ret = path_validate(key, YLIB_NOTDIR, YLIB_DIRCREATE);
                if (ret)
                        GOTO(err_ret, ret);

                ret = _get_text(key, value, MAX_BUF_LEN);
                if (ret < 0) {
                        ret = -ret;
                        if (ret == ENOENT) {
                                ng.local_nid.id = YNET_NID_NULL;
                        } else
                                GOTO(err_ret, ret);
                } else {
                        str2nid(&ng.local_nid, value);
                        DINFO("load nid %d\n", ng.local_nid.id);
                }
        } else {
                ng.local_nid.id = YNET_NID_NULL;
        }

        _strcpy(ng.name, name);

        ret = jobdock_init(netable_rname);
        if (ret)
                GOTO(err_ret, ret);

        ret = net_rpc_init();
        if (ret)
                GOTO(err_ret, ret);
        
        ret = rpc_table_init("default", &__rpc_table__, 1);
        if (ret)
                GOTO(err_ret, ret);

        rpc_inited = 1;

        return 0;
err_ret:
        return ret;
}

int rpc_destroy(void)
{
        int ret;

        DBUG("wait for net destroy...\n");

        rpc_inited = 0;

        ret = net_destroy();
        if (ret)
                GOTO(err_ret, ret);

        DINFO("net destroyed\n");

        return 0;
err_ret:
        return ret;
}

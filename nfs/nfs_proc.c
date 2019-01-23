

#include <time.h>
#include <unistd.h>
#include <errno.h>

#define DBG_SUBSYS S_YNFS

#include "ylib.h"
#include "nfs_conf.h"
#include "sdfs_lib.h"
#include "../../ynet/sock/sock_buffer.h"
#include "dbg.h"
#include "nfs_proc.h"

#define SLEEP_INTERVAL 5
#define LENSTATE 20
#define STATENUM 3

extern struct sockstate sock_state;
sy_spinlock_t nfs_conf_lock;

void *handler_ynfsstate(void *arg)
{
        int ret;
        (void) arg;
        char sstate[LENSTATE * STATENUM + 1];
        long long unsigned state[STATENUM];
        int logfd;
        time_t counter;

        ret = yroc_create(YNFS_STATE, &logfd);
        if (ret)
                GOTO(err_ret, ret);

        while (srv_running) {
                sleep(SLEEP_INTERVAL);

                counter = time(NULL);
                state[0] = sock_state.bytes_send;
                state[1] = sock_state.bytes_recv;
                state[2] = (LLU) counter;

                snprintf(sstate, LENSTATE*STATENUM+1, "%llu %llu %llu",
                                state[0], state[1], state[2]);

                if (yroc_write(logfd, (void *)sstate, strlen(sstate)) != 0)
                        GOTO(close_logfd, errno);
        }

close_logfd:
        close(logfd);
err_ret:
        return (void *) -1;
}

int __nfs_conf_register(fnotify_callback mod_callback, fnotify_callback del_callback, void *contex)
{
    int ret;
    char path[MAX_PATH_LEN];

    snprintf(path, MAX_PATH_LEN, "%s/etc/exports.conf", SDFS_HOME);
    ret = fnotify_register(path, mod_callback, del_callback, contex);
    if(ret)
            GOTO(err_ret, ret);

    return 0;
err_ret:
    return ret;
}

int __conf_modified(void *contex, uint32_t mask)
{
    int ret;

    (void) contex;
    (void) mask;

    memset(&nfsconf, 0, sizeof(nfsconf));
    ret = sy_spin_lock(&nfs_conf_lock);
    if(ret)
        GOTO(err_ret, ret);

    ret = nfs_config_init(YNFS_CONFIGURE_FILE);
    if(ret)
        GOTO(err_ret, ret);

    {
        int i;
        DINFO("nfsconf.use_export = %d\n", nfsconf.use_export);
        DINFO("nfsconf.export_size = %d\n", nfsconf.export_size);

        for(i=0; i<nfsconf.export_size; ++i)
        {
            DINFO("%s %s %s\n", 
                    nfsconf.nfs_export[i].path, nfsconf.nfs_export[i].ip,
                    nfsconf.nfs_export[i].permision);
        }
    }

    sy_spin_unlock(&nfs_conf_lock);
    return 0;
err_ret:
    return ret;
}

int __conf_delete(void *contex, uint32_t mask)
{
    (void) contex;
    (void) mask;

    DINFO("do nothing\n");
    return 0;
}

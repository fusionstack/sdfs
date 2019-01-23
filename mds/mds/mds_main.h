#ifndef MDS_MAIN_H 
#define MDS_MAIN_H


#include "configure.h"
#include "net_global.h"
#include "job_dock.h"
#include "get_version.h"
#include "ylib.h"
#include "yfsmds_conf.h"
#include "sdfs_lib.h"
#include "ylog.h"
#include "mds.h"
#include "md_lib.h"
#include "dbg.h"
#include "fnotify.h"
#include "sdfs_quota.h"
#include "flock.h"

typedef enum {
        ELECTION_NORMAL,
        ELECTION_INIT,
        ELECTION_SYNC,
        ELECTION_MASTER,
} election_type_t;

typedef struct {
        int daemon;
        int metano;
        election_type_t type;
} mds_args_t;

void mds_monitor_handler(int sig);
void mds_signal_handler(int sig);
int mds_primary();
int mds_init(const char *home, int metano);
int mds_run(void *args);

#endif

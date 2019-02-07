#include <sys/statvfs.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>
#include <semaphore.h>
#include <errno.h>
#include <sys/statfs.h>

#define DBG_SUBSYS S_YFSCDS

#include "configure.h"
#include "md_lib.h"
#include "rpc_proto.h"
#include "cd_proto.h"
#include "cds.h"
#include "cds_lib.h"
#include "cds_volume.h"
#include "job_dock.h"
#include "job_tracker.h"
#include "ylib.h"
#include "yfscds_conf.h"
#include "conn.h"
#include "ylock.h"
#include "nodeid.h"
#include "ynet.h"
#include "yfs_chunk.h"
#include "yfscli_conf.h"
#include "sdfs_lib.h"
#include "proc.h"
#include "disk.h"
#include "replica.h"
#include "schedule.h"
#include "redis.h"
#include "core.h"
#include "io_analysis.h"
#include "net_global.h"
#include "../../cds/diskio.h"
#include "bh.h"
#include "dbg.h"

cds_info_t cds_info;
uint32_t num_cds_read;
uint32_t num_cds_read_done;
uint32_t num_cds_write;
uint32_t num_cds_write_done;

extern int __fence_test_need__;

static int inited;

extern uint32_t zero_crc;
extern struct sockstate sock_state;

#if 0
#define ANALY_AVG_UPDATE_COUNT (3)  //secend

typedef struct {
        uint64_t read_count;
        uint64_t write_count;
        uint64_t read_bytes;
        uint64_t write_bytes;
        time_t last_output;
        sy_spinlock_t lock;

        /* for each seconds */
        time_t last;
        int cur;
        uint64_t readps[ANALY_AVG_UPDATE_COUNT];
        uint64_t writeps[ANALY_AVG_UPDATE_COUNT];
        uint64_t readbwps[ANALY_AVG_UPDATE_COUNT];
        uint64_t writebwps[ANALY_AVG_UPDATE_COUNT];
} io_analy_t;


io_analy_t io_analysis;
#endif

#define LEN_MOUNT_PATH (sizeof (YFS_CDS_DIR_DISK_PRE) + 10)
#define LOG_TYPE_CDS "cds"
#define SLEEP_INTERVAL 5
#define LENSTATE 20
#define STATENUM 5
#define FILE_PATH_LEN 64
#define CDS_LEVELDB_THREAD_NUM 4

extern int __is_cds_cache;

#if 0
/* handler for cds state. */
void * handler_cdsstate(void *arg)
{
        int ret;
	char sstate[LENSTATE * STATENUM + 1];
	char mount_path[LEN_MOUNT_PATH];
	char logfile[16];
	unsigned long long int state[STATENUM];
	struct statfs stat;
	int logfd;
        time_t counter;
        char file[FILE_PATH_LEN];
        FILE *fp;

        (void) arg;

	/* create log file. */
	sprintf(logfile, LOG_TYPE_CDS"_%d", cds_info.diskno);

	ret = yroc_create(logfile, &logfd);
	if (ret)
		GOTO(err_ret, errno) ;

	sprintf(mount_path, YFS_CDS_DIR_DISK_PRE"/%d", cds_info.diskno);

	/*
	  state[0...3] = (disk_used, disk_free, sock_send, sock_recv)
	  not network order.
	 */
	while (srv_running) {
		sleep(SLEEP_INTERVAL);
		if (0 != statfs(mount_path, &stat)) {
			GOTO(close_logfd, errno);
		}

                sprintf(file, YROC_ROOT"/%s/%s", logfile, "status");
                while ((fp = fopen(file, "w")) == NULL) ;
                fclose(fp);

                counter = time(NULL);
		state[0] = stat.f_bsize * (stat.f_blocks - stat.f_bfree);
		state[1] = stat.f_bsize * stat.f_blocks;
		state[2] = sock_state.bytes_send;
		state[3] = sock_state.bytes_recv;
                state[4] = (LLU) counter;
		sprintf(sstate, "%llu %llu %llu %llu %llu",
                                state[0], state[1], state[2],
                                state[3], state[4]);

		if (0 != yroc_write(logfd, (void *) sstate, strlen(sstate)))
			GOTO(close_logfd, errno);
	}

close_logfd:
	close(logfd);
	err_ret:
	return (void *) -1;
}
#endif

#undef LEN_MOUNT_PATH
#undef LOG_TYPE_CDS
#undef SLEEP_INTERVAL

void cds_monitor_handler(int sig)
{
        DINFO("got signal %d\n", sig);
}

void cds_signal_handler(int sig)
{
        (void) sig;
        //DINFO("got signal %d, load %llu queue %llu\n", sig, (LLU)jobdock_load(), (LLU)chunk_queue_length());

        jobdock_iterator();

        analysis_dump();
}

int cds_destroy(int cds_sd, int servicenum)
{


        (void) cds_sd;
        (void) servicenum;

        return 0;
}

int disk_unlink1(const chkid_t *chkid)
{
        int ret;
        char dpath[MAX_PATH_LEN] = {0}, dir[MAX_PATH_LEN];

        chkid2path(chkid, dpath);

        ret = unlink(dpath);
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = _path_split2(dpath, dir, NULL);
        if (ret)
                GOTO(err_ret, ret);

        rmdir(dir);
        
        return 0;
err_ret:
        return ret;
}


static int __chunk_unlink(const chkid_t *chkid)
{
        int i;
        int ret;
        char buf[MAX_BUF_LEN];
        chkinfo_t *chkinfo;

        chkinfo = (void *)buf;
        ret = md_chunk_load(chkid, chkinfo);
        if (ret) {
                if (ret == ENOENT) {
                        /*no op*/
                } else
                        GOTO(err_ret, ret);
        } else {
                for (i = 0; i < (int)chkinfo->repnum; i++) {
                        if (ynet_nid_cmp(&chkinfo->diskid[i], &ng.local_nid) == 0) {
                                DINFO("chk "OBJID_FORMAT" still in use\n", OBJID_ARG(chkid));
                                ret = EPERM;
                                goto err_ret;
                        }
                }
        }

        ret = disk_unlink1(chkid);
        if (ret)
                GOTO(err_ret, ret);

        DINFO("remove chunk "OBJID_FORMAT"\n", OBJID_ARG(chkid));

        return 0;

err_ret:
        return ret;
}

int chunk_cleanup(void *arg)
{
        int ret, i, count;
        chkid_t array[100], *chkid;

        (void) arg;

        DBUG("get cleanup msg\n");

        while (1) {
                count = 100;
                
                ret = rm_pop(net_getnid(), -1, array, &count);
                if (ret) {
                        ret = ENOENT;
                        goto err_ret;
                }

                if (count == 0)
                        goto out;

                for (i = 0; i < (int)count; i++) {
                        chkid = &array[i];
                        YASSERT(chkid_null(chkid) == 0);

                        ret = __chunk_unlink(chkid);
                        if (ret) {
                                if (ret == ENOENT || ret == EPERM)
                                        continue;
                                else
                                        GOTO(err_ret, ret);
                        }
                }
        }

out:
        return 0;
err_ret:
        return ret;
}


int cds_init(const char *home, int *cds_sd, int servicenum, int diskno, uint64_t max_object)
{
        int ret;
        char dpath[MAX_PATH_LEN];
        struct stat stbuf;
        char buf[YFS_CRC_SEG_LEN];

        snprintf(dpath, MAX_PATH_LEN, "%s/swap", ng.home);
        mkdir(dpath, 0644);

        snprintf(dpath, MAX_PATH_LEN, "%s/deleted", ng.home);
        mkdir(dpath, 0644);

        (void) cds_sd;

        memset(buf, 0x0, YFS_CRC_SEG_LEN);

        cds_info.diskno = diskno;
        cds_info.hb_service.inited = 0;
        cds_info.robot_service.inited = 0;

        snprintf(dpath, MAX_PATH_LEN, "%s/readonly", ng.home);

        if (stat(dpath, &stbuf) == 0) {
                cds_info.readonly = 1;
                DINFO("%s found, set to force ro mode \n", dpath);
        } else {
                cds_info.readonly = 0;
        }

        ret = disk_init(home, max_object);
        if (ret)
                GOTO(err_ret, ret);

        ret = cds_volume_init();
        if (ret)
                GOTO(err_ret, ret);

        ret = core_init(1, 1, CORE_FLAG_PASSIVE | CORE_FLAG_AIO);
        if (ret)
                GOTO(err_ret, ret);
        
retry:
        ret = network_connect_mond(1);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        sleep(5);
                        goto retry;
                } else
                        GOTO(err_ret, ret);
        }

        ret = bh_register("chunk_recycle", chunk_cleanup, NULL, (60));
        if (ret)
                GOTO(err_ret, ret);

        /* hb_service */
        ret = hb_service_init(&cds_info.hb_service, servicenum);
        if (ret)
                GOTO(err_ret, ret);

#if 0
        char hostname[MAX_NAME_LEN], key[MAX_PATH_LEN], value[MAX_BUF_LEN];
        ret = gethostname(hostname, MAX_NAME_LEN);
        if (ret)
                GOTO(err_ret, ret);
        
        snprintf(key, MAX_PATH_LEN, "%s/%d", hostname, diskno);
        snprintf(value, MAX_PATH_LEN, "%d", ng.local_nid.id);
        
        ret = etcd_create_text(ETCD_CDS, key, value, -1);
        if (ret) {
                if (ret == EEXIST) {
                        ret = etcd_update_text(ETCD_CDS, key, value, NULL, -1);
                        if (ret)
                                GOTO(err_ret, ret);
                } else {
                        GOTO(err_ret, ret);
                }
        }
#endif

        return 0;
err_ret:
        return ret;
}

inline void cds_exit_handler(int sig)
{
        if (inited == 0) {
                DINFO("got signal %d, force exit\n", sig);
                EXIT(0);
        } else {
                DINFO("got signal %d, prepare exit, please waiting\n", sig);
        }

        cds_info.running = 0;
        srv_running = 0;
}

static int cds_flush()
{
        srv_running = 0;

        DINFO("cds close safely\n");

        return 0;
}

static int __get_tier_by_diskno(int seq, uint32_t *tier)
{
        int ret;
        char path[MAX_PATH_LEN] = "", buf[MAX_BUF_LEN] = "";

        snprintf(path, MAX_PATH_LEN, "%s/cds/%d/tier", gloconf.workdir, seq);

        ret = _get_text(path, buf, MAX_BUF_LEN);
        if (ret < 0) {
                ret = -ret;
                if (ret == ENOENT) {
                        *tier = TIER_HDD;
                        return 0;
                } else {
                        GOTO(err_ret, ret);
                }
        }

        *tier = atoi(buf);

        return 0;
err_ret:
        return ret;
}

int cds_run(void *args)
{
        int ret, servicenum, config_count;
        int daemon, diskno;
        net_proto_t net_op;
        char name[MAX_NAME_LEN], path[MAX_PATH_LEN];
        char home[MAX_PATH_LEN], buf[MAX_BUF_LEN];
        struct stat stbuf;
        addr_t *addr;
        struct statvfs fsbuf;
        int64_t max_object;
        cds_args_t *cds_args;

        cds_args = args;
        daemon = cds_args->daemon;
        diskno = cds_args->diskno;

        sprintf(name, "%s/%d", ROLE_CDS, diskno);
        snprintf(home, MAX_PATH_LEN, "%s/%s", gloconf.workdir, name);

        snprintf(path, MAX_NAME_LEN, "%s/status/status.pid", home);
        ret = daemon_pid(path);
        if (ret)
                GOTO(err_ret, ret);
        
        signal(SIGUSR1, cds_signal_handler);
        signal(SIGUSR2, cds_exit_handler);
        signal(SIGTERM, cds_exit_handler);
        signal(SIGHUP, cds_exit_handler);
        signal(SIGKILL, cds_exit_handler);
        signal(SIGINT, cds_exit_handler);

        addr = (void *)buf;
        ret = config_import(addr, &config_count, "cds");
        if (ret)
                GOTO(err_ret, ret);

        if (diskno >= config_count) {
                ret = ENODEV;
                GOTO(err_ret, ret);
        }

        servicenum = 7;

        _memset(&net_op, 0x0, sizeof(net_proto_t));

        cds_info.diskno = diskno;
        ret = __get_tier_by_diskno(diskno, &cds_info.tier);
        if (ret) {
                DERROR("get diskno %d tier error, ret:%d\n", diskno, ret);
                GOTO(err_ret, ret);
        }

        ret = statvfs(home, &fsbuf);
        if (ret == -1) {
                ret = errno;
                DERROR("statvfs(%s, ...) ret (%d) %s\n", path,
                       ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        max_object = (LLU)fsbuf.f_blocks * fsbuf.f_bsize / YFS_CHK_LEN_DEF;

        ret = ly_init(daemon, name, max_object * 2 + MAX_OPEN_FILE);
        if (ret)
                GOTO(err_ret, ret);

        DINFO("cds %s avail %lluGB total %lluGB, max object %llu, tier %u\n", home,
              ((LLU)fsbuf.f_bavail * fsbuf.f_bsize) / (1024 * 1024 * 1024LL),
              ((LLU)fsbuf.f_blocks * fsbuf.f_bsize) / (1024 * 1024 * 1024LL),
              (LLU)max_object, cds_info.tier);

        ret = path_validate(home, 1, 1);
        if (ret)
                GOTO(err_ret, ret);

        if (gloconf.check_mountpoint && !sy_is_mountpoint(home, EXT3_SUPER_MAGIC)
            && !sy_is_mountpoint(home, EXT4_SUPER_MAGIC)) {
                ret = ENODEV;
                GOTO(err_ret, ret);
        }

        cds_info.running = 1;
        __fence_test_need__ = 1;

        ret = io_analysis_init("cds", diskno);
        if (ret)
                GOTO(err_ret, ret);
        
        ret = cds_init(home, NULL, servicenum, diskno, max_object);
        if (ret)
                GOTO(err_ret, ret);

        ret = replica_init();
        if (ret)
                GOTO(err_ret, ret);

        ret = diskio_init();
        if (ret)
                GOTO(err_ret, ret);
        
        DBUG("cds (diskno %d) started ...\n", diskno);

        ret = rpc_start(); /*begin serivce*/
        if (ret)
                GOTO(err_ret, ret);

        snprintf(path, MAX_PATH_LEN, "%s/dirty", ng.home);

        ret = stat(path, &stbuf);
        if (ret < 0) {
                ret = errno;
                if (ret == ENOENT) {
                        snprintf(buf, MAX_PATH_LEN, "status=on,dirty=false");

                        int fd;
                        fd = creat(path, 0644);
                        if (fd < 0) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }

                        close(fd);
                } else
                        FATAL_EXIT("disk error");
        } else {
                snprintf(buf, MAX_PATH_LEN, "status=on,dirty=true");
                DWARN("cds %s not clean\n", ng.home);
        }

        inited = 1;

        ret = ly_update_status("running", -1);
        if (ret)
                GOTO(err_ret, ret);

        while (cds_info.running) { //we got nothing to do here
                sleep(1);

#if 0
                if (time(NULL) % 10 == 0) {
                        DINFO("latency %ju\n", core_latency_get());
                }
#endif
        }

        ret = ly_update_status("stopping", -1);
        if (ret)
                GOTO(err_ret, ret);

        DINFO("exiting...\n");

        cds_flush();

        //cds_rjnl_destroy();

        ret = ly_update_status("stopped", -1);
        if (ret)
                GOTO(err_ret, ret);

        snprintf(path, MAX_PATH_LEN, "%s/dirty", ng.home);
        unlink(path);

        return 0;
err_ret:
        return ret;
}

#include <sys/wait.h>
#include <stdlib.h>
#include <openssl/md5.h>
#include <openssl/aes.h>
#include <dirent.h>

#define DBG_SUBSYS S_YFSLIB

#include "md_lib.h"
#include "../../ynet/rpc/rpc_proto.h"
#include "fnotify.h"
#include "net_global.h"
#include "file_table.h"
#include "job_tracker.h"
#include "sdevent.h"
#include "conn.h"
#include "yfscli_conf.h"
#include "sdfs_lib.h"
#include "nodeid.h"
#include "schedule.h"
#include "variable.h"
#include "timer.h"
#include "network.h"
#include "redis.h"
#include "bh.h"
#include "core.h"
#include "attr_queue.h"
#include "io_analysis.h"
#include "../../cds/cds_rpc.h"
#include "net_global.h"
#include "mem_hugepage.h"
//#include "license_helper.h"
#include "main_loop.h"
#include "dbg.h"

extern int is_daemon;
extern jobtracker_t *jobtracker;

extern analysis_t *default_analysis;

/*If more than Max_reboot number of restarts occur in the last Max_date seconds,*/
/*then the supervisor terminates all the child processes and then itself.*/
#define MAX_REBOOT 2
#define MAX_DATE 600
#define _MAX_REBOOT (MAX_REBOOT + 1)

int *exit_times;
int exit_count;
int __no_root__ = 0;

static void __exit_handler_init()
{
        int ret;

        ret = ymalloc((void**)&exit_times, _MAX_REBOOT * sizeof(int));
        if (ret) {
                fprintf(stderr, "exit_handler_init error: %d\n", ret);
                EXIT(ret);
        }

        exit_count = 0;
}

static void  __exit_handler(int *over)
{
        int index, max, min, tmp, i, diff, total;

        index = exit_count % _MAX_REBOOT;
        exit_times[index] = time(NULL);
        /*printf("exit_count: %d\n", exit_count);*/

        max = exit_times[0];
        min = exit_times[0];
        total = 0;
        for (i = 0; i < _MAX_REBOOT; i++) {
                if (exit_times[i]) {
                        tmp = exit_times[i];
                        if (max < tmp) {
                                max = tmp;
                        }

                        if (min > tmp) {
                                min = tmp;
                        }

                        total++;
                }
        }

        diff = max - min;
        if ((total >= _MAX_REBOOT) && (max - min < MAX_DATE)) {
                DERROR("sorry, it's over, count: %d, diff: %d\n", exit_count, diff);
                *over = 1;
        } else {
                DWARN("hi, just retry, conut: %d, diff: %d\n", exit_count, diff);
                *over = 0;
        }

        exit_count++;
}

int set_environment()
{
        int fd, ret;
        char len[MAX_BUF_LEN];

        memset(len, 0x0, MAX_BUF_LEN);

        sprintf(len, "%d", SO_XMITBUF);

        fd = open("/proc/sys/net/core/wmem_max", O_RDWR | O_TRUNC);
        if (fd < 0) {
                ret = errno;
                if (ret == EPERM)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        ret = _write(fd, len, strlen(len));
        if ((unsigned)ret != strlen(len)) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        sy_close(fd);

        fd = open("/proc/sys/net/core/rmem_max", O_RDWR | O_TRUNC);
        if (fd < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = _write(fd, len, strlen(len));
        if ((unsigned)ret != strlen(len)) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        sy_close(fd);

        return 0;
err_ret:
        return ret;
}

static int yfs_version_check(const char *home)
{
        int ret, fd = -1;
        char path[MAX_PATH_LEN], buf[MAX_BUF_LEN];
        struct stat stbuf;

        /*old version*/
        sprintf(path, "%s/version", home);

        ret = stat(path, &stbuf);
        if (ret == 0) {
                FATAL_EXIT("old version found");
        }

        sprintf(path, "%s/%s/version", home, YFS_STATUS_PRE);

        ret = path_validate(path, 0, 1);
        if (ret)
                GOTO(err_ret, ret);

        ret = stat(path, &stbuf);
        if (ret) {
                ret = errno;

                if (ret == ENOENT) {
                        fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0644);
                        if (fd < 0) {
                                ret = -fd;
                                GOTO(err_ret, ret);
                        }

                        ret = _write(fd, YFS_META_VERSION, strlen(YFS_META_VERSION));
                        if (ret < 0) {
                                ret = -ret;
                                GOTO(err_ret, ret);
                        }

                        goto out;
                } else {
                        DERROR("path %s\n", path);

                        GOTO(err_ret, ret);
                }
        }

        fd = open(path, O_RDONLY);
        if (fd < 0) {
                ret = -fd;
                GOTO(err_ret, ret);
        }

        memset(buf, 0x0, MAX_BUF_LEN);
        ret = read(fd, buf, MAX_BUF_LEN);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        if (strcmp(buf, YFS_META_VERSION) != 0) {
                DERROR("got data version: %s", buf);
                DERROR("supported version: %s", YFS_META_VERSION);
                DERROR("unsupported format, exit\n");
                EXIT(EINVAL);
        }

out:
        close(fd);

        return 0;
err_ret:
        close(fd);
        return ret;
}

static int fence_test_sync()
{
        return 0;
}

static void __ng_init()
{
        YASSERT(ng.inited == 0);

        memset(&ng, 0x0, sizeof(net_global_t));

        ng.port = YNET_PORT_NULL;
        ng.inited = 1;
        ng.master_magic = 1;
        UNIMPLEMENTED(__NULL__);
}

void replace(char *haystack, char needle, char src)
{
        char *pos;

        pos = haystack;
        while (1) {
                pos = strchr(haystack, needle);
                if (pos == NULL) {
                        break;
                } else {
                        *pos = src;
                }
        }
}

inline static int __hugepage_check()
{
        int ret, fd;
        char buf[MAX_BUF_LEN];

        fd = open("/sys/kernel/mm/redhat_transparent_hugepage/enabled", O_RDONLY);
        if (fd < 0) {
                ret = errno;
                if (ret == ENOENT) {
                        goto out;
                } else {
                        DERROR("unsupported system\n");
                        EXIT(ENOSYS);
                }
        }

        ret = _read(fd, buf, MAX_BUF_LEN);
        if (ret < 0) {
                DERROR("unsupported system\n");
                EXIT(ENOSYS);
        }

        close(fd);

        if (strstr(buf, "[never]") == NULL) {
                DERROR("close hugepage please\n");
                EXIT(ENOSYS);
        }

out:
        return 0;
}

static int __system_check()
{
        int ret, fd;
        char buf[MAX_BUF_LEN];
        uint32_t version;

        return 0;
        
#ifdef __CENTOS5__
        return 0;
#endif

        fd = open("/proc/version", O_RDONLY);
        if (fd < 0) {
                DERROR("unsupported system\n");
                EXIT(ENOSYS);
        }

        ret = _read(fd, buf, MAX_BUF_LEN);
        if (ret < 0) {
                DERROR("unsupported system\n");
                EXIT(ENOSYS);
        }

        close(fd);

#ifdef __CYGWIN__
        return 0;
#endif

        if (memcmp("Linux version 3", buf, strlen("Linux version 3")) == 0) {
                return 0;
        } else if  (memcmp("Linux version 2.6", buf, strlen("Linux version 2.6")) == 0) {
                ret = sscanf(buf, "Linux version 2.6.%u",&version);
                if (ret == 0) {
                        DERROR("unsupported kernel\n");
                        EXIT(ENOSYS);
                }

                if (version < 32) {
                        DERROR("unsupported kernel\n");
                        EXIT(ENOSYS);
                }

                //__hugepage_check();
        } else {
                DERROR("unsupported kernel\n");
                EXIT(ENOSYS);
        }

        return 0;
}

int need_license_check(const char *proname)
{
        int ret;

        if(!strcmp(proname, "uss.objck"))
                ret = 1;
        else if(!strcmp(proname, "uss.robjck"))
                ret = 1;
        else
                ret = 0;

        return ret;
}

#if 0
int ly_license_init(const char *name)
{
#ifdef __CYGWIN__
	return 0;
#else
	int ret;
        if (gloconf.check_license){
                if(need_license_check(name)){
                        ret = uss_license_check(gloconf.workdir);
                        if(ret)
                                GOTO(err_ret, ret);
                }
        }
        else
                DINFO("license check off\n");

        return 0;
err_ret:
        return ret;
#endif
}
#endif

void ly_set_daemon()
{
        is_daemon = 2;
}

int ly_prep(int daemon, const char *name, int64_t maxopenfile)
{
        int ret, lockfd;
        //uint32_t port;
        char home[MAX_PATH_LEN], path[MAX_PATH_LEN], logname[MAX_NAME_LEN],
                clustername[MAX_NAME_LEN];

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

#ifndef __CYGWIN__
        if(strncmp(name, "nfs", strlen("nfs")) == 0){
                ret = nfs_config_init(YNFS_CONFIGURE_FILE);
                if (ret)
                        GOTO(err_ret, ret);
        }

        if(strncmp(name, "ftp", strlen("ftp")) == 0){
                ret = yftp_config_init(YFTP_PASSWORD_FILE);
                if(ret)
                        GOTO(err_ret, ret);
        }
#endif


        //strcpy(gloconf.cluster_name, sanconf.iqn);

        snprintf(home, MAX_PATH_LEN, "%s/%s", gloconf.workdir, name);

        DBUG("daemon %u home %s\n", daemon, home);

        __system_check();
        __ng_init();
        _strcpy(ng.home, home);
        ng.daemon = daemon;

        if (daemon) {
                if (daemon == 1) {
                        strcpy(logname, name);
                        replace(logname, '/', '_');
                        snprintf(path, MAX_PATH_LEN, "%s/log/%s.log", SDFS_HOME, logname);

                        ret = path_validate(path, 0, 1);
                        if (ret)
                                GOTO(err_ret, ret);

                        (void) ylog_init(YLOG_FILE, path);
                        (void) daemonlize(1, gloconf.maxcore, NULL, ylog->logfd, maxopenfile);
                } else if (daemon == 2) {
                        (void) ylog_init(YLOG_STDERR, "");
                        (void) daemonlize(0, gloconf.maxcore, NULL, -1, maxopenfile);
                }

                is_daemon = 1;

                ret = yfs_version_check(home);
                if (ret)
                        GOTO(err_ret, ret);

                snprintf(path, MAX_PATH_LEN, "%s/status/status", home);
                lockfd = daemon_lock(path);
                if (lockfd < 0) {
                        ret = -lockfd;
                        GOTO(err_ret, ret);
                }

                ret = daemon_update(path, "starting\n");
                if (ret)
                        GOTO(err_ret, ret);

                if (strcmp(gloconf.cluster_name, "yfs") == 0 || strlen(gloconf.cluster_name) == 0) {
                        FATAL_EXIT("set your cluster name please !!!!");
                }

                snprintf(path, MAX_PATH_LEN, "%s/status/clustername", home);

                ret = _get_value(path, clustername, MAX_BUF_LEN);
                if (ret < 0) {
                        ret = -ret;
                        if (ret == ENOENT) {
                                DINFO("set %s %s\n", path, gloconf.cluster_name);
                                ret = _set_value(path, gloconf.cluster_name,
                                                 strlen(gloconf.cluster_name) + 1, O_EXCL | O_CREAT);
                                if (ret)
                                        GOTO(err_ret, ret);
                        } else
                                GOTO(err_ret, ret);
                } else {
                        if (strcmp(gloconf.cluster_name, clustername)) {
                                DERROR("cluster name error config '%s' used '%s'\n",
                                       gloconf.cluster_name, clustername);
                                EXIT(EINVAL);
                        }
                }
        } else {
                if(strcmp(name, "fsal_uss") == 0) {
                        strcpy(logname, name);
                        replace(logname, '/', '_');
                        snprintf(path, MAX_PATH_LEN, "%s/log/%s.log", SDFS_HOME, logname);

                        ret = path_validate(path, 0, 1);
                        if (ret)
                                GOTO(err_ret, ret);

                        (void) ylog_init(YLOG_FILE, path);
                } else {
                        (void) ylog_init(YLOG_STDERR, "");
                }
                (void) daemonlize(0, gloconf.maxcore, NULL, -1, maxopenfile);
        }

        YASSERT(cdsconf.queue_depth > 0);

        set_environment();
        ng.xmitbuf = SO_XMITBUF;

        return 0;
err_ret:
        return ret;
}

/**
 * @brief 初始化一些资源，注意在调用这个接口之前，要先设置下ng.role。
 * TOTO，把role放在参数里面
 *
 * @param daemon
 * @param name
 * @param maxopenfile
 *
 * @return 
 */

static int __nodeid_init(const char *name)
{
        int ret;
        nodeid_t id;

        ret = nodeid_load(&id);
        if (ret) {
                if (ret == ENOENT) {
                        ret = nodeid_init(&id, name);
                        if (ret)
                                GOTO(err_ret, ret);

                } else
                        GOTO(err_ret, ret);
        }

        ng.local_nid.id = id;
        
        return 0;
err_ret:
        return ret;
}

int init_stage1()
{
        int ret;

        fnotify_init();
        dmsg_init();

        ret = mem_cache_init();
        if (ret)
                GOTO(err_ret, ret);
        
        ret = mem_hugepage_init();
        if (ret)
                GOTO(err_ret, ret);

        ret = timer_init(0, 0);
        if (ret)
                GOTO(err_ret, ret);

        analysis_init();
        
        ret = worker_init();
        if (ret)
                GOTO(err_ret, ret);

        ret = maping_init();
        if (ret)
                GOTO(err_ret, ret);

        ret = bh_init();
        if (ret)
                GOTO(err_ret, ret);

        ret = jobtracker_create(&jobtracker, 1, "default");
        if (ret)
                GOTO(err_ret, ret);

        if (gloconf.performance_analysis) {
                ret = analysis_create(&default_analysis, "default", 0);
                if (unlikely(ret))             
                        GOTO(err_ret, ret);            
        }

        ret = variable_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        ret = schedule_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);

        DINFO("stage1 inited\n");
        
        return 0;
err_ret:
        return ret;
}

int init_stage2(const char *name, int noroot, int redis_conn)
{
        int ret, thread;
        char home[MAX_PATH_LEN], path[MAX_PATH_LEN];

        ret = etcd_init();
        if (ret)
                GOTO(err_ret, ret);

        ret = redis_init(redis_conn);
        if (ret)
                GOTO(err_ret, ret);
        
        if (!noroot) {
                ret = md_root_init();
                if (ret) {
                        GOTO(err_ret, ret);
                }
        }
        
        if (ng.daemon) {
                ret = __nodeid_init(name);
                if (ret)
                        GOTO(err_ret, ret);
        }
        
        thread = ng.daemon ? gloconf.main_loop_threads : 2;
        ret = main_loop_create(thread);
        if (ret)
                GOTO(err_ret, ret);

        snprintf(home, MAX_PATH_LEN, "%s/%s", gloconf.workdir, name);
        snprintf(path, MAX_PATH_LEN, "%s/status/status", home);
        ret = rpc_init(NULL, name, -1, home);
        if (ret)
                GOTO(err_ret, ret);

        if (ng.daemon) {
                ret = rpc_passive(-1);
                if (ret)
                        GOTO(err_ret, ret);
        }

        ret = network_init();
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (ng.daemon) {
                ret = conn_init();
                if (ret)
                        GOTO(err_ret, ret);
        }
 
        DINFO("stage2 inited\n");
        
        return 0;
err_ret:
        return ret;
}

int init_stage3()
{
        int ret;

        if (ng.daemon) {
                ret = conn_register();
                if (ret)
                        GOTO(err_ret, ret);
        }

        DINFO("stage3 inited\n");
        
        return 0;
err_ret:
        return ret;
}

int ly_init(int daemon, const char *name, int64_t maxopenfile)
{
        int ret;

        //uint32_t port;

        (void)daemon;
        (void)maxopenfile;

        ret = init_stage1();
        if (ret)
                GOTO(err_ret, ret);

        ret = init_stage2(name, __no_root__, 1);
        if (ret)
                GOTO(err_ret, ret);

        ret = init_stage3();
        if (ret)
                GOTO(err_ret, ret);

        _fence_test1_init(ng.home);

        ng.live = 1;
        ng.uptime = time(NULL);

        ret = md_init();
        if (ret)
                GOTO(err_ret, ret);

        ret = cds_rpc_init();
        if (ret)
                GOTO(err_ret, ret);
        
        main_loop_start();

        return 0;
err_ret:
        return ret;
}

int ly_run(char *home, int (*server)(void *), void *args)
{
        int ret, son, status, over, network_flag = 1;
        int daemon;
        char path[MAX_PATH_LEN];

        daemon = 1;

        __exit_handler_init();

        snprintf(path, MAX_NAME_LEN, "%s/status/parent.pid", home);
        ret = daemon_pid(path);
        if (ret)
                GOTO(err_ret, ret);

        while (srv_running && daemon) {
                son = fork();

                switch (son) {
                case -1:
                        ret = errno;
                        GOTO(err_ret, ret);
                case 0:
                        ret = server(args);
                        if (ret) {
                                DERROR("service start fail, exit\n");
                                return ret;
                        }

                        goto out;
                        break;
                default:
                        while (srv_running) {
                                ret = wait(&status);
                                if (ret == son)
                                        break;

                                ret = errno;
                                DERROR("Monitor: %d\n", ret);
                        }

                        if (WIFEXITED(status)) {
                                ret = WEXITSTATUS(status);
                                if (ret == 0) {
                                        DINFO("worker exit normally\n");
                                        goto out;
                                } else if (ret == EAGAIN) {
                                        DINFO("worker require restart, restarting...\n");
                                        continue;
                                } else if (ret == ENONET || ret == ENETUNREACH) {
                                        DWARN("network down, retry later...\n");
                                        while (1) {
                                                sleep(10);
                                                ret = fence_test_sync();
                                                if (ret) {
                                                        DWARN("network down, retry fail...\n");
                                                        continue;
                                                }

                                                break;
                                        }

                                        DWARN("network recovered, continue...\n");
                                        continue;
                                } else {
                                        DERROR("Monitor: worker exited %d, ret %u\n",
                                               WEXITSTATUS(status), ret);
                                        EXIT(ret);
                                        //continue;
                                }

                                break;
                        } else if (WIFSIGNALED(status)) {
                                DERROR("Monitor: worker exited on signal %d, runing %u\n",
                                       WTERMSIG(status), srv_running);

                                if (WTERMSIG(status) == SIGKILL) {
                                        DERROR("killed by SIGKILL?");
                                        EXIT(EINVAL);
                                }

                                if (gloconf.restart) {
                                        while (1) {
                                                ret = fence_test_sync();
                                                if (ret) {
                                                        DWARN("network down, retry later...\n");
                                                        network_flag = 0;
                                                        sleep(10);
                                                        continue;
                                                }

                                                if (!network_flag) {
                                                        network_flag = 1;
                                                        DWARN("network recovered, continue...\n");
                                                }

                                                break;
                                        }
                                        __exit_handler(&over);
                                        if (over) {
                                                EXIT(EINVAL);
                                        }
                                        continue;
                                }

                                EXIT(EINVAL);

                        } else {
                                DERROR("Monitor: worker exited (stopped?) %d\n", status);
                                EXIT(EINVAL);
                                //continue;
                        }
                }

                if (daemon == 0)
                        break;
        }

        if (!daemon) {
                ret = server(args);
                if (ret)
                        GOTO(err_ret, ret);
        }

out:
        return 0;
err_ret:
        return ret;
}

int ly_update_status(const char *status, int step)
{
        int ret;
        char path[MAX_PATH_LEN], buf[MAX_BUF_LEN];

        snprintf(path, MAX_PATH_LEN, "%s/status/status", ng.home);
        if (step == -1)
                snprintf(buf, MAX_PATH_LEN, "%s\n", status);
        else
                snprintf(buf, MAX_PATH_LEN, "%s:%d\n", status, step);

        DINFO("set %s value %s", path, buf);

        ret = daemon_update(path, buf);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int ly_destroy(void)
{
        return 0;
}

int ly_init_simple2(const char *name)
{
        int ret;

        ret = ly_prep(0, name, -1);
        if (ret)
                GOTO(err_ret, ret);

        __no_root__ = 1;
        ret = ly_init(0, name, -1);
        if (ret)
                GOTO(err_ret, ret);

        ng.live = 0;
        is_daemon = 0;

#if 0
        ret = ly_license_init(name);
        if (ret)
                GOTO(err_ret, ret);
#endif

        return 0;
err_ret:
        return ret;
}

int ly_init_simple(const char *name)
{
        int ret, retry = 0;

        ret = ly_prep(0, name, -1);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init(0, name, -1);
        if (ret)
                GOTO(err_ret, ret);

        ng.live = 0;
        is_daemon = 0;

retry:
        ret = network_connect_mond(0);
        if (ret) {
                USLEEP_RETRY(err_ret, ret, retry, retry, 10, (1000 * 1000));
        }

#if 0
        ret = ly_license_init(name);
        if (ret)
                GOTO(err_ret, ret);
#endif

        return 0;
err_ret:
        return ret;
}

int sdfs_init_verbose(const char *name, int polling_core)
{
        int ret;

#if ENABLE_CO_WORKER
        polling_core = 1;
#endif
        
        ret = init_stage1();
        if (ret)
                GOTO(err_ret, ret);

        ret = init_stage2(name, __no_root__, polling_core);
        if (ret)
                GOTO(err_ret, ret);

        ret = init_stage3();
        if (ret)
                GOTO(err_ret, ret);

        _fence_test1_init(ng.home);

        ng.live = 1;
        ng.uptime = time(NULL);

        ret = md_init();
        if (ret)
                GOTO(err_ret, ret);

        ret = cds_rpc_init();
        if (ret)
                GOTO(err_ret, ret);
        
        main_loop_start();

        ret = io_analysis_init(name, -1);
        if (ret)
                GOTO(err_ret, ret);

        ret = core_init(polling_core, gloconf.polling_timeout,
                        CORE_FLAG_ACTIVE | CORE_FLAG_PRIVATE);
        if (ret)
                GOTO(err_ret, ret);

retry:
        ret = network_connect_mond(0);
        if (ret) {
                ret = _errno(ret);
                if (ret == EAGAIN) {
                        sleep(5);
                        goto retry;
                } else
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int sdfs_init(const char *name)
{
        int ret;

        ret = ly_prep(0, name, -1);
        if(ret)
                GOTO(err_ret, ret);

        ly_set_daemon();
        
        ret = sdfs_init_verbose(name, gloconf.polling_core);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

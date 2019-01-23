

#include <limits.h>
#include <time.h>
#include <string.h>
#include <sys/epoll.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBSCHEDULE

#include "sysutil.h"
#include "net_proto.h"
#include "ylib.h"
#include "sdevent.h"
#include "../net/xnect.h"
#include "net_table.h"
#include "rpc_table.h"
#include "configure.h"
#include "main_loop.h"
#include "mem_cache.h"
#include "job_dock.h"
#include "schedule.h"
#include "cpuset.h"
#include "bh.h"
#include "net_global.h"
#include "timer.h"
#include "adt.h"
#include "dbg.h"
#include <sys/types.h>
#include <dirent.h>

#define __CPU_PATH__ "/sys/devices/system/cpu"
#define __CPUSET_INIT__       1
#define __CPUSET_UNINIT__     0


static coreinfo_t *__coreinfo__;

typedef struct {
        int threading_max;
        int polling_core;
        int aio_core;
        int hyper_threading;
} cpuinfo_t;

static cpuinfo_t cpuinfo = {0, 0, 0, 0};

static int __aio_core__[MAX_CPU_COUNT];
static int __aio_core_count__ = 0;
static int __cpuset_init__ = __CPUSET_UNINIT__;

static int __get_socket_id(int cpu_id, int *socket_id)
{
        char path[128], *nodestr;
        int ret;
        DIR *dir;
        struct dirent debuf, *de; 

        snprintf(path, 128, "%s%d", "/sys/devices/system/cpu/cpu", cpu_id);
        dir = opendir(path);
        if(dir == NULL){
                ret = errno;
                GOTO(err_ret, ret);
        }
        
        *socket_id = -1;

        while(1) {
                ret = readdir_r(dir, &debuf, &de);
                if (ret < 0){
                        ret = errno;
                        GOTO(err_close, ret);
                }

                if (de == NULL){
                        break;
                }
                
                nodestr = strstr(de->d_name, "node");
                if (nodestr != NULL) {
                        if(strlen(nodestr) != 5){
                                continue;
                        }
                        
                        nodestr += 4;

                        *socket_id = atoi(nodestr);
                        break;
                }
        }
        
        closedir(dir);

        if(*socket_id == -1) {
                //DWARN("get numa information failed, switch to compatibility mode.\r\n");
                *socket_id = 0;
        }

        return 0;
err_close:
        closedir(dir);
err_ret:
        return ret;
}


static int __cpuset_getmax(const char *parent, const char *name, void *_max)
{
        int ret, idx, *max;

        (void) parent;
        max = _max;

        ret = sscanf(name, "cpu%d", &idx);
        if (ret != 1) {
                //DINFO("skip %s\n", name);
        } else {
                //DINFO("get cpu %u\n", idx);
                *max = *max < idx ? idx : *max;
        }

        return 0;
}

int cpuset_useable()
{
        YASSERT(cpuinfo.polling_core);
        return cpuinfo.polling_core;
}

static int __cpuset_getconf(const char *conf, int max)
{
        char tmp[MAX_NAME_LEN];
        int pc, count;

        strcpy(tmp, conf);
        if (tmp[strlen(tmp) - 1] == '%') {
                tmp[strlen(tmp) - 1] = '\0';
                pc = atoi(tmp);
                count = (max + 1) * pc / 100;
        } else {
                count = atoi(tmp);
        }

        count = count < max ? count : max;

        return count;
}

static int __lookup_ht_core(int cpuid, int coreid)
{
        int i = -1;
        coreinfo_t *coreinfo;
        for (i = 0; i <= cpuinfo.threading_max; i++) {
                coreinfo = &__coreinfo__[i];
                if (coreinfo->used)
                        continue;

                if (coreinfo->physical_package_id == cpuid && coreinfo->core_id == coreid) {
                        DWARN("found ht core[%d], physical_package_id: %d.\n", coreid, cpuid);
                        return i;
                }
        }

        return -1;
}

static int __cpuset_getslave(int cpuid, int coreid)
{
        int i, idx, ht_id;
        coreinfo_t *coreinfo;

        ht_id = __lookup_ht_core(cpuid, coreid);
        if (ht_id >= 0) {
                coreinfo = &__coreinfo__[ht_id];
                if (coreinfo->used) {
                        DWARN("not found ht core.\n");
                        return -1;
                } else
                        return ht_id;
        }

        for (i = 0; i <= cpuinfo.threading_max; i++) {
                //idx = cpuinfo.threading_max - i;
                idx = i;
                coreinfo = &__coreinfo__[idx];

                if (coreinfo->used)
                        continue;

                if (coreinfo->physical_package_id == cpuid) {
                        coreinfo->used = 1;
                        return idx;
                }
        }

        return -1;
}

#define MAX_NUMA_NODE 32

int __next_node_id__ = 0;
int __cpu_node_count__ = 0;

static void __cpuset_getcpu(coreinfo_t **master, int *slave)
{
        int i;
        coreinfo_t *coreinfo;

        *master = NULL;
        *slave = -1;
        for (i = 0; i <= cpuinfo.threading_max; i++) {
                // TODO 按cpu_id降序，依次分配core
                // cpu_id与NUMA Node具有不同的映射关系
                coreinfo = &__coreinfo__[cpuinfo.threading_max - i];

                if (coreinfo->used)
                        continue;

                if (coreinfo->node_id != __next_node_id__)
                        continue;

                __next_node_id__ = (__next_node_id__ + 1 ) % __cpu_node_count__;

                coreinfo->used = 1;
                *master = coreinfo;
                *slave = __cpuset_getslave(coreinfo->physical_package_id, coreinfo->core_id);

                break;
        }
}

int cpuset_init()
{
        int i, ret, max = 0;
        char buf[MAX_BUF_LEN], path[MAX_PATH_LEN];
        coreinfo_t *coreinfo;
        int node_list[MAX_NUMA_NODE] = {0};

        if (__cpuset_init__ == __CPUSET_INIT__)
                return 0;

        ret = _dir_iterator(__CPU_PATH__, __cpuset_getmax, &max);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&__coreinfo__, sizeof(*__coreinfo__) * (max + 1));
        if (unlikely(ret))
                GOTO(err_ret, ret);

        memset(__coreinfo__, 0x0, sizeof(*__coreinfo__) * (max + 1));

        for (i = 0; i <= max; i++) {
                coreinfo = &__coreinfo__[i];

                coreinfo->cpu_id = i;

                snprintf(path, MAX_PATH_LEN, "%s/cpu%u/topology/core_id", __CPU_PATH__, i);
                ret = _get_text(path, buf, MAX_BUF_LEN);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                coreinfo->core_id = atoi(buf);

                snprintf(path, MAX_PATH_LEN, "%s/cpu%u/topology/physical_package_id", __CPU_PATH__, i);
                ret = _get_text(path, buf, MAX_BUF_LEN);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                coreinfo->physical_package_id = atoi(buf);

                ret = __get_socket_id(i, &coreinfo->node_id);
                if (ret) 
                        GOTO(err_ret, ret);

                node_list[coreinfo->node_id]++;

                DBUG("thread[%u] coreid %u physical_package_id %u node id %u\n", i,
                      coreinfo->core_id, coreinfo->physical_package_id, 
                      coreinfo->node_id);
        }

        for (i = 0; i < MAX_NUMA_NODE; i++) {
               if (node_list[i])
                        __cpu_node_count__++;
               else
                       break;
        }

        cpuinfo.polling_core = __cpuset_getconf(gloconf.polling_core, max + 1);
        cpuinfo.aio_core = __cpuset_getconf(gloconf.aio_core, max + 1);

        if (cpuinfo.polling_core < 1) {
                DINFO("force set polling_core 1\n");
                cpuinfo.polling_core = 1;
        }

        cpuinfo.threading_max = max;

        DINFO("core max %u polling %u aio %u\n", max, cpuinfo.polling_core, cpuinfo.aio_core);

        if (cpuinfo.aio_core) {
                coreinfo_t *coreinfo[MAX_CPU_COUNT];

                for (i = 0; i < cpuinfo.aio_core; i++) {
                        // TODO why x2
                        __cpuset_getcpu(&coreinfo[i], &__aio_core__[i * 2]);
                        __aio_core__[i] = coreinfo[i]->cpu_id;
                }
        }

        __cpuset_init__ = __CPUSET_INIT__;
        return 0;
err_ret:
        return ret;
}

int get_cpunode_count()
{
        return __cpu_node_count__;
}

void cpuset_getcpu(coreinfo_t **master, int *_slave)
{
        int slave;

        __cpuset_getcpu(master, &slave);

        if (cpuinfo.aio_core) {
                DINFO("count %u max %u\n", __aio_core_count__, cpuinfo.aio_core * 2);
                *_slave = __aio_core__[__aio_core_count__ %  (cpuinfo.aio_core * 2)];
                __aio_core_count__ ++;
        } else {
                *_slave = slave;
        }
        
        DINFO("get cpu %d %d\n", (*master)->cpu_id, *_slave)
}

int cpuset(const char *name, int cpu)
{
        int ret;
        cpu_set_t cmask;
        size_t n;
        coreinfo_t *coreinfo;

        if (!ng.daemon || cpu == -1)
                return 0;

        coreinfo = &__coreinfo__[cpu];
        DINFO("set %s @ cpu[%u], core[%u], thread[%u]\n", name,
              coreinfo->physical_package_id, coreinfo->core_id, cpu);

        n = sysconf(_SC_NPROCESSORS_ONLN);

        CPU_ZERO(&cmask);
        CPU_SET(cpu, &cmask);

        ret = sched_setaffinity(0, n, &cmask);
        if (unlikely(ret)) {
                ret = errno;
                DWARN("bad cpu set %u\n", cpu);
                GOTO(err_ret, ret);
        }

        CPU_ZERO(&cmask);

        return 0;
err_ret:
        return ret;
}


static void __cpuset_getcpu_by_physical_id(int *master, int *slave, int physical_package_id)
{
        int i;
        coreinfo_t *coreinfo;

        *master = -1;
        *slave = -1;
        for (i = 0; i <= cpuinfo.threading_max; i++) {
                coreinfo = &__coreinfo__[cpuinfo.threading_max - i];

                if (coreinfo->used)
                        continue;

                if (coreinfo->physical_package_id != physical_package_id) {
                        continue;
                }

                coreinfo->used = 1;
                *master = cpuinfo.threading_max - i;
                *slave = __cpuset_getslave(coreinfo->physical_package_id, coreinfo->core_id);

                break;
        }
}

void cpuset_getcpu_by_physical_id(int *master, int *slave, int physical_package_id)
{
        __cpuset_getcpu_by_physical_id(master, slave, physical_package_id);
}

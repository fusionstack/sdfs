#ifndef __CPUSET_H__
#define __CPUSET_H__

#define MAX_CPU_COUNT 256

typedef struct {
        int cpu_id;
        int core_id;
        int node_id;             ///< NUMA node id
        int physical_package_id; ///< socket
        int used;
        int lockfd;
} coreinfo_t;


int cpuset_init();

int get_cpunode_count();

int cpuset_useable();
void cpuset_getcpu(coreinfo_t **master, int *slave);
int cpuset(const char *name, int cpu);
void cpuset_unset(int cpu);


void cpuset_getcpu_by_physical_id(int *master, int *slave, int physical_package_id);

#endif


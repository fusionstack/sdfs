#ifndef SPDK_EXTRA_H
#define SPDK_EXTRA_H

#include "sdfs_buffer.h"
#include "spdk.h"
#include "schedule.h"

#if PERFORMANCE_ANALYSIS
struct spdk_performan_ayalysis_t {
        uint64_t incomplete_io_count;
        uint64_t io_max;
        uint64_t io_avg_time;

        uint64_t total_complete_write_io;
        uint64_t total_complete_read_io;
        uint64_t total_io_request;
        uint64_t total_io_time;
        uint64_t max_io_time;
        char disk_name[128];
        int io_type;
};

int get_spdk_io_count(struct spdk_performan_ayalysis_t *spa);
#endif

typedef enum {
        PREAD = 0x01,
        PWRITE,
        PREADV,
        PWRITEV,
}io_type_t;

struct non_aligned_io_handle_t {
        sem_t sem;
        sy_spinlock_t lock;
        struct list_head io_list;
};
extern struct non_aligned_io_handle_t handle_io;

struct non_aligned_io_t {
        struct list_head hook;
        io_type_t type;
        void *fd;
        size_t size;
        size_t offset;
        sgl_element_t *sgl;
        int sgl_count;
        task_t schedule_task;
};

#endif

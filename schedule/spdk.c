#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>


#include <rte_config.h>
#include <rte_cycles.h>
#include <rte_malloc.h>
#include <rte_lcore.h>

#define DBG_SUBSYS S_LIBSTORAGE

#include "core.h"
#include "dbg.h"
#include "spdk.h"
#include "schedule.h"
#include "core.h"
#include "spdk_extra.h"
#include "cpuset.h"

#include "spdk/nvme.h"
#include "spdk/string.h"
#include "spdk/nvme_intel.h"
#include "spdk/fd.h"
#include "spdk/env.h"

#undef LIST_HEAD
#include <rte_mempool.h>

#define  MAX_CTRLR  8
#define  __SPDK_INIT__    1
#define  __SPDK_UNINIT__  0

sem_t __spdk_launch_sem__;

struct ctrlr_entry {
        struct spdk_nvme_ctrlr	        *ctrlr;
        struct spdk_nvme_ns             *ns;

        uint32_t	                block_size;
        uint32_t	                outstanding_io;
        uint64_t	                size_in_bytes;

        struct pci_info {
                int                     bus;
                int                     domain;
                int                     dev;
                int                     func;
        }pci;

        struct ctrlr_entry	        *next;
        char				name[1024];
        int                             count;
};

struct spdk_nvme_dev {
        uint16_t                outstanding_io;
        struct spdk_nvme_qpair	*qpair;
        struct spdk_nvme_qpair	*aio_qpair;
        struct ctrlr_entry      *ctrlr;
};

struct spdk_request_t {
        uint32_t current_iov_index;
        uint32_t current_iov_bytes_left;

        sgl_element_t *sgl;
        uint32_t nseg;
        uint32_t io_size;
        struct spdk_request_t *parent;
        int subreq;

        struct spdk_nvme_dev *dev;
        task_t schedule_task;
        int yield;
        int err_ret;

#if PERFORMANCE_ANALYSIS
        struct timeval t1, t2;
        int io_type;
#endif
};

struct worker_ctx_t {
        uint16_t        lcore;
        uint16_t        ctrlr_count;

        //        struct spdk_request_t *request_track[MAX_TASK_TRACK];
        struct spdk_nvme_dev nvme_ctrlr[MAX_CTRLR];

        void *arg;
        void *work_fn;

#if PERFORMANCE_ANALYSIS
        struct spdk_performan_ayalysis_t spa;
#endif

        struct worker_ctx_t *next;
};

char  __cpu_mask__[32];
struct rte_mempool *request_mempool;
static struct rte_mempool *task_pool;
static int __global_spdk_init__ = __SPDK_UNINIT__;

static __thread struct worker_ctx_t *__worker_ctx__ = NULL;
static struct worker_ctx_t *g_worker = NULL;
static struct ctrlr_entry *g_controllers = NULL;

void *(*__worker_func__)(void *arg);

static int __worker_ctx_alloc(struct worker_ctx_t **worker)
{
        int ret;
        struct worker_ctx_t *_worker;

        //DINFO("worker is inited\n");
        _worker = malloc(sizeof(struct worker_ctx_t));
        if(_worker == NULL){
                ret = errno;

                GOTO(err_ret, ret);
        }
        memset(_worker, 0x0, sizeof(struct worker_ctx_t));

        *worker = _worker;
        return 0;
err_ret:
        return ret;
}

static int __register_ns(struct ctrlr_entry *ctrlr_entry , struct spdk_nvme_ns *ns)
{
        const struct spdk_nvme_ctrlr_data *cdata;
        const struct spdk_nvme_ns_data *nsdata;
        int ret;

        cdata = spdk_nvme_ctrlr_get_data(ctrlr_entry->ctrlr);

        ret = spdk_nvme_ns_is_active(ns);
        if (ret == 0) {
                DWARN("Controller %-20.20s (%-20.20s): Skipping inactive NS %u\n",
                                cdata->mn, cdata->sn,
                                spdk_nvme_ns_get_id(ns));
                GOTO(err_ret, ret);
        }

        ctrlr_entry->size_in_bytes = spdk_nvme_ns_get_size(ns);
        ctrlr_entry->block_size = spdk_nvme_ns_get_sector_size(ns);

        DINFO("the ns size is %lu, block_size is %d maxio \n", ctrlr_entry->size_in_bytes, ctrlr_entry->block_size);
        snprintf(ctrlr_entry->name, 44, "%-20.20s (%-20.20s)", cdata->mn, cdata->sn);

        nsdata = spdk_nvme_ns_get_data(ns);
        if (!nsdata || !spdk_nvme_ns_get_sector_size(ns)) {
                DERROR("Empty nsdata or wrong sector size\n");
                return 0;
        }

        ctrlr_entry->ns = ns;
        return 0;
err_ret:
        return ret;
}

static void __register_ctrlr(struct spdk_nvme_ctrlr *ctrlr)
{
        int nsid, num_ns, ret;
        struct ctrlr_entry *entry = malloc(sizeof(struct ctrlr_entry));
        const struct spdk_nvme_ctrlr_data *cdata = spdk_nvme_ctrlr_get_data(ctrlr);

        if (entry == NULL) {
                DERROR("ctrlr_entry malloc error\n");
                EXIT(1);
        }

        memset(entry, 0x00, sizeof(struct ctrlr_entry));
        snprintf(entry->name, sizeof(entry->name), "%-20.20s (%-20.20s)", cdata->mn, cdata->sn);

        entry->ctrlr = ctrlr;
        entry->next = g_controllers;
        g_controllers = entry;

        num_ns = spdk_nvme_ctrlr_get_num_ns(ctrlr);
        for (nsid = 1; nsid <= num_ns; nsid++) {
                ret = __register_ns(entry, spdk_nvme_ctrlr_get_ns(ctrlr, nsid));
                if(ret == 0)
                        break;
        }

        if(entry->ns == NULL)
                YASSERT(0);
}

static void __spdk_request_reset_sgl(void *cb_arg, uint32_t sgl_offset)
{
        uint32_t i;
        uint32_t offset = 0;
        sgl_element_t *sgl;
        struct spdk_request_t *req = (struct spdk_request_t *)cb_arg;

        //YASSERT(sgl_offset == 0);
        for (i = 0; i < req->nseg; i++) {
                sgl = &req->sgl[i];
                offset += sgl->sgl_len;
                if (offset > sgl_offset)
                        break;
        }
        req->current_iov_index = i;
        req->current_iov_bytes_left = offset - sgl_offset;

        return;
}

static int __spdk_request_next_sge(void *cb_arg, uint64_t *address, uint32_t *length)
{
        struct spdk_request_t *req = (struct spdk_request_t *)cb_arg;
        sgl_element_t *sgl;

        if (req->current_iov_index >= req->nseg) {
                *length = 0;
                *address = 0;
                return 0;
        }

        sgl = &req->sgl[req->current_iov_index];

        if (req->current_iov_bytes_left) {
                *address = sgl->phyaddr + sgl->sgl_len - req->current_iov_bytes_left;
                *length = req->current_iov_bytes_left;
                req->current_iov_bytes_left = 0;
        } else {
                *address = sgl->phyaddr;
                *length = sgl->sgl_len;
        }

        req->current_iov_index++;

        return 0;
}

static bool  __probe_cb(void *cb_ctx, struct spdk_pci_device *dev, struct spdk_nvme_ctrlr_opts *opts)
{
        (void)cb_ctx;
        (void)opts;

        DINFO("Attaching to %04x:%02x:%02x.%02x\n",
                        spdk_pci_device_get_domain(dev),
                        spdk_pci_device_get_bus(dev),
                        spdk_pci_device_get_dev(dev),
                        spdk_pci_device_get_func(dev));

        return 1;
}

static void __attach_cb(void *cb_ctx, struct spdk_pci_device *dev, struct spdk_nvme_ctrlr *ctrlr,
                const struct spdk_nvme_ctrlr_opts *opts)
{
        int *ptr;
        struct ctrlr_entry *_ctrlr;
        ptr = cb_ctx;
        if(ptr == NULL)
                DINFO("ptr is null\n");

        ptr = (void *)opts;
        DINFO("Attached to %04x:%02x:%02x.%02x\n",
                        spdk_pci_device_get_domain(dev),
                        spdk_pci_device_get_bus(dev),
                        spdk_pci_device_get_dev(dev),
                        spdk_pci_device_get_func(dev));

        __register_ctrlr(ctrlr);

        _ctrlr = g_controllers;
        _ctrlr->pci.domain = spdk_pci_device_get_domain(dev);
        _ctrlr->pci.bus = spdk_pci_device_get_bus(dev);
        _ctrlr->pci.dev = spdk_pci_device_get_dev(dev);
        _ctrlr->pci.func = spdk_pci_device_get_func(dev);
}

static int __nvme_controllers_init(void)
{
        int ret;
        int retry = 0;

        DINFO("Initializing NVMe Controllers\n");

retry:
        ret = spdk_nvme_probe(NULL, __probe_cb, __attach_cb, NULL);
        if (ret) {
                retry++;
                DERROR("spdk_nvme_probe() failed, retry: %d, error[%d]: %s\n", retry, ret, strerror(ret));
                goto retry;
        }

        return 0;
}

static int  __worker_common_fn(void *worker)
{
        struct ctrlr_entry *ctrlr = g_controllers;
        struct spdk_nvme_qpair *qpair = NULL;
        struct spdk_nvme_qpair *aio_qpair = NULL;
        struct worker_ctx_t *_worker = worker;

        //    if(ctrlr == NULL)
        //            return 0;

        while(ctrlr != NULL){

                DINFO("work common fn exec core%d\n", _worker->lcore);
                if(_worker->ctrlr_count > MAX_CTRLR){
                        YASSERT(0);
                }

                if (!_worker->work_fn) {
                        ctrlr->count++;
                        qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr->ctrlr, 0);
                        if(qpair == NULL){
                                DERROR("%04x:%02x:%02x.%02x alloc spdk qpair[%d] failed\n",
                                                ctrlr->pci.domain, ctrlr->pci.bus,
                                                ctrlr->pci.dev, ctrlr->pci.func, ctrlr->count);
                                YASSERT(0);
                        }
                }

                if (_worker->work_fn) {
                        ctrlr->count++;
                        aio_qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr->ctrlr, 0);
                        if(aio_qpair == NULL){
                                DERROR("%04x:%02x:%02x.%02x alloc spdk aio_qpair[%d] failed\n",
                                                ctrlr->pci.domain, ctrlr->pci.bus,
                                                ctrlr->pci.dev, ctrlr->pci.func, ctrlr->count);
                                YASSERT(0);
                        }
                }

                _worker->nvme_ctrlr[_worker->ctrlr_count].ctrlr = ctrlr;
                _worker->nvme_ctrlr[_worker->ctrlr_count].qpair = qpair;
                _worker->nvme_ctrlr[_worker->ctrlr_count++].aio_qpair = aio_qpair;

                ctrlr = ctrlr->next;
        }

        __worker_ctx__ = _worker;

        if(_worker->work_fn){
                //DINFO("work fn exec \n");

#if PERFORMANCE_ANALYSIS
                _worker->spa.incomplete_io_count = 0;
                _worker->spa.total_io_request = 0;
                _worker->spa.total_complete_write_io = 0;
                _worker->spa.total_complete_read_io = 0;
                _worker->spa.total_io_time = 0;
                _worker->spa.max_io_time = 0;
                sprintf(_worker->spa.disk_name, "%s", "None");
                _worker->spa.io_max = 0;
#endif

                __worker_func__(_worker->arg);
        }
        return 0;
}

core_t **__tmp_ptr__;

static core_t * __get_core(int cpuid)
{
        int i;
        core_t *core;

        for (i = 0; i < cpuset_useable(); i++) {
                core = __tmp_ptr__[i];

                if (core->main_core->cpu_id == cpuid){
                        return core;
                }
        }

        YASSERT(0);
        return NULL;
}

static int __worker_init()
{
        int ret, lcore;
        struct worker_ctx_t *worker = NULL, *prev_worker;

        ret = __worker_ctx_alloc(&worker);
        if(ret)
                GOTO(err_ret, ret);

        if(worker == NULL)
                YASSERT(0);

        worker->lcore = rte_get_master_lcore();

        worker->arg = (void*)__get_core(worker->lcore);

        worker->work_fn = __worker_common_fn;
        g_worker = worker;

        prev_worker = worker;

        RTE_LCORE_FOREACH_SLAVE(lcore){
                worker = malloc(sizeof(struct worker_ctx_t));
                if(worker == NULL){
                        ret = errno;

                        GOTO(err_ret, ret);
                }

                DINFO("worker init %d\n", lcore);
                memset(worker, 0x0, sizeof(struct worker_ctx_t));
                worker->lcore = lcore;
                worker->arg = (void *)__get_core(lcore);
                worker->work_fn = __worker_common_fn;
                prev_worker->next = worker;
                prev_worker = worker;
        }

        return 0;

err_ret:
        return ret;
}

static char *ealargs[] = {
        "lichd",
        "-c 0x1",
        "-n 4",
};

static void * __spdk_init__(void *arg)
{
        int  ret;
        struct worker_ctx_t *worker;

        YASSERT(arg == NULL);
        ealargs[1] = spdk_sprintf_alloc("-c %s", __cpu_mask__);
        if(ealargs == NULL){
                DERROR("init cpu mask failed\n");
                YASSERT(0);
        }

        DINFO("the cpu core mask is %s\n", __cpu_mask__);

        ret = rte_eal_init(sizeof(ealargs) / sizeof(ealargs[0]), ealargs);
        free(ealargs[1]);
        if (ret < 0) {
                DERROR("could not initialize dpdk\n");
                YASSERT(0);
        }

        task_pool = rte_mempool_create("task_pool", 16384,
                        sizeof(struct spdk_request_t), 64, 0,
                        NULL, NULL, NULL, NULL,
                        SOCKET_ID_ANY, 0);

        if (task_pool == NULL) {
                DERROR("could not initialize request mempool\n");
                YASSERT(0);
        }

        ret = __worker_init();
        if(ret)
                GOTO(err_ret, ret);

        ret = __nvme_controllers_init();
        if (ret) {
                GOTO(err_ret, ret);
        }

        if(g_worker == NULL){
                YASSERT(0);
        }

        __global_spdk_init__ = __SPDK_INIT__;

        /**
          ret = sem_init(&spdk_launch_sem, 0, 0);
          if (ret) {
          GOTO(err_ret, ret);
          }**/

#if 0
        ret = sem_wait(&__spdk_launch_sem__);
        if (ret) {
                GOTO(err_ret, ret);
        }
#endif
        worker = g_worker->next;
        while(worker != NULL){
                ret = rte_eal_remote_launch(__worker_common_fn, worker, worker->lcore);
                if(ret){
                        DERROR("rte_eal_remote_launch() return error: %s\n", strerror(ret));
                        GOTO(err_ret, ret);
                }
                DINFO("start after  worker %d\n", worker->lcore);
                worker = worker->next;
        }

        __worker_common_fn((void *)g_worker);

        return NULL;

err_ret:
        YASSERT(0);
        return NULL;
}

struct non_aligned_io_handle_t handle_io;
static void *__spdk_handle_non_aligned_io__(void *arg);
int __spdk_handle_non_aligned_io_init()
{
        pthread_t th;
        pthread_attr_t ta;
        int ret;

        (void)pthread_attr_init(&ta);
        (void)pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        ret = sy_spin_init(&handle_io.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = sem_init(&handle_io.sem, 0, 0);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        INIT_LIST_HEAD(&handle_io.io_list);

        ret = pthread_create(&th, &ta, __spdk_handle_non_aligned_io__, &handle_io);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int spdk_init(void **arg, char *cpu_mask_str, void *work_fn(void *_arg))
{
        pthread_t th;
        pthread_attr_t ta;
        int ret;

        (void)pthread_attr_init(&ta);
        (void)pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);


        __worker_func__ = work_fn;
        __tmp_ptr__  =  (core_t **)arg;
        memcpy(__cpu_mask__, cpu_mask_str, strlen(cpu_mask_str));

        ret = pthread_create(&th, &ta, __spdk_init__, NULL);
        if(ret)
                GOTO(err_ret, ret);

        sleep(2);

        __spdk_handle_non_aligned_io_init();

        return 0;
err_ret:
        return ret;
}

static void __spdk_request_put(struct spdk_request_t *req)
{
        rte_mempool_put(task_pool, req);
}

#if PERFORMANCE_ANALYSIS
int get_spdk_io_count(struct spdk_performan_ayalysis_t *spa)
{
        //int i;

        if (core_self()) {
                /**for(i = 0; i < __worker_ctx__->ctrlr_count; i++)
                  {
                  DINFO("Attaching to %04x:%02x:%02x.%02x, outstanding_io:%d. \n",
                  __worker_ctx__->nvme_ctrlr[i].ctrlr->pci.domain,
                  __worker_ctx__->nvme_ctrlr[i].ctrlr->pci.bus,
                  __worker_ctx__->nvme_ctrlr[i].ctrlr->pci.dev,
                  __worker_ctx__->nvme_ctrlr[i].ctrlr->pci.func,
                  __worker_ctx__->nvme_ctrlr[i].outstanding_io);

                  }**/
                spa->incomplete_io_count = __worker_ctx__->spa.incomplete_io_count;
                spa->io_max = __worker_ctx__->spa.io_max;
                spa->max_io_time = __worker_ctx__->spa.max_io_time;
                strcpy(spa->disk_name, __worker_ctx__->spa.disk_name);
                spa->total_complete_write_io = __worker_ctx__->spa.total_complete_write_io;
                spa->total_complete_read_io = __worker_ctx__->spa.total_complete_read_io;
                spa->io_type = __worker_ctx__->spa.io_type;
                spa->total_io_request = __worker_ctx__->spa.total_io_request;
                if (__worker_ctx__->spa.total_complete_write_io || __worker_ctx__->spa.total_complete_read_io)
                        spa->io_avg_time = __worker_ctx__->spa.total_io_time / (__worker_ctx__->spa.total_complete_write_io + __worker_ctx__->spa.total_complete_read_io);

                __worker_ctx__->spa.io_max = 0;
                __worker_ctx__->spa.total_io_request = 0;
                __worker_ctx__->spa.total_complete_write_io = 0;
                __worker_ctx__->spa.total_complete_read_io = 0;
                __worker_ctx__->spa.max_io_time = 0;
                sprintf(__worker_ctx__->spa.disk_name, "%s", "None");
                __worker_ctx__->spa.total_io_time = 0;
                __worker_ctx__->spa.total_io_request = 0;
                __worker_ctx__->spa.io_avg_time = 0;

                return 0;
        } else
                return 0;
}
#endif

static void __spdk_io_complete(void *ctx, const struct spdk_nvme_cpl *cpl)
{
        int error;
        struct spdk_request_t *req = ctx;
        sgl_element_t *sgl = req->sgl;

        error = spdk_nvme_cpl_is_error(cpl);
        if(unlikely(error)) {
                DERROR("sgl base %p len %ju phyaddr %ju offset %ju\n",
                                sgl->sgl_base,
                                sgl->sgl_len,
                                sgl->phyaddr,
                                sgl->offset);
                DWARN("io failed taskid %d,req %p ret %d\n", req->schedule_task.taskid, req, error);
                error = -EIO;
                YASSERT(0);
        }

        req->dev->outstanding_io--;

        if(likely(req->yield)){
                schedule_resume(&req->schedule_task, error, NULL);

#if PERFORMANCE_ANALYSIS
                _gettimeofday(&req->t2, NULL);
                uint64_t used;
                used = _time_used(&req->t1, &req->t2);
                __worker_ctx__->spa.total_io_time += used;
                if (__worker_ctx__->spa.max_io_time < used) {
                        __worker_ctx__->spa.max_io_time = used;
                        __worker_ctx__->spa.io_type = req->io_type;
                        sprintf(__worker_ctx__->spa.disk_name, "%04x:%02x:%02x.%02x",
                                        req->dev->ctrlr->pci.domain,
                                        req->dev->ctrlr->pci.bus,
                                        req->dev->ctrlr->pci.dev,
                                        req->dev->ctrlr->pci.func);
                }

                if (req->io_type)
                        __worker_ctx__->spa.total_complete_write_io++;
                else
                        __worker_ctx__->spa.total_complete_read_io++;

                __worker_ctx__->spa.incomplete_io_count--;
#endif

                DBUG("io complete ret %d buf %p, len %lu %p\n", error, sgl->sgl_base, sgl->sgl_len, req);
                __spdk_request_put(req);
                return ;
        }

        req->err_ret = error;

        return ;
}

static struct spdk_nvme_qpair *__get_spdk_nvme_qpair(struct ctrlr_entry *ctrlr, struct spdk_request_t *req)
{
        int i;
        struct spdk_nvme_qpair *qpair = NULL;

        for (i = 0; i < __worker_ctx__->ctrlr_count; i++)
        {
                if(ctrlr == __worker_ctx__->nvme_ctrlr[i].ctrlr){
                        qpair = __worker_ctx__->nvme_ctrlr[i].qpair;
                        req->dev = &__worker_ctx__->nvme_ctrlr[i];
                        break;
                }

        }

        return qpair;
}

static struct spdk_nvme_qpair *__get_spdk_nvme_aio_qpair(struct ctrlr_entry *ctrlr, struct spdk_request_t *req)
{
        int i;
        struct spdk_nvme_qpair *qpair = NULL;

        for (i = 0; i < __worker_ctx__->ctrlr_count; i++)
        {
                if(ctrlr == __worker_ctx__->nvme_ctrlr[i].ctrlr){
                        qpair = __worker_ctx__->nvme_ctrlr[i].aio_qpair;
                        req->dev = &__worker_ctx__->nvme_ctrlr[i];
                        break;
                }

        }

        return qpair;
}

void  __spdk_io_polling(struct spdk_nvme_qpair *qpair, int max_polling)
{
        int ret;

        while(1){
                ret = spdk_nvme_qpair_process_completions(qpair, max_polling);
                if(ret == 0 && max_polling)
                        continue;

                break;
        }
}

void spdk_aio_polling()
{
        struct spdk_nvme_qpair *qpair;
        int i;
        struct worker_ctx_t *worker_ctx = __worker_ctx__;

        if(unlikely(worker_ctx == NULL))
                return;

        for(i = 0; i < worker_ctx->ctrlr_count; i++)
        {
                /*if(unlikely(worker_ctx->nvme_ctrlr[i].ctrlr == NULL))
                  YASSERT(0);

                  if(unlikely(worker_ctx->nvme_ctrlr[i].outstanding_io == 0))
                  continue;
                  */
                qpair = worker_ctx->nvme_ctrlr[i].aio_qpair;
                /* if(unlikely(qpair == NULL)){
                   YASSERT(0);
                   } */

                spdk_nvme_qpair_process_completions(qpair, 32);
                //               __spdk_io_polling(qpair, 0);
        }
}

static int __get_local_worker()
{
        struct worker_ctx_t *worker;
        worker = __worker_ctx__;
        int ret;

        if(worker == NULL){
                ret = __worker_ctx_alloc(&worker);
                if(ret)
                        GOTO(err_ret, ret);

                __worker_common_fn((void *)worker);
        }

        return 0;

err_ret:
        return ret;
}

static int __spdk_read_io_submit(struct ctrlr_entry *ctrlr,  struct spdk_request_t *req, size_t size, off_t offset, struct spdk_nvme_qpair **_qpair)
{
        struct spdk_nvme_qpair *qpair = *_qpair;
        int ret;
        size_t sectors;
        off_t sector_off;

        req->dev->outstanding_io++;

        YASSERT(size % ctrlr->block_size == 0);
        YASSERT(offset % ctrlr->block_size == 0);

        sectors = size / ctrlr->block_size;
        sector_off = offset / ctrlr->block_size;

        DBUG("read size: %lu, offset %ju ,sectors: %lu, sector_off: %lu, req: %p\n", size, offset, sectors, sector_off, req);
        ret = spdk_nvme_ns_cmd_readv(ctrlr->ns, qpair,  sector_off, sectors, 
                        __spdk_io_complete, req, 0,
                        __spdk_request_reset_sgl,
                        __spdk_request_next_sge);
        if (unlikely(ret != 0)) {
                if(ret < 0)
                        ret = -ret;

                DWARN("read io error[%d] %s \n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        return 0;

err_ret:
        return ret;
}

static int __spdk_write_io_submit(struct ctrlr_entry *ctrlr,  struct spdk_request_t *req, size_t size, off_t offset, struct spdk_nvme_qpair **_qpair)
{
        struct spdk_nvme_qpair *qpair = *_qpair;
        int ret;
        size_t sectors;
        off_t sector_off;

        req->dev->outstanding_io++;

        YASSERT(size % ctrlr->block_size == 0);
        YASSERT(offset % ctrlr->block_size == 0);

        sectors = size / ctrlr->block_size;
        sector_off = offset / ctrlr->block_size;

        DBUG("write size: %lu, offset %ju ,sectors: %lu, sector_off: %lu, req: %p\n", size, offset, sectors, sector_off, req);
        ret = spdk_nvme_ns_cmd_writev(ctrlr->ns, qpair, sector_off, sectors,
                        __spdk_io_complete, req, 0,
                        __spdk_request_reset_sgl,
                        __spdk_request_next_sge);
        if (unlikely(ret != 0)) {
                if(ret < 0)
                        ret = -ret;

                DWARN("write io error[%d] %s \n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        return 0;

err_ret:
        return ret;
}

static int __spdk_request_init(sgl_element_t *sgl, int sgl_count, struct spdk_request_t **_req)
{
        struct spdk_request_t *req;
        int ret;

        if (unlikely(rte_mempool_get(task_pool, (void **)&req) != 0)){ 
                ret = -ENOMEM;

                GOTO(err_ret, ret);
        }

        memset(req, 0x0, sizeof(struct spdk_request_t));
        req->sgl = sgl;
        req->nseg = sgl_count;
        req->schedule_task.taskid = -1;

        *_req = req;

        return 0;

err_ret:
        return ret;
}

#if 0
static int __spdk_request_init(sgl_element_t **sgl, const buffer_t *buf, struct spdk_request_t **_req, int *req_count)
{
        struct spdk_request_t *req, *first_req;
        int ret,req_num, sgl_count = 0, start_seg = 0;
        uint32_t req_size, total_size = 0;
        int req_total_count ;
        struct list_head *pos;
        seg_t *seg;


        if (seg->len % 512 != 0) {
        } else {
                ret = mbuffer_sg_trans_sgl((struct iovec **)sgl, &sgl_count, sizeof(sgl_element_t),  buf);
                if (ret)

        }
        ret = __spdk_request_init__(*sgl, sgl_count, &req);
        if(ret){
                GOTO(err_ret, ret);
        }
        DWARN("more than one req size %u, sgl count %d, start seg %d seg count %u\n", req_size, sgl_count, start_seg,total_size);
        first_req = req;
        req->io_size = req_size;
        *_req = req;
        req_total_count = 1;
        sgl_count = 0;
        total_size = req_size;
        YASSERT(0);

        while ( req_num ) {

                req_num = mbuffer_sg_trans_sgl((struct iovec **)sgl, &sgl_count, sizeof(sgl_element_t),  buf, &start_seg, &req_size);
                ret = __spdk_request_init__(*sgl, sgl_count, &req);
                if(ret){
                        GOTO(err_ret, ret);
                }
                DWARN("more than one reqaaa size %u, sgl count %d, start seg %d\n", req_size, sgl_count, start_seg );
                total_size += req_size;

                req->io_size = req_size;
                req->parent = first_req;
                first_req->subreq++;
                *(++_req) = req;
                req_total_count += 1;
                sgl_count = 0;
                if(total_size == buf->len)
                        break;
        }

        YASSERT(total_size == buf->len);

        *req_count = req_total_count;
        return 0;

err_ret:
        return ret;
}

#define MAX_REQ 4
#endif
int spdk_aio_pwritev(void *ctrlr, sgl_element_t *sgl, int sgl_count, size_t size,  off_t offset)
{
        struct spdk_nvme_qpair *qpair;
        struct spdk_request_t *req;
        int ret;

        ret = __spdk_request_init(sgl, sgl_count, &req);
        if(unlikely(ret)) {
                DERROR("rte_mempool_get %s. \n", strerror(ret));
                GOTO(err_ret, ret);
        }

        req->schedule_task = schedule_task_get();

        if(unlikely(__worker_ctx__ == NULL)){
                ret = __get_local_worker();
                if(unlikely(ret)){
                        ret = -ENOMEM;
                        DERROR("__worker_ctx__ should not be NULL. \n");
                        GOTO(err_free, ret);
                }
        }

        qpair = __get_spdk_nvme_aio_qpair(ctrlr, req);
        if (unlikely(qpair == NULL)) {
                DWARN("not found nvme disk\n");
                ret = -ENODEV;
                GOTO(err_free, ret);
        }

#if PERFORMANCE_ANALYSIS
        req->io_type = 1;
        _gettimeofday(&req->t1, NULL);
        __worker_ctx__->spa.incomplete_io_count++;
        __worker_ctx__->spa.total_io_request++;
        if (__worker_ctx__->spa.io_max < __worker_ctx__->spa.incomplete_io_count)
                __worker_ctx__->spa.io_max = __worker_ctx__->spa.incomplete_io_count;
#endif
        ret = __spdk_write_io_submit(ctrlr, req, size, offset, &qpair);
        if(unlikely(ret)) {
                ret = req->err_ret;
                GOTO(err_free, ret);
        }

        req->yield = 1;
        schedule_yield("spdk_aio_pwritev", NULL, req);

        return 0;

err_free:
        __spdk_request_put(req);
err_ret:
        return ret;
}

int spdk_aio_preadv(void *ctrlr, sgl_element_t *sgl, int sgl_count, size_t size, off_t offset)
{
        struct spdk_nvme_qpair *qpair;
        struct spdk_request_t *req;
        int ret;

        ret = __spdk_request_init(sgl, sgl_count, &req);
        if(unlikely(ret))
                GOTO(err_ret, ret);

        req->schedule_task = schedule_task_get();

        if(unlikely(__worker_ctx__ == NULL)){
                ret = __get_local_worker();
                if(unlikely(ret)){
                        ret = -ENOMEM;
                        GOTO(err_free, ret);
                }
        }

        qpair = __get_spdk_nvme_aio_qpair(ctrlr, req);
        if (unlikely(qpair == NULL)) {
                DWARN("not found nvme disk\n");
                ret = -ENODEV;
                GOTO(err_free, ret);
        }

#if PERFORMANCE_ANALYSIS
        req->io_type = 0;
        _gettimeofday(&req->t1, NULL);
        __worker_ctx__->spa.incomplete_io_count++;
        __worker_ctx__->spa.total_io_request++;
        if (__worker_ctx__->spa.io_max < __worker_ctx__->spa.incomplete_io_count)
                __worker_ctx__->spa.io_max = __worker_ctx__->spa.incomplete_io_count;
#endif
        ret = __spdk_read_io_submit(ctrlr, req, size, offset, &qpair);
        if(unlikely(ret))
                GOTO(err_free, ret);

        req->yield = 1;
        schedule_yield("spdk_aio_preadv", NULL, req);

        return 0;

err_free:
        __spdk_request_put(req);
err_ret:
        YASSERT(0);
        return ret;
}

int spdk_preadv(void *ctrlr,  sgl_element_t *sgl , int sgl_count, size_t size, off_t offset)
{

        struct spdk_nvme_qpair *qpair;
        struct spdk_request_t *req;
        int ret;

        ret = __spdk_request_init(sgl, sgl_count,  &req);
        if(unlikely(ret))
                GOTO(err_ret, ret);

        if(unlikely(__worker_ctx__ == NULL)){
                ret = __get_local_worker();
                if(unlikely(ret)){
                        ret = -ENOMEM;
                        GOTO(err_free, ret);
                }
        }

        qpair = __get_spdk_nvme_qpair(ctrlr, req);
        if (unlikely(qpair == NULL)) {
                DWARN("not found nvme disk\n");
                ret = -ENODEV;
                GOTO(err_free, ret);
        }

        ret = __spdk_read_io_submit(ctrlr, req, size, offset, &qpair);
        if(unlikely(ret))
                GOTO(err_free, ret);

        __spdk_io_polling(qpair, 1);

        ret = req->err_ret;

err_free:
        __spdk_request_put(req);
err_ret:
        /**if(!core_self()) {
          int i;
          for (i = 0; i < __worker_ctx__->ctrlr_count; i++) {
          spdk_nvme_ctrlr_free_io_qpair(__worker_ctx__->nvme_ctrlr[i].qpair);
          }
          free(__worker_ctx__);
          __worker_ctx__ = NULL;
          }**/
        return ret;
}

int spdk_pwritev(void *ctrlr, sgl_element_t *sgl, int sgl_count, size_t size, off_t offset)
{

        struct spdk_nvme_qpair *qpair; 
        struct spdk_request_t *req;
        int ret;

        ret = __spdk_request_init(sgl, sgl_count, &req);
        if(unlikely(ret))
                GOTO(err_ret, ret);

        if(unlikely(__worker_ctx__ == NULL)){
                ret = __get_local_worker();
                if(unlikely(ret)){
                        ret = -ENOMEM;
                        GOTO(err_free, ret);
                }
        }

        qpair = __get_spdk_nvme_qpair(ctrlr, req);
        if (unlikely(qpair == NULL)) {
                DWARN("not found nvme disk\n");
                ret = -ENODEV;
                GOTO(err_free, ret);
        }

        ret = __spdk_write_io_submit(ctrlr, req, size, offset, &qpair);
        if(unlikely(ret))
                GOTO(err_free, ret);

        __spdk_io_polling(qpair,1);

        ret =  req->err_ret;
err_free:
        __spdk_request_put(req);
err_ret:
        /**if(!core_self()) {
          int i;
          for (i = 0; i < __worker_ctx__->ctrlr_count; i++) {
          spdk_nvme_ctrlr_free_io_qpair(__worker_ctx__->nvme_ctrlr[i].qpair);
          }
          free(__worker_ctx__);
          __worker_ctx__ = NULL;
          }**/

        return ret;
}

int spdk_pread(void *ctrlr, sgl_element_t *sgl,off_t offset)
{
        int ret, need_copy = 0;
        struct spdk_nvme_qpair *qpair; 
        void *buf = sgl->sgl_base;
        struct spdk_request_t *req;
        uint32_t old_len = sgl->sgl_len;

        DBUG("pread old buf is %p\n", buf);
        if(old_len % 512 != 0 || sgl->phyaddr == 0){
                if (old_len % 512 != 0)
                        sgl->sgl_len = (old_len / 512) * 512 + 512;
                else
                        sgl->sgl_len = (old_len / 512) * 512;

                sgl->sgl_base = rte_zmalloc(NULL, sgl->sgl_len, 0x1000);
                if(unlikely(sgl->sgl_base == NULL)){
                        ret = -ENOMEM;
                        GOTO(err_ret, ret);
                }
                sgl->phyaddr = rte_malloc_virt2phy(sgl->sgl_base);
                if(unlikely(sgl->phyaddr == 0)){
                        ret = -EIO;
                        DWARN("free addr %p\n", sgl->sgl_base);
                        GOTO(err_free, ret);
                }

                need_copy = 1;
        }

        ret = __spdk_request_init(sgl, 1, &req);
        if(unlikely(ret))
                GOTO(err_free, ret);

        if(unlikely(__worker_ctx__ == NULL)){
                ret = __get_local_worker();
                if(unlikely(ret)){
                        ret = -ENOMEM;
                        GOTO(err_free1, ret);
                }
        }

        qpair = __get_spdk_nvme_qpair(ctrlr, req);
        if (unlikely(qpair == NULL)) {
                DWARN("not found nvme disk\n");
                ret = -ENODEV;
                GOTO(err_free1, ret);
        }

        ret = __spdk_read_io_submit(ctrlr, req, sgl->sgl_len, offset, &qpair);
        if(unlikely(ret))
                GOTO(err_free1, ret);


        __spdk_io_polling(qpair, 1);

        if(likely(req->err_ret == 0)){
                if (!need_copy) {
                        __spdk_request_put(req);
                        return old_len;
                }

                memcpy(buf, sgl->sgl_base, old_len);
                ret = old_len;
        } else {
                DWARN("io error on spdk submit \n");
                ret =  req->err_ret;
        }

err_free1:
        __spdk_request_put(req);
err_free:
        rte_free(sgl->sgl_base);
err_ret:
        sgl->sgl_base = buf;
        /**if(!core_self()) {
          int i;
          for (i = 0; i < __worker_ctx__->ctrlr_count; i++) {
          spdk_nvme_ctrlr_free_io_qpair(__worker_ctx__->nvme_ctrlr[i].qpair);
          }
          free(__worker_ctx__);
          __worker_ctx__ = NULL;
          }**/
        return ret;
}

int spdk_pwrite(void *ctrlr, sgl_element_t *sgl,off_t offset)
{
        int ret;
        struct spdk_nvme_qpair *qpair;
        void *buf = sgl->sgl_base;
        struct spdk_request_t *req;
        uint32_t old_len = sgl->sgl_len;

        if(old_len % 512 != 0){
                sgl->sgl_len = (old_len / 512) * 512 + 512;
        }

        sgl->sgl_base = rte_zmalloc(NULL, sgl->sgl_len, 0x1000);
        if(unlikely(sgl->sgl_base == NULL)){
                ret = -ENOMEM;
                GOTO(err_ret, ret);
        }

        sgl->phyaddr = rte_malloc_virt2phy(sgl->sgl_base);
        if(unlikely(sgl->phyaddr == 0)){
                ret = -EIO;
                GOTO(err_free, ret);
        }

        if (old_len != sgl->sgl_len) {

                ret = spdk_pread(ctrlr, sgl,offset);
                if(unlikely(ret <= 0))
                        YASSERT(0);
        }

        DBUG("write new buf %p, old buf %p, old_len %u new len %lu\n", sgl->sgl_base, buf, old_len, sgl->sgl_len);
        memcpy(sgl->sgl_base, buf, old_len);

        ret = __spdk_request_init(sgl, 1, &req);
        if(unlikely(ret))
                GOTO(err_free, ret);

        if(unlikely(__worker_ctx__ == NULL)){
                ret = __get_local_worker();
                if(unlikely(ret)){
                        ret = -ENOMEM;
                        GOTO(err_free1, ret);
                }
        }

        qpair = __get_spdk_nvme_qpair(ctrlr, req);
        if (unlikely(qpair == NULL)) {
                DWARN("not found nvme disk\n");
                ret = -ENODEV;
                GOTO(err_free1, ret);
        }

        ret = __spdk_write_io_submit(ctrlr, req, sgl->sgl_len, offset, &qpair);
        if(unlikely(ret))
                GOTO(err_free1, ret);

        __spdk_io_polling(qpair, 1);


        if(likely(req->err_ret == 0))
                ret = old_len;
        else
                ret = req->err_ret;

err_free1:
        __spdk_request_put(req);
err_free:
        rte_free(sgl->sgl_base);
err_ret:
        sgl->sgl_base = buf;
        /**if(!core_self()) {
          int i;
          for (i = 0; i < __worker_ctx__->ctrlr_count; i++) {
          spdk_nvme_ctrlr_free_io_qpair(__worker_ctx__->nvme_ctrlr[i].qpair);
          }
          free(__worker_ctx__);
          __worker_ctx__ = NULL;
          }**/
        return ret;

}

void spdk_nvme_disk_size(void *ctrlr, uint64_t *disk_size)
{
        // uint64_t size = 0;

        *disk_size = ((struct ctrlr_entry *)ctrlr)->size_in_bytes;
        /*if (size > ((LLU)2) * 1000 * 1000 * 1000 * 1000)
         *disk_size = ((LLU)2000) * 1000 * 1000 * 1000;
         else
         *disk_size = size; */
}

void *lookup_spdk_nvme_controller(int domain, int bus, int dev, int func)
{
        int ret;
        struct ctrlr_entry *ctrlr = g_controllers;

        if (__global_spdk_init__ == __SPDK_UNINIT__) {
                DWARN("spdk not yet initialized !!!!!\n");

                ret = cpuset_init();
                if (unlikely(ret)) {
                        DERROR("cpuset init failed !!!\n");
                        EXIT(EAGAIN);
                }

                core_spdk_init(CORE_FLAG_PASSIVE|CORE_FLAG_AIO);
                ctrlr = g_controllers;
        }

        if ((__global_spdk_init__ == __SPDK_INIT__) && (ctrlr == NULL))
                YASSERT(0);

        DINFO("the bus %d domain %d dev %d func %d addr %p\n", bus, domain, dev, func, ctrlr);
        while(ctrlr != NULL)
        {
                if(bus == ctrlr->pci.bus && domain == ctrlr->pci.domain
                                && dev == ctrlr->pci.dev 
                                &&func == ctrlr->pci.func){

                        DINFO("ctrlr ptr %p\n", ctrlr);
                        return ctrlr;
                }

                ctrlr = ctrlr->next;
        }

        return NULL;
}

static void *__spdk_handle_non_aligned_io__(void *arg)
{
        int ret = 0;
        struct non_aligned_io_handle_t *handle = arg;
        struct list_head *pos;
        struct non_aligned_io_t *io;

        DINFO("the thread start ,which handle Non Aligned Io ...\n");
        while (1) {
                ret = sem_wait(&handle->sem);
                if (ret) {
                        DERROR("sem_wait failed, error[%d]: %s.\n", errno, strerror(errno));
                        YASSERT(0);
                }

                while (1) {
                        ret = sy_spin_lock(&handle->lock);
                        if (unlikely(ret))
                                YASSERT(0);

                        if (list_empty(&handle->io_list)) {
                                sy_spin_unlock(&handle->lock);
                                break;
                        } else {
                                pos = handle->io_list.next;
                                io  = (void *)pos;
                                list_del(pos);
                        }
                        sy_spin_unlock(&handle->lock);

                        DBUG("io->type: %d, io: %p \n", io->type, io);
                        switch (io->type) {
                        case PREAD:
                                ret = spdk_pread(io->fd, io->sgl, io->offset);
                                if (ret <= 0) {
                                        DWARN("spdk pread task retval %u\n", -ret);
                                }
                                break;
                        case PWRITE:
                                ret = spdk_pwrite(io->fd, io->sgl, io->offset);
                                if (ret <= 0) {
                                        DWARN("spdk pread task retval %u\n", -ret);
                                }
                                break;
                        case PREADV:
                                ret = spdk_preadv(io->fd, io->sgl, io->sgl_count, io->size, io->offset);
                                if (unlikely(ret)) {
                                        DWARN("spdk preadv task retval %u\n", ret);
                                }
                                break;
                        case PWRITEV:
                                ret = spdk_pwritev(io->fd, io->sgl, io->sgl_count, io->size, io->offset);
                                if (unlikely(ret)) {
                                        DWARN("spdk pwritev task retval %u\n", ret);
                                }
                                break;
                        default:
                                DERROR("Incorrect IO operation. !!!!\n");
                                continue;
                        }

                        schedule_resume(&io->schedule_task, ret, NULL);
                }
        }

        return NULL;
}

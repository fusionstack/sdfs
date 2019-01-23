#ifndef __SPDK_H_
#define __SPDK_H_

#ifdef SPDK

#include "sdfs_buffer.h"

sem_t spdk_launch_sem;

typedef struct sgl_element {
	void *sgl_base;
	size_t sgl_len;
	uint64_t phyaddr;
	size_t offset;
}sgl_element_t;

void *lookup_spdk_nvme_controller(int bus, int domain, int dev, int func);
void spdk_nvme_disk_size(void *ctrlr, uint64_t *disk_size);
int spdk_pwrite(void *ctrlr, sgl_element_t *sgl,off_t offset);
int spdk_pread(void *ctrlr, sgl_element_t *sgl,off_t offset);
int spdk_pwritev(void *ctrlr, sgl_element_t *sgl, int sgl_count, size_t size, off_t offset);
int spdk_preadv(void *ctrlr, sgl_element_t *sgl, int sgl_count, size_t size,  off_t offset);
int spdk_aio_preadv(void *ctrlr, sgl_element_t *sgl, int sgl_count, size_t size, off_t offset);
int spdk_aio_pwritev(void *ctrlr, sgl_element_t *sgl, int sgl_count, size_t size, off_t offset);
void spdk_aio_polling(void);
int spdk_init(void **arg, char *cpu_mask_str,void *work_fn(void *_arg));

#endif //SPDK

#endif //__SPDK_H_


#ifndef __CHKINFO_H__
#define __CHKINFO_H__

#include <linux/aio_abi.h>

#include "chk_meta.h"
#include "yfscds_conf.h"
#include "cache.h"
#include "sdfs_buffer.h"

typedef struct {
        chkid_t id;
        uint32_t size;
        int hit;
} chklist_t;

int chkinfo_dump(chklist_t **_chklist, int *_count);
int chkinfo_init();
int chkinfo_add(const chkid_t *chkid, int size);
int chkinfo_del(const chkid_t *chkid, int size);

#endif

#ifndef __ATOMIC_ID_H
#define __ATOMIC_ID_H

#include <sys/uio.h>
#include <sys/ioctl.h>
#include <stdint.h>
#include <errno.h>

#include "ylock.h"
#include "adt.h"
#include "dbg.h"
#include "sdfs_conf.h"
#include "ylib.h"

int yatomic_get_and_inc_dura_init();
int yatomic_get_and_inc_dura(const char *id_path, uint64_t *id);
int yatomic_get_and_dec_dura(const char *id_path, uint64_t *id);
int yatomic_set_nolock(const char *id_path, uint64_t id);

typedef struct {
        uint32_t crc;
        uint32_t len;
        char buf[0];
} container_id_t;

#endif

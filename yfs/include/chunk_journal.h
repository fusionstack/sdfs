#ifndef __CHK_JNL_H
#define __CHK_JNL_H

#include "chk_proto.h"

int chkjnl_init();
int chkjnl_get(const chkid_t *id, uint64_t verison, chkop_t *op);
int chkjnl_prep(const chkid_t *id);
int chkjnl_add(const chkid_t *id, uint64_t verison, const chkop_t *op);

#endif

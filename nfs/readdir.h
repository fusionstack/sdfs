#ifndef __READ_DIR_H__
#define __READ_DIR_H__

#include <stdint.h>

#include "attr.h"
#include "sdfs_lib.h"
#include "nfs_state_machine.h"

#define MAX_ENTRIES MAX_READDIR_ENTRIES
#define MAX_DIRPLUS_ENTRIES MAX_READDIR_ENTRIES

int read_dir(sdfs_ctx_t *ctx, const fileid_t *fileid, uint64_t offset, char *verf,
             uint32_t count, readdir_ret *res, entry *entrys,
             char *obj);

int readdirplus(sdfs_ctx_t *ctx, const fileid_t *fileid, uint64_t offset, char *verf,
                uint32_t count, readdirplus_ret *res, entryplus *_entryplus,
                char *obj, fileid_t *fharray);

#endif

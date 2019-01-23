#ifndef __CD_PROTO_H__
#define __CD_PROTO_H__

#include <stdint.h>

#include "sdfs_conf.h"
#include "chk_proto.h"
#include "sdfs_ec.h"
#include "file_proto.h"
#include "md_proto.h"
#include "chk_meta.h"
#include "sdfs_conf.h"

typedef chkid_t objid_t;

typedef struct {
        objid_t id;
        uint64_t info_version;
        uint32_t mode;
        uint32_t status;
        uint32_t size;
        uint32_t repnum;
        uint32_t master;
        uint16_t __pad__;
        diskid_t diskid[0];
} objinfo_t;

#define OBJINFO_SIZE(__repnum__) (sizeof(objinfo_t) + sizeof(diskid_t) * (__repnum__))
#define OBJREPS_SIZE(__repnum__) (sizeof(objreps_t) + sizeof(objrep_t) * (__repnum__))

static inline void chk2obj(objinfo_t *objinfo, chkinfo_t *chkinfo)
{
        objinfo->id = chkinfo->chkid;
        objinfo->info_version = chkinfo->md_version;
        objinfo->mode = 0;
        objinfo->status = chkinfo->status;
        objinfo->size = chkinfo->size;
        objinfo->repnum = chkinfo->repnum;
        objinfo->master = chkinfo->master;
        objinfo->__pad__ = 0;
        memcpy(objinfo->diskid, chkinfo->diskid, sizeof(diskid_t) * chkinfo->repnum);
}

static inline void obj2chk(chkinfo_t *chkinfo, objinfo_t *objinfo)
{
        chkinfo->chkid = objinfo->id;
        chkinfo->md_version = objinfo->info_version;
        chkinfo->status = objinfo->status;
        chkinfo->size = objinfo->size;
        chkinfo->repnum = objinfo->repnum;
        chkinfo->master = objinfo->master;
        memcpy(chkinfo->diskid, objinfo->diskid, sizeof(diskid_t) * objinfo->repnum);
}

#endif

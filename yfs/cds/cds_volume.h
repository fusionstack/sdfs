#ifndef __CDS_VOLUME_H__
#define __CDS_VOLUME_H__

#include "md_proto.h"


int cds_volume_init();
int cds_volume_update(uint32_t volid, int size);
int cds_volume_get(volinfo_t **_info);

#endif

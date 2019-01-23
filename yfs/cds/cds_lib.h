#ifndef __CDS_LIB_H__
#define __CDS_LIB_H__

#include <stdint.h>

#include "file_proto.h"
#include "cd_proto.h"
#include "chk_meta.h"
#include "cds.h"

/* cds_lib.c */
int cds_getinfo(const chkid_t *chkid, chkmeta2_t *md, uint64_t *unlost_version,
                uint64_t *max_version);

/* cds_hb.c */
extern int hb_service_init(hb_service_t *, int servicenum);
extern void *cds_hb(void *);
int hb_service_destroy();

/* cds_robot.c */
extern int robot_service_init(robot_service_t *);
extern int robot_service_destroy(robot_service_t *);
extern void *cds_robot(void *);

#endif

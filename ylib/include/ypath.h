#ifndef __YPATH_H__
#define __YPATH_H__

#include "sdfs_list.h"

typedef struct {
        struct list_head list;
        off_t d_off;
        char  d_name[0];
} df_dir_list_t;

#endif /* __YPATH_H__ */

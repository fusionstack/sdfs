#ifndef __YNFS_ERROR_H__
#define __YNFS_ERROR_H__

#include "nfs_state_machine.h"

extern nfs3_stat lookup_err(int syserr);
extern nfs3_stat rename_err(int syserr);
extern nfs3_stat read_err(int syserr);
extern nfs3_stat write_err(int syserr);
extern nfs3_stat create_err(int syserr);
extern nfs3_stat mkdir_err(int syserr);
extern nfs3_stat remove_err(int syserr);
extern nfs3_stat rmdir_err(int syserr);
extern nfs3_stat readdir_err(int syserr);
extern nfs3_stat setattr_err(int syserr);
extern nfs3_stat symlink_err(int syserr);
extern nfs3_stat mknod_err(int syserr);

#endif

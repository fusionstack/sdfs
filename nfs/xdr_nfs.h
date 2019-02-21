#ifndef __YNFS_XDR_H__
#define __YNFS_XDR_H__

#include "nfs3.h"
#include "xdr.h"

/* MOUNT protocol */

typedef int (*xdr_arg_t)(xdr_t *, void *);

int xdr_dirpath(xdr_t *, char **dir);
int xdr_exports(xdr_t * xdrs, exports *objp);
int xdr_dump(xdr_t * xdrs, mountlist *objp);


/* NFS protocol */
int xdr_getattrargs(xdr_t *, getattr_args *);
int xdr_getattrret(xdr_t *, getattr_ret *);
int xdr_lookupargs(xdr_t *, lookup_args *);
int xdr_lookupret(xdr_t *, lookup_ret *);
int xdr_accessargs(xdr_t *, access_args *);
int xdr_accessret(xdr_t *, access_ret *);
int xdr_readargs(xdr_t *, read_args *);
int xdr_readret(xdr_t *, read_ret *);
int xdr_writeargs(xdr_t *, write_args *);
int xdr_writeret(xdr_t *, write_ret *);
int xdr_createargs(xdr_t *, create_args *);
int xdr_createret(xdr_t *, create_ret *);
int xdr_mkdirargs(xdr_t *, mkdir_args *);
int xdr_mkdirret(xdr_t *, mkdir_ret *);
int xdr_removeargs(xdr_t *, remove_args *);
int xdr_removeret(xdr_t *, remove_ret *);
int xdr_LINK3args(xdr_t * xdrs, LINK3args *args);
int xdr_rmdirargs(xdr_t *, rmdir_args *);
int xdr_rmdirret(xdr_t *, rmdir_ret *);
int xdr_readdirargs(xdr_t *, readdir_args *);
int xdr_readdirret(xdr_t *, readdir_ret *);
int xdr_readdirplusargs(xdr_t *, readdirplus_args *);
int xdr_readdirplusret(xdr_t *, readdirplus_ret *);
int xdr_fsstatargs(xdr_t *, fsstat_args *);
int xdr_fsstatret(xdr_t *, fsstat_ret *);
int xdr_fsinfoargs(xdr_t *, fsinfo_args *);
int xdr_fsinforet(xdr_t *, fsinfo_ret *);
int xdr_pathconf3_args(xdr_t *, pathconf3_args*);
//int xdr_pathconf3_retok(xdr_t *, pathconf3_resok*);
//int xdr_pathconf3_retfail(xdr_t *, pathconf3_resfail*);
int xdr_pathconf3_ret(xdr_t *, pathconf3_res*);
int xdr_commitargs(xdr_t *, commit_args *);
int xdr_commitret(xdr_t *, commit_ret *);
int xdr_setattrargs(xdr_t *, setattr3_args *);
int xdr_setattrret(xdr_t *, setattr3_res *);
int xdr_renameargs(xdr_t *, rename_args *);
int xdr_renameret(xdr_t *, rename_ret *);
int xdr_mountret(xdr_t *xdrs, mount_ret *mntret);
int xdr_symlinkargs(xdr_t *xdrs, symlink_args *args);
int xdr_symlinkret(xdr_t * xdrs, symlink_res * res);
int xdr_mknodargs(xdr_t *xdrs, mknod_args *args);
int xdr_mknodret(xdr_t * xdrs, mknod_res * res);
int xdr_readlinkargs(xdr_t *xdrs, readlink_args *args);
int xdr_readlinkret(xdr_t * xdrs, readlink_res * res);
int xdr_linkret(xdr_t * xdrs, LINK3res * res);

uint64_t hash_getattr(getattr_args *args);
uint64_t hash_lookup(lookup_args *args);
uint64_t hash_access(access_args *args);
uint64_t hash_read(read_args *args);
uint64_t hash_write(write_args *args);
uint64_t hash_create(create_args *args);
uint64_t hash_mkdir(mkdir_args *args);
uint64_t hash_remove(remove_args *args);
uint64_t hash_rmdir(rmdir_args *args);
uint64_t hash_readdir(readdir_args *args);
uint64_t hash_readdirplus(readdirplus_args *args);
uint64_t hash_fsstat(fsstat_args *args);
uint64_t hash_fsinfo(fsinfo_args *args);
uint64_t hash_pathconf3(pathconf3_args *args);
uint64_t hash_setattr(setattr3_args *args);

#endif

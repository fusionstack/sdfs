#ifndef __NFS_JOB_CONTEXT_H__
#define __NFS_JOB_CONTEXT_H__



#include "sdfs_buffer.h"
#include "nlm_state_machine.h"
#include "file_proto.h"

typedef union {
        setattr3_args setattr3_arg;
        rename_args rename_arg;
        getattr_args getattr_arg;
        lookup_args lookup_arg;
        access_args access_arg;
        read_args read_arg;
        write_args write_arg;
        create_args create_arg;
        mkdir_args mkdir_arg;
        remove_args remove_arg;
        rmdir_args rmdir_arg;
        readdir_args readdir_arg;
        readdirplus_args readdirplus_arg;
        fsstat_args fsstat_arg;
        fsinfo_args fsinfo_arg;
        pathconf3_args pathconf_arg;
        commit_args commit_arg;
        symlink_args symlink_arg;
        mknod_args mknod_arg;
        readlink_args readlink_arg;
        LINK3args link3arg;
        char *mnt_arg;
        char *umnt_arg;

        nlm_lockargs   lockargs;
        nlm_cancargs   cancargs;
        nlm_testargs   testargs;
        nlm_unlockargs unlockargs;
        nlm_notifyargs notifyargs;
} nfsarg_t;

typedef union {
        read_ret read_res;
        getattr_ret stat_res;
        access_ret access_res;
        lookup_ret lookup_res;
        mkdir_ret mkdir_res;
        create_ret create_res;
        write_ret write_res;
        rename_ret rename_res;
        commit_ret commit_res;
} nfsret_t;

#endif

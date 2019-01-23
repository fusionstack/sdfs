#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include "ylib.h"
#include "sdfs_conf.h"
#include "diskpool.h"
#include "inode_proto.h"
#include "dbg.h"
#include "namei.h"
#include "license_helper.h"

int mds_get_create_time(time_t *create_time)
{
        int ret;
        fileid_t fileid;
        size_t size;
        char str_create_time[MAX_BUF_LEN];

        ret = namei_lookup("/system", &fileid);
        if(ret)
                GOTO(err_ret, ret);

        size = sizeof(str_create_time);
        ret = inode_getxattr(&fileid, "create_time", str_create_time, &size);
        if(ret)
                GOTO(err_ret, ret);

        sscanf(str_create_time, "%ld", create_time);

        return 0;
err_ret:
        return ret;
}

int mds_check_cap_valid(const unsigned long cap)
{
        int ret;
        unsigned long long total;
        unsigned long long permite_cap;
        diskinfo_stat_t stat;

        ret = diskpool_statvfs(&stat);
        if(ret) {
                GOTO(err_ret, ret);
        }

        total = stat.ds_bsize * stat.ds_blocks;
        permite_cap = cap * 1024 * 1024 * 1024;

        DWARN("total = %llu, permite_cap : %llu\n",
                        (unsigned long long)total,
                        (unsigned long long)permite_cap);

        if(total > permite_cap) {
                ret = ENOSPC;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int mds_check_license_valid(const char *license_file)
{
        int ret;
        unsigned char secret_key[MAX_INFO_LEN] = {0};
        unsigned char mac[MAX_INFO_LEN] = {0};
        time_t due_time;
        unsigned long capacity;

        ret = get_secret_key(license_file, secret_key);
        if(ret) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        ret = dump_mac(license_file, secret_key, mac);
        if(ret) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        ret = check_mac_valid(mac);
        if(ret) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        ret = dump_time(license_file, secret_key, &due_time);
        if(ret) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        ret = check_time_valid(due_time);
        if(ret) {
                ret = ETIME;
                GOTO(err_ret, ret);
        }

        ret = dump_capacity(license_file, secret_key, &capacity);
        if(ret) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        ret = mds_check_cap_valid(capacity);
        if(ret) {
                ret = ENOSPC;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int mds_license_check(const char *home)
{
        int ret, is_trial = 0;
        time_t create_time, now;
        char path[MAX_PATH_LEN];
        struct stat buf;

        ret = mds_get_create_time(&create_time);
        if(ret)
                GOTO(err_ret, ret);

        now = time(NULL);
        if(now - create_time <= FREE_LICENSE) {
                is_trial = 1;
        }

        snprintf(path, MAX_PATH_LEN, "%s/license", home);
        ret = stat(path, &buf);
        if(ret == 0) {
                if((buf.st_size == 0) && is_trial){
                        return 0;
                } else if((buf.st_size == 0) && !is_trial) {
                        DWARN("License expired\n");
                        ret = ETIME;
                        goto err_ret;
                } else {
                        //check_license_valid
                }
        } else {
                if(!is_trial) {
                        ret = ENOENT;
                        DERROR("No license found.\n");
                        goto err_ret;
                } else {
                        return 0;
                }
        }

        ret = mds_check_license_valid(path);
        if(ret){
                if(ret == ETIME){
                        DWARN("License expired\n");
                }else if(ret == ENOSPC){
                        DWARN("Excess capacity\n");
                }else{
                        DWARN("Invalid license\n");
                }

                goto err_ret;
        }

        return 0;
err_ret:
        return ret;
}

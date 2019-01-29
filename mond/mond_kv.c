#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/resource.h>
#include <unistd.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <semaphore.h>
#include <poll.h> 
#include <pthread.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSLIB

#include "ynet_rpc.h"
#include "job_dock.h"
#include "net_global.h"
#include "ynet_rpc.h"
#include "rpc_proto.h"
#include "mond_rpc.h"
#include "network.h"
#include "mond_kv.h"
#include "mem_cache.h"
#include "schedule.h"
#include "dbg.h"

int mond_kv_set(const char *key, const void *value, uint32_t valuelen)
{
        int ret;
        char path[MAX_PATH_LEN];

        snprintf(path, MAX_LINE_LEN, "%s/mond/%s",  SHM_ROOT, key);

        ret = path_validate(path, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = _set_value(path, value, valuelen, O_CREAT | O_TRUNC);
        if (unlikely(ret))
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

static int __mond_kv_get_file(const char *path, void *entry, uint32_t *valuelen)
{
        int ret, len;
        char buf[1024 * 64];

        ret = _get_value(path, buf, 1024 * 64);
        if (unlikely(ret) < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        len = ret;
        memcpy(entry, buf, len);
        *valuelen = len;
        
        return 0;
err_ret:
        return ret;
}

static int __mond_kv_get_dir(const char *path, uint64_t offset, void *de0, uint32_t *valuelen)
{
        int ret, size, buflen;
	struct dirent *de;
        char tmp[MAX_PATH_LEN] = {0}, buf[MAX_PATH_LEN];
        DIR *dir;
        mon_entry_t *ent;

        dir = opendir(path);
        seekdir(dir, offset);

        int doff = 0, dlen = MAX_BUF_LEN, eof = 1;
        de = NULL;
        ent = NULL;
	while((de = readdir(dir)) != NULL) {
		if (strcmp(de->d_name,".") == 0 
		    ||strcmp(de->d_name,"..") == 0)
			continue;

		sprintf(tmp,"%s/%s",path, de->d_name);
                DINFO("get %s, type %u\n", tmp, de->d_type);

                if (de->d_type != DT_REG) {
                        continue;
                }

                ret = _get_value(tmp, buf, MAX_BUF_LEN);
                if (ret < 0) {
                        DWARN("get %s, ret %u\n", tmp, ret);
                        ret = errno;
                        continue;
                }

                buflen = ret;

                size = sizeof(*ent) + buflen + strlen(de->d_name) + 1;
                if (doff + size > dlen) {
                        if (doff + size > MON_ENTRY_MAX) {
                                eof = 0;
                                break;
                        }
                }

                ent = de0 + doff;
                ent->klen = strlen(de->d_name) + 1;
                ent->vlen = buflen;
                ent->type = de->d_type;
                ent->offset = de->d_off;
                ent->eof = 0;
                memcpy(ent->buf, de->d_name, ent->klen);
                memcpy(ent->buf + ent->klen, buf, ent->vlen);
                doff += size;
                DINFO("%s %s size %ju %u %u\n", de->d_name, buf, sizeof(*ent), strlen(de->d_name) + 1, buflen);
	}

        if (ent) {
                ent->eof = eof;
        }
        
	closedir(dir);

        *valuelen = doff;
        
        return 0;
}

int mond_kv_get(const char *key, int offset, void *entry, uint32_t *valuelen)
{
        int ret;
        char path[MAX_PATH_LEN];
        struct stat stbuf;

        snprintf(path, MAX_LINE_LEN, "%s/mond/%s",  SHM_ROOT, key);

        ret = stat(path, &stbuf);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        if (S_ISREG(stbuf.st_mode)) {
                ret = __mond_kv_get_file(path, entry, valuelen);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        } else {
                ret = __mond_kv_get_dir(path, offset, entry, valuelen);
                if (unlikely(ret))
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int mond_kv_init()
{
        int ret;
        char path[MAX_PATH_LEN];

        snprintf(path, MAX_LINE_LEN, "%s/mond",  SHM_ROOT);
        ret = _delete_path(path);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

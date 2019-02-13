#include <unistd.h>
#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSLIB


#include "md_lib.h"
#include "chk_proto.h"
#include "chk_proto.h"
#include "file_table.h"
#include "job_dock.h"
#include "ylib.h"
#include "net_global.h"
#include "yfs_file.h"
#include "sdfs_lib.h"
#include "yfs_limit.h"
#include "dbg.h"
#include "posix_acl.h"
#include "xattr.h"

#define YFS_FILE_ALLOC_INC 8

#ifdef YFS_DEBUG
#define yfs_node_dump(__yn__) \
{                                                               \
        DBUG("==node info==\n");                                \
        DBUG("path_len %u\n", __yn__->path_len);                \
        DBUG("md::md_size %u\n", __yn__->md->md_size);          \
        DBUG("md::file_len %llu\n", (LLU)__yn__->md->file_len); \
        DBUG("md::chk_len %u\n", __yn__->md->chk_len);          \
        DBUG("md::chk_rep %u\n", __yn__->md->chkrep);          \
        DBUG("md::chk_num %u\n", __yn__->md->chknum);           \
}

#define yfs_chunk_dump(__chk__) \
{                                                               \
        DBUG("==chunk info==\n");                               \
        DBUG("chkid (%llu %u)\n", (LLU)__chk__->chkid.id, __chk__->chkid.version); \
        DBUG("chkno %u chkrep %u\n", __chk__->no, __chk__->rep);        \
        DBUG("chklen %u\n", __chk__->chklen);                       \
        DBUG("loaded %d\n", __chk__->loaded);                       \
}

#define yfs_file_dump(__yf__) \
{                                  \
        yfs_node_dump(__yf->node__);   \
}

#else
#define yfs_node_dump(yn) {}
#define yfs_chunk_dump(chk) {}
#define yfs_file_dump(yf) {}
#endif

#if 1
extern jobtracker_t *jobtracker;

int ly_open(const char *path)
{
        int ret;
        fileid_t fileid;

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int ly_read(const char *path, char *buf, size_t size, yfs_off_t offset)
{
        int ret;
        fileid_t fileid;
        buffer_t pack;

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        mbuffer_init(&pack, 0);

        ret = sdfs_read_sync(NULL, &fileid, &pack, size, offset);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_free, ret);
        }

        mbuffer_get(&pack, buf, ret);

        mbuffer_free(&pack);
        return ret;
err_free:
        mbuffer_free(&pack);
err_ret:
        return -ret;
}

int ly_create(const char *path, mode_t mode)
{
        int ret;
        fileid_t parent;
        char name[MAX_NAME_LEN];
        fileid_t fileid;
        uid_t uid;
        gid_t gid;

        ret = sdfs_splitpath(path, &parent, name);
        if (ret)
                GOTO(err_ret, ret);


        uid = geteuid();
        gid = getegid();

        DBUG("parent "FID_FORMAT" name %s\n", FID_ARG(&parent), name);
        ret = sdfs_create(NULL, &parent, name, &fileid, mode, uid , gid);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int ly_write(const char *path, const char *buf, size_t size, yfs_off_t offset)
{
        int ret;
        fileid_t fileid;
        buffer_t pack;

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        mbuffer_init(&pack, 0);

        ret = mbuffer_copy(&pack, buf, size);
        if (ret) {
                ret = -ret;
                GOTO(err_free, ret);
        }

        ret = sdfs_write_sync(NULL, &fileid, &pack, size, offset);
        if (ret < 0) {
                GOTO(err_free, -ret);
        }

        mbuffer_free(&pack);
        return ret;
err_free:
        mbuffer_free(&pack);
err_ret:
        return ret;
}

#if 0
int ly_pwrite(int fd, const char *buf, size_t size, yfs_off_t offset)
{
        int ret;
        struct aiocb iocb;
        sem_t sem;
        buffer_t pack;

        DBUG("write fd %d size %lu off %lu\n", fd, (unsigned long)size,
                        (unsigned long)offset);

        sem_init(&sem, 0, 0);

        mbuffer_init(&pack, 0);

        ret = mbuffer_copy(&pack, buf, size);
        if (ret)
                GOTO(err_ret, ret);

        _memset(&iocb, 0x0, sizeof(struct aiocb));
        iocb.aio_nbytes = size;
        iocb.aio_offset = offset;
        iocb.aio_fildes = fd;
        iocb.aio_buf = &pack;
        iocb.aio_sigevent.sigev_notify = SIGEV_THREAD;
        iocb.aio_sigevent.sigev_notify_function
                = __ly_pread_callback;
        iocb.aio_sigevent.sigev_notify_attributes = NULL;
        iocb.aio_sigevent.sigev_value.sival_ptr = &sem;

        UNIMPLEMENTED(__DUMP__);
#if 0
        ret = ly_write_aio(&iocb);
        if (ret)
                GOTO(err_ret, ret);
#endif

        ret = _sem_wait(&sem);
        if (ret)
                GOTO(err_ret, ret);

        ret = __aio_error(&iocb);
        if (ret)
                GOTO(err_ret, ret);

        ret = __aio_return(&iocb);

        mbuffer_free(&pack);

        return ret;
err_ret:
        return -ret;
}
#endif

int ly_release(int fd)
{
        (void) fd;
        UNIMPLEMENTED(__DUMP__);

        return 0;
}

int ly_truncate(const char *path, off_t length)
{
        int ret;
        fileid_t fileid;

        ret = sdfs_lookup_recurive(path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        ret = sdfs_truncate(NULL, &fileid, length);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int ly_symlink(const char *link_target, const char *link_name)
{
        int ret;
        char lname[MAX_NAME_LEN];
        fileid_t lparent;

        YASSERT(link_target[0] != '\0');
        YASSERT(link_name[0] != '\0');

        ret = sdfs_splitpath(link_name, &lparent, lname);
        if(ret)
                GOTO(err_ret, ret);

        ret = sdfs_symlink(NULL, &lparent, lname, link_target, 0777, -1, -1);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int ly_link(const char *target, const char *link)
{
        int ret;
        char lname[MAX_NAME_LEN];
        fileid_t lparent, fileid;

        YASSERT(target[0] != '\0');
        YASSERT(link[0] != '\0');

        ret = sdfs_lookup_recurive(target, &fileid);
        if(ret)
                GOTO(err_ret, ret);

        ret = sdfs_splitpath(link, &lparent, lname);
        if(ret)
                GOTO(err_ret, ret);

        ret = sdfs_link2node(NULL, &fileid, &lparent, lname);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int ly_readlink(const char *path, char *buf, size_t *buflen)
{
        int ret;
        fileid_t fileid;

        ret = sdfs_lookup_recurive(path, &fileid);
        if(ret)
                GOTO(err_ret, ret);

        ret = sdfs_readlink(NULL, &fileid, buf, (uint32_t*)buflen);
        if(ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

#endif
#if 0
int ly_set_default_acl(const char *path, mode_t mode)
{
        int ret;
        fileid_t fileid;
        void *acl_buf;
        size_t acl_buf_size;

        acl_buf_size = posix_acl_ea_size(ACL_DEFAULT_EA_ENTRY_COUNT);
        ret = ymalloc(&acl_buf, acl_buf_size);
        if (ret)
                GOTO(err_ret, ret);

        ret = posix_acl_default_get(acl_buf, acl_buf_size, mode);
        if (ret) {
                ret = EINVAL;
                GOTO(err_free, ret);
        }

        ret = sdfs_lookup_recurive(path, &fileid);
        if(ret)
                GOTO(err_free, ret);

        ret = sdfs_setxattr(NULL, &fileid, ACL_EA_ACCESS,
                           acl_buf, acl_buf_size, USS_XATTR_DEFAULT);
        if(ret)
                GOTO(err_free, ret);

        yfree(&acl_buf);
        return 0;

err_free:
        yfree(&acl_buf);
err_ret:
        return ret;
}
#endif

#include "atomic_id.h"

static sy_rwlock_t atomic_lock;

static int _check_rbuf(container_id_t *rbuf) {
        int ret;
        
        if (rbuf->len == 0) {
                DWARN("reply len %u\n", rbuf->len);
                ret = ENOENT;
                GOTO(err_ret, ret);
        }
        
        if (rbuf->crc != crc32_sum(rbuf->buf, rbuf->len)) {
                ret = EIO;
                GOTO(err_ret, ret);
        }
        
        return 0;
err_ret:
        return ret;
}


static inline int __getid_dec_nolock(const char *id_path, uint64_t *_id)
{
        int ret, fd;
        char path[MAX_PATH_LEN], tmp[MAX_PATH_LEN];
        char buf[MAX_NAME_LEN];
        uint64_t id;
        container_id_t *rbuf;

        snprintf(path, MAX_PATH_LEN, "%s/atomic_id", id_path);
        snprintf(tmp, MAX_PATH_LEN, "%s/atomic_id_tmp", id_path);

        /*DINFO("path: %s\n", path);*/
        ret = _get_value(path, buf, MAX_NAME_LEN);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        rbuf = (void *)buf;
        ret = _check_rbuf(rbuf);
        if (ret)
                GOTO(err_ret, ret);

        ret = sscanf(rbuf->buf, "%llu", (LLU *)&id);
        if (ret != 1) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }

        id--;

        fd = _open_for_samba(tmp, O_WRONLY | O_SYNC | O_CREAT, 0777);
        if (fd < 0) {
                ret = -fd;
                GOTO(err_ret, ret);
        }

        snprintf(rbuf->buf, MAX_NAME_LEN, "%llu", (LLU)id);
        rbuf->len = strlen(rbuf->buf) + 1;
        rbuf->crc = crc32_sum(rbuf->buf, rbuf->len);

        ret = _write(fd, rbuf, sizeof(*rbuf) + rbuf->len);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_close, ret);
        }

        ret = fsync(fd);
        if (ret)
                GOTO(err_close, ret);

        close(fd);

        ret = rename(tmp, path);
        if (ret){
                ret = errno;
                GOTO(err_ret, ret);
        }

        DBUG("get %llu\n", (LLU)id);
        *_id = id;
        chmod(path, 0777);

        return 0;
err_close:
        close(fd);
err_ret:
        YASSERT(0);
        return ret;
}

static inline int __getid_inc_nolock(const char *id_path, uint64_t *_id)
{
        int ret, fd;
        char path[MAX_PATH_LEN], tmp[MAX_PATH_LEN];
        char buf[MAX_NAME_LEN];
        uint64_t id;
        container_id_t *rbuf;

        snprintf(path, MAX_PATH_LEN, "%s/atomic_id", id_path);
        snprintf(tmp, MAX_PATH_LEN, "%s/atomic_id_tmp", id_path);

        /*DINFO("path: %s\n", path);*/
        ret = _get_value(path, buf, MAX_NAME_LEN);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        rbuf = (void *)buf;
        ret = _check_rbuf(rbuf);
        if (ret)
                GOTO(err_ret, ret);

        ret = sscanf(rbuf->buf, "%llu", (LLU *)&id);
        if (ret != 1) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }

        id++;

        fd = _open_for_samba(tmp, O_WRONLY | O_SYNC | O_CREAT, 0777);
        if (fd < 0) {
                ret = -fd;
                GOTO(err_ret, ret);
        }

        snprintf(rbuf->buf, MAX_NAME_LEN, "%llu", (LLU)id);
        rbuf->len = strlen(rbuf->buf) + 1;
        rbuf->crc = crc32_sum(rbuf->buf, rbuf->len);

        ret = _write(fd, rbuf, sizeof(*rbuf) + rbuf->len);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_close, ret);
        }

        ret = fsync(fd);
        if (ret)
                GOTO(err_close, ret);

        close(fd);

        ret = rename(tmp, path);
        if (ret){
                ret = errno;
                GOTO(err_ret, ret);
        }

        DBUG("get %llu\n", (LLU)id);
        chmod(path, 0777);
        *_id = id;

        return 0;
err_close:
        close(fd);
err_ret:
        YASSERT(0);
        return ret;
}

int yatomic_set_nolock(const char *id_path, uint64_t id)
{
        int ret, fd;
        char path[MAX_PATH_LEN], tmp[MAX_PATH_LEN];
        char buf[MAX_NAME_LEN];
        struct stat stbuf;
        container_id_t *rbuf;

        snprintf(path, MAX_PATH_LEN, "%s/atomic_id", id_path);
        snprintf(tmp, MAX_PATH_LEN, "%s/atomic_id_tmp", id_path);

        ret = stat(path, &stbuf);
        if (ret) {
                ret = errno;
                if (ret != ENOENT)
                        GOTO(err_ret, ret);
        } else {
                ret = unlink(path);
                if(ret) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        fd = _open_for_samba(tmp, O_WRONLY | O_SYNC | O_CREAT, 0777);
        if (fd < 0) {
                ret = -fd;
                GOTO(err_ret, ret);
        }

        rbuf = (void *)buf;
        snprintf(rbuf->buf, MAX_NAME_LEN, "%llu", (LLU)id);
        rbuf->len = strlen(rbuf->buf) + 1;
        rbuf->crc = crc32_sum(rbuf->buf, rbuf->len);

        ret = _write(fd, rbuf, sizeof(*rbuf) + rbuf->len);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_close, ret);
        }

        ret = fsync(fd);
        if (ret)
                GOTO(err_close, ret);

        close(fd);

        ret = rename(tmp, path);
        if (ret){
                ret = errno;
                GOTO(err_ret, ret);
        }

        chmod(path, 0777);

        return 0;
err_close:
        close(fd);
err_ret:
        return ret;
}

int yatomic_get_and_inc_dura_init()
{
        sy_rwlock_init(&atomic_lock, NULL);
        return 0;
}

int yatomic_get_and_inc_dura(const char*id_path, uint64_t *id)
{
        int ret;

        ret = sy_rwlock_wrlock(&atomic_lock);
        if (ret)
                GOTO(err_ret, ret);

        ret = __getid_inc_nolock(id_path, id);
        if (ret)
                GOTO(err_ret, ret);

        sy_rwlock_unlock(&atomic_lock);
        return 0;
err_ret:
        YASSERT(0);
        return ret;
}

int yatomic_get_and_dec_dura(const char*id_path, uint64_t *id)
{
        int ret;

        ret = sy_rwlock_wrlock(&atomic_lock);
        if (ret)
                GOTO(err_ret, ret);

        ret = __getid_dec_nolock(id_path, id);
        if (ret)
                GOTO(err_ret, ret);

        sy_rwlock_unlock(&atomic_lock);
        return 0;
err_ret:
        YASSERT(0);
        return ret;
}



#include <sys/types.h>
#include <sys/stat.h>
#include <sys/poll.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSCDS

#include "yfs_conf.h"
#include "net_global.h"
#include "yfscds_conf.h"
#include "chk_meta.h"
#include "cds_volume.h"
#include "disk.h"
#include "chkinfo.h"
#include "jnl_proto.h"
#include "cds_hb.h"
#include "dbg.h"

#if 0
typedef struct {
        int inited;

        sy_spinlock_t lock;

        uint64_t offset; /*journal play offset*/
        jnl_handle_t jnl;

        uint64_t count; /*chunk count*/
        hashtable_t tab; /*chunk entry table*/
} __chkinfo_t;

static __chkinfo_t chkinfo;

typedef struct {
        chkid_t id;
        uint64_t hit;
        int size;
} entry_t;

typedef struct {
        entry_t **array;
        int count;
} entry_array_t;

int chkinfo_add(const chkid_t *chkid, int size)
{
        int ret;
        chkjnl_t chkjnl;

        memset(&chkjnl, 0x0, sizeof(chkjnl_t));

        chkjnl.op = CHKOP_WRITE;
        chkjnl.increase = size;
        chkjnl.chkid = *chkid;

        //do  nothing
        goto out;

        ret = cds_volume_update(chkid->volid, chkjnl.increase);
        if (ret)
                GOTO(err_ret, ret);

        ret = jnl_append1(&chkinfo.jnl, (char *)&chkjnl, sizeof(chkjnl));
        if (ret) {
                GOTO(err_ret, ret);
        }

out:
        return 0;
err_ret:
        return ret;
}

int chkinfo_del(const chkid_t *chkid, int size)
{
        int ret;
        chkjnl_t chkjnl;

        memset(&chkjnl, 0x0, sizeof(chkjnl_t));

        chkjnl.op = CHKOP_DEL;
        chkjnl.increase = -size;
        chkjnl.chkid = *chkid;

        ret = cds_volume_update(chkid->volid, chkjnl.increase);
        if (ret)
                GOTO(err_ret, ret);

        ret = jnl_append1(&chkinfo.jnl, (char *)&chkjnl, sizeof(chkjnl));
        if (ret) {
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __chkinfo_merge_jnl(const void *buf, int len, int64_t offset, void *arg)
{
        int ret;
        hashtable_t tab = arg;
        const chkjnl_t *op = buf;
        entry_t *ent;

        (void) len;
        (void) offset;

        if (op->op == CHKOP_WRITE) {
                ent = hash_table_find(tab, (void *)&op->chkid);
                if (ent == NULL) {
                        ret = ymalloc((void **)&ent, sizeof(*ent));
                        if (ret)
                                GOTO(err_ret, ret);

                        ent->id = op->chkid;
                        ent->hit = 0;
                        ent->size = op->increase;

                        ret = hash_table_insert(tab, (void *)ent, (void *)&ent->id, 0);
                        if (ret)
                                GOTO(err_free, ret);


                        chkinfo.count++;
                }
        } else if (op->op == CHKOP_DEL) {
                ret = hash_table_remove(tab, (void *)&op->chkid, (void **)&ent);
                if (ret) {
                        if (ret != ENOENT)
                                GOTO(err_ret, ret);
                }

                chkinfo.count--;
                yfree((void **)&ent);
        } else {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        return 0;
err_free:
        yfree((void **)&ent);
err_ret:
        return ret;
}



static int __chkinfo_merge(hashtable_t tab)
{
        int ret, left, len, eof;
        char path[MAX_PATH_LEN], buf[MAX_BUF_LEN];
        uint64_t off;
        jnl_handle_t jnl;

        ret = sy_spin_lock(&chkinfo.lock);
        if (ret)
                GOTO(err_ret, ret);

        snprintf(path, MAX_PATH_LEN, "%s/chkinfo/%s",
                 ng.home, YFS_CDS_DIR_JNL_PRE);

        ret = jnl_open(path, &jnl, 0);
        if (ret)
                GOTO(err_lock, ret);

        off = chkinfo.offset;
        while (1) {
                ret = jnl_pread(&jnl, buf, MAX_BUF_LEN, off);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_jnl, ret);
                }

                if (ret == 0)
                        break;

                len = ret;

                ret = jnl_iterator_buf(buf, len, off, __chkinfo_merge_jnl, tab, &left, &eof);
                if (ret)
                        GOTO(err_jnl, ret);

                off += (len - left);

                if (eof)
                        break;

                DBUG("left %u len %u off %llu\n", left, len, (LLU)off);
        }

        jnl_close(&jnl);

        chkinfo.offset = off;

        sy_spin_unlock(&chkinfo.lock);

        return 0;
err_jnl:
        jnl_close(&jnl);
err_lock:
        sy_spin_unlock(&chkinfo.lock);
err_ret:
        return ret;
}

static void __chkinfo_scan_entry(void *_array, void *_ent)
{
        int ret;
        entry_array_t *array = _array;
        entry_t *ent = _ent;

        ret = chunk_gethit(&ent->id, &ent->hit);
        if (ret) {
                if (ret == ENOENT) {
                        return;
                } else
                        UNIMPLEMENTED(__DUMP__);
        }

        array->array[array->count] = ent;
        array->count++;
}

int __chkinfo_scan(entry_t **_array)
{
        entry_array_t array;

        array.count = 0;
        array.array = _array;

        hash_iterate_table_entries(chkinfo.tab, __chkinfo_scan_entry, &array);

        return array.count;
}

static int __chkinfo_sort_cmp(const void *_s1, const void *_s2)
{
        const entry_t *s1 = *(entry_t **)_s1, *s2 = *(entry_t **)_s2;

        if (s2->hit > s1->hit)
                return 1;
        else if (s2->hit < s1->hit)
                return -1;
        else
                return 0;
}

static void __chkinfo_sort(entry_t **array, int count)
{
        ARRAY_SORT(array, count, __chkinfo_sort_cmp);
}

static int __chkinfo_save(entry_t **array, int count, uint64_t offset)
{
        int ret, left, max, cp, i, j, fd;
        char buf[MAX_BUF_LEN], path[MAX_PATH_LEN], newpath[MAX_PATH_LEN];
        chklist_t *chklist;
        entry_t *ent;

        snprintf(path, MAX_PATH_LEN, "%s/chkinfo/chklist.tmp", ng.home);

        fd = _open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
        if (fd < 0) {
                ret = -fd;
                GOTO(err_ret, ret);
        }

        ret = _write(fd, &offset, sizeof(offset));
        if (ret < 0) {
                ret = -ret;
                GOTO(err_fd, ret);
        }

        max = MAX_BUF_LEN / sizeof(*chklist);
        left = count;
        j = 0;
        while (left) {
                cp = max < left ? max : left;
                chklist = (void *)buf;
                for (i = 0; i < cp; i++) {
                        ent = array[j * max + i];
                        chklist[i].id = ent->id;
                        chklist[i].size = ent->size;
                }

                ret = _write(fd, buf, sizeof(*chklist) * cp);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_fd, ret);
                }

                left -= cp;
                j++;
        }

        close(fd);

        snprintf(newpath, MAX_PATH_LEN, "%s/chkinfo/chklist", ng.home);
        unlink(newpath);
        rename(path, newpath);

        return 0;
err_fd:
        close(fd);
err_ret:
        return ret;
}

static int __chkinfo_load(hashtable_t tab)
{
        int ret, max, i, fd, got, buflen;
        char buf[MAX_BUF_LEN], path[MAX_PATH_LEN];
        chklist_t *chklist;
        entry_t *ent;
        struct stat stbuf;
        uint64_t offset;

        snprintf(path, MAX_PATH_LEN, "%s/chkinfo/chklist", ng.home);

        fd = open(path, O_RDONLY);
        if (fd < 0) {
                ret = errno;
                if (ret == ENOENT) {
                        goto out;
                } else
                        GOTO(err_ret, ret);
        }

        ret = fstat(fd, &stbuf);
        if (ret < 0) {
                ret = errno;
                GOTO(err_fd, ret);
        }

        if (stbuf.st_size < (int)sizeof(chkinfo.offset)) {
                ret = EIO;
                GOTO(err_fd, ret);
        }

        ret = _read(fd, &chkinfo.offset, sizeof(chkinfo.offset));
        if (ret < 0) {
                ret = -ret;
                GOTO(err_fd, ret);
        }

        offset = sizeof(chkinfo.offset);
        max = (MAX_BUF_LEN / sizeof(*chklist));
        buflen = max * sizeof(*chklist);
        chkinfo.count = 0;
        while (1) {
                ret = _pread(fd, buf, buflen, offset);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_fd, ret);
                }

                if (ret == 0) {
                        break;
                }

                got = ret / sizeof(*chklist);
                offset += ret;

                chklist = (void *)buf;

                for (i = 0; i < got ; i++) {
                        ret = ymalloc((void **)&ent, sizeof(*ent));
                        if (ret)
                                GOTO(err_fd, ret);

                        ent->id = chklist[i].id;
                        ent->size = chklist[i].size;
                        ent->hit = 0;

                        ret = hash_table_insert(tab, (void *)ent, (void *)&ent->id, 0);
                        if (ret)
                                GOTO(err_fd, ret);

                        chkinfo.count++;
                }
        }

        close(fd);

out:
        DBUG("load chk %llu\n", (LLU)chkinfo.count);

        return 0;
err_fd:
        close(fd);
err_ret:
        return ret;
}

static uint32_t __key(const void *args)
{
        return ((chkid_t *)args)->id;
}

static int __cmp(const void *v1, const void *v2)
{
        const entry_t *ent = v1;
        const chkid_t *chkid = v2;

        return chkid_cmp(chkid, &ent->id);
}

int chkinfo_dump(chklist_t **_chklist, int *_count)
{
        int ret, count, i;
        entry_t **array, *ent;
        chklist_t *chklist;

        YASSERT(chkinfo.tab == NULL);
        YASSERT(chkinfo.count == 0);
        chkinfo.tab = hash_create_table(__cmp, __key, "chkinfo_tab");
        if (chkinfo.tab == NULL) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        ret = __chkinfo_load(chkinfo.tab);
        if (ret)
                GOTO(err_ret, ret);

        ret = __chkinfo_merge(chkinfo.tab);
        if (ret)
                GOTO(err_ret, ret);

        DINFO("load chk %llu\n", (LLU)chkinfo.count);

        if (chkinfo.count == 0)
                goto out;

        ret = ymalloc((void **)&array, sizeof(array) * chkinfo.count);
        if (ret)
                GOTO(err_tab, ret);

        count = __chkinfo_scan(array);

        DBUG("scan count %u\n", count);

        __chkinfo_sort(array, count);

        ret = __chkinfo_save(array, count, chkinfo.offset);
        if (ret)
                GOTO(err_free, ret);

        if (count > (1024 * 1024)) {
                DWARN("too many chunk %u\n", count);
        }

        chklist = *_chklist;
        if (*_count < count) {
                ret = yrealloc((void **)&chklist, *_count * sizeof(*chklist), count * sizeof(*chklist));
                if (ret)
                        GOTO(err_free, ret);
        }

        for (i = 0; i < count ; i++) {
                ent = array[i];
                chklist[i].id = ent->id;
                chklist[i].size = ent->size;
                chklist[i].hit = ent->hit;

                DBUG("chk "CHKID_FORMAT" size %u hit %llu\n", CHKID_ARG(&ent->id), ent->size, (LLU)ent->hit);
        }

        *_count = count;
        *_chklist = chklist;

        yfree((void **)&array);
        hash_destroy_table(chkinfo.tab, NULL);
        chkinfo.tab = NULL;
        chkinfo.count = 0;

out:
        return 0;
err_free:
        yfree((void **)&array);
err_tab:
        hash_destroy_table(chkinfo.tab, NULL);
        chkinfo.tab = NULL;
err_ret:
        return ret;
}

int chkinfo_init()
{
        int ret;
        char path[MAX_PATH_LEN];

        if (chkinfo.inited)
                return 0;

        snprintf(path, MAX_PATH_LEN, "%s/chkinfo/%s",
                 ng.home, YFS_CDS_DIR_JNL_PRE);

        ret = sy_spin_init(&chkinfo.lock);
        if (ret)
                GOTO(err_ret, ret);

        ret = jnl_open(path, &chkinfo.jnl, O_RDWR);
        if (ret)
                GOTO(err_ret, ret);

        chkinfo.inited = 1;
        chkinfo.tab = NULL;

        return 0;
err_ret:
        return ret;
}

#else

int chkinfo_add(const chkid_t *chkid, int size)
{
        (void) chkid;
        (void) size;
        UNIMPLEMENTED(__WARN__);

        return 0;
}

int chkinfo_del(const chkid_t *chkid, int size)
{
        (void) chkid;
        (void) size;
        UNIMPLEMENTED(__WARN__);

        return 0;
}

int chkinfo_init()
{
        return 0;
}

#endif

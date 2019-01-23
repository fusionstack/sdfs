#ifndef __YCOMMANDS_H
#define __YCOMMANDS_H

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

extern "C" {

#include "configure.h"
#include "sdfs_lib.h"
#include "chk_meta.h"
#include "yfscds_conf.h"
#include "net_global.h"
#include "ylib.h"
#include "job_dock.h"
#include "proxy_lib.h"
#include "yfs_proxy_file.h"
}

#ifdef dir_for_each
#undef dir_for_each
#endif
#define dir_for_each(buf, buflen, de, off)                              \
        for (de = (struct dirent *)(buf);                               \
                        (char *)de < (char *)(buf) + buflen ;              \
                        off = de->d_off, de = (struct dirent *)(de + de->d_reclen))

#define YFS_CDS_DIR_DISK_PRE "/sysy/yfs/cds"

net_global_t ng;

static inline int __load_cmp(const void *arg1, const void *arg2) {
        return ((diskping_stat_t *)arg2)->measurement
                - ((diskping_stat_t *)arg1)->measurement;
}

int cmd_rmdir(const char *path) {
        int ret;

        ret = ly_rmdir(path);
        if (ret) {
                fprintf(stderr, "ly_rmdir(%s) %s\n", path, strerror(ret));
        }

        return 0;
}

int cmd_stat_fs() {
        return 0;
}

int cmd_stat_file(const char *path) {
        int ret;
        fileid_t fileid;
        struct stat stbuf;
        uint32_t mode;

        if (path[0] == '/') {
                ret = sdfs_lookup_recurive(path, &fileid);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
#ifdef __x86_64__
                ret = sscanf(path, "%lu_v%u", &fileid.id, &fileid.version);
#else
                ret = sscanf(path, "%llu_v%u", &fileid.id, &fileid.version);
#endif
                if (ret != 2) {
                        ret = EINVAL;
                        GOTO(err_ret, ret);
                }
        }

        ret = sdfs_getattr(&fileid, &stbuf);
        if (ret)
                GOTO(err_ret, ret);

        printf("%s mode %s/%s %o, %llu link:%lu\n", path,
                        S_ISDIR((stbuf).st_mode) ? "d" : "",
                        S_ISREG((stbuf).st_mode) ? "f" : "", stbuf.st_mode & 00777,
                        (LLU)(stbuf).st_size,
                        (unsigned long)stbuf.st_nlink);

        if (S_ISDIR((stbuf).st_mode))
                goto out;       /* not file */

        ret = raw_printfile(&fileid);
        if (ret)
                GOTO(err_ret, ret);

out:
        return 0;
err_ret:
        return 0;
}

int cmd_stat_chunk(int cn, const char *chkspec, int clear) {
        int ret, fd;
        struct stat stbuf;
        uint32_t count;
        chkmeta2_t md;
        char cascade[MAX_PATH_LEN];
        char chkpath[MAX_PATH_LEN];
        uint32_t crc[YFS_CDS_CRC_COUNT], crc1;
        char buf[YFS_CRC_SEG_LEN];
        uint64_t i,j;
        chkid_t chkid;

#ifdef  __x86_64_
        ret = sscanf(chkspec, "%lu_v%u/%u", &chkid.id, &chkid.version, &chkid.idx);
#else
        ret = sscanf(chkspec, "%llu_v%u/%u", (LLU *)&chkid.id, &chkid.version, &chkid.idx);
#endif
        if (ret != 3) {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        cascade_id2path(cascade, MAX_PATH_LEN, chkid.id);

        sprintf(chkpath, "%s/%d/%s%s_v%u/%u", YFS_CDS_DIR_DISK_PRE,
                        cn,"ychunk", cascade, chkid.version, chkid.idx);
        sprintf(ng.home, "%s/%d", YFS_CDS_DIR_DISK_PRE,cn);

        fd = open(chkpath, O_RDWR);
        if (fd == -1) {
                ret = errno;
                fprintf(stderr, "open(%s, ...) %s\n", chkpath, strerror(ret));
                return 1;
        }

        ret = fstat(fd, &stbuf);
        if (ret == -1) {
                ret = errno;
                fprintf(stderr, "fstat(%s, ...) %s\n", chkpath, strerror(ret));
                return 1;
        }

        count = sizeof(chkmeta2_t);

        ret = sy_pread(fd, (char *)&md, &count, 0);
        if (ret) {
                fprintf(stderr, "read chk_meta %s\n", strerror(ret));
                return 1;
        } else if (count != sizeof(chkmeta2_t)) {
                fprintf(stderr, "too few data toread %d\n", count);
                return 1;
        }

        DINFO("size %llu %llu\n", (LLU)sizeof(chkmeta2_t), (LLU)sizeof(fileid_t));

        ret = crc32_md_verify((void *)&md, sizeof(chkmeta2_t));
        if (ret) {
                ret = EIO;
                DERROR("bad file %s\n", chkpath);
                GOTO(err_ret, ret);
        }

        memset(crc, 0x0, YFS_CDS_CRC_LEN);

        ret = _pread(fd, crc, YFS_CDS_CRC_LEN, sizeof(chkmeta2_t));
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        for (i = 0; i < YFS_CDS_CRC_COUNT; i++) {
                memset(buf, 0x0, YFS_CRC_SEG_LEN);
                ret = _pread(fd, buf, YFS_CRC_SEG_LEN, YFS_CRC_SEG_LEN * i + YFS_CDS_CHK_OFF);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                if (ret == 0)
                        break;

                crc1 = crc32_sum(buf, ret);

                if (crc[i] != crc1 && crc[i] != 0) {
                        DERROR("seg %llu crc %x:%x len %u\n", (LLU)i, crc[i], crc1, ret);
                } else {
                        DBUG("seg %llu crc %x:%x\n", (LLU)i, crc[i], crc1);
                }
        }

        if (clear) {
                memset(crc, 0x0, YFS_CDS_CRC_LEN);

                ret = _pwrite(fd, crc, YFS_CDS_CRC_LEN, sizeof(chkmeta2_t));
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }
        }

        printf(
                        "Chunk: %s, clear %u id chkid %llu_v%u[%u] md version %x, crc %u\n"
                        " offset %u count %u max %u merged version %llu, rept version %llu\n"
                        " volid %u\n",
                        chkpath, clear, (LLU)md.proto.chkid.id, md.proto.chkid.version,
                        md.proto.chkid.idx,
                        md.crc.version, md.crc.crc,
                        md.chkoff, md.chklen, md.chkmax,
                        (LLU)md.chk_version, (LLU)md.proto.max_version,
                        md.proto.chkid.volid);

        (void) sy_close(fd);

        return 0;
err_ret:
        return ret;
}

int cmd_ping() {
        int ret, len;
        char buf[MAX_BUF_LEN], name[MAX_NAME_LEN];
        diskping_stat_t *ds;
        struct sockaddr_in sin;

        len = MAX_BUF_LEN;

        ret = ly_pingmds(buf, &len);
        if (ret) {
                fprintf(stderr, "ly_pingmds() %s\n", strerror(ret));
                return 0;
        }

        qsort(buf, len / sizeof(diskping_stat_t),
                        sizeof(diskping_stat_t), __load_cmp);
        ds = (diskping_stat_t *)buf;

        _memset(&sin, 0, sizeof(sin));
        sin.sin_family = AF_INET;

        while ((unsigned)len >= sizeof(diskping_stat_t)) {
                sin.sin_addr.s_addr = htonl(ds->sockaddr);

                sprintf(name, "%s:%u", inet_ntoa(sin.sin_addr), ds->sockport);

                printf("%s nid (%llu_v%u) status:%d %llu %llu load %u\n",
                                strlen(ds->rname) == 0 ? name : ds->rname,
                                (LLU)ds->diskid.id, ds->diskid.version,
                                ds->diskstat,
                                (LLU)ds->disktotal,
                                (LLU)(ds->disktotal - ds->diskfree),
                                ds->measurement);

                len -= sizeof(diskping_stat_t);
                ds = (diskping_stat_t *)((char *)ds + sizeof(diskping_stat_t));
        }

        return 0;
}

int cmd_lvm_create(const char* name, uint64_t size) {
        int ret;

        ret = ly_lvcreate(name, size);
        if (ret) {
                printf("%s\n", strerror(ret));
        }

        return 0;
}

int cmd_lvm_list() {
        int ret;

        ret = ly_lvlist();
        if (ret) {
                printf("%s\n", strerror(ret));
        }

        return 0;
}

int cmd_ls(const char *path) {
        int ret;
        char depath[MAX_PATH_LEN], perms[11], date[64];
        off_t offset;
        int delen;//, len;
        char *de0;
        struct dirent *de;
        struct stat stbuf;

        offset = 0;
        de0 = NULL;
        delen = 0;

        while (1) {
                ret = ly_readdirplus(path, offset, (void **)&de0, &delen);
                if (ret) {
                        fprintf(stderr, "ly_readdir(%s, ...) %s\n", path, strerror(ret));
                        return 0;
                } else if (delen == 0) {
                        break;
                }

                if (delen > 0) {
                        dir_for_each(de0, delen, de, offset) {
                                YASSERT(de->d_reclen <= delen);
                                if (strcmp(de->d_name, ".") == 0
                                                || strcmp(de->d_name, "..") == 0) {
                                        //offset = de->d_off;
                                        continue;
                                }

                                if (strcmp(path, "/") == 0)
                                        sprintf(depath, "/%s", de->d_name);
                                else
                                        snprintf(depath, MAX_PATH_LEN, "%s/%s", path,
                                                        de->d_name);

                                _memset(&stbuf, 0x0, sizeof(struct stat));

                                /* stat() the file. Of course there's a race condition -
                                 * the directory entry may have gone away while we
                                 * read it, so ignore failure to stat
                                 */
                                ret = ly_getattr(depath, &stbuf);
                                if (ret) {
                                        //offset = de->d_off;
                                        continue;
                                }

                                mode_to_permstr((uint32_t)stbuf.st_mode, perms);
                                stat_to_datestr(&stbuf, date);

                                /* lrwxrwxrwx 1 500 500 12 Jun 30 04:31 sy -> wk/sy
                                 * drwxrwxr-x 2 500 500 4096 Jun 28 10:59 good
                                 * -rw-rw-r-- 1 500 500 677859 Feb 27  2006 lec01.pdf
                                 */
                                printf("%s %lu yfs yfs %llu %s %s\n", perms,
                                                (unsigned long)stbuf.st_nlink,
                                                (unsigned long long)stbuf.st_size, date,
                                                de->d_name);
                                //offset = de->d_off;
                        }
                } else
                        break;

                yfree((void **)&de0);
        }

        return 0;
}

int cmd_mkdir(const char* path) {
        int ret;

        ret = ly_mkdir(path, 0755);
        if (ret) {
                fprintf(stderr, "ly_mkdir(%s,...) %s\n", path, strerror(ret));
        }

        printf("mkdir finished\n");
        return 0;
}

#define BUF_LEN (524288 * 2)
//#define BUF_LEN 1000
#define THREADS 16
#define YFS_FILE 1
#define LOCAL_FILE 0
#define BLK_MIN (1024 * 1024 * 64)

const char *get_name(const char *path)
{
        int len;
        const char *c, *name;

        len = strlen(path);

        c = &path[len - 1];
        name = c;

        while (c > path) {
                c --;

                if (*c == '/')
                        break;

                name = c;
        }

        return name;
}

typedef struct {
        int srctype;
        int src;
        int desttype;
        int dest;
        uint64_t offset;
        uint64_t size;
        sem_t sem;
        int percentage;
} interval_t;

static void *__worker(void *arg)
{
        int ret, cp;
        interval_t *interval;
        int64_t left, off;
        char *buf;

        ret = ymalloc((void **)&buf, BUF_LEN);
        if (ret)
                GOTO(err_ret, ret);

        DBUG("inv %p\n", arg);

        interval = (interval_t *)arg;

        left = interval->size;
        off = interval->offset;

        DBUG("copy off %llu size %llu\n", (LLU)interval->offset, (LLU)interval->size);

        while (left > 0) {
                cp = left < BUF_LEN ? left : BUF_LEN;

                if (interval->srctype == YFS_FILE) {
                        ret = yfs_pread(interval->src, buf, cp, off);
                        if (ret < 0) {
                                ret = -ret;

                                GOTO(err_ret, ret);
                        }
                } else {
                        ret = pread(interval->src, buf, cp, off);
                        if (ret < 0) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }
                }

                cp = ret;

                if (interval->desttype == YFS_FILE) {
                        ret = yfs_pwrite(interval->dest, buf, cp, off);
                        if (ret < 0) {
                                ret = -ret;

                                GOTO(err_ret, ret);
                        }
                } else {
                        ret = pwrite(interval->dest, buf, cp, off);
                        if (ret < 0) {
                                ret = errno;

                                GOTO(err_ret, ret);
                        }
                }

                off += cp;
                left -= cp;

                DBUG("copy left %llu\n", (LLU)left);

                interval->percentage = ((interval->size - left) * 100) / interval->size;
        }

        DBUG("copy finish\n");

        sem_post(&interval->sem);

        return NULL;
err_ret:
        interval->percentage = -1;
        return NULL;
}

int cmd_cp(const char *_src, const char *_dest)
{
        int ret, srctype = 0, desttype = 0, from, to, trunc = 0, works;
        char src[MAX_PATH_LEN], dest[MAX_PATH_LEN], msg[MAX_PATH_LEN * 4];
        struct stat stbuf, stbuf1;
        uint64_t step, off;
	int threads, i, percentage;
        interval_t interval[THREADS], *inv;
        pthread_attr_t ta;
        pthread_t th;

        static int proxy_inited = 0;
        if (!proxy_inited) {
                // ret = proxy_client_init("ycp");
                // if (ret) {
                //         fprintf(stderr, "ly_init() %s\n", strerror(ret));
                //         return 0;
                // }
                // proxy_inited = 1;
        }

        memset(msg, 0x0, MAX_PATH_LEN * 4);
        memset(src, 0x0, MAX_PATH_LEN);
        memset(dest, 0x0, MAX_PATH_LEN);
        sprintf(msg, "copy ");

        if (_src[0] == ':') {
                srctype = YFS_FILE;
                memcpy(src, _src + 1, strlen(_src));
                sprintf(msg + strlen(msg), "from yfs:%s ", src);

                from = yfs_open(src);
                if (from < 0) {
                        ret = -from;

                        GOTO(err_ret, ret);
                }

                ret = yfs_fstat(from, &stbuf);
                if (ret)
                        GOTO(err_ret, ret);
        } else {
                srctype = LOCAL_FILE;
                memcpy(src, _src, strlen(_src));
                sprintf(msg + strlen(msg), "from local:%s ", src);

                from = open(src, O_RDONLY);
                if (from < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                ret = stat(src, &stbuf);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        if (_dest[0] == ':') {
                desttype = YFS_FILE;
                memcpy(dest, _dest + 1, strlen(_dest));

                sprintf(msg + strlen(msg), "to :%s ", dest);


                to = yfs_create(dest, stbuf.st_mode);
                if (to < 0) {
			printf("create fail\n");
                        ret = -to;
                        GOTO(err_ret, ret);
                }

                if (trunc) {
                }
        } else {
                desttype = LOCAL_FILE;
                memcpy(dest, _dest, strlen(_dest));

                ret = stat(dest, &stbuf1);
                if (ret < 0) {
                        ret = errno;

                        if (ret != ENOENT)
                                GOTO(err_ret, ret);
                }

                if (S_ISDIR(stbuf1.st_mode)) {
                        sprintf(dest + strlen(dest), "/%s", get_name(src));
                }

                sprintf(msg + strlen(msg), "to local:%s", dest);

                to = open(dest, O_CREAT | O_EXCL | O_WRONLY, stbuf.st_mode);
                if (to < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }
        }

        printf("%s\n", msg);

        pthread_attr_init(&ta);
        pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        step = stbuf.st_size / THREADS;

        if (step < BLK_MIN) {
                step = BLK_MIN;
                threads = stbuf.st_size / BLK_MIN;
		if (threads == 0)
			threads = 1;
        } else
                threads = THREADS;

        DINFO("size %llu step %llu threads %u\n", (LLU)stbuf.st_size, (LLU)step, threads);

        works = 0;
        off = 0;
        for (i = 0; i < threads; i++) {
                inv = &interval[i];

                inv->srctype = srctype;
                inv->src = from;
                inv->desttype = desttype;
                inv->dest = to;
                inv->offset = off;
                inv->size = step;
                inv->percentage = 0;

                ret = sem_init(&inv->sem, 0, 0);
                if (ret)
                        GOTO(err_ret, ret);

                DBUG("inv %p\n", inv);

                if (i == threads - 1) {
                        inv->size = stbuf.st_size - off;
                }

                ret = pthread_create(&th, &ta, __worker, inv);
                if (ret)
                        GOTO(err_ret, ret);

                works |= 1 << i;
                off += step;
        }

        while (1) {
                sleep(1);
                percentage = 0;
                for (i = 0; i < threads; i++) {
                        inv = &interval[i];
                        if (inv->percentage == -1) {
                                ret = EIO;
                                GOTO(err_ret, ret);
                        } else if (inv->percentage == 100) {
                                if (works & (1 << i))
                                        works ^= 1 << i;
                                DBUG("thread %u finish work %x\n", i, works);
                        }

                        percentage += inv->percentage;
                }

                printf("%u\n", percentage / threads);

                if (works == 0)
                        break;
        }

        if (srctype == YFS_FILE) {
                yfs_close(interval->src);
        } else {
                close(interval->src);
        }

        if (desttype == YFS_FILE) {
                yfs_close(interval->dest);
        } else {
                close(interval->dest);
        }

        printf("copy finished\n");

        return 0;
err_ret:
        printf("copy failed\n");
        return ret;
}

#endif

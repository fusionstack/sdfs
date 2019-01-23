

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#include "chk_proto.h"
#include "disk_proto.h"
#include "ylib.h"
#include "skiplist.h"
#include "yfscds_conf.h"

struct chunk_pool {
        int maxlevel;
        int chunksize;
        chkid_t min;
        chkid_t max;

        struct skiplist *chunkjnl_list;
        struct skiplist *chunkimg_list;
};

static int verbose = 0;

struct chunk_pool cp;

static char chk_basedir[MAX_PATH_LEN];

void usage(const char *prog)
{
        fprintf(stderr, "%s:\n", prog);
        fprintf(stderr, "-c dump chunk on disk(i.e. iterate all chunk dir)\n");
        fprintf(stderr, "-h print this usage\n");
        fprintf(stderr, "-i print disk imagine information\n");
        fprintf(stderr, "-j exmanine journal, whether all chunk are exist\n");
        fprintf(stderr, "-k exmanine chunk, whether it has been journaled\n");
        fprintf(stderr, "-m merge journal (remove add which del after\n");
        fprintf(stderr, "-n cds No.\n");
        fprintf(stderr, "-p print chunk journal\n");
        fprintf(stderr, "-v verbose mode\n");
}

int dump_diskinfo(int diskno)
{
        int ret, fd;
        char dpath[MAX_PATH_LEN], buf[MAX_BUF_LEN];
        diskinfo_img_file_t *diskinfo;
        uint32_t buflen;

        snprintf(dpath, MAX_PATH_LEN, "%s/%d/chkinfo/%s", YFS_CDS_DIR_DISK_PRE,
                 diskno, YFS_DISKINFO_FILE);

        fd = open(dpath, O_RDONLY);
        if (fd == -1) {
                ret = errno;
                fprintf(stderr, "open(%s, ...) %s\n", dpath, strerror(ret));
                goto err_ret;
        }

        buflen = sizeof(diskid_t);

        ret = sy_read(fd, buf, &buflen);
        if (ret) {
                fprintf(stderr, "read() %s\n", strerror(ret));
                goto err_fd;
        }
        diskinfo = (diskinfo_img_file_t *)buf;

        printf("DiskNo %d: DiskID %llu_v%u\n", diskno,
               (unsigned long long)diskinfo->diskid.id,
               diskinfo->diskid.version);

        ret = sy_close(fd);
        if (ret) {
                fprintf(stderr, "close() %s\n", strerror(ret));
                goto err_ret;
        }

        return 0;
err_fd:
        (void) sy_close(fd);
err_ret:
        return ret;
}

int jnl_to_list(jnl_handle_t *jnl, struct skiplist *list)
{
        int ret;
        uint32_t count, buflen, i, no, len;
        chkjnl_t *chkrepts;
        char buf[MAX_BUF_LEN];
        chkid_t *chkid;

        ret = jnl_open(jnl.path_prefix, JNL_READ, &jnl, 0);
        if (ret) {
                fprintf(stderr, "jnl_open %s\n", strerror(ret));
                goto err_ret;
        }

        no = 0;
        chkrepts = (chkjnl_t *)buf;

        len = sizeof(chkid_t);

        while (srv_running) {
                count = MAX_BUF_LEN / sizeof(chkjnl_t);
                buflen = count * sizeof(chkjnl_t);

                ret = jnl_next(jnl, buf, &buflen, NULL);
                if (ret) {
                        fprintf(stderr, "jnl_next() %s\n", strerror(ret));
                        goto err_jnl;
                } else if (buflen == 0)
                        break;
                else
                        count = buflen / sizeof(chkjnl_t);

                for (i = 0; i < count; i++, no++) {
                        ret = ymalloc((void **)&chkid, sizeof(chkid_t));
                        if (ret) {
                                fprintf(stderr, "ymalloc return (%d) %s\n", ret,
                                        strerror(ret));
                                goto err_jnl;
                        }

                        chkid->id = chkrepts[i].chkid.id;
                        chkid->version = chkrepts[i].chkid.version;

                        ret = skiplist_put(list, (void *)chkid, (void *)chkid);
                        if (ret) {
                                yfree((void **)&chkid);

                                fprintf(stderr, "skiplist_put return (%d) %s\n",
                                        ret, strerror(ret));

                                if (ret == EEXIST) {
                                        fprintf(stderr,
                                                "Hoops, chk %llu_v%u exists\n",
                                                (unsigned long long)
                                                           chkrepts[i].chkid.id,
                                                chkrepts[i].chkid.version);
                                } else
                                        goto err_jnl;
                        }

                        if (verbose) {
                                printf("No [%2d] %s chk %2llu_v%u\n", no,
                                       chkrepts[i].op  == CHKOP_WRITE
                                       ? "ADD" : "DEL",
                                       (unsigned long long)chkrepts[i].chkid.id,
                                       chkrepts[i].chkid.version);
                        }
                }
        }

        ret = jnl_close(jnl);
        if (ret) {
                fprintf(stderr, "jnl_close() %s\n", strerror(ret));
                goto err_ret;
        }

        return 0;
err_jnl:
        (void) jnl_close(jnl);
err_ret:
        return ret;
}

void dump_onechk(void *data)
{
        chkid_t *chkid;

        chkid = (chkid_t *)data;

        printf("%llu_v%u\n", (unsigned long long)chkid->id, chkid->version);
}

void path_2list(void *data)
{
        int ret;
        char *path;
        struct stat stbuf;
        uint32_t len;
        chkid_t key, *chkid;

        path = (char *)data;

        ret = stat(path, &stbuf);
        if (ret == -1) {
                ret = errno;
                fprintf(stderr, "stat(%s, ...) ret (%d) %s\n", path, ret,
                        strerror(ret));
                goto err_ret;
        }

        /* not a chunk file */
        if (!S_ISREG(stbuf.st_mode))
                return;

        len = _strlen(chk_basedir);

        ret = memcmp(chk_basedir, path, len);
        if (ret != 0) {
                fprintf(stderr, "path(%s) != basedir(%s)\n", path, chk_basedir);
                goto err_ret;
        }

        path += len;

        ret = cascade_path2idver((const char *)path, &key.id, &key.version);
        if (ret) {
                fprintf(stderr, "path2idver path(%s) ret (%d) %s\n", path, ret,
                        strerror(ret));
                goto err_ret;
        }

        ret = ymalloc((void **)&chkid, sizeof(chkid_t));
        if (ret) {
                fprintf(stderr, "ymalloc return (%d) %s\n", ret, strerror(ret));
                goto err_ret;
        }

        chkid->id = key.id;
        chkid->version = key.version;

        ret = skiplist_put(cp.chunkimg_list, (void *)&key, (void *)chkid);
        if (ret) {
                fprintf(stderr, "skiplist_put return (%d) %s\n", ret,
                        strerror(ret));

                if (ret == EEXIST) {
                        fprintf(stderr, "Hoops, chk %llu_v%u exists\n",
                                (unsigned long long)chkid->id, chkid->version);
                }

                yfree((void **)&chkid);
        }

err_ret:
        return;
}

int img_to_list(char *cpath)
{
        int ret;
        df_dir_list_t bflist;

        snprintf(chk_basedir, MAX_PATH_LEN, "%s", cpath);

        ret = df_iterate_tree((const char *)cpath, &bflist, path_2list);
        if (ret) {
                fprintf(stderr, "ret (%d) %s\n", ret, strerror(ret));
                goto err_ret;
        }

        return 0;
err_ret:
        return ret;
}

/* skiplist_iterate(cp.chunkjnl_list, jnl_exam); */
void jnl_exam(void *data)
{
        int ret;
        chkid_t *key, *chkid;

        key = (chkid_t *)data;

        ret = skiplist_get(cp.chunkimg_list, (void *)key, (void **)&chkid);
        if (ret) {
                if (ret == ENOENT)
                        fprintf(stderr, "journaled chk %llu_v%u lost\n",
                                (unsigned long long)key->id, key->version);
                else
                        fprintf(stderr, "skiplist_get ret (%d) %s\n", ret,
                                strerror(ret));
        }
}

/* skiplist_iterate(cp.chunkimg_list, chk_exam); */
void chk_exam(void *data)
{
        int ret;
        chkid_t *key, *chkid;

        key = (chkid_t *)data;

        ret = skiplist_get(cp.chunkjnl_list, (void *)key, (void **)&chkid);
        if (ret) {
                if (ret == ENOENT)
                        fprintf(stderr, "disk chk %llu_v%u not journaled\n",
                                (unsigned long long)key->id, key->version);
                else
                        fprintf(stderr, "skiplist_get ret (%d) %s\n", ret,
                                strerror(ret));
        }
}

int main(int argc, char *argv[])
{
        int ret;
        int diskno, dump_info, dump_chunk, dump_jnl, merge_jnl, exam_jnl,
            exam_chunk;
        const char *prog;
        char c_opt, cpath[MAX_PATH_LEN];
        jnl_handle_t jnl;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        diskno = -1;
        dump_info = -1;
        dump_jnl = -1;
        dump_chunk = -1;
        exam_jnl = exam_chunk = -1;

        while ((c_opt = getopt(argc, argv, "chijkmn:pv")) > 0)
                switch (c_opt) {
                case 'c':
                        dump_chunk = 1;
                        break;
                case 'i':
                        dump_info = 1;
                        break;
                case 'j':
                        exam_jnl = 1;
                        break;
                case 'k':
                        exam_chunk = 1;
                        break;
                case 'm':
                        merge_jnl = 1;
                        break;
                case 'n':
                        diskno = atoi(optarg);
                        break;
                case 'p':
                        dump_jnl = 1;
                        break;
                case 'v':
                        verbose = 1;
                        break;
                case 'h':
                default:
                        usage(prog);
                        exit(1);
                }

        if (diskno == -1 || diskno == 0 /* XXX atoi return it --yf */) {
                fprintf(stderr, "Hoops, please give cds no.\n");
                exit(1);
        }

        if (verbose)
                printf("diskno %d\n", diskno);

        if (dump_info == 1) {
                ret = dump_diskinfo(diskno);
                if (ret)
                        exit(1);
        }

        cp.maxlevel = SKIPLIST_MAX_LEVEL;
        cp.chunksize = SKIPLIST_CHKSIZE_DEF;
        cp.min.id = 0;
        cp.min.version = 0;
        cp.max.id = UINT64_MAX;
        cp.max.version = UINT32_MAX;

        ret = skiplist_create(verid64_void_cmp, cp.maxlevel, cp.chunksize,
                              (void *)&cp.min, (void *)&cp.max,
                              &cp.chunkjnl_list);
        if (ret) {
                fprintf(stderr, "Hoops, skiplist_create return (%d) %s\n",
                        ret, strerror(ret));
                exit(1);
        }

        ret = skiplist_create(verid64_void_cmp, cp.maxlevel, cp.chunksize,
                              (void *)&cp.min, (void *)&cp.max,
                              &cp.chunkimg_list);
        if (ret) {
                fprintf(stderr, "Hoops, skiplist_create return (%d) %s\n",
                        ret, strerror(ret));
                exit(1);
        }

        if (dump_jnl == 1 || merge_jnl == 1 || exam_jnl == 1
            || exam_chunk == 1) {
                snprintf(jnl.path_prefix, MAX_PATH_LEN, "%s/%d/chkinfo/%s",
                         YFS_CDS_DIR_DISK_PRE, diskno, YFS_CDS_DIR_JNL_PRE);

                ret = jnl_to_list(&jnl, cp.chunkjnl_list);
                if (ret)
                        exit(1);

                if (dump_jnl == 1) {
                        printf("dump chunk journal:\n");

                        skiplist_iterate(cp.chunkjnl_list, dump_onechk);
                }
        }

        if (merge_jnl == 1) {
        }

        if (dump_chunk == 1 || exam_jnl == 1 || exam_chunk == 1) {
                snprintf(cpath, MAX_PATH_LEN, "%s/%d/ychunk",
                         YFS_CDS_DIR_DISK_PRE, diskno);

                ret = img_to_list(cpath);
                if (ret)
                        exit(1);

                if (dump_chunk == 1) {
                        printf("dump chunk:\n");

                        skiplist_iterate(cp.chunkimg_list, dump_onechk);
                }
        }

        if (exam_jnl == 1) {
                printf("examine journal:\n");

                skiplist_iterate(cp.chunkjnl_list, jnl_exam);
        }

        if (exam_chunk == 1) {
                printf("examine chunk:\n");

                skiplist_iterate(cp.chunkimg_list, chk_exam);
        }

        return 0;
}


#include <sys/socket.h>
#include <getopt.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <dirent.h>

#include "yfs_conf.h"
#include "configure.h"
#include "../objc/objc.h"
#include "sdfs_lib.h"
#include "net_global.h"
#include "md_lib.h"
#include "configure.h"
#include "yatomic.h"
#include "network.h"
#include "sdfs_list.h"
#include "sdfs_chunk.h"
#include "../../mds/mds/recovery.h"
#include "nodectl.h"

#define RECOVER_MAX 100
#define SCAN_MAX 24

void usage(const char *prog)
{
        printf("%s [-h] [p]\n", prog);
}

typedef struct {
        struct list_head hook;
        fileid_t fid;
        char path[MAX_PATH_LEN];
} dir_objck_t;

#if 0
typedef struct {
        int fd;
        uint64_t lost;
        uint64_t offset;
        sy_rwlock_t rwlock;
        char home[MAX_PATH_LEN];
} rept_t;
#endif

static int __full__ = 0;
static int __retval__ = 0;
yatomic_t __total__;
yatomic_t __need__;
static uint64_t __succ__ = 0;
static bool __check_flag__ = false;

static int __get_qos_sleep()
{
        int qos_sleep = 0;

        qos_sleep = nodectl_get_int("recovery/qos_sleep", "0");
        qos_sleep = qos_sleep <= 0 ? 0 : qos_sleep;
        qos_sleep = qos_sleep > 10 * MSEC_PERMIN ? 10 * MSEC_PERMIN : qos_sleep;

        return qos_sleep;
}

static int __object_check(void *_rept, const void *k, const void *_chkinfo, const fileinfo_t *_md)
{
        int ret, online, i, needcheck;
        objinfo_t *objinfo;
        char _buf[MAX_BUF_LEN], _buf2[MAX_BUF_LEN];
        diskid_t *diskid;
        net_handle_t nh;
        rept_t *rept;
        chkinfo_t *chkinfo;

        objinfo = (void *)_buf;
        chkinfo = (void *)_buf2;
        (void)k;

        chkinfo = memcpy(chkinfo, _chkinfo, CHK_SIZE(((chkinfo_t *)_chkinfo)->repnum));
        rept = _rept;

        if (chkinfo->chkid.id == CHKID_NULL && chkinfo->chkid.id == CHKVER_NULL)
                return 0;

        yatomic_get_and_inc(&__total__, NULL);

        chk2obj(objinfo, chkinfo);

        if (__full__) {
                printf("  object "CHKID_FORMAT" force sync\n", CHKID_ARG(&chkinfo->chkid));

                ret = objc_open(objinfo, &objinfo->id, 0);
                if (ret)
                        GOTO(err_ret, ret);

                ret = sdfs_chunk_recovery(&objinfo->id);
                if (ret)
                        GOTO(err_ret, ret);

                return 0;
        }

#if 0
        for (i = 0; i < (int)chkinfo->repnum; i++) {
                if (chkinfo->diskid[i].status & __S_DIRTY) {
                        printf("  object %llu_v%u[%u] force sync\n", (LLU)chkinfo->chkid.id,
                                        chkinfo->chkid.version, chkinfo->chkid.idx);

                        ret = sdfs_chunk_recovery(&objinfo->id);
                        if (ret)
                                GOTO(err_ret, ret);

                        goto out;
                }
        }
#endif

        online = 0;
        needcheck = 0;
        if (chkinfo->repnum > _md->repnum) {
                needcheck++;
        } else {
                for (i = 0; i < (int)objinfo->repnum; i++) {
                                diskid = &objinfo->diskid[i];

                                if (is_null(diskid)) {
                                        DWARN("object "OBJID_FORMAT" rep %d is null\n",
                                                        OBJID_ARG(&chkinfo->chkid), i);
                                        continue;
                                }

                                id2nh(&nh, diskid);
                                ret = network_connect2(&nh, 0);
                                if (ret) {
                                        //YASSERT(0);
                                        continue;
                                }

                                online++;

                                if (diskid->status & __S_DIRTY) {
                                        needcheck++;
                                }
                        }

        }

        if (needcheck || online != (int)objinfo->repnum ) {
                printf("  object "OBJID_FORMAT" rep %u online %u need check %u\n",
                                OBJID_ARG(&chkinfo->chkid), chkinfo->repnum, online, needcheck);

                yatomic_get_and_inc(&__need__, NULL);

                ret = sy_rwlock_wrlock(&rept->rwlock);
                if (ret)
                        GOTO(err_ret, ret);

                ret = _write(rept->fd, &objinfo->id, sizeof(objinfo->id));
                if (ret < 0) {
                        YASSERT(0);
                }
                rept->lost++;

                sy_rwlock_unlock(&rept->rwlock);
        }

        return 0;
err_ret:
        return ret;
}

static int __object_recover_send(objid_t *id, int count)
{
        int ret, i;
        objid_t *objid;

        for (i = 0; i < count; i++) {
                objid = &id[i];
                ret = sdfs_chunk_recovery(objid);
                if (ret) {
                        DWARN("object "OBJID_FORMAT" ret: %d\n", OBJID_ARG(objid), ret);
                        objid->id = 0;
                        objid->volid = 0;
                        objid->idx = 0;
                        continue;
                }
        }

        return 0;
}

static int __object_recover_check(const objid_t *id, int count)
{
        int ret, i, retry, slp = 0;
        objid_t *objid;

        printf("\n");

        for (i = 0; i < count; i++) {
                objid = (void *)&id[i];

                if (objid->id == 0 && objid->volid == 0 && objid->idx == 0)
                        continue;

                slp = __get_qos_sleep();
                if (slp > 0) {
                        DINFO("recover qos obj["OBJID_FORMAT"] usleep[%d]\n", OBJID_ARG(objid), slp * 1000);
                        usleep(slp * 1000);
                }

                retry = 0;
retry:
                ret = sdfs_chunk_check(objid);
                if (ret) {
                        if (retry < 2) {
                                printf("  sync object "OBJID_FORMAT" timeout\n", OBJID_ARG(objid));
                                retry++;
                                sleep(1);
                                goto retry;
                        } else {
                                continue;
                        }
                }

                __succ__++;

                printf("+");
                fflush(NULL);
        }

        return 0;
}

static int __object_recover(rept_t *rept, int max)
{
        int ret, count;
        objid_t buf[RECOVER_MAX];
        char path[MAX_PATH_LEN], value[MAX_BUF_LEN];;
        struct stat stbuf;

        ret = fstat(rept->fd, &stbuf);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        printf("lost %llu, begin recover from offset %llu size %llu\n", (LLU)rept->lost,
                        (LLU)rept->offset, (LLU)stbuf.st_size);

        ret = lseek(rept->fd, rept->offset, SEEK_SET);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        snprintf(path, MAX_PATH_LEN, "%s/losted.offset", ng.home);

        while (1) {
                ret = _read(rept->fd, buf, sizeof(objid_t) * max);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                if (ret == 0)
                        break;

                YASSERT(ret % sizeof(objid_t) == 0);

                rept->offset += ret;
                count = ret / sizeof(objid_t);

                ret = __object_recover_send(buf, count);
                if (ret)
                        YASSERT(0);

                ret = __object_recover_check(buf, count);
                if (ret)
                        YASSERT(0);

                snprintf(value, MAX_BUF_LEN, "%llu", (LLU)rept->offset);
                printf("\nset %s %s finished %u\n", path, value, count);
                ret = _set_value(path, value, strlen(value) + 1, O_CREAT);
                if (ret)
                        GOTO(err_ret, ret);
        }

        snprintf(path, MAX_PATH_LEN, "%s/losted", ng.home);
        unlink(path);

        return 0;
err_ret:
        return ret;
}

static int __object_scan_file(rept_t *rept, fileinfo_t *_md)
{
        int ret, chkno, chknum;
        fileinfo_t *md = _md;
        char buf[MAX_BUF_LEN] = "";
        chkinfo_t *chkinfo;
        chkid_t chkid;

        DINFO("scan file "CHKID_FORMAT " chknum %u \n", CHKID_ARG(&md->fileid), md->chknum);
        
        chkinfo = (chkinfo_t *)buf;
        chknum = md->chknum;

        for (chkno = 0; chkno < chknum; chkno++) {
                fid2cid(&chkid, &md->fileid, chkno);
                DINFO("scan chunk "CHKID_FORMAT " \n", CHKID_ARG(&chkid));

                ret = md_chkload(chkinfo, &chkid, NULL);
                if (ret) {
                        DWARN("chk[%d] not exist\n",
                              chkno);
                        continue;
                }

                ret = __object_check(rept, NULL, chkinfo, md);
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static dir_objck_t *__dir_objck_malloc(const fileid_t *fileid, const char *path)
{
        dir_objck_t *pos;
        int ret;

        ret = ymalloc((void**)&pos, sizeof(*pos));
        if (ret)
                GOTO(err_ret, ret);

        pos->fid = *fileid;
        strcpy(pos->path, path);

        return pos;
err_ret:
        return NULL;
}

static int __object_scan_dir(rept_t *rept, dir_objck_t *dir_objck, struct list_head *child_dir_list)
{
        int ret, stop;
        char depath[MAX_PATH_LEN], *path;
        off_t offset;
        void *de0;
        int delen;//, len;
        struct dirent *de;
        struct stat stbuf;
        fileid_t *fileid;
        fileinfo_t *md;
        dir_objck_t *child_dir_objck;

        offset = 0;
        de0 = NULL;
        delen = 0;
        path = dir_objck->path;
        fileid = &dir_objck->fid;
        //XXX
        if (path[0] != '/') {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        ret = sdfs_getattr(NULL, fileid, &stbuf);
        if (ret)
                GOTO(err_ret, ret);

        if (S_ISREG((stbuf).st_mode)) {
                YASSERT(0);
                return 0;

        }

        stop = 0;
        while (srv_running) {
                DINFO("path %s\n", path);
                
                ret = ly_readdirplus(path, offset, &de0, &delen, EXEC_USS_CMD);
                if (ret) {
                        DERROR("ly_readdir(%s, ...) %s\n", path,
                                        strerror(ret));
                        GOTO(err_ret, ret);
                } else if (delen == 0) {
                        stop = 1;
                        /*printf("delen==0, break\n");*/
                        break;
                }

                /*printf("delen %d\n", (int)delen);*/
                /*sleep(1);*/
                if (delen > 0) {
                        dir_for_each(de0, delen, de, offset) {
                                YASSERT(de->d_reclen <= delen);
                                if (strcmp(de->d_name, ".") == 0
                                                || strcmp(de->d_name, "..") == 0) {
                                        //offset = de->d_off;
                                        continue;
                                }

                                if (strcmp(path, "/") == 0)  {
                                        sprintf(depath, "/%s", de->d_name);
                                } else
                                        snprintf(depath, MAX_PATH_LEN, "%s/%s", path,
                                                        de->d_name);

                                md = (void *)de + de->d_reclen - sizeof(md_proto_t);
                                MD2STAT(md, &stbuf);

                                switch (stbuf.st_mode & S_IFMT) {
                                case S_IFREG:
                                        ret = __object_scan_file(rept, md);
                                        break;
                                case S_IFDIR:
                                        child_dir_objck = __dir_objck_malloc(&md->fileid, depath);
                                        if (child_dir_objck == NULL) {
                                                ret = ENOMEM;
                                                GOTO(err_ret, ret);
                                        }

                                        list_add(&child_dir_objck->hook, child_dir_list);
                                        break;
                                default:
                                        break;
                                }

                        }

                        if (offset == 0)
                                stop = 1;

                        if (stop) {
                                /*printf("stop break\n");*/
                                break;
                        }
                } else {
                        DWARN("%d\n", ret);
                        break;
                }

                yfree((void **)&de0);
        }

        return 0;
err_ret:
        return ret;
}

static int __object_scan(rept_t *rept)
{
        int ret;
        struct list_head dir_list;
        struct list_head child_dir_list;
        struct list_head *pos, *n;
        dir_objck_t *dir_objck, *root_objck;
        fileid_t root_id;

        yatomic_init(&__total__, 0);
        yatomic_init(&__need__, 0);
        INIT_LIST_HEAD(&dir_list);
        INIT_LIST_HEAD(&child_dir_list);
        sy_rwlock_init(&rept->rwlock, NULL);

        ret = sdfs_lookup_recurive("/", &root_id);
        if (ret){
                DERROR("__object_scan / error, ret:%d\n", ret);
                GOTO(err_ret, ret);
        }

        DINFO("root " CHKID_FORMAT " \n", CHKID_ARG(&root_id));
        
        root_objck = __dir_objck_malloc(&root_id, "/");
        if (root_objck == NULL) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        list_add(&root_objck->hook, &dir_list);

        while (1) {
                list_for_each_safe(pos, n, &dir_list) {
                        dir_objck = (dir_objck_t *)pos;
                        list_del(pos);
                        __object_scan_dir(rept, dir_objck, &child_dir_list);
                        yfree((void **)&dir_objck);
                }

                if (list_empty(&child_dir_list)) {
                        break;
                }

                list_splice_tail_init(&child_dir_list, &dir_list);
        }

        return 0;
err_ret:
        return ret;
}

static int __stage_load(rept_t *rept)
{
        int ret, fd;
        char path[MAX_PATH_LEN], tmp[MAX_PATH_LEN], buf[MAX_BUF_LEN];

        snprintf(path, MAX_PATH_LEN, "%s/losted", ng.home);

        ret = path_validate(path, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (ret)
                GOTO(err_ret, ret);
        
        rept->fd = -1;
        rept->lost = 0;
        rept->offset = 0;

        fd = open(path, O_RDWR, 0644);
        if (fd < 0) {
                ret = errno;
                if (ret == ENOENT) {
                        snprintf(tmp, MAX_PATH_LEN, "%s/losted.tmp", ng.home);

                        fd = open(tmp, O_CREAT | O_RDWR, 0644);
                        if (fd < 0) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }

                        snprintf(buf, MAX_BUF_LEN, "0");
                        snprintf(path, MAX_PATH_LEN, "%s/losted.offset", ng.home);
                        ret = _set_value(path, buf, strlen(buf) + 1, O_CREAT);
                        if (ret)
                                GOTO(err_fd, ret);

                        rept->fd = fd;
                        ret = __object_scan(rept);
                        if (ret)
                                GOTO(err_fd, ret);

                        snprintf(buf, MAX_BUF_LEN, "%llu", (LLU)rept->lost);
                        snprintf(path, MAX_PATH_LEN, "%s/losted.count", ng.home);
                        ret = _set_value(path, buf, strlen(buf) + 1, O_CREAT);
                        if (ret)
                                GOTO(err_fd, ret);

                        snprintf (path, MAX_PATH_LEN, "%s/losted", ng.home);

                        rename(tmp, path);
                } else
                        GOTO(err_fd, ret);
        } else {
                snprintf(path, MAX_PATH_LEN, "%s/losted.count", ng.home);
                ret = _get_value(path, buf, MAX_BUF_LEN);
                if (ret < 0) {
                        ret = -ret;
                        if (ret == ENOENT)
                                rept->lost = 0;
                        else
                                GOTO(err_fd, ret);
                } else {
#ifdef __x86_64__
                        ret = sscanf(buf, "%lu", &rept->lost);
#else
                        ret = sscanf(buf, "%llu", &rept->lost);
#endif
                }

                snprintf(path, MAX_PATH_LEN, "%s/losted.offset", ng.home);
                ret = _get_value(path, buf, MAX_BUF_LEN);
                if (ret < 0) {
                        ret = -ret;
                        if (ret == ENOENT)
                                rept->offset = 0;
                        else
                                GOTO(err_fd, ret);
                } else {
#ifdef __x86_64__
                        ret = sscanf(buf, "%lu", &rept->offset);
#else
                        ret = sscanf(buf, "%llu", &rept->offset);
#endif
                }
        }

        rept->fd = fd;

        return 0;
err_fd:
        close(fd);
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret, t;
        char home[MAX_NAME_LEN];
        rept_t rept;
        char c_opt;

        t = 100;
        while (srv_running) {
                int option_index = 0;

                static struct option long_options[] = {
                        {"check", 0, NULL, 'c'},
                        {"full", 0, NULL, 'f'},
                        {"thread", required_argument, NULL, 't'},
                        {NULL, 0, NULL, 0}
                };

                c_opt = getopt_long(argc, argv, "cft:",
                                long_options, &option_index);
                if (c_opt == -1)
                        break;

                switch (c_opt) {
                        case 'c':
                                __check_flag__ = true;
                                break;
                        case 'f':
                                __full__ = 1;
                                break;
                        case 't':
                                t = atoi(optarg);
                                break;
                        default:
                                fprintf(stderr, "Hoops, wrong op got!\n");
                                usage(argv[0]);
                                exit(1);
                }
        }

        t = t < RECOVER_MAX ? t : RECOVER_MAX;

        strcpy(home, SDFS_HOME"/mds/0"); //SDFS_HOME not end with'/'

        if (home[0] != '/') {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        //printf("scan home: %s\n", home);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if(ret)
                exit(1);

        dbg_info(0);
        
        ret = ly_init_simple("uss.lobjck");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        }

        strcpy(rept.home, home);
        DWARN("home %s\n", home);

        if (false == __check_flag__) {
                printf("start recover\n");
        }
        else {
                printf("start check.\n");
        }

        ret = __stage_load(&rept);
        if (ret) {
                GOTO(err_ret, ret);
        }
        else if(true == __check_flag__){
                printf("check finish.\n");
                exit(ret);
        }

        ret = __object_recover(&rept, t);
        if (ret)
                GOTO(err_ret, ret);

#if 0
        ret = md_setopt("consistent_leak", "false");
        if (ret)
                GOTO(err_ret, ret);
#endif

        printf("scan %llu\n", (LLU)__total__.value);

        if (__need__.value == __succ__) {
                printf("recover ok\n");
                __retval__ = 0;
        } else {
                printf("recover (%llu/%llu), need retry!\n",
                                (LLU)__succ__, (LLU)__need__.value);
                __retval__ = EAGAIN;
        }

        exit(_errno(__retval__));
err_ret:
        exit(_errno(ret));
}

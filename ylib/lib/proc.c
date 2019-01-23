#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/inotify.h>

#include "configure.h"
#include "sdfs_conf.h"
#include "ylib.h"
#include "sysutil.h"
#include "proc.h"
#include "dbg.h"

#define __PROC_LOG(EX, VAL) \
        do { \
                snprintf(buf, 2, "%d", *VAL); \
                snprintf(path, sizeof(path), "/dev/shm/uss/yfs/proc/%s/dbg/%s", name, #EX); \
                ret = __proc(path, VAL, buf, EX, __proc_cb_log);          \
                if (ret) \
                GOTO(err_ret, ret); \
        } while (0)

static int inotify_fd;
extern uint32_t ylib_sub;
static hash_table_t *proc_table = NULL;

#define PROC_HTABLE_INIT_SIZE        20

static int __key_from_int (int key)
{
        return key;
}

/* hash table functions for proc:
 * hashtable_init
 * hashtable_destroy
 * hashtable_insert
 * hashtable_remove
 * hashtable_search
 */
static int hashtable_init(hash_table_t **hashtable, int (*hash)(int))
{
        int ret;
        void *ptr;

        ret = ymalloc(&ptr, sizeof(hash_table_t));
        if (ret)
                GOTO(err_ret, ret);

        *hashtable = (hash_table_t *)ptr;

        ret = ymalloc(&ptr, sizeof(proc_node_t *) * PROC_HTABLE_INIT_SIZE);
        if (ret)
                GOTO(err_free, ret);

        (*hashtable)->entrys = (proc_node_t **)ptr;
        (*hashtable)->size = PROC_HTABLE_INIT_SIZE;
        (*hashtable)->entrys_num = 0;
        (*hashtable)->hash = hash;

        return 0;
err_free:
        yfree((void **)hashtable);
        *hashtable = NULL;
err_ret:
        return ret;
}

static int hashtable_destroy(hash_table_t **hashtable)
{
        int idx;
        proc_node_t *ptr, *next;

        for (idx = 0; idx < (*hashtable)->size; ++idx)
                for (ptr = (*hashtable)->entrys[idx]; ptr; ptr = next) {
                        next = ptr->next;
                        yfree((void **)&ptr);
                }
        
        yfree((void **)&(*hashtable)->entrys);
        yfree((void **)hashtable);

        *hashtable = NULL;

        return 0;
}

static int hashtable_insert(hash_table_t *hashtable, int key, proc_node_t *node)
{
        int ret, idx; 
        void *ptr;
        proc_node_t *entry;
        
        idx = (*hashtable->hash)(key) % hashtable->size;

        ret = ymalloc(&ptr, sizeof(proc_node_t));
        if (ret)
                GOTO(err_ret, ret);

        entry = (proc_node_t *)ptr;

        entry->next = hashtable->entrys[idx];
        hashtable->entrys[idx] = entry;

        hashtable->entrys_num++;

        entry->key = node->key;
        entry->target = node->target;
        entry->extra = node->extra;
        _strncpy(entry->buf, node->buf, sizeof(entry->buf) - 1);
        _strncpy(entry->path, node->path, sizeof(entry->path) -1);
        entry->parse = node->parse;

        return 0;
err_ret:
        return ret;
}

static int hashtable_remove(hash_table_t *hashtable, int key)
{
        int idx;
        proc_node_t **ptr, *temp, **next;

        idx = (*hashtable->hash)(key) % hashtable->size;

        for (ptr = &hashtable->entrys[idx]; *ptr; ptr = next)
                if ((*ptr)->key == key) {
                        temp = *ptr;
                        *ptr = (*ptr)->next;

                        yfree((void **)&temp);

                        next = ptr;
                } else { 
                        next = &(*ptr)->next;
                }

        hashtable->entrys_num--;

        return 0;
}

static int hashtable_search(hash_table_t *hashtable, int key, proc_node_t **node)
{
        int idx;
        proc_node_t *ptr = NULL;

        idx = (*hashtable->hash)(key) % hashtable->size;

        *node = NULL;

        for (ptr = hashtable->entrys[idx]; ptr; ptr = ptr->next)
                if (ptr->key == key)
                        *node = ptr;

        return 0;
}
/* end of hash table functions */

static int __build_path(const char *path, const char *buf)
{
        int ret, fd, count;

        ret = path_validate(path, YLIB_NOTDIR, YLIB_DIRCREATE);
        if (ret)
                GOTO(err_ret, ret);

        fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }   

        count = _write(fd, buf, strlen(buf));
        if (count < 0) {
                ret = -count;
                GOTO(err_ret, ret);
        }   

        close(fd);

        return 0;
err_ret:
        return ret;
}
static void *__proc_thr_fn(void *arg)
{
        int ret, fd, count, wd;
        char buf[1024], val[1024];
        uint32_t len, idx;
        struct inotify_event *event;
        proc_node_t *node, *ptr, entry;

        (void) arg;
        len = sizeof(buf);

        while (srv_running) {
                _memset(buf, 0x0, sizeof(buf));

                ret = sy_read(inotify_fd, buf, &len);
                if (ret)
                        GOTO(err_ret, ret);

                for (idx = 0; idx < len; idx += sizeof(struct inotify_event) + event->len) {
                        event = (struct inotify_event *)(buf + idx);

                        hashtable_search(proc_table, event->wd, &node);
                        if (!node)
                                continue;

                        switch(event->mask) {
                        case IN_MODIFY:
                                fd = open(node->path, O_RDONLY);
                                if (fd == -1) {
                                        ret = errno;
                                        GOTO(err_ret, ret);
                                }
                                
                                _memset(val, 0x0, sizeof(val));

                                count = _read(fd, val, sizeof(val) - 1);
                                if (count == -1) {
                                        ret = -count;
                                        GOTO(err_ret, ret);
                                }
                                close(fd);

                                ret = node->parse(node->target, val, node->extra);
                                if (ret)
                                        break;

                                _strncpy(node->buf, val, sizeof(node->buf));

                                break;
                        case IN_DELETE_SELF:
                                ret = __build_path(node->path, node->buf);
                                if (ret)
                                        GOTO(err_ret, ret);

                                wd = inotify_add_watch(inotify_fd, node->path, IN_MODIFY | IN_DELETE_SELF);
                                if (wd == -1) {
                                        ret = errno;
                                        GOTO(err_ret, ret);
                                }

                                hashtable_search(proc_table, event->wd, &ptr);
                                entry = *ptr;
                                hashtable_remove(proc_table, event->wd);
                                entry.key = wd;
                                hashtable_insert(proc_table, entry.key, &entry);

                                break;
                        default:
                                break;
                        }

                }
        }

        return NULL;
err_ret:
        return NULL;
}

/* proc functions :
 * proc_init
 * proc_destroy
 * proc
 */
int proc_init()
{
        int ret;
        pthread_t th;
        pthread_attr_t ta;

        ret = hashtable_init(&proc_table, __key_from_int);
        if (ret)
                GOTO(err_ret, ret);

        inotify_fd = inotify_init();
        if (inotify_fd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        pthread_attr_init(&ta);
        pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        ret = pthread_create(&th, &ta, __proc_thr_fn, NULL);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int proc_destroy()
{
        hashtable_destroy(&proc_table);
        
        return 0;
}

static int __proc(char *path, void *target, char *buf, uint32_t extra, proc_cb_fn parse)
{
        int ret, wd;
        struct stat statbuf;
        proc_node_t node;

        memset(&statbuf, 0x0, sizeof(struct stat));

        stat(path, &statbuf);

        if (!S_ISREG(statbuf.st_mode)) {
                ret = __build_path(path, buf);
                if (ret)
                        GOTO(err_ret, ret);
        }

        node.target = target;
        node.parse = parse;
        node.extra = extra;
        _strncpy(node.path, path, sizeof(node.path) - 1);
        _strncpy(node.buf, buf, sizeof(node.buf) - 1);

        wd = inotify_add_watch(inotify_fd, path, IN_MODIFY | IN_DELETE_SELF);
        if (wd == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        node.key = wd;
        
        ret = hashtable_insert(proc_table, node.key, &node);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}
/* end of proc funtions */

static int __proc_cb_log(void *target, char *buf, uint32_t extra)
{       
        int ret, on;

        on = atoi(buf);

        if (on == 0) {
                *(int *)target = 0;
                ylib_sub &= ~extra;
        } else if (on == 1) {
                *(int *)target = 1;
                ylib_sub |= extra;
        } else {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        DINFO("proc file modified: mask (0x%08x) %s\n", extra, on ? "on" : "off");

        return 0;
err_ret:
        return ret;
}       

int proc_log(const char *name)
{       
        int ret;
        char path[MAX_PATH_LEN], buf[1024];

        __PROC_LOG(S_LIBYLIB, &logconf.log_ylib);
        __PROC_LOG(S_LIBYLIBLOCK, &logconf.log_yliblock);
        __PROC_LOG(S_LIBYLIBMEM, &logconf.log_ylibmem);
        __PROC_LOG(S_LIBYLIBSKIPLIST, &logconf.log_ylibskiplist);
        __PROC_LOG(S_LIBYLIBNLS, &logconf.log_ylibnls);
        __PROC_LOG(S_YSOCK, &logconf.log_ysock);
        __PROC_LOG(S_LIBYNET, &logconf.log_ynet);
        __PROC_LOG(S_YRPC, &logconf.log_yrpc);
        __PROC_LOG(S_YFSCDC, &logconf.log_yfscdc);
        __PROC_LOG(S_YFSMDC, &logconf.log_yfsmdc);
        __PROC_LOG(S_FSMACHINE, &logconf.log_fsmachine);
        __PROC_LOG(S_YFSLIB, &logconf.log_yfslib);
        __PROC_LOG(S_YFSFUSE, &logconf.log_yfsfuse);
        __PROC_LOG(S_YWEB, &logconf.log_yweb);
        __PROC_LOG(S_YFTP, &logconf.log_yftp);
        __PROC_LOG(S_YISCSI, &logconf.log_yiscsi);
        __PROC_LOG(S_YP2P, &logconf.log_yp2p);
        __PROC_LOG(S_YNFS, &logconf.log_ynfs);
        __PROC_LOG(S_YFSMDS, &logconf.log_yfsmds);
        __PROC_LOG(S_YOSS, &logconf.log_yoss);
        __PROC_LOG(S_CDSMACHINE, &logconf.log_cdsmachine);
        __PROC_LOG(S_YFSCDS, &logconf.log_yfscds);
        __PROC_LOG(S_YFSCDS_ROBOT, &logconf.log_yfscds_robot);
        __PROC_LOG(S_YTABLE, &logconf.log_ytable);
        __PROC_LOG(S_PROXY, &logconf.log_proxy);
        __PROC_LOG(S_YFUSE, &logconf.log_yfuse);

        return 0;
err_ret:
        return ret;
}


#include <sys/statvfs.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <semaphore.h>
#include <errno.h>
#include <sys/statfs.h>


#define DBG_SUBSYS S_YFSLIB

#include "configure.h"
#include "schedule.h"
#include "net_global.h"
#include "mond_rpc.h"
#include "network.h"
#include "io_analysis.h"
#include "core.h"
#include "dbg.h"

#define ANALY_AVG_UPDATE_COUNT (3)  //secend
typedef struct {
        char name[MAX_NAME_LEN];
        int seq;
        uint64_t read_count;
        uint64_t write_count;
        uint64_t read_bytes;
        uint64_t write_bytes;
        time_t last_output;
        sy_spinlock_t lock;

        /* for each seconds */
        time_t last;
        int cur;
        uint64_t readps[ANALY_AVG_UPDATE_COUNT];
        uint64_t writeps[ANALY_AVG_UPDATE_COUNT];
        uint64_t readbwps[ANALY_AVG_UPDATE_COUNT];
        uint64_t writebwps[ANALY_AVG_UPDATE_COUNT];
} io_analysis_t;

static io_analysis_t *__io_analysis__ = NULL;

static void __io_analysis_get(uint32_t *readps, uint32_t *writeps,
                uint32_t *readbwps, uint32_t *writebwps)
{
        int prev = (__io_analysis__->cur + ANALY_AVG_UPDATE_COUNT - 1) % ANALY_AVG_UPDATE_COUNT;

        *readps = __io_analysis__->readps[prev];
        *writeps = __io_analysis__->writeps[prev];
        *readbwps = __io_analysis__->readbwps[prev];
        *writebwps = __io_analysis__->writebwps[prev];
}

static int __io_analysis_dump()
{
        int ret;
        time_t now;
        char path[MAX_PATH_LEN], buf[MAX_INFO_LEN];
        uint32_t readps, writeps, readbwps, writebwps;
        static time_t last_log = 0;

        now = time(NULL);
        memset(buf, 0x0, sizeof(buf));

        __io_analysis_get(&readps, &writeps, &readbwps, &writebwps);

        if (now - __io_analysis__->last_output > 2) {
                snprintf(buf, MAX_INFO_LEN,
                         "read: %llu\n"
                         "read_bytes: %llu\n"
                         "write: %llu\n"
                         "write_bytes: %llu\n"
                         "read_ps:%u\n"
                         "read_bytes_ps:%u\n"
                         "write_ps:%u\n"
                         "write_bytes_ps:%u\n"
                         "latency:%ju\n",
                         (LLU)__io_analysis__->read_count,
                         (LLU)__io_analysis__->read_bytes,
                         (LLU)__io_analysis__->write_count,
                         (LLU)__io_analysis__->write_bytes,
                         readps,
                         readbwps,
                         writeps,
                         writebwps,
                         core_latency_get());
                __io_analysis__->last_output = now;

                time_t now = time(NULL);
                if (now - last_log > 10) {
                        DINFO("READ BWPS %u WRITE BWPS %u READ OPS %u WRITE OPS %u LATENCY %f\n",
                              readbwps, writebwps, readps, writeps, (float)core_latency_get() / 1000);
                }
        }

        if (now - __io_analysis__->last_output < 0) {
                __io_analysis__->last_output = now;
        }

        if (strlen(buf)) {
                snprintf(path, MAX_PATH_LEN, "%s/io/%s.%d", SHM_ROOT,
                         __io_analysis__->name, __io_analysis__->seq);
                ret = _set_value(path, buf, strlen(buf) + 1, O_CREAT | O_TRUNC);
                if (ret)
                        GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int io_analysis(analysis_op_t op, int count)
{
        int ret, next, cnt, i;
        time_t now;

        if (__io_analysis__ == NULL) {
                goto out;
        }
        
        now = time(NULL);

        ret = sy_spin_lock(&__io_analysis__->lock);
        if (ret)
                GOTO(err_ret, ret);

        if (op == ANALYSIS_IO_READ) {
                __io_analysis__->read_count++;
                __io_analysis__->read_bytes += count;
        } else if (op == ANALYSIS_IO_WRITE) {
                __io_analysis__->write_count++;
                __io_analysis__->write_bytes += count;
        } else if (op == ANALYSIS_OP_READ) {
                __io_analysis__->read_count++;
        } else if (op == ANALYSIS_OP_WRITE) {
                __io_analysis__->write_count++;
        } else {
                YASSERT(0);
        }

        if (__io_analysis__->last == 0) {
                __io_analysis__->cur = 0;
                if (op == ANALYSIS_IO_READ || op == ANALYSIS_OP_READ ) {
                        __io_analysis__->readps[0] = 1;
                        __io_analysis__->readbwps[0] = count;
                } else if (op == ANALYSIS_IO_WRITE || op == ANALYSIS_OP_WRITE) {
                        __io_analysis__->writeps[0] = 1;
                        __io_analysis__->writebwps[0] = count;
                }
                __io_analysis__->last = now;
        } else if (now > __io_analysis__->last) {
                cnt = now - __io_analysis__->last;
                for (i = 1; i <= cnt; i++) {
                        next = (__io_analysis__->cur + i) % ANALY_AVG_UPDATE_COUNT;
                        __io_analysis__->readps[next] = 0;
                        __io_analysis__->readbwps[next] = 0;
                        __io_analysis__->writeps[next] = 0;
                        __io_analysis__->writebwps[next] = 0;
                }

                YASSERT(next >= 0 && next < ANALY_AVG_UPDATE_COUNT);
                __io_analysis__->cur = next;
                if (op == ANALYSIS_IO_READ || op == ANALYSIS_OP_READ ) {
                        __io_analysis__->readps[next] = 1;
                        __io_analysis__->readbwps[next] = count;
                } else if (op == ANALYSIS_IO_WRITE || op == ANALYSIS_OP_WRITE) {
                        __io_analysis__->writeps[next] = 1;
                        __io_analysis__->writebwps[next] = count;
                }
                __io_analysis__->last = now;
        } else {
                if (op == ANALYSIS_IO_READ || op == ANALYSIS_OP_READ ) {
                        __io_analysis__->readps[__io_analysis__->cur] += 1;
                        __io_analysis__->readbwps[__io_analysis__->cur] += count;
                } else if (op == ANALYSIS_IO_WRITE || op == ANALYSIS_OP_WRITE) {
                        __io_analysis__->writeps[__io_analysis__->cur] += 1;
                        __io_analysis__->writebwps[__io_analysis__->cur] += count;
                }
        }

        ret = __io_analysis_dump();
        if (ret)
                GOTO(err_lock, ret);

        sy_spin_unlock(&__io_analysis__->lock);

out:
        return 0;
err_lock:
        sy_spin_unlock(&__io_analysis__->lock);
err_ret:
        return ret;
}

static void *__io_analysis_rept(void *arg)
{
        int ret;
        char path[MAX_PATH_LEN], buf[MAX_INFO_LEN];
        io_analysis_t *io_analysis = __io_analysis__;

        (void) arg;
        
        snprintf(path, MAX_PATH_LEN, "/analysis/%s/%d", __io_analysis__->name,
                 net_getnid()->id);
        
        while (1) {
                snprintf(buf, MAX_PATH_LEN, "read_count:%ju;write_count:%ju;"
                         "read_bytes:%ju;write_bytes:%ju;latency:%ju",
                         io_analysis->read_count, io_analysis->write_count,
                         io_analysis->read_bytes, io_analysis->write_bytes,
                         core_latency_get());

                mond_rpc_set(net_getnid(), path, buf, strlen(buf) + 1);

                ret = sy_spin_lock(&__io_analysis__->lock);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);
                
                ret = __io_analysis_dump();
                if (ret)
                        UNIMPLEMENTED(__DUMP__);
                
                sy_spin_unlock(&__io_analysis__->lock);

                sleep(2);
        }

        pthread_exit(NULL);
}

#define mond_for_each(buf, buflen, mon, off)                \
        for (mon = (void *)(buf);                                       \
             (void *)mon < (void *)(buf) + buflen ;                     \
             off = mon->offset, mon = (void *)mon + (sizeof(*(mon)) + mon->klen + mon->vlen))

int io_analysis_dump(const char *type)
{
        int ret, buflen, eof;
        char buf[MON_ENTRY_MAX], path[MAX_PATH_LEN];
        mon_entry_t *ent;
        uint64_t offset;
        const char *key, *value;

        offset = 0;
        snprintf(path, MAX_PATH_LEN, "/analysis/%s", type);
retry:
        buflen = MON_ENTRY_MAX;
        ret = mond_rpc_get(net_getnid(), path, offset, buf, &buflen);
        if (ret)
                GOTO(err_ret, ret);

        nid_t nid;
        mond_for_each(buf, buflen, ent, offset) {
                key = ent->buf;
                value = key + ent->klen;
                eof = ent->eof;
                
                str2nid(&nid, key);
                printf("%s %s\n", network_rname(&nid), value);
        }

        return 0;
        
        if (!eof) {
                goto retry;
        }
        
        return 0;
err_ret:
        return ret;
}

int io_analysis_init(const char *name, int seq)
{
        int ret;

        ret = ymalloc((void **)&__io_analysis__, sizeof(*__io_analysis__));
        if (ret)
                GOTO(err_ret, ret);
        
        ret = sy_spin_init(&__io_analysis__->lock);
        if (ret)
                GOTO(err_ret, ret);

        if (seq == -1) {
                __io_analysis__->seq = getpid();
        } else {
                __io_analysis__->seq = seq;
        }
        
        strcpy(__io_analysis__->name, name);

        ret = sy_thread_create2(__io_analysis_rept, NULL, "core_check_health");
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

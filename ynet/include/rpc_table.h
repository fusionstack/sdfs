#ifndef __RPC_TABLE_H__
#define __RPC_TABLE_H__

typedef struct {
        sy_spinlock_t lock;
        sy_spinlock_t used;
        msgid_t msgid;
        sockid_t sockid;
        nid_t nid;
        uint32_t timeout;
        uint32_t begin;
        uint32_t figerprint_prev;
        char name[MAX_NAME_LEN];
        void *arg;
        func3_t func;
} solt_t;

typedef struct {
        char name[MAX_NAME_LEN];
        uint32_t count;
        uint32_t cur;
        int tabid;
        time_t last_scan;
        uint32_t sequence;
        solt_t solt[0];
} rpc_table_t;

#define RPC_TABLE_MAX 8192

extern rpc_table_t *__rpc_table__;

int rpc_table_init(const char *name, rpc_table_t **rpc_table, int scan);
void rpc_table_destroy(rpc_table_t **_rpc_table);

void rpc_table_scan(rpc_table_t *rpc_table, int interval, int newtask);

int rpc_table_getsolt(rpc_table_t *rpc_table, msgid_t *msgid, const char *name);
int rpc_table_setsolt(rpc_table_t *rpc_table, const msgid_t *msgid, func3_t func, void *arg,
                      const sockid_t *sockid, const nid_t *nid, int timeout);

int rpc_table_post(rpc_table_t *rpc_table, const msgid_t *msgid, int retval, buffer_t *buf, uint64_t latency);
int rpc_table_free(rpc_table_t *rpc_table, const msgid_t *msgid);
void rpc_table_reset(rpc_table_t *rpc_table, const sockid_t *sockid, const nid_t *nid);

#endif

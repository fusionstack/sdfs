/*###################################################################
  > File: ylib/lib/redis_utils.c
  > Author: Vurtune
  > Mail: vurtune@foxmail.com
  > Created Time: Wed 23 Aug 2017 08:31:20 PM PDT
###################################################################*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "ylib.h"
#include "dbg.h"
#include "redis_util.h"
#include "schedule.h"

static int __redis_error(redis_conn_t *conn, const char *func, redisReply *reply)
{
        int ret;
        
        DWARN("srv %s, %s reply->type %u, reply->str %s\n", conn->key, func, reply->type, reply->str);

        if (strcmp(reply->str, "LOADING Redis is loading the dataset in memory") == 0) {
                ret = EAGAIN;
        } else if (strncmp(reply->str, "READONLY", strlen("READONLY")) == 0) {
                ret = ECONNRESET;
        } else {
                ret = EIO;
                UNIMPLEMENTED(__DUMP__);
        }

        return ret;
}

int connect_redis(const char *ip, short port, redis_ctx_t **ctx)
{
        int ret;
        redisContext *c = NULL;
        struct timeval timeout = {1, 500000}; // 1.5s

        c = redisConnectWithTimeout(ip, port, timeout);
        if (!c || c->err) {
                if (c) {
                        DINFO("Connection error: %s, addr %s:%d\n", c->errstr, ip, port);
                } else {
                        DINFO("Connection error: can't allocate redis context\n");
                }
                ret = ENONET;
                GOTO(err_ret, ret);
        }

        *ctx = c;
        DBUG("redis connected ip [%s] port [%d]\n", ip, port);

        return 0;
err_ret:
        redisFree(c);
        return ret;
}

int connect_redis_unix(const char *path, redis_ctx_t **ctx)
{
        int ret;
        redisContext *c = NULL;

        c = redisConnectUnix(path);
        if (!c || c->err) {
                if (c) {
                        DERROR("Connection error: %s\n", c->errstr);
                } else {
                        DERROR("Connection error: can't allocate redis context\n");
                }
                ret = ENONET;
                GOTO(err_ret, ret);
        }

        *ctx = c;
        DBUG("redis connected unix %s\n", path);

        return 0;
err_ret:
        redisFree(c);
        return ret;
}

#if 0
int disconnect_redis(redis_ctx_t **ctx)
{
        redisFree(*ctx);
        *ctx = NULL;

        return 0;
}

int flush_redis(redis_ctx_t *ctx)
{
        int ret;
        redisReply *reply;

        reply = redisCommand(ctx, "FLUSHALL");
        if (reply != NULL && reply->type == REDIS_REPLY_STATUS) {
        } else {
                DWARN("redis flush error reply->type: %d %s\n", reply->type, reply->str);
                ret = __redis_error(__FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
        return ret;

}

int key_exist(redis_ctx_t *ctx, const char *key, int *exist)
{
        int ret;
        redisReply *reply;

        reply = redisCommand(ctx, "EXISTS %s", key);
        if (reply != NULL && reply->type == REDIS_REPLY_INTEGER) {
                *exist = reply->integer;
        } else {
                DWARN("redis reply->type: %d\n", reply->type);
                ret = __redis_error(__FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
        return ret;
}

int exec_begin(redis_ctx_t *ctx)
{
        int ret;
        redisReply *reply;

        reply = redisCommand(ctx, "MULTI");
        if (reply != NULL && reply->type == REDIS_REPLY_STATUS) {
        } else {
                DWARN("redis reply->type: %d\n", reply->type);
                ret = __redis_error(__FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
        return ret;
}

int exec_end(redis_ctx_t *ctx)
{
        int ret;
        redisReply *reply;

        reply = redisCommand(ctx, "EXEC");
        if (reply != NULL && reply->type == REDIS_REPLY_STATUS) {
        } else {
                DWARN("redis reply->type: %d\n", reply->type);
                ret = __redis_error(__FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
        return ret;
}

#endif


#define TYPE_SCHE 1
#define TYPE_SEM 2

typedef struct {
        int type;
        task_t task;
        sem_t sem;
} args_t;

int redis_connect(redis_conn_t **_conn, const char *addr, const int *port, const char *key)
{
        int ret;
        redisContext *c;
        redis_conn_t *conn;
        //char path[MAX_PATH_LEN];
        //const char *sock;
        //snprintf(path, MAX_PATH_LEN, "%s/data/redis/redis.sock", gloconf.workdir);
        
        if (port == NULL) {
                ret = connect_redis_unix(addr, &c);
        } else {
                ret = connect_redis(addr, *port, &c);
        }
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&conn, sizeof(*conn));
        if ((unlikely(ret)))
                UNIMPLEMENTED(__DUMP__);

        strcpy(conn->key, key);
#if 0
        ret = sy_spin_init(&conn->lock);
        if ((unlikely(ret)))
                UNIMPLEMENTED(__DUMP__);

        INIT_LIST_HEAD(&conn->list);
#endif

        ret = sy_rwlock_init(&conn->rwlock, "redis_session");
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

        conn->ctx = c;
        *_conn = conn;

        DBUG("redis connected\n");

        return 0;
err_ret:
        return ret;
}

int redis_hget(redis_conn_t *conn, const char *hash, const char *key, void *buf, size_t *len)
{
        int ret;
        redisReply *reply;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

#if 0
        char req[MAX_BUF_LEN];
        snprintf(req, MAX_BUF_LEN, "HGET %s %s", hash, key);
        reply = redisCommand(conn->ctx, req);
#else
        reply = redisCommand(conn->ctx, "HGET %s %s", hash, key);
#endif

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset, hash %s, key %s\n", hash, key);
                GOTO(err_ret, ret);
        }
        
        if (reply->type == REDIS_REPLY_NIL) {
                ret = ENOENT;
                GOTO(err_free, ret);
        }
                
        if (reply->type != REDIS_REPLY_STRING) {
                DWARN("redis reply->type: %d\n", reply->type);
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        *len = reply->len;
        memcpy(buf, reply->str, reply->len);

        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int redis_kget(redis_conn_t *conn, const char *key, void *buf, size_t *len)
{
        int ret;
        redisReply *reply;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

#if 0
        char req[MAX_BUF_LEN];
        snprintf(req, MAX_BUF_LEN, "GET %s", key);
        reply = redisCommand(conn->ctx, req);
#else
        reply = redisCommand(conn->ctx, "GET %s", key);
#endif

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }
        
        if (reply->type == REDIS_REPLY_NIL) {
                ret = ENOENT;
                GOTO(err_free, ret);
        }
                
        if (reply->type != REDIS_REPLY_STRING) {
                DWARN("redis reply->type: %d\n", reply->type);
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        *len = reply->len;
        memcpy(buf, reply->str, reply->len);

        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int redis_kset(redis_conn_t *conn, const char *key, const void *value,
              size_t size, int flag, int _ttl)
{
        int ret;
        redisReply *reply;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

        if (_ttl != -1) {
                size_t ttl = _ttl;
                if (flag & O_EXCL) {
                        reply = redisCommand(conn->ctx, "SET %s %b EX %d NX", key, value, size, ttl);
                } else {
                        reply = redisCommand(conn->ctx, "SET %s %b EX %d", key, value, size, ttl);
                }
        } else {
                if (flag & O_EXCL) {
                        reply = redisCommand(conn->ctx, "SET %s %b NX", key, value, size);
                } else {
                        reply = redisCommand(conn->ctx, "SET %s %b", key, value, size);
                }
        }

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }

        if (flag & O_EXCL && reply->type == REDIS_REPLY_NIL) {
                ret = EEXIST;
                GOTO(err_free, ret);
        }
        
        if (reply->type != REDIS_REPLY_STATUS || strcmp(reply->str, "OK") != 0) {
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

#if 0
        DINFO("reply->integer  %u\n", reply->integer);

        if (flag & O_EXCL && reply->integer == 0) {
                ret = EEXIST;
                GOTO(err_free, ret);
        }
#endif
        
        freeReplyObject(reply);

        return 0;
err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int redis_hset(redis_conn_t *conn, const char *hash, const char *key, const void *value, size_t size, int flag)
{
        int ret;
        redisReply *reply;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

        if (flag & O_EXCL) {
                reply = redisCommand(conn->ctx, "HSETNX %s %s %b", hash, key, value, size);
        } else {
                reply = redisCommand(conn->ctx, "HSET %s %s %b", hash, key, value, size);
        }

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }

        if (reply->type != REDIS_REPLY_INTEGER) {
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        //DINFO("reply->integer  %u\n", reply->integer);
        if (flag & O_EXCL && reply->integer == 0) {
                ret = EEXIST;
                GOTO(err_free, ret);
        }
        
        freeReplyObject(reply);

        return 0;
err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int redis_hdel(redis_conn_t *conn, const char *hash, const char *key)
{
        int ret;
        redisReply *reply;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

        reply = redisCommand(conn->ctx, "HDEL %s %s", hash, key);

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }
        
        if (reply->type != REDIS_REPLY_INTEGER) {
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        if (reply->integer == 0) {
                ret = ENOENT;
                GOTO(err_free, ret);
        }
        
        freeReplyObject(reply);

        return 0;
err_free:
        freeReplyObject(reply);
err_ret:
        return ret;

}

int redis_hexist(redis_conn_t *conn, const char *hash, const char *key, int *exist)
{
        int ret;
        redisReply *reply;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

        reply = redisCommand(conn->ctx, "HEXISTS %s %s", hash, key);

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }
        
        if (reply->type != REDIS_REPLY_INTEGER) {
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        *exist = reply->integer;

        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}


static int __redis_hiterator(redis_conn_t *conn, const char *hash, size_t *_cur, const char *match, func2_t func2, void *arg)
{
        int ret;
        redisReply *reply, *e1, *e2;
        size_t i, cur = *_cur;

        //DINFO("HSCAN cur %u\n", cur);

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

#if 1
        char buf[MAX_BUF_LEN];
        if (match)
                snprintf(buf, MAX_BUF_LEN, "HSCAN %s %ju MATCH %s count 100", hash, cur, match);
        else
                snprintf(buf, MAX_BUF_LEN, "HSCAN %s %ju count 100", hash, cur);
        
        reply = redisCommand(conn->ctx, buf);
#else
        if (match) {
                reply = redisCommand(conn->ctx, "HSCAN %s %b MATCH %s count 100", hash, cur, match);
        } else {
                reply = redisCommand(conn->ctx, "HSCAN %s %b count 100", hash, cur);
        }
#endif
        
        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }

        if (reply->type != REDIS_REPLY_ARRAY) {
                DWARN("redis reply->type: %d\n", reply->type);
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        
        YASSERT(reply->elements == 2);
        YASSERT(reply->element[0]->type == REDIS_REPLY_STRING);
        YASSERT(reply->element[1]->type == REDIS_REPLY_ARRAY);
        *_cur = atol(reply->element[0]->str);

        DBUG("scan %s count %ju\n", hash, reply->element[1]->elements);
        for (i = 0; i < reply->element[1]->elements; i += 2) {
                e1 = reply->element[1]->element[i];
                e2 = reply->element[1]->element[i + 1];
                YASSERT(e1->type == REDIS_REPLY_STRING);
                YASSERT(e2->type == REDIS_REPLY_STRING);

                DBUG("key %s, value %s\n", e1->str, e2->str);
                func2(e1->str, e2->str, arg);
        }

        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int redis_hiterator(redis_conn_t *conn, const char *hash, const char *match, func2_t func2, void *arg)
{
        int ret;//, i = 0;
        size_t cur = 0;

        while (1) {
                ret = __redis_hiterator(conn, hash, &cur, match,  func2, arg);
                if ((unlikely(ret))) {
                        GOTO(err_ret, ret);
                }

                //i++;
                //DINFO("%u %u\n", i, cur);
                //sleep(1);
                
                if (cur == 0)
                        break;
        }

        return 0;
err_ret:
        return ret;
}

redisReply *redis_hscan(redis_conn_t *conn, const char *key, const char *match,
                        uint64_t cursor, uint64_t _count)
{
        int ret;
        redisReply *reply;
        uint64_t count = _count == (uint64_t)-1 ? 1000 : _count;

        count = count < 20 ? 20 : count;

        DBUG("scan %s, cursor %ju count %ju\n", key, cursor, count);
        
        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

        if (match) {
                reply = redisCommand(conn->ctx, "HSCAN %s %lu MATCH %s count %d",
                                     key, cursor, match, count);
        } else {
                reply = redisCommand(conn->ctx, "HSCAN %s %lu count %d",
                                     key, cursor, count);
        }

        sy_rwlock_unlock(&conn->rwlock);
        
        if(reply == NULL){
                DERROR("HSCAN failed\n");
                return NULL;
        }

        return reply;
err_ret:
        return NULL;
}

int redis_keys(redis_conn_t *conn, func1_t func, void *arg)
{
        int ret;
        redisReply *reply, *e;
        size_t i;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);
        
        reply = redisCommand(conn->ctx, "KEYS *");

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }

        YASSERT(reply->type == REDIS_REPLY_ARRAY);
        for (i = 0; i < reply->elements; i++) {
                e = reply->element[i];
                YASSERT(e->type == REDIS_REPLY_STRING);
                func(e->str, arg);
        }

        freeReplyObject(reply);

        return 0;
//err_free:
//        freeReplyObject(reply);
err_ret:
        return ret;
}

static int __redis_siterator(redis_conn_t *conn, const char *set, size_t *_cur, func1_t func, void *arg)
{
        int ret;
        redisReply *reply, *e1;
        size_t i, cur = *_cur;
        

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

#if 1
        char buf[MAX_BUF_LEN];
        snprintf(buf, MAX_BUF_LEN, "SSCAN %s %ju count 100", set, cur);
        reply = redisCommand(conn->ctx, buf);
#else
        reply = redisCommand(conn->ctx, "SSCAN %s %b count 100", set, cur);
#endif

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = EAGAIN;
                DERROR("(SSCAN %s %b count 10)\n", set, cur);
                GOTO(err_ret, ret);
        }

        if (reply->type != REDIS_REPLY_ARRAY) {
                DWARN("redis reply->type: %d\n", reply->type);
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        YASSERT(reply->elements == 2);
        YASSERT(reply->element[0]->type == REDIS_REPLY_STRING);
        YASSERT(reply->element[1]->type == REDIS_REPLY_ARRAY);
        *_cur = atol(reply->element[0]->str);

        DBUG("scan %s count %ju\n", set, reply->element[1]->elements);
        for (i = 0; i < reply->element[1]->elements; i++) {
                e1 = reply->element[1]->element[i];
                YASSERT(e1->type == REDIS_REPLY_STRING);

                DBUG("key %s\n", e1->str);
                func(e1->str, arg);
        }

        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int redis_siterator(redis_conn_t *conn, const char *set, func1_t func, void *arg)
{
        int ret;//, i = 0;
        size_t cur = 0;

        while (1) {
                ret = __redis_siterator(conn, set, &cur, func, arg);
                if ((unlikely(ret))) {
                        GOTO(err_ret, ret);
                }

                //i++;
                //DINFO("%u %u\n", i, cur);
                if (cur == 0)
                        break;
        }

        return 0;
err_ret:
        return ret;
}

int redis_sset(redis_conn_t *conn, const char *set, const char *key)
{
        int ret;
        redisReply *reply;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);
        
        reply = redisCommand(conn->ctx, "SADD %s %s", set, key);

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }

        if (reply->type != REDIS_REPLY_INTEGER) {
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int redis_sdel(redis_conn_t *conn, const char *set, const char *key)
{
        int ret;
        redisReply *reply;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);
        
        reply = redisCommand(conn->ctx, "SREM %s %s", set, key);

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }

        if (reply->type != REDIS_REPLY_INTEGER) {
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int redis_scount(redis_conn_t *conn, const char *set, uint64_t *count)
{
        int ret;
        redisReply *reply;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);
        
        reply = redisCommand(conn->ctx, "SCARD %s", set);

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }

        if (reply->type != REDIS_REPLY_INTEGER) {
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        *count = reply->integer;
        
        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int redis_hlen(redis_conn_t *conn, const char *key, uint64_t *count)
{
        int ret;
        redisReply *reply;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);
        
        reply = redisCommand(conn->ctx, "HLEN %s", key);

        sy_rwlock_unlock(&conn->rwlock);

        if (reply != NULL && reply->type == REDIS_REPLY_INTEGER) {
                *count = reply->integer;
        } else {
                ret = EIO;
                GOTO(err_free, ret);
        }

        freeReplyObject(reply);

        return 0;
err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}


#if 0
int redis_exec(redis_conn_t *conn, const char *buf)
{
        int ret;
        redisReply *reply;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);
        
        reply = redisCommand(conn->ctx, buf);

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }

        if (reply->type != REDIS_REPLY_INTEGER) {
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        DINFO("reply %u\n", reply->integer);
        
        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int redis_multi(redis_conn_t *conn, ...)
{
        int ret;
        redisReply *reply;
        const void *pos;
        va_list ap;

        va_start(ap, conn);
        
        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

        reply = redisCommand(conn->ctx, "MULTI");
        YASSERT(reply);
        YASSERT(reply->type == REDIS_REPLY_STATUS);
        YASSERT(strcmp(reply->str, "OK") == 0);
        freeReplyObject(reply);
        
        while (1) {
                pos = va_arg(ap, const char *);
                if (pos == NULL)
                        break;

                reply = redisCommand(conn->ctx, pos);
                YASSERT(reply);
                YASSERT(reply->type == REDIS_REPLY_STATUS);
                YASSERT(strcmp(reply->str, "QUEUED") == 0);
                freeReplyObject(reply);
        }

        va_end(ap);

        reply = redisCommand(conn->ctx, "EXEC");
        YASSERT(reply);
        YASSERT(reply->type == REDIS_REPLY_ARRAY);
        //YASSERT(strcmp(reply->str, "OK") == 0);
        freeReplyObject(reply);
        
        sy_rwlock_unlock(&conn->rwlock);

        return 0;
err_ret:
        return ret;
}

int redis_exec_array(redis_conn_t *conn, const char **array, int count)
{
        int ret;
        redisReply *reply;
        const char *pos;
        
        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

        reply = redisCommand(conn->ctx, "MULTI");
        YASSERT(reply);
        YASSERT(reply->type == REDIS_REPLY_STATUS);
        YASSERT(strcmp(reply->str, "OK") == 0);
        freeReplyObject(reply);
        
        for (int i = 0; i < count; i++) {
                pos = array[i];
                DINFO("%s\n", pos);
                
                reply = redisCommand(conn->ctx, pos);
                YASSERT(reply);
                YASSERT(reply->type == REDIS_REPLY_STATUS);
                YASSERT(strcmp(reply->str, "QUEUED") == 0);
                freeReplyObject(reply);
        }

        reply = redisCommand(conn->ctx, "EXEC");
        YASSERT(reply);
        YASSERT(reply->type == REDIS_REPLY_ARRAY);
        //YASSERT(strcmp(reply->str, "OK") == 0);
        freeReplyObject(reply);
        
        sy_rwlock_unlock(&conn->rwlock);

        return 0;
err_ret:
        return ret;
}

void redis_trans_sset(redis_conn_t *conn, const char *set, const char *key)
{
        redisReply *reply;

        reply = redisCommand(conn->ctx, "SADD %s %s", set, key);
        YASSERT(reply);
        YASSERT(reply->type == REDIS_REPLY_STATUS);
        YASSERT(strcmp(reply->str, "QUEUED") == 0);
        freeReplyObject(reply);
}

void redis_trans_hset(redis_conn_t *conn, const char *hash, const char *key, const void *value, size_t size)
{
        redisReply *reply;

        reply = redisCommand(conn->ctx, "HSET %s %s %b", hash, key, value, size);
        YASSERT(reply);
        YASSERT(reply->type == REDIS_REPLY_STATUS);
        YASSERT(strcmp(reply->str, "QUEUED") == 0);
        freeReplyObject(reply);
}

void redis_trans_sdel(redis_conn_t *conn, const char *set, const char *key)
{
        redisReply *reply;

        reply = redisCommand(conn->ctx, "SREM %s %s", set, key);
        YASSERT(reply);
        YASSERT(reply->type == REDIS_REPLY_STATUS);
        YASSERT(strcmp(reply->str, "QUEUED") == 0);
        freeReplyObject(reply);
}

void redis_trans_hdel(redis_conn_t *conn, const char *hash, const char *key)
{
        redisReply *reply;

        reply = redisCommand(conn->ctx, "HDEL %s %s", hash, key);
        YASSERT(reply);
        YASSERT(reply->type == REDIS_REPLY_STATUS);
        YASSERT(strcmp(reply->str, "QUEUED") == 0);
        freeReplyObject(reply);
}

void redis_trans_hget(redis_conn_t *conn, const char *hash, const char *key)
{
        redisReply *reply;

        char req[MAX_BUF_LEN];
        snprintf(req, MAX_BUF_LEN, "HGET %s %s", hash, key);

        //reply = redisCommand(conn->ctx, "HGET %s %s", hash, key);
        reply = redisCommand(conn->ctx, req);
        YASSERT(reply);
        YASSERT(reply->type == REDIS_REPLY_STATUS);
        YASSERT(strcmp(reply->str, "QUEUED") == 0);
        freeReplyObject(reply);
}

void redis_trans_begin(redis_conn_t *conn)
{
        int ret;
        redisReply *reply;
        
        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                UNIMPLEMENTED(__DUMP__);

        reply = redisCommand(conn->ctx, "MULTI");
        YASSERT(reply);
        YASSERT(reply->type == REDIS_REPLY_STATUS);
        YASSERT(strcmp(reply->str, "OK") == 0);
        freeReplyObject(reply);
}

void redis_trans_end(redis_conn_t *conn)
{
        redisReply *reply;
        
        reply = redisCommand(conn->ctx, "EXEC");
        YASSERT(reply);
        YASSERT(reply->type == REDIS_REPLY_ARRAY);
        freeReplyObject(reply);
        
        sy_rwlock_unlock(&conn->rwlock);
}

void redis_trans_end1(redis_conn_t *conn, func1_t func, void *arg, int count)
{
        redisReply *reply, *e;
        
        reply = redisCommand(conn->ctx, "EXEC");
        YASSERT(reply);
        YASSERT(reply->type == REDIS_REPLY_ARRAY);
        YASSERT(reply->elements == count);

        sy_rwlock_unlock(&conn->rwlock);

        for (int i = 0; i < reply->elements; i++) {
                e = reply->element[i];
                DBUG("e[%u] type %u\n", i, e->type);

                if (e->type == REDIS_REPLY_NIL) {
                        func(NULL, arg);
                } else {
                        YASSERT(e->type == REDIS_REPLY_STRING);
                        func(e->str, arg);
                }
        }

        freeReplyObject(reply);
}
#endif

int redis_multi_exec(redis_conn_t *conn, const char *op, const char *tab,
                     mctx_t *ctx, func1_t func, void *arg)
{
        int i = 0, ret, argc, count;
        size_t *arglen;
        mseg_t *seg;
        const char **argv;
        redisReply *reply;
        struct list_head *pos, *n;

        if (ctx->segcount <= 0)
                return 0;

        /*  CMD key value key value ...  */
        argc = 2 + ctx->segcount * 2;

        ret = ymalloc((void **)&argv, argc * sizeof(char*));
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&arglen, argc * sizeof(size_t));
        if ((unlikely(ret)))
                GOTO(err_free1, ret);

        argv[i] = op;
        arglen[i++] = strlen(op);

        if (tab) {
                argv[i] = tab;
                arglen[i++] = strlen(tab);
        }

        list_for_each_safe(pos, n, &ctx->kvlist) {
                seg = (mseg_t *)pos;
                DBUG("mset entry[%u] seg key %s %s len %llu\n",
                      i, seg->key, seg->value, (LLU)seg->size);
                argv[i] = seg->key;
                arglen[i++] = strlen(seg->key);

                if (seg->value) {
                        argv[i] = seg->value;
                        arglen[i++] = seg->size;
                }
        }

        count = i;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                UNIMPLEMENTED(__DUMP__);
        
        ANALYSIS_BEGIN(0);
        reply = redisCommandArgv(conn->ctx, count, argv, arglen);
        ANALYSIS_END(0, 0, NULL);

        sy_rwlock_unlock(&conn->rwlock);
        
        YASSERT(reply);
        if (reply->type == REDIS_REPLY_STATUS) {

                //TODO status OK?
        } else if (reply->type == REDIS_REPLY_ARRAY) {
                YASSERT(strcmp(op, "HMSET") == 0
                        || strcmp(op, "HMGET") == 0);
                YASSERT((int)reply->elements == (int)ctx->segcount);

                if (func) {
                        for (i = 0; i < (int)reply->elements; i++) {
                                redisReply *e = reply->element[i];
                                if (e->type == REDIS_REPLY_STRING) {
                                        func(e->str, arg);
                                } else {
                                        YASSERT(e->type == REDIS_REPLY_NIL);
                                        func(NULL, arg);
                                }
                        }
                }

        } else if (reply->type == REDIS_REPLY_INTEGER) {
                YASSERT(strcmp(op, "SADD") == 0
                        || strcmp(op, "HDEL") == 0
                        || strcmp(op, "SREM") == 0);
        } else {
                DWARN("redis reply->type: %d\n", reply->type);
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_ret, ret);
        }

        freeReplyObject(reply);
        
        yfree((void **)&argv);
        yfree((void **)&arglen);
        return 0;

err_ret:
        yfree((void **)&argv);
err_free1:
        yfree((void **)&arglen);
        return ret;
}

void redis_multi_append(mctx_t *ctx, char *key, void *value, uint32_t len)
{
        int ret;
        mseg_t *seg;

        ret = ymalloc((void **)&seg, sizeof(mseg_t));
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        seg->key = seg->__key__;
        strcpy(seg->key, key);
        if (value) {
                seg->value = seg->__value__;
                strcpy(seg->value, value);
        } else {
                seg->value = NULL;
        }

        seg->size = len;

        DBUG("key %s, value %s\n", key, value);
        
        ctx->segcount++;
        list_add_tail(&seg->hook, &ctx->kvlist);
}

int redis_multi_destory(mctx_t *ctx)
{
        mseg_t *seg;
        struct list_head *pos, *n;
        list_for_each_safe(pos, n, &ctx->kvlist) {
                seg = (mseg_t *)pos;
                list_del(pos);
                yfree((void **) &seg);
        }

        if (ctx->segcount) {
                //freeReplyObject(ctx->reply);
        }

        return 0;
}

void redis_multi_init(mctx_t *ctx)
{
        ctx->segcount = 0;
        INIT_LIST_HEAD(&ctx->kvlist);
}

int redis_kdel(redis_conn_t *conn, const char *key)
{
        int ret;
        redisReply *reply;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

        reply = redisCommand(conn->ctx, "DEL %s", key);

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }
        
        if (reply->type != REDIS_REPLY_INTEGER) {
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        if (reply->integer == 0) {
                ret = ENOENT;
                GOTO(err_free, ret);
        }
        
        freeReplyObject(reply);

        return 0;
err_free:
        freeReplyObject(reply);
err_ret:
        return ret;

}

int
redis_disconnect(redis_conn_t *conn)
{
        int ret;
        
        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);
        
        redisFree(conn->ctx);

        sy_rwlock_unlock(&conn->rwlock);

        yfree((void **) &conn);

        return 0;
err_ret:
        return ret;
}

int redis_del(redis_conn_t *conn, const char *key)
{
        int ret;
        redisReply *reply;

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

        reply = redisCommand(conn->ctx, "DEL %s", key);

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }
        
        if (reply->type != REDIS_REPLY_INTEGER) {
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        if (reply->integer == 0) {
                ret = ENOENT;
                GOTO(err_free, ret);
        }
        
        freeReplyObject(reply);

        return 0;
err_free:
        freeReplyObject(reply);
err_ret:
        return ret;

}

static int __redis_iterator(redis_conn_t *conn, const char *match, size_t *_cur,
                            func1_t func, void *arg)
{
        int ret;
        redisReply *reply, *e1;
        size_t i, cur = *_cur;

        //DINFO("HSCAN cur %u\n", cur);

        ret = sy_rwlock_wrlock(&conn->rwlock);
        if ((unlikely(ret)))
                GOTO(err_ret, ret);

#if 1
        char buf[MAX_BUF_LEN];

        if (match) {
                snprintf(buf, MAX_BUF_LEN, "SCAN %ju MATCH %s count 100", cur, match);
        } else {
                snprintf(buf, MAX_BUF_LEN, "SCAN %ju count 100", cur);
        }
        
        reply = redisCommand(conn->ctx, buf);
#else
        if (match) {
                reply = redisCommand(conn->ctx, "SCAN %b MATCH %s count 100", cur, match);
        } else {
                reply = redisCommand(conn->ctx, "SCAN %b count 100", cur);
       }
#endif

        sy_rwlock_unlock(&conn->rwlock);

        if (reply == NULL) {
                ret = ECONNRESET;
                DWARN("redis reset\n");
                GOTO(err_ret, ret);
        }

        if (reply->type != REDIS_REPLY_ARRAY) {
                DWARN("redis reply->type: %d\n", reply->type);
                ret = __redis_error(conn, __FUNCTION__, reply);
                GOTO(err_free, ret);
        }

        
        YASSERT(reply->elements == 2);
        YASSERT(reply->element[0]->type == REDIS_REPLY_STRING);
        YASSERT(reply->element[1]->type == REDIS_REPLY_ARRAY);
        *_cur = atol(reply->element[0]->str);

        //DBUG("scan %s count %ju\n", hash, reply->element[1]->elements);
        for (i = 0; i < reply->element[1]->elements; i++) {
                e1 = reply->element[1]->element[i];
                YASSERT(e1->type == REDIS_REPLY_STRING);
                //DBUG("key %s, value %s\n", e1->str);
                func(e1->str, arg);
        }

        freeReplyObject(reply);
        return 0;

err_free:
        freeReplyObject(reply);
err_ret:
        return ret;
}

int redis_iterator(redis_conn_t *conn, const char *match, func1_t func, void *arg)
{
        int ret;
        size_t cur = 0;

        while (1) {
                ret = __redis_iterator(conn, match, &cur, func, arg);
                if ((unlikely(ret))) {
                        GOTO(err_ret, ret);
                }
                
                if (cur == 0)
                        break;
        }

        return 0;
err_ret:
        return ret;
}

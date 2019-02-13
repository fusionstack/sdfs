#ifndef  __ETCD_API_H__
#define __ETCD_API_H__


/*
 * Description of an etcd server.  For now it just includes the name and
 * port, but some day it might include other stuff like SSL certificate
 * information.
 */

typedef enum {
        ETCD_OK = 0,
        ETCD_PROTOCOL_ERROR,
                                /* TBD: add other error categories here */
        ETCD_ERR,                /* anything we can't easily categorize */
    	ETCD_ENOENT,		/*  no such file when get interface */
        ETCD_PREVCONT,           /* set or get, when prevcontition is not equal */
} etcd_result;


typedef enum{
    ETCD_VALUE = 0,
    ETCD_DIR,
    ETCD_ORDER,
    ETCD_DELETE,
} etcd_set_flag;


typedef enum{
    prevValue = 0,
    prevIndex,
    prevExist,
} etcd_prevcond_flag;


typedef struct{
    etcd_prevcond_flag type;
    char *value;
} etcd_prevcond_t;


typedef struct {
        char            *host;
        unsigned short  port;
} etcd_server;

typedef struct {
    etcd_server     *servers;
} _etcd_session;

typedef struct{
    char *key;
    char *value;
    int dir;
    int modifiedIndex;
    int createdIndex;
    int num_node;
    void **nodes;
} etcd_node_t;


typedef struct{
    char *leader_id;
    char *state;
    char *self_id;
} etcd_self_t;

typedef void *etcd_session;

/*
 * etcd_open
 *
 * Establish a session to an etcd cluster, with automatic reconnection and
 * so on.
 *
 *      server_list
 *      Array of etcd_server structures, with the last having host=NULL.  The
 *      caller is responsible for ensuring that this remains valid as long as
 *      the session exists.
 */
etcd_session    etcd_open       (etcd_server *server_list);
void etcd_api_init();


/*
 * etcd_open_str
 *
 * Same as etcd_open, except that the servers are specified as a list of
 * host:port strings, separated by comma/semicolon or whitespace.
 */
etcd_session    etcd_open_str   (char *server_names);


/*
 * etcd_close
 *
 * Terminate a session, closing connections and freeing memory (or any other
 * resources) associated with it.
 */
void            etcd_close      (etcd_session session);


/*
 * etcd_close
 *
 * Same as etcd_close, but also free the server list as etcd_open_str would
 * have allocated it.
 */
void            etcd_close_str  (etcd_session session);


/*
 * etcd_get
 *
 * Fetch a key from one of the servers in a session.  The return value is a
 * newly allocated string, which must be freed by the caller.
 *
 *      key
 *      The etcd key (path) to fetch.
 */
etcd_result etcd_get (etcd_session session, char *key, long timeout, etcd_node_t **ppnode, int consistent);

int free_etcd_node(etcd_node_t *node);
/*
 * etcd_watch
 * Watch the set of keys matching a prefix.
 *
 *      pfx
 *      The etcd key prefix (like a path) to watch.
 *
 *      index_in
 *      Pointer to an index to be used for *issuing* the watch request, or
 *      NULL for a watch without an index.
 *
 * In normal usage, index_in will be NULL and index_out will be set to receive
 * the index for the first watch.  Subsequently, index_in will be set to
 * provide the previous index (plus one) and index_out will be set to receive
 * the next.  It's entirely legitimate to point both at the same variable.
 */

etcd_result etcd_watch (etcd_session session, char *pfx, const int *index_in, etcd_node_t **ppnode, int timeout);


/*
 * etcd_set
 *
 * Write a key, with optional TTL and/or previous value (as a precondition).
 *
 *      key
 *      The etcd key (path) to set.
 *
 *      value
 *      New value as a null-terminated string.  Unlike etcd_get, we can derive
 *      the length ourselves instead of needing it to be passed in separately.
 *
 *      precond
 *      Required previous value as a null-terminated string, or NULL to mean
 *      an unconditional set.
 *
 *      ttl
 *      Time in seconds after which the value will automatically expire and be
 *      deleted, or zero to mean no auto-expiration.
 */

etcd_result etcd_set(etcd_session session, char *key, char *value,
                     etcd_prevcond_t *precond, etcd_set_flag flag, unsigned int ttl, long timeout);

etcd_result etcd_set_dir(etcd_session session, char *key, etcd_prevcond_t *precond, unsigned int ttl);    

etcd_result etcd_batch_set(etcd_session session, char **key, char **value, char *dir, int num);    
/*
 * etcd_delete
 *
 * Delete a key from one of the servers in a session.
 *
 *      key
 *      The etcd key (path) to delete.
 */

etcd_result etcd_delete(etcd_session session, char *key);


etcd_self_t *etcd_self(etcd_session session);

int free_etcd_self(etcd_self_t *self);

etcd_result
etcd_update_ttl (etcd_session session_as_void, char *key,  unsigned int ttl);

etcd_result
etcd_deletedir (etcd_session session_as_void, char *key, int recursive);

#endif

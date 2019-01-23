/*
 * =====================================================================================
 *
 *       Filename:  hostcache.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  04/11/2011 05:51:58 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */
#ifndef HOST_CACHE_H_
#define HOST_CACHE_H_
#include "sdfs_lib.h"
#include "ylib.h"
#include "dbg.h"

typedef struct hostentry_s {
        char *nm;
        int len;
        int lkn;
} hostentry_t;

typedef struct hostcache_s {
        hashtable_t hashtb;
} hostcache_t;

extern hostcache_t hostcache;

int hostcache_compare(const void *d1, const void *d2);
uint32_t hostcache_hash(const void *d1);
int hostcache_init(hostcache_t *hostcache);
int hostcache_insert(hostcache_t *hostcache, hostentry_t *ent);
int hostcache_delete(hostcache_t *hostcache, hostentry_t *ent, hostentry_t **retval);
void hostcache_find(hostcache_t *hostcache, hostentry_t *ent, hostentry_t **retval);
int hostentry_destroy(hostentry_t *entry);
#endif

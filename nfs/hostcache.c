/*
 * =====================================================================================
 *
 *       Filename:  hostcache.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  04/11/2011 06:03:05 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */
#include "hostcache.h"
hostcache_t hostcache;
int hostcache_compare(const void *d1, const void *d2)
{
        hostentry_t *v1,*v2;
        v1 = (hostentry_t*)d1;
        v2 = (hostentry_t*)d2;

        if (v1->len == v2->len && (0==memcmp(v1->nm,v2->nm, v1->len)))
                return 0;
        return 1;
}

uint32_t hostcache_hash(const void *d1)
{
        hostentry_t *v1;
        v1 = (hostentry_t*)d1;
        return hash_mem(v1->nm, v1->len);
}

int hostcache_init(hostcache_t *hostcache)
{
        hostcache->hashtb = hash_create_table(hostcache_compare, hostcache_hash, "hostcache");
        if (!hostcache->hashtb)
                return ENOMEM;
        return 0;
}
int hostcache_insert(hostcache_t *hostcache, hostentry_t *ent)
{
        int ret = 0;
        ret =  hash_table_insert(hostcache->hashtb, (void*)ent, (void*)ent, 0);
        if (ret)
                GOTO(err_ret, ret);
        return 0;
err_ret:
        return ret;
}
int hostcache_delete(hostcache_t *hostcache, hostentry_t *ent, hostentry_t **retval)
{
        int ret = 0;

        (*retval) = NULL;
        ret = hash_table_remove(hostcache->hashtb, (void*)ent, (void**)retval);
        if (ret)
                GOTO(err_ret, ret);
        return 0;
err_ret:
        return ret;
}
void hostcache_find(hostcache_t *hostcache, hostentry_t *ent, hostentry_t **retval)
{
        (*retval) = NULL;
        (*retval) = hash_table_find(hostcache->hashtb, (void*)ent);
}
int hostentry_destroy(hostentry_t *entry)
{
        (void)entry;
        return 0;
}



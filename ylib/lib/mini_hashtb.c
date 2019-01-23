/*
 * ===================================================================================== *
 *       Filename:  mini_hashtb.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  03/10/2011 03:54:49 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */
#include "../include/mini_hashtb.h"
#include "dbg.h"
#if 0
#define GOTO(lab, ret)\
do {\
        ret = errno;\
        goto lab;\
} while(0);
#endif

extern int ymalloc(void **ptr, size_t size);
#if 0
int ymalloc(void **ptr, size_t size)
{
        (*ptr) = malloc(size);
        memset(*ptr, 0x00, size);
        return (*ptr)?0:ENOMEM;
}
int    mini_hashtb_pfidnmcmp(void *d1, void *d2)
{
       pfid2cfid_t *v1,*v2;
       v1 = d1;
       v2 = d2;
       return !(v1->parent == v2->parent && 0==strcmp(v1->name,v2->name));
}
size_t hash_pfid2cfid(void *data)
{
        mini_ent_t *ent;
        pfid2cfid_t *pnm2cfid;
        size_t key;
        int i = 0;

        ent = data;
        pnm2cfid = (pfid2cfid_t*)ent->data;
        key = pnm2cfid->parent->id + pnm2cfid->parent->version;
        while( pnm2cfid->name[i++] != '\0')
                key = key + pnm2cfid->name[i];
        return key;
}
#endif


int mini_hashtb_malloc(void *pool, void **_ptr, size_t size)
{
        int ret;
        ret = ymalloc(_ptr, size);
        if (ret)
                GOTO(err_mem, ret);
        (void)pool;
        return 0;
err_mem:
        return ret;
}

int mini_hashtb_realloc(void *pool, void **_ptr, size_t size)
{
        int ret;
        ret = ymalloc(_ptr,size);
        if (ret)
                GOTO(err_mem, ret);
        (void)pool;
        memset(*_ptr, 0x00, size);
        return 0;
err_mem:
        return ret;
}

void *mini_hashtb_meminit(size_t size)
{
        void *ptr;
        if (ymalloc(&ptr, size))
                return NULL;
        return ptr;
}

size_t mini_hashtb_getidx(mini_hashtb_t *hashtb, void *data)
{
        return hashtb->hash(data) & (hashtb->size-1);
}

void mini_hashtb_lockidx(mini_hashtb_t *hashtb, size_t idx)
{
        sy_spin_lock(&hashtb->lock[idx]);
}
void mini_hashtb_unlockidx(mini_hashtb_t *hashtb, size_t idx)
{
        sy_spin_unlock(&hashtb->lock[idx]);
}
int mini_hashtb_insertidx(mini_hashtb_t *hashtb, mini_ent_t *ent,size_t idx)
{
        if (hashtb->ents == hashtb->maxents) 
                return ENOMEM;

        if ( hashtb->array[idx] ) {
                ent->next = hashtb->array[idx];
                hashtb->array[idx] = ent;
        }
        else {
                hashtb->array[idx] = ent;
                ent->next = NULL;
        }
        hashtb->ents++;
        return 0;
}

int mini_hashtb_deleteidx(mini_hashtb_t *hashtb, mini_ent_t *ent, size_t idx)
{
        mini_ent_t *sib;
        int i = 0;
        hashtb->opnum++;

        sib = hashtb->array[idx];
        if ( sib ) {
                if (hashtb->cmp(sib->data, ent->data) == 0) {
                        hashtb->array[idx] =  sib->next;
                        hashtb->ents--;
                } else {
                        while( sib->next ) {
                                if (hashtb->cmp(sib->next->data, ent->data)==0) {
                                        sib->next = sib->next->next;
                                        hashtb->ents--;
                                        goto out;
                                }
                                i++;
                                if (i > 102400) {
                                        DINFO("mini_hashtb colison %d\n", i);
                                }
                                hashtb->colision++;
                        }
                }
        } 
        return ENOENT;
out:
        return 0;
}
void mini_hashtb_findidx(mini_hashtb_t *hashtb, void *data, mini_ent_t **retval, size_t idx)
{
        struct mini_ent_s *sib;
        hashtb->opnum++;
        int i = 0;

        sib = hashtb->array[idx];
        while(sib && hashtb->cmp(sib->data, data)) {
                sib = sib->next;
                i++;
                hashtb->colision++;
        }
        (*retval) = sib;
        if (i > 102400) {
                DINFO("mini_hashtb colison %d\n", i);
        }
}


int mini_hashtb_insert(mini_hashtb_t *hashtb, mini_ent_t *ent)
{
        size_t key;

        if (hashtb->ents == hashtb->maxents) 
                return ENOMEM;
        key = hashtb->hash(ent->data) & (hashtb->size-1);
        sy_spin_lock(&hashtb->lock[key]);
        if ( hashtb->array[key] ) {
                ent->next = hashtb->array[key];
                hashtb->array[key] = ent;
        }
        else {
                hashtb->array[key] = ent;
                ent->next = NULL;
        }
        sy_spin_unlock(&hashtb->lock[key]);
        hashtb->ents++;
        return 0;
}
int mini_hashtb_delete(mini_hashtb_t *hashtb, mini_ent_t *ent)
{
        mini_ent_t *sib;
        size_t idx;
        int i = 0;

        hashtb->opnum++;
        idx =  hashtb->hash(ent->data) & (hashtb->size-1);
        sy_spin_lock(&hashtb->lock[idx]);
        sib = hashtb->array[idx];
        if ( sib ) {
                if (sib == ent) {
                        hashtb->array[idx] =  sib->next;
                        hashtb->ents--;
                } else {
                        while( sib->next ) {
                                if (sib == ent) {
                                        sib->next = sib->next->next;
                                        hashtb->ents--;
                                        goto out;
                                        i++;
                                }
                                if (i > 102400) {
                                        DINFO("mini_hashtb colison %d\n", i);
                                }
                                hashtb->colision++;
                        }
                }
        } 
        sy_spin_unlock(&hashtb->lock[idx]);
        return ENOENT;
out:
        sy_spin_unlock(&hashtb->lock[idx]);
        return 0;
}
int mini_hashtb_delete_raw(mini_hashtb_t *hashtb, mini_ent_t *ent, mini_ent_t **retval)
{
        mini_ent_t *sib;
        size_t idx;
        int i;

        hashtb->opnum++;
        idx =  hashtb->hash(ent->data) & (hashtb->size-1);
        sy_spin_lock(&hashtb->lock[idx]);
        sib = hashtb->array[idx];
        if ( sib ) {
                if (hashtb->cmp(sib->data, ent->data) == 0) {
                        hashtb->array[idx] =  sib->next;
                        hashtb->ents--;
                } else {
                        while( sib->next ) {
                                if (hashtb->cmp(sib->next->data, ent->data)==0) {
                                        (*retval) = sib->next;
                                        sib->next = sib->next->next;
                                        hashtb->ents--;
                                        goto out;
                                        i++;
                                        if (i > 102400) {
                                                DINFO("mini_hashtb colison %d\n", i);
                                        }
                                }
                                hashtb->colision++;
                        }
                }
        } 
        sy_spin_unlock(&hashtb->lock[idx]);
        (*retval) = NULL;
        return ENOENT;
out:
        sy_spin_unlock(&hashtb->lock[idx]);
        return 0;
}

void mini_hashtb_find(mini_hashtb_t *hashtb, void *data, mini_ent_t **retval)
{
        struct mini_ent_s *sib;
        size_t key;
        int i = 0;
        hashtb->opnum++;
        key =  hashtb->hash(data) & (hashtb->size-1);
        sy_spin_lock(&hashtb->lock[key]);
        sib = hashtb->array[hashtb->hash(data) & (hashtb->size-1)];
        while(sib && hashtb->cmp(sib->data, data)) {
                sib = sib->next;
                hashtb->colision++;
                i++;
        }
        (*retval) = sib;
        if (i > 102400) {
                DINFO("mini_hashtb colison %d\n", i);
        }
        sy_spin_unlock(&hashtb->lock[key]);
}

int mini_hashtb_free(mini_hashtb_t *hashtb, mini_ent_t *ent)
{
        (void)hashtb;
        (void)ent;
        return 0;
}
int mini_hashtb_init(mini_hashtb_t **hashtb, int (*cmp)(void *, void *), size_t hash(void *), 
                     size_t size,size_t maxent, void *mem)
{
        size_t i;
        if (!mem)
                return ENOMEM;
        (*hashtb) = mem;
        (*hashtb)->array   = (void**)((char*)(*hashtb) + sizeof(mini_hashtb_t));
        (*hashtb)->lock    = (sy_spinlock_t*)((char*)((*hashtb)->array) + sizeof(void*)*size); 

        for (i = 0; i < size; i++) {
                sy_spin_init((*hashtb)->lock+i);
        }
        (*hashtb)->size    = size;
        (*hashtb)->ins     = mini_hashtb_insert;
        (*hashtb)->del     = mini_hashtb_delete;
        (*hashtb)->find    = mini_hashtb_find;

        (*hashtb)->ins_idx  = mini_hashtb_insertidx;
        (*hashtb)->del_idx  = mini_hashtb_deleteidx;
        (*hashtb)->del_raw  = mini_hashtb_delete_raw;
        (*hashtb)->find_idx = mini_hashtb_findidx;

        (*hashtb)->getidx  = mini_hashtb_getidx;
        (*hashtb)->lockidx = mini_hashtb_lockidx;
        (*hashtb)->unlockidx = mini_hashtb_unlockidx;
        (*hashtb)->cmp     = cmp;
        (*hashtb)->hash    = hash;
        (*hashtb)->maxents  = maxent;
        (*hashtb)->ents    = 0;
        (*hashtb)->colision = 0;
        (*hashtb)->opnum   = 0;
        /*besure size = 2^m
         */
        if((((size_t)(-1)) % size)  != (((size_t)(-1)) & (size-1)))
                return EINVAL;
        return 0;
}
#ifdef TEST_MINI_HASHTB
#include <string.h>
int main()
{
        char* str[6] = {"aaaaaa","bbbbbb","eeeecccc","ffffff","ggggdssdfs","hhhhhhhh"};
        struct fidsimp_s parent[6] = { {0,1,1},{2,1,2}, {2,6,8}, {1,7,9}, {4,7,9}, {5,6,8} };

        int i,j,ret;
        mini_hashtb_t *hashtb;
        mini_ent_t *ent;
        pfid2cfid_t *entry;
        mini_ent_t *retval;

        ret = mini_hashtb_init(&hashtb, mini_hashtb_pfidnmcmp, hash_pfid2cfid, 1024, 2048,
                         mini_hashtb_meminit(1024*sizeof(void*) + sizeof(mini_hashtb_t)));
        if (ret)
                printf("mini_hashtb_init error\n");
        for(i=0; i <= 1000; i++) {
                ent = malloc(sizeof(mini_ent_t) + sizeof(pfid2cfid_t)+ 1+strlen(str[i%6]));
                entry = ent->data; 
                entry->fid.id = random() % 1000;
                entry->fid.version = random() % 1000;
                entry->fid.volid   = 0;
                entry->parent = &parent[i % 6];
                if (entry->parent == NULL)
                        printf("error\n");
                memcpy(entry->name, str[i % 6], strlen(str[i%6]));
                entry->name[strlen(str[i%6])] = '\0';
                ret = hashtb->ins(hashtb, ent);
                if (ret)
                        printf("%s\n", strerror(ret));
                mini_hashtb_find(hashtb, ent, &retval);
                if (retval != ent)
                        printf("some error\n");
                else {
                }
                ret =  mini_hashtb_delete(hashtb, ent);
                if (ret)
                        printf("some error\n");
                mini_hashtb_find(hashtb, ent, &retval);
                if (retval)
                        printf("delete full ? ");

        }
#if 0
        for(i=0; i <= 1000; i++) {
                ent = malloc(sizeof(mini_ent_t) + sizeof(pfid2cfid_t)+ 1+strlen(str[i%6]));
                entry = ent->data; 
                entry->fid.id = random() % 1000;
                entry->fid.version = random() % 1000;
                entry->fid.volid   = 0;
                entry->parent = &parent[i % 6];
                if (entry->parent == NULL)
                        printf("error\n");
                memcpy(entry->name, str[i % 6], strlen(str[i%6]));
                entry->name[strlen(str[i%6])] = '\0';
                ret = hashtb->ins(hashtb, ent);
                if (ret)
                        printf("%s\n", strerror(ret));
        }
#endif
        return 0;
}
#endif

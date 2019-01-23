/*
 * =====================================================================================
 *
 *       Filename:  mini_hashtb.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  03/10/2011 03:55:02 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */
#ifndef MINI_HASHTB_H_
#define MINI_HASHTB_H_
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>
#include "dbg.h"
#include "ylock.h"
#if 0
#include "sysutil.h"
#include "ylib.h"
#include "dbg.h"
#include "sdfs_id.h"
#endif
#if 0
typedef struct {
        uint64_t id;
        uint32_t version;
        uint32_t idx; /*chunk idx*/
        uint32_t mark;/*figer mark*/
        uint32_t volid;
} verid64_new_t;
#endif
#if 0
typedef struct fidsimp_s {
        uint64_t id;
        uint32_t version;
        uint32_t volid;
} fidsimp_t;

typedef struct pfid2cfid_s {
        /*can not work well in 32 plat 
         * */
        fidsimp_t *parent;
        fidsimp_t fid;
        char      name[0];
} pfid2cfid_t;
#endif

typedef struct mini_ent_s {
        struct mini_ent_s *next;
        char data[0];
} mini_ent_t;
typedef struct mini_hashtb_s {
        int  (*ins)(struct mini_hashtb_s *, mini_ent_t *ent);       
        int  (*del)(struct mini_hashtb_s *, mini_ent_t *ent);
        int  (*del_raw)(struct mini_hashtb_s *, mini_ent_t *, mini_ent_t **retval);

        int  (*cmp)(void *, void *);
        void (*find)(struct mini_hashtb_s *, void *, mini_ent_t **);
        int  (*des)(struct mini_hashtb_s *);
        size_t (*hash)(void *);
        size_t (*getidx)(struct mini_hashtb_s *, void *data);
        void (*lockidx)(struct mini_hashtb_s *, size_t idx);
        void (*unlockidx)(struct mini_hashtb_s *, size_t idx);

        int  (*ins_idx)(struct mini_hashtb_s *, mini_ent_t *ent, size_t idx);       
        int  (*del_idx)(struct mini_hashtb_s *, mini_ent_t *ent, size_t idx);
        void (*find_idx)(struct mini_hashtb_s *, void *, mini_ent_t **, size_t idx);

        size_t size;
        size_t maxents;
        size_t ents;
        uint32_t colision;
        uint32_t opnum;
        void **array;
	sy_spinlock_t *lock;
} mini_hashtb_t;

int mini_hashtb_init(mini_hashtb_t **hashtb, int (*cmp)(void *, void *), size_t hash(void *), 
                     size_t size,size_t maxent, void *mem);
int mini_hashtb_insert(mini_hashtb_t *hashtb, mini_ent_t *ent);
int mini_hashtb_delete(mini_hashtb_t *hashtb, mini_ent_t *ent);
int mini_hashtb_destory(mini_hashtb_t *hashtb);
#endif

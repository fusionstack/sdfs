/*
*Date   : 2017.09.13
*Author : JinagYang
*/
#ifndef __FLOCK_H__
#define __FLOCK_H__

typedef struct {
        uint8_t type; // shared(readlock), exclusive(writelock), remove(unlock)
        uint64_t sid; // which server (nfs, samba...) requests/holds the lock
        uint64_t owner; // who requests/holds the lock
        uint64_t start; // initial location to lock
        uint64_t length; // num bytes to lock from start
}uss_flock_t;

typedef enum {
        USS_RDLOCK,
        USS_WRLOCK,
        USS_UNLOCK,
}uss_flock_type_t;

typedef enum {
        USS_SETFLOCK,
        USS_GETFLOCK,
}uss_flock_op_t;

//remove locks info with server id which disconnect from mds
extern void flock_remove_sid_locks(IN const uint64_t sid);
extern int flock_mds_init(void);
extern int flock_mds_op(IN const fileid_t *fileid,
                        IN uss_flock_op_t flock_op,
                        INOUT uss_flock_t *plock);
extern void ussflock_to_flock(IN const uss_flock_t *uss_flock,
                              INOUT struct flock *flock);
extern void flock_to_ussflock(IN const struct flock *flock,
                              INOUT uss_flock_t *uss_flock);
#endif
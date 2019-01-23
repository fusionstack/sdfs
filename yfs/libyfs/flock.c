/*
*Date   : 2017.09.11
*Author : JinagYang
*flock.c : implement the file range byte lock feature for sdfs
*/
#include <pthread.h>

#define DBG_SUBSYS S_YFSLIB
#include "sdfs_id.h"
#include "hash_table.h"
#include "sdfs_lib.h"
#include "sdfs_conf.h"
#include "leveldb_util.h"
#include "flock.h"

typedef struct {
        fileid_t fileid;
        uint32_t locks_num;
        uss_flock_t *locks_data;
}uss_flock_table_t;

typedef struct {
        hashtable_t fhashtable;
        pthread_mutex_t lock;
}uss_flock_hashtable_t;

static uss_flock_hashtable_t g_uss_flock_hashtable;

static uint32_t __flock_hashkey(IN const void *fileid)
{
        fileid_t *fileid_tmp = (fileid_t *)fileid;
        return (fileid_tmp->id + fileid_tmp->volid);
}

static int __flock_hashcmp(IN const void *fileid1,
                           IN const void *fileid2)
{
        fileid_t *fileid_tmp1 = (fileid_t *)fileid1;
        fileid_t *fileid_tmp2 = (fileid_t *)fileid2;

        if ((fileid_tmp1->id == fileid_tmp2->id) &&
            (fileid_tmp1->volid == fileid_tmp2->volid))
                return 0;
        else
                return 1;
}

static int __flock_hashtable_init(void)
{
        int ret;

        ret = pthread_mutex_init(&g_uss_flock_hashtable.lock, NULL);
        if (ret)
                GOTO(err_ret, ret);

        g_uss_flock_hashtable.fhashtable = hash_create_table(__flock_hashcmp,
                                                             __flock_hashkey,
                                                             "uss_flock_hash");
        if (NULL == g_uss_flock_hashtable.fhashtable) {
                ret = ENOMEM;
                DERROR("ret (%d) %s\n", ret, strerror(ret));
                goto err_ret;
        }

        return 0;
err_ret:
        return ret;
}

// start + length <= UINT64_MAX
static uint64_t __flock_get_end(IN const uint64_t start, IN const uint64_t length)
{
        uint64_t end;

        //if length is 0, the file lock the whole file from start
        if (0 == length)
                end = UINT64_MAX;
        else
                end = start + length; // less than or equal UINT64_MAX

        return end;
}

static bool __flock_overlap(IN const uss_flock_t *flock1, IN const uss_flock_t *flock2)
{
        uint64_t start1 = flock1->start;
        uint64_t start2 = flock2->start;
        uint64_t end1;
        uint64_t end2;

        end1 = __flock_get_end(start1, flock1->length);
        end2 = __flock_get_end(start2, flock2->length);

        //no overlap [start1, end1] <= [start2, end2] or
        //           [start2, end2] <= [start1, end1]
        if (start1 >= end2 || start2 >= end1 )
                return false;
        else
                return true;
}

static bool __flock_same_user(IN uss_flock_t *flock1, IN uss_flock_t *flock2)
{
        return (flock1->sid == flock2->sid &&
                flock1->owner == flock2->owner);
}

static bool __flock_conflict(IN uss_flock_t *flock1, IN uss_flock_t *flock2)
{
        //Read locks never conflict.
        if (USS_RDLOCK == flock1->type && USS_RDLOCK == flock2->type)
                return false;

        //Locks on the same client user don't conflict
        if (__flock_same_user(flock1, flock2))
                return false;

        //Not all read locks, the client user is different, do they overlap
        return __flock_overlap(flock1, flock2);

}

/*
*file lock range splits and merges.
*  elock : existing lock
*  plock : proposed lock
*  lck_arr : output lock array
*/
static uint32_t __flock_split_merge(INOUT uss_flock_t *elock,
                                    INOUT uss_flock_t *plock,
                                    OUT uss_flock_t *lck_arr)
{
        bool lock_types_differ = true;
        uint64_t e_start = elock->start;
        uint64_t p_start = plock->start;
        uint64_t e_end;
        uint64_t p_end;

        // We can't merge non-conflicting locks on differet client user
        if (false == __flock_same_user(elock, plock)) {
                //Just copy.
                memcpy(&lck_arr[0], elock, sizeof(uss_flock_t));
                return 1;
        }

        if (elock->type == plock->type)
                lock_types_differ = false;

        e_end = __flock_get_end(e_start, elock->length);
        p_end = __flock_get_end(p_start, plock->length);

        /* We now know we have the same context. */
        /* Did we overlap ? */

/*********************************************
                                        +---------+
                                        | elock   |
                                        +---------+
                         +-------+
                         | plock |
                         +-------+
OR....
        +---------+
        |  elock  |
        +---------+
**********************************************/
        if (elock->start > p_end || plock->start > e_end) {
                // No overlap with this lock - copy existing.
                memcpy(&lck_arr[0], elock, sizeof(uss_flock_t));
                return 1;
        }

/*********************************************
        +---------------------------+
        |          elock            |
        +---------------------------+
        +---------------------------+
        |       plock               | -> replace with plock.
        +---------------------------+
OR
             +---------------+
             |     elock     |
             +---------------+
        +---------------------------+
        |       plock               | -> replace with plock.
        +---------------------------+

**********************************************/
        if (e_start >= p_start && e_end <= p_end) {
                /* Replace - discard existing lock. */
                return 0;
        }

/*********************************************
Adjacent after.
                        +-------+
                        |  elock|
                        +-------+
        +---------------+
        |   plock       |
        +---------------+

BECOMES....
        +---------------+-------+
        |   plock       | elock | - different lock types.
        +---------------+-------+
OR.... (merge)
        +-----------------------+
        |   plock               | - same lock type.
        +-----------------------+
**********************************************/
        if (p_end == e_start) {

                /* If the lock types are the same, we merge, if different, we
                   add the remainder of the old lock. */
                if (lock_types_differ) {
                        /* Add existing. */
                        memcpy(&lck_arr[0], elock, sizeof(uss_flock_t));
                        return 1;
                } else {
                        /* Merge - adjust incoming lock as we may have more
                         * merging to come. */
                        plock->length += elock->length;
                        return 0;
                }
        }

/*********************************************
Adjacent before.
        +-------+
        |  elock|
        +-------+
                +---------------+
                |   plock       |
                +---------------+
BECOMES....
        +-------+---------------+
        | elock |   plock       | - different lock types
        +-------+---------------+

OR.... (merge)
        +-----------------------+
        |      plock            | - same lock type.
        +-----------------------+

**********************************************/
        if (e_end == p_start) {

                /* If the lock types are the same, we merge, if different, we
                   add the existing lock. */
                if (lock_types_differ) {
                        memcpy(&lck_arr[0], elock, sizeof(uss_flock_t));
                        return 1;
                } else {
                        /* Merge - adjust incoming lock as we may have more
                         * merging to come. */
                        plock->start = e_start;
                        plock->length += elock->length;
                        return 0;
                }
        }

/*********************************************
Overlap after.
        +-----------------------+
        |          elock        |
        +-----------------------+
        +---------------+
        |   plock       |
        +---------------+
OR
               +----------------+
               |       elock    |
               +----------------+
        +---------------+
        |   plock       |
        +---------------+

BECOMES....
        +---------------+-------+
        |   plock       | elock | - different lock types.
        +---------------+-------+
OR.... (merge)
        +-----------------------+
        |   plock               | - same lock type.
        +-----------------------+
**********************************************/
        if (e_start >= p_start &&
            e_start <= p_end &&
            e_end > p_end) {

                /* If the lock types are the same, we merge, if different, we
                   add the remainder of the old lock. */
                if (lock_types_differ) {
                        /* Add remaining existing. */
                        memcpy(&lck_arr[0], elock, sizeof(uss_flock_t));
                        /* Adjust existing start and size. */
                        lck_arr[0].start = p_end;
                        lck_arr[0].length = e_end - p_end;
                        return 1;
                } else {
                        /* Merge - adjust incoming lock as we may have more
                         * merging to come. */
                        plock->length += e_end - p_end;
                        return 0;
                }
        }

/*********************************************
Overlap before.
        +-----------------------+
        |  elock                |
        +-----------------------+
                +---------------+
                |   plock       |
                +---------------+
OR
        +-------------+
        |  elock      |
        +-------------+
                +---------------+
                |   plock       |
                +---------------+

BECOMES....
        +-------+---------------+
        | elock |     plock     | - different lock types
        +-------+---------------+

OR.... (merge)
        +-----------------------+
        |      plock            | - same lock type.
        +-----------------------+

**********************************************/
        if (e_start < p_start &&
            e_end >=  p_start &&
            e_end <= p_end) {

                /* If the lock types are the same, we merge, if different, we
                   add the truncated old lock. */
                if (lock_types_differ) {
                        memcpy(&lck_arr[0], elock, sizeof(uss_flock_t));
                        /* Adjust existing size. */
                        lck_arr[0].length = p_start - e_start;
                        return 1;
                } else {
                        /* Merge - adjust incoming lock as we may have more
                         * merging to come. MUST ADJUST plock SIZE FIRST ! */
                        plock->length += p_start - e_start;
                        return 0;
                }
        }

/*********************************************
Complete overlap.
        +---------------------------+
        |        elock              |
        +---------------------------+
                +---------+
                |  plock  |
                +---------+
BECOMES.....
        +-------+---------+---------+
        | elock |  plock  | elock   | - different lock types.
        +-------+---------+---------+
OR
        +---------------------------+
        |        plock              | - same lock type.
        +---------------------------+
**********************************************/

        if (e_start < p_start && e_end > p_end) {

                if (lock_types_differ) {
                        /* We have to split elock into two locks here. */
                        memcpy(&lck_arr[0], elock, sizeof(uss_flock_t));
                        memcpy(&lck_arr[1], elock, sizeof(uss_flock_t));

                        /* Adjust first existing size. */
                        lck_arr[0].length = p_start - e_start;

                        /* Adjust second existing start and size. */
                        lck_arr[1].start = p_end;
                        lck_arr[1].length = e_end - p_end;
                        return 2;
                } else {
                        /* Just eat the existing locks, merge them into plock. */
                        plock->start = e_start;
                        plock->length = elock->length;
                        return 0;
                }
        }

        return 0;
}

static int __flock_lock(INOUT uss_flock_t *plock,
                        INOUT uss_flock_table_t *ftable)
{
        uint32_t i;
        int ret = 0;
        uss_flock_t *locks = ftable->locks_data;
        uss_flock_t *locks_arr = NULL;
        uss_flock_t *curr_lock = NULL;
        size_t flock_size = sizeof(uss_flock_t);
        uint32_t locks_num = ftable->locks_num;
        uint32_t count;
        uint32_t count_tmp;

        /* The worst case scenario here is we have to split an
	   existing POSIX lock range into two, and add our lock,
	   so we need at most 2 more entries. */
	ret = ymalloc((void **)&locks_arr, flock_size * (locks_num  + 2));
        if (ret)
                GOTO(err_assert, ret);

        count = 0;
        for (i = 0; i < locks_num; i++) {
                curr_lock = &locks[i];
                if (__flock_conflict(curr_lock, plock)) {
                        ret = EAGAIN;
                        DWARN("\n");
                        yfree((void **)&locks_arr);
                        goto err_ret;
                }

                //Work out overlaps
                count_tmp = __flock_split_merge(curr_lock, plock, &locks_arr[count]);
                count += count_tmp;
        }

        // Try and add the lock in order, sorted by lock start.
        for (i = 0; i < count; i++) {
                curr_lock = &locks_arr[i];
                if (curr_lock->start > plock->start) {
                        break;
                }
        }

        if (i < count) {
                memmove(&locks_arr[i + 1], &locks_arr[i],
                        (count - i) * flock_size);
        }
        memcpy(&locks_arr[i], plock, flock_size);
        count++;

        /* If we didn't use all the allocated size,
         * Realloc so we don't leak entries per lock call. */
        if (count < locks_num + 2) {
                locks_arr = realloc(locks_arr, count * flock_size);
                if (NULL == locks_arr) {
                        ret = errno;
                        GOTO(err_assert, ret);
                }
        }

        ftable->locks_num = count;
        yfree((void **)&ftable->locks_data);
        ftable->locks_data = locks_arr;

        return 0;

err_assert:
        YASSERT(0);
err_ret:
        return ret;
}

static int __flock_unlock(INOUT uss_flock_t *plock,
                          INOUT uss_flock_table_t *ftable)
{
        uint32_t i;
        int ret = 0;
        uss_flock_t *locks = ftable->locks_data;
        uss_flock_t *locks_arr = NULL;
        uss_flock_t *curr_lock = NULL;
        size_t flock_size = sizeof(uss_flock_t);
        uint32_t locks_num = ftable->locks_num;
        uint32_t count;
        uint32_t count_tmp;
        bool overlap_found = false;

        /* The worst case scenario here is we have to split an
	   existing POSIX lock range into two, and add our lock,
	   so we need at most 1 more entries. */
	ret = ymalloc((void **)&locks_arr, flock_size * (locks_num  + 1));
        if (ret)
                GOTO(err_assert, ret);

        count = 0;
        for (i = 0; i < locks_num; i++) {
                curr_lock = &locks[i];

                //Work out overlaps
                count_tmp = __flock_split_merge(curr_lock, plock, &locks_arr[count]);
                if (0 == count_tmp) {
                        /* plock overlapped the existing lock completely,
			   or replaced it. Don't copy the existing lock. */
                        overlap_found = true;
                }else if (1 == count_tmp) {

                        /* Either no overlap, (simple copy of existing lock) or
			 * an overlap of an existing lock. */
			/* If the lock changed size, we had an overlap. */
                        if (locks_arr[count].length != curr_lock->length)
                                overlap_found = true;
                        count += count_tmp;
                }else if (2 == count_tmp) {
                        overlap_found = true;
                        count += count_tmp;
                        if (i < locks_num - 1) {
                                memcpy(&locks_arr[count], &locks[i + 1],
                                       flock_size * (locks_num - 1 - i));
                                count += locks_num - 1 - i;
                        }

                        break;
                }
        }

        // Just ignore - no change.
        if (false == overlap_found) {
                yfree((void **)&locks_arr);
                goto out;
        }

        // Realloc so we don't leak entries per unlock call.
        if (count) {
                locks_arr = realloc(locks_arr, count * flock_size);
                if (NULL == locks_arr) {
                        ret = errno;
                        GOTO(err_assert, ret);
                }
        } else {
                // We deleted the last lock.
                yfree((void **)&locks_arr);
                locks_arr = NULL;
        }
        ftable->locks_num = count;
        yfree((void **)&ftable->locks_data);
        ftable->locks_data = locks_arr;

out:
        return 0;

err_assert:
        YASSERT(0);
        return ret;
}

static int __flock_leveldb_put(IN const uss_flock_table_t *ftable)
{
        int ret = 0;
        char key[MAX_NAME_LEN];
        int keylen = sizeof(key);
        uss_flock_t *locks = ftable->locks_data;
        int len = ftable->locks_num * sizeof(uss_flock_t);

        uss_leveldb_fileid_encodekey(&ftable->fileid, NULL, key, &keylen);
        ret = uss_infodb_put(FLOCK_DB, key, keylen, (char *)locks,
                             len, VTYPE_FLOCKINFO);
        YASSERT(0 == ret);

        return ret;
}

static int __flock_leveldb_get(IN const fileid_t *fileid,
                               INOUT int *buf_len,
                               OUT char *locks_buf)
{
        int ret = 0;
        char key[MAX_NAME_LEN];
        int keylen = sizeof(key);

        uss_leveldb_fileid_encodekey(fileid, NULL, key, &keylen);
        ret = uss_infodb_get(FLOCK_DB, key, keylen, locks_buf,
                             buf_len, VTYPE_FLOCKINFO);

        if (ret && ENOENT !=  ret) {
                DERROR("get locks info of fileid:"
                        ID_VID_FORMAT" failed, error is %s.\n ",
                        ID_VID_ARG(fileid), strerror(ret));
                YASSERT(0);
        }

        return ret;
}

static int __flock_leveldb_remove(IN const fileid_t *fileid)
{
        int ret = 0;
        char key[MAX_NAME_LEN];
        int keylen = sizeof(key);

        uss_leveldb_fileid_encodekey(fileid, NULL, key, &keylen);
        ret = uss_infodb_remove(FLOCK_DB, key, keylen);
        if (ret)
                DERROR("remove locks info of fileid:"
                        ID_VID_FORMAT" failed, error is %s.\n ",
                        ID_VID_ARG(fileid), strerror(ret));

        return ret;
}

static int __flock_create_ftable(IN const fileid_t *fileid,
                                 IN const uss_flock_t *locks,
                                 IN const uint32_t locks_num,
                                 OUT uss_flock_table_t **ftable)
{
        int ret;
        size_t size = locks_num * sizeof(uss_flock_t);
        uss_flock_table_t *ftable_tmp = NULL;

        ret = ymalloc((void **)&ftable_tmp, sizeof(uss_flock_table_t));
        if (ret)
                GOTO(err_ret, ret);
        ret = ymalloc((void **)&ftable_tmp->locks_data, size);
        if (ret)
                GOTO(err_free, ret);
        memcpy(ftable_tmp->locks_data, locks, size);
        ftable_tmp->locks_num = locks_num;
        ftable_tmp->fileid = *fileid;

        *ftable = ftable_tmp;

        return 0;

err_free:
        yfree((void **)&ftable_tmp);
err_ret:
        return ret;
}

static uss_flock_table_t *__flock_get_ftable(IN const fileid_t *fileid)
{
        int ret = 0;
        char locks_buf[MAX_BUF_LEN];
        int buf_len = sizeof(locks_buf);
        uss_flock_table_t *ftable = NULL;
        uint32_t locks_num = 0;

        ftable = hash_table_find(g_uss_flock_hashtable.fhashtable, (void *)fileid);
        if (NULL != ftable)
                return ftable;

        ret = __flock_leveldb_get(fileid, &buf_len, locks_buf);
        if (ret)
                return NULL;

        locks_num = buf_len / sizeof(uss_flock_t);
        ret = __flock_create_ftable(fileid, (uss_flock_t *)locks_buf, locks_num, &ftable);
        if (0 == ret)
                ret = hash_table_insert(g_uss_flock_hashtable.fhashtable,
                                        (void *)ftable, (void *)fileid, 0);
        YASSERT(0 == ret);

        return ftable;
}

static int __flock_setlock(IN const fileid_t *fileid, INOUT uss_flock_t *plock)
{
        int ret = 0;
        uss_flock_table_t *p_table = NULL; //proposed flock table

        if (UINT64_MAX - plock->start < plock->length) {
                ret = EINVAL;
                DERROR("flock range is larger than UINT64_MAX "
                       "filied:"ID_VID_FORMAT
                       "type: %u, sid:%llu, owner:%llu,"
                       "start:%llu, length:%llu\n",
                       ID_VID_ARG(fileid), plock->type, (LLU)plock->sid,
                       (LLU)plock->owner, (LLU)plock->start, (LLU)plock->length);
                goto err_ret;
        }

        ret = pthread_mutex_lock(&g_uss_flock_hashtable.lock);
        if (ret)
                GOTO(err_assert, ret);

        p_table = __flock_get_ftable(fileid);
        //non file lock of the fileid
        if (NULL == p_table) {
                if (USS_UNLOCK == plock->type)
                        goto out;

                ret = __flock_create_ftable(fileid, plock, 1, &p_table);
                if (ret)
                        GOTO(err_assert, ret);

                ret = hash_table_insert(g_uss_flock_hashtable.fhashtable,
                                        (void *)p_table, (void *)fileid, 0);
                if (ret)
                        GOTO(err_assert, ret);
        } else {
                if (USS_UNLOCK != plock->type) {
                        ret = __flock_lock(plock, p_table);
                        if (ret)
                                goto err_ret;
                } else {
                        ret = __flock_unlock(plock, p_table);
                        if (ret)
                                goto err_ret;
                }
        }

        if (0 == p_table->locks_num) {
                // remove lock info of fileid from cache and leveldb
                hash_table_remove(g_uss_flock_hashtable.fhashtable,
                                  (void *)fileid, NULL);
                __flock_leveldb_remove(fileid);
        } else {
                //update locks info of the fileid for leveldb
                __flock_leveldb_put(p_table);
        }

out:
        pthread_mutex_unlock(&g_uss_flock_hashtable.lock);
        return 0;
err_assert:
        YASSERT(0);
err_ret:
        pthread_mutex_unlock(&g_uss_flock_hashtable.lock);
        return ret;
}

/*
*If the lock could be placed, does not actually place it, but returns USS_UNLOCK
*in the type field of lock and leaves the other fields of the structure unchanged.
*If one or more incompatible locks would prevent this lock being placed,
*then returns details about one of these locks.
*/
static int __flock_getlock(IN const fileid_t *fileid, INOUT uss_flock_t *plock)
{
        uint32_t i;
        int ret = 0;
        uss_flock_table_t *p_table = NULL; //proposed flock table
        uss_flock_t *locks;

        if (UINT64_MAX - plock->start < plock->length) {
                ret = EINVAL;
                DERROR("flock range is larger than UINT64_MAX "
                       "filied:"ID_VID_FORMAT
                       "type: %u, sid:%llu, owner:%llu,"
                       "start:%llu, length:%llu\n",
                       ID_VID_ARG(fileid), plock->type, (LLU)plock->sid,
                       (LLU)plock->owner, (LLU)plock->start, (LLU)plock->length);
                return ret;
        }

        ret = pthread_mutex_lock(&g_uss_flock_hashtable.lock);
        YASSERT(0 == ret);

        p_table = __flock_get_ftable(fileid);
        if (NULL == p_table) {
                //non file lock of the fileid
                plock->type = USS_UNLOCK;
        } else {
                locks = p_table->locks_data;
                for (i = 0; i < p_table->locks_num; i++) {
                        if (__flock_conflict(&locks[i], plock))
                                break;
                }

                if (i == p_table->locks_num)
                        plock->type = USS_UNLOCK;
                else
                        memcpy(plock, &locks[i], sizeof(uss_flock_t));
        }

        pthread_mutex_unlock(&g_uss_flock_hashtable.lock);
        return ret;
}

static int __flock_remove_ftable_sid_locks(void *sid, void *flock_table)
{
        int ret;
        uint32_t i;
        uint64_t sid_tmp = *((uint64_t *)sid);
        uss_flock_table_t *ftable = (uss_flock_table_t *)flock_table;
        uss_flock_t *locks = ftable->locks_data;
        uss_flock_t *locks_arr = NULL;
        uint32_t locks_num = ftable->locks_num;
        uint32_t count;
        int not_remove_ftable = 1;
        size_t flock_size = sizeof(uss_flock_t);

        ret = ymalloc((void **)&locks_arr, locks_num * flock_size);
        YASSERT(0 == ret);

        count = 0;
        for (i = 0; i < locks_num; i++) {
                if (sid_tmp == locks[i].sid) {
                        memcpy(&locks_arr[count], &locks[i], flock_size);
                        count++;
                }
        }

        // Realloc so we don't leak entries .
        if (count) {
                locks_arr = realloc(locks_arr, count * flock_size);
                YASSERT(NULL != locks_arr);
        } else {
                // We deleted the last lock.
                yfree((void **)&locks_arr);
                locks_arr = NULL;
        }
        ftable->locks_num = count;
        yfree((void **)&ftable->locks_data);
        ftable->locks_data = locks_arr;

        if (count) {
                //update locks info of the fileid for leveldb
                __flock_leveldb_put(ftable);
        } else {
                // remove lock info of fileid from cache and leveldb
                __flock_leveldb_remove(&ftable->fileid);
                not_remove_ftable = 0;
        }

        return not_remove_ftable;
}

static int __flock_getall_locks_from_leveldb(void)
{
        int ret = 0;
        size_t iter_keylen = 0;
        const char *iter_key = NULL;
        size_t iter_valuelen = 0;
        const char *iter_value = NULL;
        leveldb_iterator_t *iter;
        leveldb_readoptions_t *roptions;
        uss_leveldb_t *flockdb = NULL;
        uint32_t locks_num = 0;
        uss_flock_table_t *ftable = NULL;
        fileid_t fileid;
        const value_t *val = NULL;

        flockdb = uss_leveldb_get_infodb(FLOCK_DB);
        if (NULL == flockdb) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        roptions = uss_leveldb_readoptions_create(0);
        iter = leveldb_create_iterator(flockdb->db, roptions);
        if (iter == NULL) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }
        leveldb_readoptions_destroy(roptions);

        leveldb_iter_seek_to_first(iter);
        while (leveldb_iter_valid(iter)) {
                iter_key = leveldb_iter_key(iter, &iter_keylen);
                uss_leveldb_decode_fileid(iter_key, &fileid);

                iter_value = leveldb_iter_value(iter, &iter_valuelen);
                locks_num = (iter_valuelen - sizeof(value_t)) / sizeof(uss_flock_t);

                val = (void*)iter_value;
                ret = __flock_create_ftable(&fileid, (uss_flock_t *)val->buf,
                                            locks_num, &ftable);
                if (0 == ret)
                        ret = hash_table_insert(g_uss_flock_hashtable.fhashtable,
                                                (void *)ftable, (void *)&fileid, 0);
                YASSERT(0 == ret);
                leveldb_iter_next(iter);
        }

        leveldb_iter_destroy(iter);

        return 0;
err_ret:
        return ret;
}

//remove locks info with server id which disconnect from mds
void flock_remove_sid_locks(IN const uint64_t sid)
{
        int ret = 0;

        ret = pthread_mutex_lock(&g_uss_flock_hashtable.lock);
        YASSERT(0 == ret);

        hash_filter_table_entries(g_uss_flock_hashtable.fhashtable,
                                  __flock_remove_ftable_sid_locks,
                                  (void *)&sid, NULL);

        pthread_mutex_unlock(&g_uss_flock_hashtable.lock);
}

int flock_mds_init(void)
{
        int ret = 0;

        ret = __flock_hashtable_init();
        if (ret)
                GOTO(err_ret, ret);

        ret = __flock_getall_locks_from_leveldb();
        if (ret)
                GOTO(err_ret, ret);

        return 0;

err_ret:
        return ret;
}

int flock_mds_op(IN const fileid_t *fileid,
                 IN uss_flock_op_t flock_op,
                 INOUT uss_flock_t *plock)
{
        int ret = 0;

        if (USS_SETFLOCK == flock_op)
                ret = __flock_setlock(fileid, plock);
        else if (USS_GETFLOCK == flock_op)
                ret = __flock_getlock(fileid, plock);
        else
                ret = EINVAL;

        if(ret) {
                DERROR("flock op failed, error is %s, op type is %u,"
                       "filied:"ID_VID_FORMAT
                       ",type: %u, sid:%llu, owner:%llu,"
                       "start:%llu, length:%llu\n",
                       strerror(ret), flock_op,
                       ID_VID_ARG(fileid), plock->type, (LLU)plock->sid,
                       (LLU)plock->owner, (LLU)plock->start, (LLU)plock->length);
        }

        return ret;
}

void ussflock_to_flock(IN const uss_flock_t *uss_flock,
                       INOUT struct flock *flock)
{
        flock->l_type   = uss_flock->type;
        flock->l_whence = SEEK_SET;
        flock->l_start  = uss_flock->start;
        flock->l_len    = uss_flock->length;
}

void flock_to_ussflock(IN const struct flock *flock,
                       INOUT uss_flock_t *uss_flock)
{
        uss_flock->type = flock->l_type;
        uss_flock->start = flock->l_start;
        uss_flock->length = flock->l_len;
}

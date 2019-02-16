#include <sys/types.h>
#include <string.h>
#include <stdint.h>
#include <dirent.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSMDS

#include "dir.h"
#include "net_global.h"
#include "redis.h"
#include "redis_pipeline.h"
#include "md.h"
#include "md_db.h"
#include "dbg.h"

static int dir_lookup(const volid_t *volid, const fileid_t *parent, const char *name, fileid_t *fid, uint32_t *type) {
        int ret;
        dir_entry_t *ent;
        char buf[sizeof(dir_entry_t)];
        size_t buflen;

        //DWARN("--------------pipeline test--------------------\n");
        ret = hget(volid, parent, name, buf, &buflen);
        if (ret)
                GOTO(err_ret, ret);

        if (buflen != (int)sizeof(dir_entry_t)) {
                ret = EIO;
                GOTO(err_ret, ret);
        }

        ent = (dir_entry_t *)buf;
        *fid = ent->fileid;
        *type = ent->d_type;

        return 0;
err_ret:
        return ret;
}

static int dir_newrec(const volid_t *volid, const fileid_t *parent, const char *name,
                      const fileid_t *fileid, uint32_t type, int flag)
{
        dir_entry_t ent;
        int ret;
        uint64_t count;

        ANALYSIS_BEGIN(0);
        
        DBUG(""FID_FORMAT"/"FID_FORMAT" name %s\n", FID_ARG(parent), FID_ARG(fileid), name);

        ret = hlen(volid, parent, &count);
        if (ret)
                GOTO(err_ret, ret);

        if (count > MAX_SUB_FILES) {
                ret = EPERM;
                DWARN("limt max sub files %llu", (LLU)MAX_SUB_FILES);
                GOTO(err_ret, ret);
        }

        ent.fileid = *fileid;
        ent.d_type = type;

        ret = hset(volid, parent, name, &ent, sizeof(dir_entry_t), flag);
        if (ret) {
                DBUG(""FID_FORMAT" / "FID_FORMAT" name %s\n", FID_ARG(parent), FID_ARG(fileid), name);
                GOTO(err_ret, ret);
        }

        ANALYSIS_QUEUE(0, IO_WARN, NULL);
        
        return 0;
err_ret:
        return ret;
}

static int dir_unlink(const volid_t *volid, const fileid_t *parent, const char *name)
{
        int ret;

        ret = hdel(volid, parent, name);
        if (ret)
                GOTO(err_ret, ret);
        
        return 0;
err_ret:
        return ret;
}

static int __readdir(const volid_t *volid, const fileid_t *fid, void *buf, int *_buflen,
                     uint64_t _offset, const filter_t *filter, int is_plus)
{
        int ret, reclen, buflen;
        redisReply *reply, *e0, *e1, *k1, *v1;
        dir_entry_t *ent;
        md_proto_t *md1;
        //uint32_t begin, b;
        uint64_t next;
        uint32_t i;
        struct dirent *curr;
        uint64_t offset, count;

        offset = filter ? filter->offset : _offset;
        count = filter ? filter->count : (uint64_t)-1;

retry:
        buflen = *_buflen;
        reply = hscan(volid, fid, NULL, offset, count);
        if (reply == NULL || reply->type != REDIS_REPLY_ARRAY) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        e0 = reply->element[0];
        e1 = reply->element[1];

        next = atol(e0->str);

        DBUG("req:(offset %ju count %jd), bulen %u, rep:(count %ju, next %ju)\n",
              offset, count, buflen, e1->elements, next);

        curr = (void *)buf;
        for (i = 0; i < (uint32_t)e1->elements; i += 2) {
                // name + value
                k1 = e1->element[i];
                if (strncmp(k1->str, SDFS_MD_SYSTEM, strlen(SDFS_MD_SYSTEM)) == 0) {
                        continue;
                }

                v1 = e1->element[i+1];
                ent = (dir_entry_t *)v1->str;
                        
                reclen = sizeof(*curr) + strlen(k1->str) + 1 - sizeof(curr->d_name);
                if ((void *)curr + reclen + (is_plus ? sizeof(md_proto_t) : 0) - buf > buflen
                    || i / 2 > MAX_READDIR_ENTRIES) {
                        DBUG("parent "FID_FORMAT" reclen %d name %s d_type %d i %d,"
                             " used %u, need %u\n",  FID_ARG(fid), reclen,
                             k1->str, ent->d_type, i, (int)((void *)curr - buf),
                             (reclen + is_plus ? sizeof(md_proto_t) : 0 ));

                        freeReplyObject(reply);
                        count = i / 2 - 2;
                        goto retry;
                }
        
                //DBUG("parent "FID_FORMAT" name %s len %d\n", FID_ARG(fid), k1->str, (int)k1->len);
                DBUG("parent "FID_FORMAT" reclen %d name %s d_type %d i %d, left %u, need %u\n", 
                      FID_ARG(fid), reclen, k1->str, ent->d_type, i,
                      MAX_BUF_LEN - (int)((void *)curr - buf),
                      reclen + (is_plus ? sizeof(md_proto_t) : 0 ));
                        
                // TODO
                curr->d_reclen = reclen;
                curr->d_type = ent->d_type;
                curr->d_ino = 0;
                curr->d_off = next;//porint to next
                strncpy(curr->d_name, k1->str, k1->len);
                curr->d_name[k1->len] = '\0';

                DBUG("%s offset %ju\n", curr->d_name, curr->d_off);
                
                if (is_plus) {
                        md1 = (void*)curr + curr->d_reclen;
                        md1->fileid = ent->fileid;
                        curr->d_reclen += sizeof(md_proto_t);
                }

                YASSERT((void *)curr + curr->d_reclen - buf <= buflen);
                curr = (void *)curr + curr->d_reclen;
        }

        freeReplyObject(reply);

        *_buflen = (void *)curr - buf;

        return 0;
err_ret:
        freeReplyObject(reply);
        return ret;
}

static int dir_readdir(const volid_t *volid, const fileid_t *fileid, void *buf, int *buflen,
                       uint64_t offset)
{
        return __readdir(volid, fileid, buf, buflen, offset, NULL, 0);
}

static int dir_readdirplus(const volid_t *volid, const fileid_t *fid, void *buf, int *buflen,
                           uint64_t offset)
{
        return __readdir(volid, fid, buf, buflen, offset, NULL, 1);
}

static int __readdirplus_filter(const volid_t *volid, const fileid_t *fid, void *buf, int *buflen,
                                uint64_t offset, const filter_t *filter)
{
        return __readdir(volid, fid, buf, buflen, offset, filter, 1);
}

static int __dir_list(const volid_t *volid, const dirid_t *dirid,
                      uint32_t count, uint64_t offset, dirlist_t **dirlist)
{
        int ret, idx;
        redisReply *reply, *e0, *e1, *k1, *v1;
        dir_entry_t *ent;
        uint64_t next;
        uint32_t i;
        dirlist_t *array;
        __dirlist_t *node;
        

        reply = hscan(volid, dirid, NULL, offset, count);
        YASSERT(reply);
        if (reply->type != REDIS_REPLY_ARRAY) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        e0 = reply->element[0];
        e1 = reply->element[1];

        next = atol(e0->str);

        DBUG("req:(offset %ju count %jd), rep:(count %ju, next %ju)\n",
              offset, count, e1->elements, next);

        ret = ymalloc((void **)&array, DIRLIST_SIZE(e1->elements / 2));
        if (ret)
                GOTO(err_ret, ret);

        idx = 0;
        for (i = 0; i < (uint32_t)e1->elements; i += 2) {
                // name + value
                k1 = e1->element[i];
                if (strncmp(k1->str, SDFS_MD_SYSTEM, strlen(SDFS_MD_SYSTEM)) == 0) {
                        continue;
                }

                v1 = e1->element[i+1];
                ent = (dir_entry_t *)v1->str;
                node = &array->array[idx];
                node->fileid = ent->fileid;
                node->d_type = ent->d_type;
                strcpy(node->name, k1->str);

                DBUG("name %s "CHKID_FORMAT"\n", node->name, CHKID_ARG(&node->fileid));
                idx++;
        }
        
        freeReplyObject(reply);

        array->count = idx;
        array->cursor = 0;
        array->offset = next;
        *dirlist = array;

        return 0;
err_ret:
        freeReplyObject(reply);
        return ret;
}

dirop_t __dirop__ = {
        .lookup = dir_lookup,
        .readdir = dir_readdir,
        .readdirplus = dir_readdirplus,
        .readdirplus_filter = __readdirplus_filter,
        .newrec = dir_newrec,
        .unlink = dir_unlink,
        .dirlist = __dir_list,
};

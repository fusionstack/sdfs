

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#include "sdfs_buffer.h"
#include "chk_meta.h"
#include "ylib.h"
#include "jnl_proto.h"
#include "chk_proto.h"
#include "chk_meta.h"
#include "yfscds_conf.h"
#include "md_proto.h"
#include "dbg.h"

#pragma pack(8)
typedef struct {
        uint32_t reqlen;
        uint32_t reslen;
        char buf[0];
} head_t;
#pragma pack()

static int __scan(const void *_buf, int len, int64_t offset, void *arg)
{
        int ret;
        uint32_t op;
        char *req, *res, buf[MAX_BUF_LEN];
        head_t *head;
        fileid_t *fileid, fileid1;
        mdp_create_req_t *req_create __attribute__((unused));
        fileinfo_t *md;
        mdp_chkget_req_t *req_chkget __attribute__((unused));
        chkinfo_t *chkinfo;
        //mdp_unlink_req_t *req_unlink;

        fileid = arg;
        (void) offset;

        YASSERT(len <= MAX_BUF_LEN);

        memcpy(buf, _buf, len);
        head = (void *)buf;
        req = head->buf;
        if (head->reslen)
                res = head->buf + head->reqlen;
        else
                res = NULL;

        op = *(mdp_op_t*)req;

	DBUG("op %u\n", op);
        switch (op) {
        case MDP_UNKNOWN:
        case MDP_LOOKUP:     /*1*/
        case MDP_LOOKUP1:
        case MDP_GETATTR:
        case MDP_READLINK:
        case MDP_READDIR:
        case MDP_READDIRPLUS:
        case MDP_STATVFS:
        case MDP_SETOPT:
        case MDP_PING:
        case MDP_DISKJOIN:
        case MDP_DISKHB:
        case MDP_SHADOWJOIN:
        case MDP_LVLIST:
        case MDP_LVLOOKUP:
        case MDP_LVGETATTR:
        case MDP_REPCHECK:

                ret = EINVAL;
                GOTO(err_ret, ret);

                break;
        case MDP_MKDIR:
        case MDP_CHMOD:
        case MDP_CHOWN:
                break;
        case MDP_UNLINK:
#if 0
                req_unlink = (void *)req;

                if (fileid_cmp(&req_unlink->fileid, fileid) == 0) {
                        DINFO("unlink file %llu_v%u\n", (LLU)fileid->id, fileid->version);
                }

                break;
#endif
        case MDP_RENAME:
        case MDP_RMDIR:
        case MDP_LINK2UNIQUE:
        case MDP_SETXATTR:
        case MDP_GETXATTR:
        case MDP_REMOVEXATTR:
        case MDP_LISTXATTR:
        case MDP_SYMLINK:
                break;
        case MDP_CREATE:
                req_create = (void *)req;
                md = (void *)res;

                if (fileid_cmp(&md->fileid, fileid) == 0) {
                        DINFO("create file %llu_v%u\n", (LLU)fileid->id, fileid->version);
                }

                break;
        case MDP_FSYNC:
        case MDP_TRUNCATE:
                break;
        case MDP_CHKGET:
                req_chkget = (void *)req;
                chkinfo = (void *)res;

                cid2fid(&fileid1, &chkinfo->chkid);

                if (fileid_cmp(&fileid1, fileid) == 0) {
                        DINFO("object create %llu_v%u/%u\n", (LLU)fileid->id, fileid->version,
                              chkinfo->chkid.idx);
                }

                break;
        case MDP_NEWREP:
        case MDP_LVCREATE:
        case MDP_LVSETATTR:
        case MDP_LVSET:
        case MDP_DELREP:
        case MDP_CHKAVAILABLE:
        case MDP_CHKSETWRITEBACK:
        case MDP_CHKUNLINK:
        case MDP_UTIME:
                break;
        default:
                YASSERT(0);
        }

        return 0;
err_ret:
        return ret;
}

int __jouranl_scan(const fileid_t *fileid, const char *path)
{
        int ret, left, len, eof;
        char buf[MAX_BUF_LEN];
        uint64_t off;
        jnl_handle_t jnl;

        ret = jnl_open(path, &jnl, 0);
        if (ret)
                GOTO(err_ret, ret);

        off = 0;
        while (1) {
                ret = jnl_pread(&jnl, buf, MAX_BUF_LEN, off);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_jnl, ret);
                }

                if (ret == 0)
                        break;

                len = ret;

                ret = jnl_iterator_buf(buf, len, off, __scan, (void *)fileid, &left, &eof);
                if (ret)
                        GOTO(err_jnl, ret);

                off += (len - left);

                if (eof)
                        break;

                DBUG("left %u len %u off %llu\n", left, len, (LLU)off);
        }

        jnl_close(&jnl);

        return 0;
err_jnl:
        jnl_close(&jnl);
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret;
        char c_opt;
        char *path;
        fileid_t fileid;

        while ((c_opt = getopt(argc, argv, "f:")) > 0)
                switch (c_opt) {
                case 'f':
#ifdef __x86_64__
                        ret = sscanf(optarg, "%lu_v%u", &fileid.id, &fileid.version);
#else
                        ret = sscanf(optarg, "%llu_v%u", &fileid.id, &fileid.version);
#endif
                        YASSERT(ret == 2);
                        break;
                default:
                        exit(1);
                }

        path = argv[3];

        if (path == NULL || path[0] != '/') {
                ret = EINVAL;
                GOTO(err_ret, ret);
        }

        ret = __jouranl_scan(&fileid, path);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}


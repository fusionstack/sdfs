

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <semaphore.h>
#include <pthread.h>
#include <errno.h>
#include <sys/statvfs.h>

#define DBG_SUBSYS S_YFSCDS

#include "ylib.h"
#include "yfscds_conf.h"
#include "job_dock.h"
#include "cds_hb.h"
#include "chk_proto.h"
#include "cd_proto.h"
#include "disk.h"
#include "md_lib.h"
#include "cds_lib.h"
#include "dbg.h"

typedef struct {
        UINT4 state[4];    /* state (ABCD) */
        UINT4 count[2];        /* number of bits, modulo 2^64 (lsb first) */
        unsigned char buffer[64]; /* input buffer */
} MD5_CTX;

extern void MD5Init (MD5_CTX *context);
extern void MD5Update(MD5_CTX *context, unsigned char *input,unsigned int inputLen);
extern void MD5Final (unsigned char digest[16], MD5_CTX *context);
extern char* MDString(char *);
extern char* MDFile(char *, char*);
extern char* MDFile1(char *, char*, uint64_t);
extern char* hmac_md5(char* text, char* key);

int cds_getinfo(const chkid_t *chkid, chkmeta2_t *md, uint64_t *unlost_version,
                uint64_t *max_version)
{
        (void) chkid;
        (void) md;
        (void) unlost_version;
        (void) max_version;


        return EINVAL;
#if 0

        int ret;
        cache_entry_t *cent;
        cds_cache_entry_t *ent;

        ret = cds_cache_get(chkid, &cent);
        if (ret) {
                if (ret == ENOENT)
                        goto err_ret;
                else
                        GOTO(err_ret, ret);
        }

        ret = cache_wrlock(cent);
        if (ret)
                GOTO(err_release, ret);

        ent = cent->value;
        *md = *ent->md;
        *unlost_version = 0;
        *max_version = ent->max_version;

        DBUG("chk %llu_v%u[%u] version %llu max version %llu\n",
             (LLU)chkid->id, chkid->version, chkid->idx, (LLU)md->chk_version,
             (LLU)ent->max_version);

        cache_unlock(cent);
        cds_cache_release(cent);

        return 0;
err_release:
        cds_cache_release(cent);
err_ret:
        return ret;
#endif
}

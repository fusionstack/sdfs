#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>		// for memset, memcmp
#include <unistd.h>
#include <isa-l/erasure_code.h>

#include "sdfs_ec.h"
#include "ylib.h"

/*typedef unsigned char u8;*/

#define NO_INVERT_MATRIX -2
// Generate decode matrix from encode matrix
static int __gf_gen_decode_matrix(unsigned char *encode_matrix,
                unsigned char *decode_matrix,
                unsigned char *invert_matrix,
                unsigned int *decode_index,
                unsigned char *src_err_list,
                unsigned char *src_in_err,
                int nerrs, int nsrcerrs, int k, int m)
{
        int ret;
        int i, j, p;
        int r;
        unsigned char *backup, *b, s;
        int incr = 0;

        ret = ymalloc((void **)&b, EC_MMAX * EC_KMAX);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&backup, EC_MMAX * EC_KMAX);
        if (unlikely(ret))
                GOTO(err_free1, ret);

        // Construct matrix b by removing error rows
        for (i = 0, r = 0; i < k; i++, r++) {
                while (src_in_err[r])
                        r++;
                for (j = 0; j < k; j++) {
                        b[k * i + j] = encode_matrix[k * r + j];
                        backup[k * i + j] = encode_matrix[k * r + j];
                }
                decode_index[i] = r;
        }

        incr = 0;
        while (gf_invert_matrix(b, invert_matrix, k) < 0) {
                if (nerrs == (m - k)) {
                        DERROR("BAD MATRIX\n");
                        ret = NO_INVERT_MATRIX;
                        GOTO(err_free2, ret);
                }

                incr++;
                memcpy(b, backup, EC_MMAX * EC_KMAX);
                for (i = nsrcerrs; i < nerrs - nsrcerrs; i++) {
                        if (src_err_list[i] == (decode_index[k - 1] + incr)) {
                                // skip the erased parity line
                                incr++;
                                continue;
                        }
                }

                if ((int)(decode_index[k - 1] + incr) >= m) {
                        DERROR("BAD MATRIX\n");
                        ret = NO_INVERT_MATRIX;
                        GOTO(err_free2, ret);
                }

                decode_index[k - 1] += incr;
                for (j = 0; j < k; j++)
                        b[k * (k - 1) + j] = encode_matrix[k * decode_index[k - 1] + j];

        };

        for (i = 0; i < nsrcerrs; i++) {
                for (j = 0; j < k; j++) {
                        decode_matrix[k * i + j] = invert_matrix[k * src_err_list[i] + j];
                }
        }

        /* src_err_list from encode_matrix * invert of b for parity decoding */
        for (p = nsrcerrs; p < nerrs; p++) {
                for (i = 0; i < k; i++) {
                        s = 0;
                        for (j = 0; j < k; j++)
                                s ^= gf_mul(invert_matrix[j * k + i],
                                                encode_matrix[k * src_err_list[p] + j]);

                        decode_matrix[k * p + i] = s;
                }
        }

        yfree((void **)&backup);
        yfree((void **)&b);
        return 0;
err_free2:
        yfree((void **)&backup);
err_free1:
        yfree((void **)&b);
err_ret:
        return ret;
}

//m = k + r
int ec_encode(char **data, char **coding, int blocksize, int m, int k)
{
        int ret;
        unsigned char *encode_matrix, *g_tbls;

        YASSERT(m <= EC_MMAX);
        YASSERT(k <= EC_KMAX);

        ret = ymalloc((void **)&encode_matrix, EC_MMAX * EC_KMAX);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&g_tbls, EC_KMAX * EC_MMAX * 32);
        if (unlikely(ret))
                GOTO(err_free, ret);

        gf_gen_rs_matrix(encode_matrix, m, k);
        ec_init_tables(k, m - k, &encode_matrix[k * k], g_tbls);
        ec_encode_data_sse(blocksize, k, m - k, g_tbls, (unsigned char**)data, (unsigned char**)coding);

        yfree((void **)&g_tbls);
        yfree((void **)&encode_matrix);
        return 0;
err_free:
        yfree((void **)&encode_matrix);
err_ret:
        return ret;
}

//m = k + r
int ec_decode(unsigned char *src_in_err, char **data, char **coding, int blocksize, int m, int k)
{
        int ret, i, nerrs = 0, nsrcerrs = 0;
        int s, r;
        unsigned int decode_index[EC_MMAX];
        unsigned char src_err_list[EC_MMAX];
        unsigned char *recover_source[EC_MMAX];
        unsigned char *recover_target[EC_MMAX];
        unsigned char *encode_matrix, *decode_matrix, *invert_matrix, *g_tbls;

        YASSERT(m <= EC_MMAX);
        YASSERT(k <= EC_KMAX);

        for (i = 0; i < m; i++) {
                if (src_in_err[i]) {
                        src_err_list[nerrs++] = i;
                        if (i < k) {
                                nsrcerrs++;
                        }
                }
        }

        memset(recover_source, 0, sizeof (recover_source));
        memset(recover_target, 0, sizeof (recover_target));

        for (i = 0, s = 0, r = 0; ((r < k) || (s < nerrs)) && (i < m); i++) {
                if (!src_in_err[i]) {
                        if (r < k) {
                                if (i < k) {
                                        recover_source[r] = (unsigned char*) data[i];
                                } else {
                                        recover_source[r] = (unsigned char*) coding[i - k]; 
                                }   
                                r++;
                        }
                } else {
                        if (s < m) {
                                if (i < k) {
                                        recover_target[s] = (unsigned char*) data[i];
                                } else {
                                        recover_target[s] = (unsigned char*) coding[i - k];
                                }
                                s++;
                        }
                }
        }

        ret = ymalloc((void **)&encode_matrix, EC_MMAX * EC_KMAX);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        ret = ymalloc((void **)&decode_matrix, EC_MMAX * EC_KMAX);
        if (unlikely(ret))
                GOTO(err_free1, ret);

        ret = ymalloc((void **)&invert_matrix, EC_MMAX * EC_KMAX);
        if (unlikely(ret))
                GOTO(err_free2, ret);

        ret = ymalloc((void **)&g_tbls, EC_KMAX * EC_MMAX * 32);
        if (unlikely(ret))
                GOTO(err_free3, ret);

        gf_gen_rs_matrix(encode_matrix, m, k);
        ret = __gf_gen_decode_matrix(encode_matrix, decode_matrix,
                        invert_matrix, decode_index, src_err_list, src_in_err,
                        nerrs, nsrcerrs, k, m);
        if (unlikely(ret)) {
                DERROR("Fail to __gf_gen_decode_matrix\n");
                GOTO(err_free4, ret);
        }

        ec_init_tables(k, nerrs, decode_matrix, g_tbls);
        ec_encode_data_sse(blocksize, k, nerrs, g_tbls, recover_source, recover_target);

        yfree((void **)&g_tbls);
        yfree((void **)&invert_matrix);
        yfree((void **)&decode_matrix);
        yfree((void **)&encode_matrix);

        return 0;
err_free4:
        yfree((void **)&g_tbls);
err_free3:
        yfree((void **)&invert_matrix);
err_free2:
        yfree((void **)&decode_matrix);
err_free1:
        yfree((void **)&encode_matrix);
err_ret:
        return ret;
}

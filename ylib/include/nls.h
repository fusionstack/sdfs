#ifndef __NLS_H__
#define __NLS_H__

#include <stdint.h>

/* unicode character */
typedef uint16_t uchar_t;

struct nls_table {
        char *charset;
        char *alias;

        int (*uni2char) (uchar_t uni, int boundlen, unsigned char *out,
                         int *outlen);
        int (*char2uni) (const unsigned char *rawstring, int boundlen,
                         uchar_t *uni, int *outlen);

        unsigned char *charset2lower;
        unsigned char *charset2upper;

        struct nls_table *next;
};

/* this value hold the maximum octet of charset */
#define NLS_MAX_CHARSET_SIZE 6 /* for UTF-8 */

#endif

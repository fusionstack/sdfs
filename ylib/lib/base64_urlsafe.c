/**
* @file b64.c  Base64 encoding/decoding functions
*
* Copyright (C) 2010 Creytiv.com
*/

#include "base64_urlsafe.h"
#include <errno.h>

static const char b64_table[65] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz"
                "0123456789+/";

/**
* Base-64 encode a buffer
*
* @param in   Input buffer
* @param ilen Length of input buffer
* @param out  Output buffer
* @param olen Size of output buffer, actual written on return
*
* @return 0 if success, otherwise errorcode
*/
int b64_encode(const uint8_t *in, size_t ilen, char *out, size_t *olen) {
    const uint8_t *in_end = in + ilen;
    const char *o = out;

    if (!in || !out || !olen) {
        return EINVAL;
    }

    if (*olen < 4 * ((ilen + 2) / 3)) {
        return EOVERFLOW;
    }

    for (; in < in_end;) {
        uint32_t v;
        int pad = 0;

        v = *in++ << 16;
        if (in < in_end) {
            v |= *in++ << 8;
        } else {
            ++pad;
        }

        if (in < in_end) {
            v |= *in++ << 0;
        } else {
            ++pad;
        }

        *out++ = b64_table[v >> 18 & 0x3f];
        *out++ = b64_table[v >> 12 & 0x3f];
        *out++ = (pad >= 2) ? '=' : b64_table[v >> 6 & 0x3f];
        *out++ = (pad >= 1) ? '=' : b64_table[v >> 0 & 0x3f];
    }

    *olen = out - o;

    return 0;
}

/* convert char -> 6-bit value */
static inline uint32_t b64val(char c) {
    if ('A' <= c && c <= 'Z') {
        return c - 'A' + 0;
    } else if ('a' <= c && c <= 'z') {
        return c - 'a' + 26;
    } else if ('0' <= c && c <= '9') {
        return c - '0' + 52;
    } else if ('+' == c) {
        return 62;
    } else if ('/' == c) {
        return 63;
    } else if ('=' == c) {
        return 1 << 24; /* special trick */
    } else {
        return 0;
    }
}


/**
* Decode a Base-64 encoded string
*
* @param in   Input buffer
* @param ilen Length of input buffer
* @param out  Output buffer
* @param olen Size of output buffer, actual written on return
*
* @return 0 if success, otherwise errorcode
*/
int b64_decode(const char *in, size_t ilen, uint8_t *out, size_t *olen) {
    const char *in_end = in + ilen;
    const uint8_t *o = out;

    if (!in || !out || !olen) {
        return EINVAL;
    }

    if (*olen < 3 * (ilen / 4)) {
        return EOVERFLOW;
    }

    for (; in + 3 < in_end;) {
        uint32_t v;

        v = b64val(*in++) << 18;
        v |= b64val(*in++) << 12;
        v |= b64val(*in++) << 6;
        v |= b64val(*in++) << 0;

        *out++ = v >> 16;
        if (!(v & (1 << 30))) {
            *out++ = (v >> 8) & 0xff;
        }
        if (!(v & (1 << 24))) {
            *out++ = (v >> 0) & 0xff;
        }
    }

    *olen = out - o;

    return 0;
}

static inline char b64_to_safe(char c) {
    switch (c) {
        case '+':
            return '-';
            break;
        case '/':
            return '_';
            break;
        default:
            return c;
    }
}

static inline char safe_to_b64(char c) {
    switch (c) {
        case '-':
            return '+';
            break;
        case '_':
            return '/';
            break;
        default:
            return c;
    }
}

int urlsafe_b64_encode(const uint8_t *in, size_t ilen, char *out, size_t *olen) {
    int ret = 0;
    char *out_end = NULL;
    char *out_index = NULL;

    ret = b64_encode(in, ilen, out, olen);
    if (ret != 0) {
        return ret;
    }

    out_end = out + *olen;
    out_index = out;

    for (; out_index < out_end;) {
        *out_index = b64_to_safe(*out_index);
        ++out_index;
    }

    *out_index = '\0';

    return 0;

}

int urlsafe_b64_decode(const char *in, size_t ilen, uint8_t *out, size_t *olen) {
    int ret = 0;
    char *in_end = NULL;
    char *in_index = NULL;

    in_end = (char *)(in + ilen);
    in_index = (char *)in;

    for (; in_index < in_end;) {
        *in_index = safe_to_b64(*in_index);
        ++in_index;
    }

    *in_index = '\0';

    ret = b64_decode(in, ilen, out, olen);

    if (ret != 0) {
        return ret;
    }

    return 0;
}

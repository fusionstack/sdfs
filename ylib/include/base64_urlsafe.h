/**
* @file re_base64.h  Interface to Base64 encoding/decoding functions
*
* Copyright (C) 2010 Creytiv.com
*/

#ifndef B64_H_
#define B64_H_ (1)

#include <stdint.h>
#include <stddef.h>

int b64_encode(const uint8_t *in, size_t ilen, char *out, size_t *olen);

int b64_decode(const char *in, size_t ilen, uint8_t *out, size_t *olen);

int urlsafe_b64_encode(const uint8_t *in, size_t ilen, char *out, size_t *olen);

int urlsafe_b64_decode(const char *in, size_t ilen, uint8_t *out, size_t *olen);

#endif // B64_H_

#ifndef __MD5_H
#define __MD5_H

#include <string.h>
#include <stdint.h>

typedef unsigned char *POINTER; /* POINTER defines a generic pointer type */
typedef unsigned short int UINT2; /* UINT2 defines a two byte word */
typedef unsigned long int UINT4; /* UINT4 defines a four byte word */

typedef struct {
	UINT4 state[4]; /* state (ABCD) */
	UINT4 count[2];  /* number of bits, modulo 2^64 (lsb first) */
	unsigned char buffer[64]; /* input buffer */
} MD5_CTX;

//#define MAX_BUF_LEN   (1024 * 64)

void MD5Init (MD5_CTX *context);
void MD5Update(MD5_CTX *context, unsigned char *input,unsigned int inputLen);
void MD5Final (unsigned char digest[16], MD5_CTX *context);

#endif 

#ifndef _AES_H_
#define _AES_H_
#include <stdint.h>
#include <stdlib.h>

//add for bug #11160
#define BUF_SIZE_4K (1024 * 4)

int str2hex(const uint8_t *str, size_t slen, uint8_t *hex, size_t hlen);
int hex2str(const uint8_t *hex, size_t hlen, uint8_t *str, size_t *slen);
int encrypt_aes(unsigned char *plaintext, int plaintext_len, unsigned char *key,
                unsigned char *iv, unsigned char *ciphertext);
int decrypt_aes(unsigned char *ciphertext, int ciphertext_len, unsigned char *key,
                unsigned char *iv, unsigned char *plaintext);
int decrypt_with_final(unsigned char *ciphertext, int ciphertext_len, unsigned char *key,
                unsigned char *iv, unsigned char *plaintext);
#endif

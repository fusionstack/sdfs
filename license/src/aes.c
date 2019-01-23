#include "../include/aes.h"
#include <string.h>
#include <openssl/evp.h>
#include <openssl/err.h>

#define LICENSE_S2HLEN(len)             ((len) * 2)
#define LICENSE_H2SLEN(len)             ((len) / 2)
#define LICENSE_HEXFMT                  "%02x"

void handleErrors(void)
{
        ERR_print_errors_fp(stderr);
        abort();
}

int str2hex(const uint8_t *str, size_t slen, uint8_t *hex, size_t hlen)
{
        size_t i;

        if (LICENSE_S2HLEN(slen) <= hlen) {
                for (i = 0; i < slen; ++i) {
                        snprintf((char *)hex + i * 2, 3, LICENSE_HEXFMT, str[i]);
                }

                return 0;
        } else {
                return EINVAL;
        }
}

int hex2str(const uint8_t *hex, size_t hlen, uint8_t *str, size_t *slen)
{
        size_t i, len = 0, t_len = *slen;

        if (!(hlen % 2) && LICENSE_H2SLEN(hlen) <= t_len) {
                for (i = 0; i < hlen; i += 2) {
                        sscanf((char *)hex + i, LICENSE_HEXFMT, (int *)&str[i / 2]);
                        len++;
                }
                str[i / 2] = 0;

                *slen = len;

                return 0;
        } else {
                return EINVAL;
        }
}

int encrypt_aes(unsigned char *plaintext, int plaintext_len, unsigned char *key,
                unsigned char *iv, unsigned char *ciphertext)
{
        EVP_CIPHER_CTX *ctx;

        int len;

        int ciphertext_len;

        /* Create and initialise the context */
        if(!(ctx = EVP_CIPHER_CTX_new()))
                handleErrors();

        /*  Initialise the encryption operation. IMPORTANT - ensure you use a key
         *  and IV size appropriate for your cipher
         *  In this example we are using 256 bit AES (i.e. a 256 bit key). The
         *  IV size for *most* modes is the same as the block size. For AES this
         *  is 128 bits
         */
        if(1 != EVP_EncryptInit_ex(ctx, EVP_aes_128_cbc(), NULL, key, iv))
                handleErrors();

        /*  Provide the message to be encrypted, and obtain the encrypted output.
         *  EVP_EncryptUpdate can be called multiple times if necessary
         */
        if(1 != EVP_EncryptUpdate(ctx, ciphertext, &len, plaintext, plaintext_len))
                handleErrors();
        ciphertext_len = len;

        /* Finalise the encryption. Further ciphertext bytes may be written at
         * this stage.
         */
        if(1 != EVP_EncryptFinal_ex(ctx, ciphertext + len, &len)) handleErrors();
        ciphertext_len += len;

        /* Clean up */
        EVP_CIPHER_CTX_free(ctx);

        return ciphertext_len;
}

int decrypt_with_final(unsigned char *ciphertext, int ciphertext_len, unsigned char *key,
                unsigned char *iv, unsigned char *plaintext)
{
        EVP_CIPHER_CTX *ctx;

        int len;

        int plaintext_len;

        /* Create and initialise the context */
        if(!(ctx = EVP_CIPHER_CTX_new())) handleErrors();

        /* Initialise the decryption operation. IMPORTANT - ensure you use a key
         *    * and IV size appropriate for your cipher
         *       * In this example we are using 256 bit AES (i.e. a 256 bit key). The
         *          * IV size for *most* modes is the same as the block size. For AES this
         *             * is 128 bits */
        if(1 != EVP_DecryptInit_ex(ctx, EVP_aes_128_cbc(), NULL, key, iv))
                handleErrors();

        /* Provide the message to be decrypted, and obtain the plaintext output.
         *    * EVP_DecryptUpdate can be called multiple times if necessary
         *       */
        if(1 != EVP_DecryptUpdate(ctx, plaintext, &len, ciphertext, ciphertext_len))
                handleErrors();
        plaintext_len = len;

        /* Finalise the decryption. Further plaintext bytes may be written at
         *    * this stage.
         *       */
        if(1 != EVP_DecryptFinal_ex(ctx, plaintext + len, &len)) handleErrors();
        plaintext_len += len;

        /* Clean up */
        EVP_CIPHER_CTX_free(ctx);

        return plaintext_len;
}

int decrypt_aes(unsigned char *ciphertext, int ciphertext_len, unsigned char *key,
                unsigned char *iv, unsigned char *plaintext)
{
        EVP_CIPHER_CTX *ctx;
        int padding_len, len;
        unsigned char decrypt_with_padding_hex[BUF_SIZE_4K] = {0};

        /* Create and initialise the context */
        if(!(ctx = EVP_CIPHER_CTX_new())) handleErrors();

        /* Initialise the decryption operation. IMPORTANT - ensure you use a key
         *    * and IV size appropriate for your cipher
         *       * In this example we are using 256 bit AES (i.e. a 256 bit key). The
         *          * IV size for *most* modes is the same as the block size. For AES this
         *             * is 128 bits */
        if(1 != EVP_DecryptInit_ex(ctx, EVP_aes_128_cbc(), NULL, key, iv))
                handleErrors();

        /* Provide the message to be decrypted, and obtain the plaintext output.
         *    * EVP_DecryptUpdate can be called multiple times if necessary
         *       */
        if(1 != EVP_DecryptUpdate(ctx, decrypt_with_padding_hex, &len, ciphertext, ciphertext_len))
                handleErrors();

        padding_len = strlen((char*)decrypt_with_padding_hex);

        /* Finalise the decryption. Further plaintext bytes may be written at
         *    * this stage.
         *       */
        /* if(1 != EVP_DecryptFinal_ex(ctx, decrypt_with_padding + len, &len)) handleErrors(); */
        /* plaintext_len += len; */
        memcpy(plaintext, decrypt_with_padding_hex, padding_len);

        /* Clean up */
        EVP_CIPHER_CTX_free(ctx);

        return padding_len;
}

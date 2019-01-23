#include <openssl/md5.h>
#include <openssl/evp.h>
#include <openssl/conf.h>
#include <openssl/err.h>
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <termios.h>
#include <errno.h>
#include "license_helper.h"
#include "sdfs_conf.h"
#include "aes.h"
#include "configure.h"
#include "sdfs_lib.h" //for ly_init_simple
#include "dbg.h" //for dinfo_off

#define LICENSE_MONTH_MIN       (0)
#define LICENSE_MONTH_MAX       (12 * 10)       /* 10 years */

#define LICENSE_SHADOW          "6d3cf879a478a65506118fd25227bcd8"
#define char2int(x) ((x) - 0)

typedef enum {
        MODE_GEN,
        MODE_DUMP,
        MODE_CHECK,
        MODE_INVAL
}license_mode_t;

static void usage() {
        fprintf(stderr,
                        "uss.license {generate|dump} \n"
                        "--generate|-g      --info|-i filename --license|-l filename        Generate a license file\n"
                        "                   --info|-i filename                              specify a info filename\n"
                        "                   --license|-l filename                           specify a license filename\n"
                        "--dump|-d          --license|-l filename                           print license content as human-readable\n"
                        "--check|-c                                                         check validity of license\n"
                        "-h|-?|--help                                                       print usage\n"
               );
}

static void print_invalid_month()
{
        fprintf(stderr, "Invalid month, valid region of month is [%d~%d], 0 means permanent\n",
                        LICENSE_MONTH_MIN, LICENSE_MONTH_MAX);
}

static void print_invalid_capacity()
{
        fprintf(stderr, "Invalid capacity, capacity must be a unsigned integer, 0 means infinite\n");
}

static int __getch()
{
        char ch;
        struct termios old, new;

        (void) tcgetattr(STDIN_FILENO, &old);

        memcpy(&new, &old, sizeof(struct termios));
        new.c_lflag &= ~(ICANON | ECHO);

        (void) tcsetattr(STDIN_FILENO, TCSANOW, &new);

        ch = getchar();

        (void) tcsetattr(STDIN_FILENO, TCSANOW, &old);

        return ch;
}

int read_password(char *pass, int len)
{
        int i, j;
        char ch;

        printf("Password: ");

        for (i = 0, j = 0; i < len - 1; ++i) {
                ch = __getch();
                if (ch == '\n' || ch == '\r')
                        break;
                if (ch == 0x7f) { /* Backspace */
                        --i;
                        if (j)
                                --j;
                } else {
                        pass[j++] = ch;
                }
        }
        pass[j] = 0;
        printf("\n");

        return 0;
}

int read_month(unsigned int *_month)
{
        int month, len;
        char readbuf[MAX_INFO_LEN] = {0};

        printf("Validity of License (in months): ");

        if(fgets(readbuf, MAX_INFO_LEN, stdin) == NULL) {
                return -1;
        }

        len = strlen(readbuf);

        if(readbuf[len-1] == '\n') {
                readbuf[len-1] = '\0';
        }

        if(is_digit(readbuf)) {
                month = atoll(readbuf);
        } else {
                print_invalid_month();
                return -1;
        }

        if(month < LICENSE_MONTH_MIN || month > LICENSE_MONTH_MAX) {
                print_invalid_month();
                return -1;
        }

        *_month = month;

        return 0;
}

int read_capacity(unsigned int *_capacity)
{
        int capacity = 0, len;
        char readbuf[MAX_INFO_LEN];

        printf("Validity of License (in Gigas, input enter to use info file capacity): ");

        if (fgets(readbuf, MAX_INFO_LEN, stdin) == NULL) {
                return -1;
        }

        len = strlen(readbuf);

        if(readbuf[len-1] == '\n') {
                readbuf[len-1] = '\0';
        }

        if(is_digit(readbuf)) {
                capacity = atoll(readbuf);
        } else {
                print_invalid_capacity();
                return -1;
        }

        if(capacity < 0) {
                print_invalid_capacity();
                return -1;
        }

        *_capacity = capacity;

        return 0;
}

time_t month_to_second(unsigned int month)
{
        return (month * 30 * 24 * 60 *60);
}

unsigned long gb_to_byte(unsigned int capacity)
{
        return (capacity * 1024 * 1024 *1024);
}


/* param @in padding_str */
/* param @out raw_str */
/* param @in size of raw_str */

int unpadding(const char *padding_str, char *raw_str, int size)
{
        int len, padding, raw_len;

        len = strlen(padding_str);
        padding = char2int(padding_str[len-1]);

        raw_len = len - padding;

        memset(raw_str, 0, size);
        memcpy(raw_str, padding_str, raw_len);

        return raw_len;
}

int get_permit_mac(const char *info_file, unsigned char *secret_key,
                unsigned char *mac)
{
        int ret, line_count = 1;
        int decryptedlen, cipherhexlen, cipherlen, padding_len;
        char line[MAX_LINE_LEN] = {0};

        unsigned char cipher_hex[MAX_LINE_LEN] = {0};
        unsigned char cipher[MAX_LINE_LEN] = {0};
        unsigned char padding_hex[MAX_LINE_LEN] = {0};
        unsigned char padding[MAX_LINE_LEN] = {0};
        unsigned char raw_str[MAX_LINE_LEN] = {0};

        FILE *fp = fopen(info_file, "r");
        if(fp == NULL) {
                ret = ENOENT;
                goto err_ret;
        }

        while(fgets(line, MAX_LINE_LEN, fp) != NULL) {
                if(line_count == 2) {
                        memcpy(cipher_hex, line, strlen(line));
                }

                line_count ++;
        }

        line_count--;

        if(line_count != 4) {
                fprintf(stderr, "Invalid Info File\n");
                goto err_close;
        }
        cipherhexlen = strip_newline((char*)cipher_hex);

        cipherlen = sizeof(cipher);
        hex2str(cipher_hex, cipherhexlen, cipher, (size_t*)&cipherlen);

        decryptedlen = decrypt_aes(cipher, cipherlen, secret_key,
                        secret_key, padding_hex);

        padding_hex[decryptedlen] = '\0';

        padding_len = sizeof(padding);
        hex2str(padding_hex, decryptedlen, padding, (size_t*)&padding_len);

        unpadding((char*)padding, (char*)raw_str, MAX_LINE_LEN);

        memcpy(mac, raw_str, strlen((char*)raw_str));

        if(fp)
                fclose(fp);

        return 0;
err_close:
        if(fp) fclose(fp);
err_ret:
        return ret;
}

int get_permit_date(const char *info_file, unsigned char *secret_key,
                unsigned int month, time_t *_permit_time)
{
        int ret, line_count = 1;
        int decryptedlen, cipherhexlen, cipherlen, padding_len;
        char line[MAX_LINE_LEN] = {0};

        unsigned char cipher_hex[MAX_LINE_LEN] = {0};
        unsigned char cipher[MAX_LINE_LEN] = {0};
        unsigned char padding_hex[MAX_LINE_LEN] = {0};
        unsigned char padding[MAX_LINE_LEN] = {0};
        unsigned char raw_str[MAX_LINE_LEN] = {0};
        time_t create_time, permit_time;

        /* unlimit time */
        if(month == 0) {
                *_permit_time = 0;
                return 0;
        }

        FILE *fp = fopen(info_file, "r");
        if(fp == NULL) {
                ret = ENOENT;
                goto err_ret;
        }

        while(fgets(line, MAX_LINE_LEN, fp) != NULL) {
                if(line_count == 3) {
                        memcpy(cipher_hex, line, strlen(line));
                }

                line_count ++;
        }

        line_count--;

        if(line_count != 4) {
                fprintf(stderr, "Invalid Info File\n");
                goto err_close;
        }
        cipherhexlen = strip_newline((char*)cipher_hex);

        cipherlen = sizeof(cipher);
        hex2str(cipher_hex, cipherhexlen, cipher, (size_t*)&cipherlen);

        decryptedlen = decrypt_aes(cipher, cipherlen, secret_key,
                        secret_key, padding_hex);

        padding_hex[decryptedlen] = '\0';

        padding_len = sizeof(padding);
        hex2str(padding_hex, decryptedlen, padding, (size_t*)&padding_len);

        unpadding((char*)padding, (char*)raw_str, MAX_LINE_LEN);

        if(is_digit((char*)raw_str)) {
                create_time = atoll((char*)raw_str);
        } else {
                fprintf(stderr, "invalid create_time\n");
                goto err_close;
        }

        permit_time = month_to_second(month);
        *_permit_time = create_time + permit_time;

        if(fp)
                fclose(fp);

        return 0;
err_close:
        if(fp) fclose(fp);
err_ret:
        return ret;
}

int get_permit_capacity(const char *info_file, unsigned char *secret_key,
                unsigned int capacity, unsigned long *_permit_capacity)
{
        int ret, line_count = 1;
        int decryptedlen, cipherhexlen, cipherlen, padding_len;
        char line[MAX_LINE_LEN] = {0};

        unsigned char cipher_hex[MAX_LINE_LEN] = {0};
        unsigned char cipher[MAX_LINE_LEN] = {0};
        unsigned char padding_hex[MAX_LINE_LEN] = {0};
        unsigned char padding[MAX_LINE_LEN] = {0};
        unsigned char raw_str[MAX_LINE_LEN] = {0};
        unsigned long cur_capacity;

        /* unlimit capacity */
        if(capacity == 0) {
                *_permit_capacity = 0;
                return 0;
        }

        FILE *fp = fopen(info_file, "r");
        if(fp == NULL) {
                ret = ENOENT;
                goto err_ret;
        }

        while(fgets(line, MAX_LINE_LEN, fp) != NULL) {
                if(line_count == 4) {
                        memcpy(cipher_hex, line, strlen(line));
                }

                line_count ++;
        }

        line_count--;

        if(line_count != 4) {
                fprintf(stderr, "Invalid Info File\n");
                goto err_close;
        }

        cipherhexlen = strip_newline((char*)cipher_hex);

        cipherlen = sizeof(cipher);
        hex2str(cipher_hex, cipherhexlen, cipher, (size_t *)&cipherlen);

        decryptedlen = decrypt_aes(cipher, cipherlen, secret_key,
                        secret_key, padding_hex);

        padding_hex[decryptedlen] = '\0';

        padding_len = sizeof(padding);
        hex2str(padding_hex, decryptedlen, padding, (size_t *)&padding_len);

        unpadding((char*)padding, (char*)raw_str, MAX_LINE_LEN);

        if(is_digit((char*)raw_str)) {
                cur_capacity = atoll((char*)raw_str);
        } else {
                fprintf(stderr, "invalid capacity\n");
                return -1;
        }

        /* 如果当前系统容量比给用户设置的容量大，则把当前系统容量作为 */
        /* 允许用户使用的最大容量，否则设置的容量作为最大容量 */
        if(cur_capacity > capacity)
                *_permit_capacity = cur_capacity;
        else
                *_permit_capacity = capacity;

        if(fp)
                fclose(fp);

        return 0;
err_close:
        if(fp) fclose(fp);
err_ret:
        return ret;
}

int do_login()
{
        int i;
        unsigned char md[MD5_DIGEST_LENGTH];
        char pass[MAX_PASSWD_LEN];
        char shadow[MD5_DIGEST_LENGTH * 2] = {0};

        read_password(pass, MAX_PASSWD_LEN);

        MD5((unsigned char*)pass, strlen(pass), md);

        for (i = 0; i < MD5_DIGEST_LENGTH; ++i) {
                snprintf(shadow + strlen(shadow), 3, "%02x", md[i]);
        }

        if (memcmp(LICENSE_SHADOW, shadow, MD5_DIGEST_LENGTH * 2)) {
                fprintf(stderr, "Authentication failure\n");
                return -1;
        }

        return 0;
}

int int_to_str(const unsigned long num, char *string)
{
        unsigned long remainder, result = num;
        char out[MAX_INFO_LEN] = {0};
        char temp[MAX_INFO_LEN] = {0};

        while(result) {
                remainder = result % 10;
                result = result / 10;
                strncpy(temp, out, strlen(out));
                snprintf(out, 256, "%lu%s", remainder, temp);
        }

        memcpy(string, out, strlen(out));

        return 0;
}

int write_license_file(const char *license_file, unsigned char *secret_key,
                const unsigned char *mac_str, const time_t permit_time,
                const unsigned long permit_capacity)
{
        int ret, cipher_time_len, cipher_cap_len, cipher_mac_len;
        unsigned char time_str[MAX_INFO_LEN] = {0}, capacity_str[MAX_INFO_LEN] = {0};

        unsigned char mac_hex[MAX_INFO_LEN] = {0};
        unsigned char time_hex[MAX_INFO_LEN] = {0};
        unsigned char cap_hex[MAX_INFO_LEN] = {0};

        unsigned char cipher_mac[MAX_INFO_LEN] = {0};
        unsigned char cipher_mac_hex[MAX_INFO_LEN] = {0};

        unsigned char cipher_time[MAX_INFO_LEN] = {0};
        unsigned char cipher_time_hex[MAX_INFO_LEN] = {0};

        unsigned char cipher_cap[MAX_INFO_LEN] = {0};
        unsigned char cipher_cap_hex[MAX_INFO_LEN] = {0};

        FILE *fp = fopen(license_file, "w+");
        if(fp == NULL) {
                ret = errno;
                goto err_ret;
        }

        int_to_str(permit_time, (char*)time_str);
        int_to_str(permit_capacity, (char*)capacity_str);

        str2hex(mac_str, strlen((char*)mac_str), mac_hex, sizeof(mac_hex));
        str2hex(time_str, strlen((char*)time_str), time_hex, sizeof(time_hex));
        str2hex(capacity_str, strlen((char*)capacity_str), cap_hex, sizeof(cap_hex));

        cipher_mac_len = encrypt_aes((unsigned char*)mac_hex, strlen((char*)mac_hex), secret_key,
                        secret_key, cipher_mac);
        str2hex(cipher_mac, cipher_mac_len, cipher_mac_hex, sizeof(cipher_mac_hex));

        cipher_time_len = encrypt_aes((unsigned char*)time_hex, strlen((char*)time_hex), secret_key,
                        secret_key, cipher_time);
        str2hex(cipher_time, cipher_time_len, cipher_time_hex, sizeof(cipher_time_hex));

        cipher_cap_len = encrypt_aes((unsigned char*)cap_hex, strlen((char*)cap_hex), secret_key,
                        secret_key, cipher_cap);
        str2hex(cipher_cap, cipher_cap_len, cipher_cap_hex, sizeof(cipher_cap_hex));

        fprintf(fp, "secret_key:%s\n%s\n%s\n%s\n", secret_key, cipher_mac_hex, cipher_time_hex, cipher_cap_hex);

        if(fp) fclose(fp);

        return 0;
err_ret:
        return ret;
}

void init_aes()
{
        /* Initialise the library */
        ERR_load_crypto_strings();
        OpenSSL_add_all_algorithms();
        OPENSSL_config(NULL);
}

int license_generate(const char *info_file, const char *license_file)
{
        int ret;
        time_t permit_time;
        unsigned int month, capacity;
        unsigned long permit_capacity;
        unsigned char secret_key[MAX_INFO_LEN] = {0};
        unsigned char mac[MAX_INFO_LEN] = {0};

        init_aes();

        ret = do_login();
        if(ret) {
                return -1;
        }

        ret = get_secret_key(info_file, secret_key);
        if(ret)
                return -1;

        ret = get_permit_mac(info_file, secret_key, mac);
        if(ret)
                return -1;

        fprintf(stdout, "mac : %s\n", mac);

        ret = read_month(&month);
        if(ret)
                return -1;

        ret = get_permit_date(info_file, secret_key, month, &permit_time);
        if(ret)
                return -1;

        fprintf(stdout, "month : %d, permit_time : %lu\n", month, permit_time);

        ret = read_capacity(&capacity);
        if(ret)
                return -1;

        ret = get_permit_capacity(info_file, secret_key, capacity, &permit_capacity);
        if(ret)
                return -1;

        fprintf(stdout, "capacity : %d, permit_capacity : %lu\n", capacity, permit_capacity);

        write_license_file(license_file, secret_key, mac, permit_time, permit_capacity);

        return 0;
}

int license_dump(const char *license_file)
{
        int ret;
        time_t time;
        unsigned long cap;
        unsigned char secret_key[MAX_INFO_LEN] = {0};
        unsigned char mac[MAX_INFO_LEN] = {0};

        ret = get_secret_key(license_file, secret_key);
        if(ret)
                return -1;

        ret = dump_mac(license_file, secret_key, mac);
        if(ret)
                return -1;

        ret = dump_time(license_file, secret_key, &time);
        if(ret)
                return -1;

        ret = dump_capacity(license_file, secret_key, &cap);
        if(ret)
                return -1;

        return 0;
}

int license_check()
{
        int ret;

        dbg_info(0);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if(ret)
                return -1;

        ret = ly_init_simple("uss.license");
        if(ret)
                return -1;

        uss_license_check(gloconf.workdir);

        return 0;
}

int main(int argc, char *argv[])
{
        int opt, options_index;
        struct stat st;
        license_mode_t mode = MODE_INVAL;
        char license_f[MAX_NAME_LEN] = {0}, info_f[MAX_NAME_LEN] = {0};
        static const struct option long_options[] = {
                {"generate",no_argument,NULL,'g'},
                {"info",required_argument,NULL,'i'},
                {"license",required_argument,NULL,'l'},
                {"dump",no_argument,NULL,'d'},
                {"check",no_argument,NULL,'c'},
                {"help",no_argument,NULL,'h'},
                {0,0,0,0}
        };

        if(argc == 1) {
                usage();
                return -1;
        }

        while((opt = getopt_long(argc, argv, "gdci:l:h?", long_options, &options_index)) != -1) {
                switch(opt) {
                        case 'g':
                                mode = MODE_GEN;
                                break;
                        case 'd':
                                mode = MODE_DUMP;
                                break;
                        case 'c':
                                mode = MODE_CHECK;
                                break;
                        case 'i':
                                memcpy(info_f, optarg, strlen(optarg));
                                break;
                        case 'l':
                                memcpy(license_f, optarg, strlen(optarg));
                                break;
                        case 'h':
                        case '?':
                                usage();
                                return -1;
                        default:
                                printf("wrong argument\n");
                                return -1;
                }

        }

        if (optind < argc) {
                printf("non-option ARGV-elements: ");
                while (optind < argc)
                        printf("%s ", argv[optind++]);
                printf("\n");
        }

        if (mode == MODE_GEN) {
                if((strlen(license_f) == 0) || (strlen(info_f) == 0)) {
                        fprintf(stderr, "please specify license file and info file\n");
                        return -1;
                }

                if(stat(info_f, &st) == -1) {
                        fprintf(stderr, "%s : No such file or directory\n", info_f);
                        return -1;
                }

                license_generate(info_f, license_f);

        } else if (mode == MODE_DUMP) {
                if(strlen(license_f) == 0) {
                        fprintf(stderr, "please specify license file\n");
                        return -1;
                }

                if(stat(license_f, &st) == -1) {
                        fprintf(stderr, "%s : No such file or directory\n", license_f);
                        return -1;
                }

                license_dump(license_f);
        } else if (mode == MODE_CHECK) {
                license_check();
        }

        return 0;
}

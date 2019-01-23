#ifndef _LICENSE_HELPER_H_
#define _LICENSE_HELPER_H_

#define FREE_LICENSE (60 * 60 * 24 * 30 * 3)

int is_digit(const char *str);
int is_startwith(const char *string, const char *substr);
int string_split(char *string, const char *delim, unsigned char *outStr);
int strip_newline(char *string);
int get_secret_key(const char *info_file, unsigned char *_secret_key);
int dump_mac(const char *license, unsigned char *secret_key, unsigned char *mac);
int dump_time(const char *license_file, unsigned char *secret_key, time_t *time);
int dump_capacity(const char *license_file, unsigned char *secret_key,
                unsigned long *cap);
int uss_get_create_time(time_t *create_time);
int check_mac_valid(const unsigned char *_mac);
int check_time_valid(const time_t due_time);
int check_cap_valid(const unsigned long cap);
int check_license_valid(const char *license_file);
int uss_license_check(const char *home);
#endif

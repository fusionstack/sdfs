#include "sdfs_lib.h"
#include "sdfs_conf.h"
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include "dbg.h"
#include "sysutil.h"
#include "../include/license_helper.h"
#include "../include/aes.h"

/* 判断字符串是否为数字 */
int is_digit(const char *str)
{
        return (strspn(str, "0123456789") == strlen(str));
}

int is_startwith(const char *string, const char *substr)
{
        if(strstr(string, substr) == NULL)
                return 0;
        else
                return 1;
}

int string_split(char *string, const char *delim, unsigned char *outStr)
{
        char *pstr = strtok(string, delim);
        if(pstr != NULL) {
                pstr = strtok(NULL, delim);
                memcpy(outStr, pstr, strlen(pstr));
                return 0;
        } else {
                return -1;
        }
}

int strip_newline(char *string)
{
        int len = strlen(string);

        len = strlen((char*)string);

        if(string[len-1] == '\n') {
                string[len-1] = '\0';
                len -= 1;
        }

        return len;
}

int get_secret_key(const char *info_file, unsigned char *_secret_key)
{
        int ret, found = 0, len;
        unsigned char secret_key[MAX_LINE_LEN] = {0};
        char line[MAX_LINE_LEN] = {0};
        FILE *fp = fopen(info_file, "r");
        if(fp == NULL) {
                goto err_ret;
        }

        while(fgets(line, MAX_LINE_LEN, fp) != NULL) {
                if(is_startwith(line, "secret_key")) {
                        ret = string_split(line, ":", secret_key);
                        if(ret) {
                                goto err_close;
                        }

                        found = 1;
                }
        }

        if(!found) {
                fprintf(stderr, "not found secret_key\n");
                goto err_close;
        }

        len = strip_newline((char*)secret_key);

        memcpy(_secret_key, secret_key, len);

        if(fp)
                fclose(fp);

        return 0;
err_close:
        if(fp) fclose(fp);
err_ret:
        return ret;
}

int dump_mac(const char *license, unsigned char *secret_key, unsigned char *mac)
{
        int ret, line_count = 1;
        int cipher_len, decrypt_len, c_len, d_len;
        char line[MAX_LINE_LEN] = {0};
        unsigned char cipher_hex[MAX_INFO_LEN] = {0};
        unsigned char cipher[MAX_INFO_LEN] = {0};
        unsigned char decrypt_hex[MAX_INFO_LEN] = {0};
        unsigned char decrypt[MAX_INFO_LEN] = {0};

        FILE *fp = fopen(license, "r");
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
                fprintf(stderr, "Invalid License File\n");
                goto err_close;
        }

        cipher_len = strip_newline((char*)cipher_hex);

        c_len = sizeof(cipher);
        hex2str(cipher_hex, cipher_len, cipher, (size_t*)&c_len);

        decrypt_len = decrypt_with_final(cipher, c_len, secret_key,
                        secret_key, decrypt_hex);
        decrypt_hex[decrypt_len] = '\0';

        d_len = sizeof(decrypt);
        hex2str(decrypt_hex, decrypt_len, decrypt, (size_t*)&d_len);

        memcpy(mac, decrypt, d_len);
        //fprintf(stdout, "decrypt mac : %s\n", decrypt);

        if(fp) fclose(fp);
        return 0;
err_close:
        if(fp) fclose(fp);
err_ret:
        return ret;
}

int dump_time(const char *license_file, unsigned char *secret_key, time_t *time)
{
        int ret, line_count = 1;
        int cipher_len, decrypt_len, c_len, d_len;
        char line[MAX_LINE_LEN] = {0};
        unsigned char cipher_hex[MAX_INFO_LEN] = {0};
        unsigned char cipher[MAX_INFO_LEN] = {0};
        unsigned char decrypt_hex[MAX_INFO_LEN] = {0};
        unsigned char decrypt[MAX_INFO_LEN] = {0};

        FILE *fp = fopen(license_file, "r");
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
                fprintf(stderr, "Invalid License File\n");
                goto err_close;
        }

        cipher_len = strip_newline((char*)cipher_hex);

        c_len = sizeof(cipher);
        hex2str(cipher_hex, cipher_len, cipher, (size_t*)&c_len);

        decrypt_len = decrypt_with_final(cipher, c_len, secret_key,
                        secret_key, decrypt_hex);
        decrypt_hex[decrypt_len] = '\0';

        d_len = sizeof(decrypt);
        hex2str(decrypt_hex, decrypt_len, decrypt, (size_t*)&d_len);

        if(is_digit((char*)decrypt)) {
                *time = atoll((char*)decrypt);
        } else {
                fprintf(stderr, "Invalid License File\n");
                return -1;
        }

        //fprintf(stdout, "decrypt time : %s\n", decrypt);

        if(fp) fclose(fp);
        return 0;
err_close:
        if(fp) fclose(fp);
err_ret:
        return ret;
}

int dump_capacity(const char *license_file, unsigned char *secret_key,
                unsigned long *cap)
{
        int ret, line_count = 1;
        int cipher_len, decrypt_len, c_len, d_len;
        char line[MAX_LINE_LEN] = {0};
        unsigned char cipher_hex[MAX_INFO_LEN] = {0};
        unsigned char cipher[MAX_INFO_LEN] = {0};
        unsigned char decrypt_hex[MAX_INFO_LEN] = {0};
        unsigned char decrypt[MAX_INFO_LEN] = {0};

        FILE *fp = fopen(license_file, "r");
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
                fprintf(stderr, "Invalid License File\n");
                goto err_close;
        }

        cipher_len = strip_newline((char*)cipher_hex);

        c_len = sizeof(cipher);
        hex2str(cipher_hex, cipher_len, cipher, (size_t*)&c_len);

        decrypt_len = decrypt_with_final(cipher, c_len, secret_key,
                        secret_key, decrypt_hex);
        decrypt_hex[decrypt_len] = '\0';

        d_len = sizeof(decrypt);
        hex2str(decrypt_hex, decrypt_len, decrypt, (size_t*)&d_len);

        if(is_digit((char*)decrypt)) {
                *cap = atoll((char*)decrypt);
        } else {
                fprintf(stderr, "Invalid License File\n");
                return -1;
        }

        //fprintf(stdout, "decrypt capacity : %s\n", decrypt);

        if(fp) fclose(fp);
        return 0;
err_close:
        if(fp) fclose(fp);
err_ret:
        return ret;
}

int uss_get_create_time(time_t *create_time)
{
        int ret;
        char str_create_time[MAX_LINE_LEN];
        size_t size = MAX_LINE_LEN;


        memset(str_create_time, 0, sizeof(str_create_time));
        ret = ly_getxattr("/system", "create_time", str_create_time, &size);
        if(ret)
                GOTO(err_ret, ret);

        sscanf(str_create_time, "%ld", create_time);

        return 0;
err_ret:
        return ret;
}

int check_mac_valid(const unsigned char *_mac)
{
        int ret, fd, valid = 0;
        DIR *dir;
        struct dirent *ent;
        char path[MAX_PATH_LEN], parent[] = "/sys/class/net";
        unsigned char mac[MAX_INFO_LEN] = {0};

        dir = opendir(parent);
        if (!dir) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        while ((ent = readdir(dir))) {
                if (!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..") || !strcmp(ent->d_name, "lo"))
                        continue;

                snprintf(path, MAX_PATH_LEN, "%s/%s/address", parent, ent->d_name);

                fd = open(path, O_RDONLY);
                if (fd < 0)
                        continue;

                ret = read(fd, mac, sizeof(mac));
                if(ret < 0) {
                        ret = errno;
                        close(fd);
                        GOTO(err_ret, ret);
                }

                //DWARN("mac = %s, _mac : %s\n", mac, _mac);
                strip_newline((char*)mac);

                if(strstr((char*)_mac, (char*)mac) != NULL) {
                        valid = 1;
                        break;
                }

                close(fd);
        }

        closedir(dir);

        if(!valid) {
                ret = EINVAL;
                goto err_ret;
        }

        return 0;
err_ret:
        return ret;
}

int check_time_valid(time_t due_time)
{
        time_t now;
        char due_time_str[MAX_NAME_LEN] = {0};

        now = time(NULL);

        //DWARN("now = %ld, due_time : %ld\n", now, due_time);

        if(now > due_time && due_time) {
                return ETIME;
        }

        if(due_time == 0) {
                fprintf(stdout, "Permanent free license.\n");
        } else {
                time2str(due_time_str, &due_time);
                fprintf(stdout, "Free License, license expiration time: %s\n",
                                due_time_str);
        }

        return 0;
}

int check_cap_valid(const unsigned long cap)
{
        int ret;
        unsigned long long total;
        unsigned long long permite_cap;
        fileid_t fileid;
        struct statvfs vfs;

        ret = sdfs_lookup_recurive("/system", &fileid);
        if(ret) {
                GOTO(err_ret, ret);
        }

        ret = sdfs_statvfs(NULL, &fileid, &vfs);
        if(ret) {
                GOTO(err_ret, ret);
        }

        total = vfs.f_bsize * vfs.f_blocks;
        permite_cap = cap * 1024 * 1024 * 1024;

        /* DWARN("total = %llu, permite_cap : %llu\n", */
                        /* (unsigned long long)total, */
                        /* (unsigned long long)permite_cap); */

        if(total > permite_cap && permite_cap) {
                ret = ENOSPC;
                GOTO(err_ret, ret);
        }

        if(permite_cap == 0) {
                fprintf(stdout, "Infinite capacity.\n");
        } else {
                fprintf(stdout, "Permit capacity:%luGB\n", cap);
        }

        return 0;
err_ret:
        return ret;
}

int check_license_valid(const char *license_file)
{
        int ret;
        unsigned char secret_key[MAX_INFO_LEN] = {0};
        unsigned char mac[MAX_INFO_LEN] = {0};
        time_t due_time;
        unsigned long capacity;

        ret = get_secret_key(license_file, secret_key);
        if(ret) {
                ret = EINVAL;
                goto err_ret;
        }

        ret = dump_mac(license_file, secret_key, mac);
        if(ret) {
                ret = EINVAL;
                goto err_ret;
        }

        ret = check_mac_valid(mac);
        if(ret) {
                ret = EINVAL;
                goto err_ret;
        }

        ret = dump_time(license_file, secret_key, &due_time);
        if(ret) {
                ret = EINVAL;
                goto err_ret;
        }

        ret = check_time_valid(due_time);
        if(ret) {
                ret = ETIME;
                goto err_ret;
        }

        ret = dump_capacity(license_file, secret_key, &capacity);
        if(ret) {
                ret = EINVAL;
                goto err_ret;
        }

        ret = check_cap_valid(capacity);
        if(ret) {
                ret = ENOSPC;
                goto err_ret;
        }

        return 0;
err_ret:
        return ret;
}

int uss_license_check(const char *home)
{
        int ret, is_trial = 0;
        time_t create_time, now, due_time;
        char path[MAX_PATH_LEN] = {0}, due_time_str[MAX_NAME_LEN] = {0};
        struct stat buf;

        ret = uss_get_create_time(&create_time);
        if(ret)
                GOTO(err_ret, ret);

        now = time(NULL);

        if(now - create_time <= FREE_LICENSE) {
                is_trial = 1;
        }

        snprintf(path, MAX_PATH_LEN, "%s/license", home);
        ret = stat(path, &buf);
        if(ret == 0){
                if((buf.st_size == 0) && is_trial){
                        return 0;
                } else if((buf.st_size == 0) && !is_trial) {
                        fprintf(stderr, "License expired.\n");
                        ret = ETIME;
                        goto err_ret;
                } else {
                        //check_license_valid
                }
        } else {
                if(!is_trial) {
                        fprintf(stderr, "No license found.\n");
                        ret = ENOENT;
                        goto err_ret;
                } else {
                        due_time = create_time + FREE_LICENSE;
                        time2str(due_time_str, &due_time);
                        fprintf(stdout,
                                        "Free License, license expiration time: %s\n",
                                        due_time_str);
                        return 0;
                }
        }

        ret = check_license_valid(path);
        if(ret){
                if(ret == ETIME){
                        fprintf(stderr, "License expired.\n");
                }else if(ret == ENOSPC){
                        fprintf(stderr, "Excess capacity.\n");
                }else{
                        fprintf(stderr, "Invalid license.\n");
                }

                goto err_ret;
        }

        return 0;
err_ret:
        return ret;
}

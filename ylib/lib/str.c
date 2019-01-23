

#include <stdint.h>
#include <ctype.h>

#define DBG_SUBSYS S_LIBYLIB

#include "dbg.h"
#include "sdfs_conf.h"

int str_replace_char(char *str, char from, char to)
{
        int i;

        if (str == NULL)
                return 0;

        i = 0;
        while (str[i] != '\0') {
                if (str[i] == from)
                        str[i] = to;

                i++;
        }

        return 0;
}

int str_replace_str(char *str, char *from, char *to)
{
        (void) str; (void) from; (void) to;

        return 0;
}

int str_upper(char *str)
{
        int i;

        if (str == NULL)
                return 0;

        i = 0;
        while (str[i] != '\0') {
                str[i] = toupper(str[i]);

                i++;
        }

        return 0;
}

unsigned int str_octal_to_uint(const char *str)
{
        unsigned int res = 0;
        int digit, seen_non_zero_digit = 0;

        /* note - avoiding using sscanf() parser */

        while (*str != '\0') {
                digit = *str;

                if (!isdigit(digit) || digit > '7')
                        break;

                if (digit != '0')
                        seen_non_zero_digit = 1;

                if (seen_non_zero_digit) {
                        res <<= 3;
                        res += (digit - '0');
                }

                str++;
        }

        return res;
}

int str_endwith(const char *string, const char *substr)
{
        size_t i, len = strlen(string), slen = strlen(substr);
        char buf[MAX_NAME_LEN] = {0}, sbuf[MAX_NAME_LEN] = {0};
        char *ptr = NULL, *sptr = NULL;

        memcpy(buf, string, len);
        memcpy(sbuf, substr, slen);

        ptr = &buf[len-1];
        sptr = &sbuf[slen-1];

        if(slen > len) {
                return 0;
        }

        for(i=0; i<slen; ++i) {
                if(*ptr != *sptr)
                       return 0;
                else {
                        ptr--;
                        sptr--;
                }
        }

        return 1;
}

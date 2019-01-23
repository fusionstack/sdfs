

#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#include "array.h"
#include "sysutil.h"
#include "ylib.h"

#define BUFLEN  50

typedef struct {
        uint32_t i;
        char buf[BUFLEN];
} entry_t; /*just for test*/

entry_t entry_array[18] = {
        {0, "9caea6aa-286d-4f7a-8bd2-002b648e9f6b"},
        {1, "ba1ee665-e586-4e67-80ff-fff758dec3ee"},
        {2, "fa5bd117-3f3c-4543-a854-ea68dc499e0e"},
        {3, "d3115fca-9f9f-4533-b5b1-7108fafa03b8"},
        {4, "d4854db3-d49e-4e05-aaa8-c5d9a68e28a4"},
        {5, "ff30bf28-6ac3-43e5-b132-afaa99640bcd"},
        {6, "4ab446a9-40a4-438f-863c-27b9d1ba0c0d"},
        {7, "8f7f510b-e4fb-4c37-b01a-3f5d003ed6e9"},
        {8, "54bf18e8-6ab7-4604-9c31-b0206b856d8c"},
        {9, "8e14a3d4-f6ae-4592-996f-c33b02b610f5"},
        {10, "456b63d2-f43c-4156-a017-f1e5b8fac8cf"},
        {11, "4b790014-fa6c-4d9d-9563-c6cc57738d34"},
        {12, "608cbb4a-9b51-439b-b61f-49978bcfe35e"},
        {14, "bea278bf-e012-4317-9a32-df299f039908"},
        {15, "c06587df-c1fb-4f11-93eb-78af25e37662"},
        {16, "f1ff2bdc-e02c-4647-b877-dc621f2c898e"},
        {17, "5fad7eec-1530-40ff-ad14-d451bbc04f32"}
};

int main()
{
        return 0;
}
#if 0
int main()
{
        int ret;
        array_t array;
        uint32_t array_len, i;
        entry_t *ent, *ent_orig;

        array_len = 7;

        printf("--test create array size %u--\n", array_len);

        ret = array_md_chk_t_create(&array, sizeof(entry_t), array_len);
        if (ret) {
                printf("!!create fail, ret %d seg_len %u!!\n", ret,
                       array.seg_len);
                return 1;
        } else
                printf("--create ok size %u --\n", array_len);

        printf("--test array_idx write--\n");

        for (i = 0; i < array_len; i ++) {
                ent = array_md_chk_t_insert(&array, i);

                if (!ent) {
                        printf("!!write error, i %d!!\n", i);
                        return 1;
                }

                ent_orig = &entry_array[i];

                ent->i = ent_orig->i;

                _strcpy(ent->buf, ent_orig->buf);

                printf("--write (%u:%s) into %p--\n", ent->i, ent->buf, ent);

        }

        printf("--test array_idx read--\n");

        for (i = 0; i < array_len; i ++) {
                ent = array_md_chk_t_insert(&array, i);

                if (!ent) {
                        printf("!!read error!!\n");
                        return 1;
                }

                ent_orig = &entry_array[i];

                if (ent->i != ent_orig->i || _strcmp(ent->buf, ent_orig->buf)) {
                        printf("!!read check error!!, i (%u:%u) string (%s:%s) ent %p\n",
                               i, ent->i, ent_orig->buf, ent->buf, ent);
                        return 1;
                }
        }

        printf("--test array_memcpy--\n");

        ret = array_md_chk_t_memcpy(&array, &entry_array[array_len], array_len, 11);
        if (ret) {
                printf("!!test array_memcpy fail!!\n");
                return 1;
        }

        printf("--check array_memcpy--\n");

        for (i = 0; i < array_len + 11; i++) {
                ent = array_md_chk_t_insert(&array, i);

                if (!ent) {
                        printf("!!read error!!\n");
                        return 1;
                }

                ent_orig = &entry_array[i];

                if (ent->i != ent_orig->i || _strcmp(ent->buf, ent_orig->buf)) {
                        printf("!!memcpy check error!!, i (%u:%u) string (%s:%s) ent %p\n",
                               ent->i, ent_orig->i, ent_orig->buf, ent->buf, ent);
                        return 1;
                }

        }
        

        printf("--test finished, array looks good--\n");

        return 0;
}
#endif

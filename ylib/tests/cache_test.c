

#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#include "array.h"
#include "ylib.h"
#include "cache.h"

typedef struct {
        int i;
        char buf[MAX_BUF_LEN];
} entry_t;

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

int drop_entry(void *arg)
{
        entry_t *entry = arg;
        printf("entry %d:%s droped\n", entry->i, entry->buf);
        return 1;
}

int cmp(const void *key, const void *value)
{
        if (*(int *)key == ((entry_t *)value)->i)
                return 1;
        else
                return 0;
}

uint32_t hash(const void *key)
{
        return *(int *)key;
}

int main()
{
        int ret;
        cache_t *cache;

        printf("--test cache--\n");

        ret = cache_create(&cache, 64, 16, sizeof(entry_t), cmp, hash, drop_entry);
        if (ret) {
                printf("!!!create fail!!!\n");
                return 1;
        }

        printf("--test insert--\n");

//        ret = cache_insert_r(cache, &i

        return 0;
}

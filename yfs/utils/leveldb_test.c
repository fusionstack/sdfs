/*************************************************************************
Author:lucifer
Created Time: Wed 12 Apr 2017 02:33:44 AM EDT
************************************************************************/
#include <stdio.h>
#include <inttypes.h>
#include <dirent.h>

#include "dbg.h"
#include "sdfs_id.h"
#include "leveldb_util.h"

#define TEST_DB 100

int _leveldb_show(void *arg, const void *key, size_t key_len, const void *value, size_t val_len, int *finish)
{
        uint64_t parent;
        char name[256] = "";

        (void)val_len;
        (void)key_len;
        (void)arg;
        (void)finish;

        uss_leveldb_decodekey(key, &parent, name, 256);
        printf("parent %lu name %s value %s\n", parent, name, (char *)value);

        return 0;
}

int main(int argc, char *argv[])
{
        int i, ret, key_len = 0, empty = 0, offset_len = 0, value_len = 0;
        uint64_t parent, count = 0;
        char name[256] = "", key[512] = "", value[256] = "", buf[64*1024] = "", expect_value[256] = "";
        char prefix[8] = "";
        struct dirent *de, *de0, *last_de;
        char *offset = NULL;

        (void)argc;
        (void)argv;

        ret = uss_volumedb_init("/var/lib/leveldb/volumedb");
        if (ret)
                GOTO(err_ret, ret);

        ret = uss_volumedb_create(TEST_DB);
        if (ret)
                GOTO(err_ret, ret);

        printf("leveldb init ok !\n");
        parent = 100;
        for (i = 1; i < 100; i++) {
                sprintf(name, "file%d", i);
                sprintf(value, "value%d", i);
                key_len = sizeof(key);
                uss_leveldb_encodekey(parent, name, key, &key_len);

                ret = uss_leveldb_put(TEST_DB, key, key_len, value, strlen(value) + 1, VTYPE_NULL);
                if (ret)
                        GOTO(err_close, ret);
        }

        printf("leveldb put ok !\n");

        uss_leveldb_prefix(parent, prefix);
        ret = uss_leveldb_prefix_empty(TEST_DB, prefix, &empty, ".");
        if (ret)
                GOTO(err_ret, ret);

        printf("leveldb parent %lu, %s\n", parent, (empty == 1) ? "empty" : "not empty");

        ret = uss_leveldb_prefix_count(TEST_DB, prefix, &count);
        if (ret)
                GOTO(err_ret, ret);
        printf("leveldb parent %lu, count %"PRIu64"\n", parent, count);

        (void)offset;
        (void)last_de;
        (void)de0;
        (void)de;
        (void)buf;
        (void)offset_len;

        for (i = 1; i < 100; i++) {
                memset(value, 0, sizeof(value));
                memset(expect_value, 0, sizeof(expect_value));
                memset(name, 0, sizeof(name));

                sprintf(name, "file%d", i);
                sprintf(expect_value, "value%d", i);
                value_len = strlen(expect_value) + 1;
                key_len = sizeof(key);
                uss_leveldb_encodekey(parent, name, key, &key_len);

                ret = uss_leveldb_get(TEST_DB, key, key_len, value, &value_len, VTYPE_NULL);
                if (ret)
                        GOTO(err_close, ret);

                printf("leveldb get parent:%lu ok name:%s, len:%d, value:%s\n", parent, name, value_len, value);
        }

        memset(name, 0, sizeof(name));

        sprintf(name, "file%d", 99);
        key_len = sizeof(key);
        uss_leveldb_encodekey(parent, name, key, &key_len);

        ret = uss_leveldb_delete(TEST_DB, key, key_len);
        if (ret)
                GOTO(err_close, ret);

        printf("leveldb parent:%lu name:%s delete ok !\n", parent, name);

        memset(value, 0, sizeof(value));
        sprintf(expect_value, "value%d", 99);
        value_len = strlen(expect_value) + 1;
        key_len = sizeof(key);
        ret = uss_leveldb_get(TEST_DB, key, key_len, value, &value_len, VTYPE_NULL);
        if (ret)
                GOTO(err_close, ret);

        printf("leveldb get parent:%lu ok, name:%s, len:%d, value:%s\n", parent, name, value_len, value);

        uss_leveldb_close(TEST_DB);

        return 0;
err_close:
        uss_leveldb_close(TEST_DB);
err_ret:
        return ret;
}

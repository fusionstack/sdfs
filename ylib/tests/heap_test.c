

#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#include "ylib.h"
#include "dbg.h"

struct test_t {
        int a;
};

static int cmp_fun(const void *data_a, const void *data_b)
{
        struct test_t *da;
        struct test_t *db;

        da = (struct test_t *) data_a;
        db = (struct test_t *) data_b;

        if (da->a > db->a)
                return 1;
        else if (da->a == db->a)
                return 0;
        else
                return -1;
}

static void print(const void *data)
{
        struct test_t *_data;
        _data = (struct test_t *)data;
        printf(" %d ", _data->a);
}


int main(int argc, char *argv[])
{
        (void) argc;
        (void) argv;
        int i, ret;
        struct heap_t hp;
        struct test_t *tmp[11];
        struct test_t *aa;

        struct test_t *min;

        min = (struct test_t *)malloc(sizeof(struct test_t));
        min->a = -10000;

        ret = heap_init(&hp, cmp_fun, NULL, print, 100, min);

        printf("heap size: %u\n", hp.size);
        printf("init finished\n");

        for (i = 0; i < 10; i++) {
                tmp[i] = (struct test_t *)malloc(sizeof(struct test_t));
                tmp[i]->a = -1 * i;
                ret = heap_insert(&hp, (void *)tmp[i]);
                if (ret) 
                        GOTO(err_ret, ret);
                printf("heap size is:%d\n", hp.size);
                heap_print(&hp);
        }
        tmp[11]->a = 0;
        ret = heap_insert(&hp, (void *)tmp[11]);

        for (i = 0; i < 11; i++) {
                aa = NULL;
                ret = heap_pop(&hp, (void *)&aa);
                if (ret)
                        GOTO(err_ret, ret);

#if 0
                heap_print(&hp);
#endif
                printf("%d\n", aa->a);
                free(aa);
        }
        
        return 0;
err_ret:
        return ret;
}


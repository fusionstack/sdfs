#include <math.h>

#include "ylib.h"
#include "skiplist.h"
#include "ylock.h"
#include "chk_proto.h"
#include "dbg.h"

struct some_pool {
        int maxlevel;
        int chunksize;
        chkid_t min_key;
        chkid_t max_key;
        int group;

        struct skiplist *chunk_list[10];
        sy_rwlock_t chunk_rwlock[10];
};

struct some_info {
        chkid_t chkid;
        sy_rwlock_t rwlock;
        int diskid_max;
        int diskid_num;
        // diskid_t diskid[0];
};

void __check(void *arg)
{
        struct some_info *si;

        si = arg;
        if (si->chkid.id % 100 == 0) {
                // printf("%llu_%u\n", (LLU)si->chkid.id, si->chkid.version);
        }
}

int verid64_void_cmp(const void *a, const void *b)
{
	(void) a;
	(void) b;
	YASSERT(0);
	return 0;
}

int main(int argc, char **argv)
{
        int ret;
        int i, j;
        struct some_pool sp;
        struct some_info *si;
        void *ptr;
        uint64_t id = 0;
        int group = 10;
        int group_count = 10000;

        for (i = 0; i < 3; ++i)
            sleep(1);
        return 0;

        sp.maxlevel = SKIPLIST_MAX_LEVEL;
        sp.chunksize = SKIPLIST_CHKSIZE_DEF;
        sp.min_key.id = 0;
        sp.min_key.version = 0;
        sp.max_key.id = UINT64_MAX;
        sp.max_key.version = UINT32_MAX;
        sp.group = group;

        if (argc == 1) {
                group_count = 10000;
        } else if (argc == 2) {
                group_count = atoi(argv[1]);
        } else {
                fprintf(stderr, "Usage: ./test_skiplist group_count");
                EXIT(1);
        }

        printf("--------------------------------------------------\n");
        printf("size %llu count %llu group %d\n", (LLU)sizeof(struct some_info), (LLU)group_count, group);
        printf("metadata size:%f MB\n", (double)sizeof(struct some_info)*group*group_count/pow(2,20));

        printf("--------------------------------------------------build\n");

        for (i = 0; i < sp.group; ++i) {
                sy_rwlock_init(&sp.chunk_rwlock[i]);
                ret = skiplist_create(verid64_void_cmp, sp.maxlevel, sp.chunksize, (void *)&sp.min_key, (void *)&sp.max_key, &sp.chunk_list[i]);

                for (j = 0; j < group_count; ++j) {
                        ret = ymalloc(&ptr, sizeof(struct some_info));
                        si = ptr;

                        si->chkid.id = id++;
                        si->chkid.version = 0;

                        skiplist_put(sp.chunk_list[i], (void *)&si->chkid, (void *)si);
                }
                printf("group %d len %u\n", i, skiplist_get_size(sp.chunk_list[i]));
        }

        printf("--------------------------------------------------access\n");
        time_t begin, end;
        for (i = 0; i < sp.group; ++i) {
                begin = time(NULL);
                skiplist_iterate(sp.chunk_list[i], __check);
                end = time(NULL);
                printf("group %d time %f\n", i, difftime(end, begin));
        }

        /**
         */
        for (i = 0; i < sp.group; ++i) {
                skiplist_clear(sp.chunk_list[i], 1);
                skiplist_destroy(sp.chunk_list[i]);
        }

        return 0;
}

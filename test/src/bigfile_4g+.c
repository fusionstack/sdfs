#include <string.h>
#include <stdio.h>
#include <sys/stat.h>

#include "sdfs_lib.h"

#define YFS_SANITY_TEST_STR  "123456789012345678901234567890123"
#define YFS_SANITY_TEST_FILE  "/mds/data/bigone"
#define YS_BUF_LEN 1024

int main(void)
{
        char buf[YS_BUF_LEN];
        int yfd, ret, buflen;
        uint32_t bit;
        struct stat stbuf;

        ret = ly_init(0);
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        }

        yfd = ly_create(YFS_SANITY_TEST_FILE, 0644);
        if (yfd < 0) {
                fprintf(stderr, "ERROR ly_create()\n");
                exit (1);
        }

        buflen = sizeof(YFS_SANITY_TEST_STR);
        bit = ~0;

        ret = ly_pwrite(yfd, YFS_SANITY_TEST_STR, buflen, bit - buflen);
        if (ret < 0){
                fprintf (stderr, "ERROR ly_pwrite()\n");
                exit (1);
        }

        ret = ly_pwrite(yfd, YFS_SANITY_TEST_STR, buflen, bit);
        if (ret < 0){
                fprintf (stderr,"ERROR ly_pwrite()\n");
                exit (1);
        }

        ret = ly_fsync(yfd);
        if (ret) {
                fprintf(stderr, "ERROR ly_fsync\n");        
                exit(1);
        }

        sleep (3);

        ret = ly_getattr(YFS_SANITY_TEST_FILE, &stbuf);
        if (ret) {
                fprintf(stderr, "ly_getattr() \n");
                exit(1);
        } 

        if (stbuf.st_size != (bit + buflen))
                fprintf(stderr, "file length %llu ERROR \n",
                                (unsigned long long)stbuf.st_size);
        else {
                fprintf(stderr, "file length %llu\n",
                                (unsigned long long)stbuf.st_size);
        }

        yfd = ly_open(YFS_SANITY_TEST_FILE);
        if (yfd < 0) {
                fprintf(stderr, "ERROR: ly_open()\n");
                exit(1);
        }
        
        buf[0] = '\0';
        ret = ly_pread(yfd, buf, buflen, bit);
        if (ret < 0) {
                fprintf(stderr, "ERROR: ly_pread()\n");
                goto DESTORY;
        }
        
        ret = strncmp(buf, YFS_SANITY_TEST_STR, sizeof(YFS_SANITY_TEST_STR));
        if (ret == 0) {
                fprintf(stdout, "readout data is same to writein data \n");
        } else {
                fprintf(stderr, "ERROR: data readout is not same to writen \n");
        }

        ret = ly_release(yfd);
        if (ret < 0){
                fprintf (stderr, "ERROR ly_release\n");
                exit (1);
        }

DESTORY:
        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ly_destroy() \n");
                exit(1);
        }

        exit(0);
}

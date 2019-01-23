

#define __USE_FILE_OFFSET64

#include <sys/statvfs.h>
#include <string.h>
#include <stdio.h>

#include "configure.h"
#include "chk_proto.h"
#include "sdfs_lib.h"

uint64_t human_scale(uint64_t bsize, char * ch, int *remainder)
{
        uint64_t block = bsize;

	*remainder = 0;
        if (block < 1024 ){
                *ch = 'B';                        
        } else if (block >= 1024 && block < (1 << 20)){
                *ch = 'K';
                block /= 1024;
                if ((bsize % 1024) != 0)
                        block += 1;
        } else if (block >= (1 << 20) && block < (1 << 30)){
                *ch = 'M';
                block /= (1 << 20);
                if ((bsize % (1 << 20)) != 0)
                        block += 1;
        } else if ((block >= (1 << 30)) && (block < (1ULL << 40))){
                *ch = 'G';
                block /= (1 << 30);
		if ((block >= 1) && (block < 10)){
                	if ((bsize % (1 << 30)) != 0)
				*remainder = (bsize % (1 << 30)
                                              / ((1 << 30) / 10));
		}
                if ((bsize % (1 << 30)) != 0)
                        block += 1;
        } else if (block >= (1ULL << 40)){
                *ch = 'T';
                block /= (1ULL << 40);
		if ((block >= 1) && (block < 10)){
                        if ((bsize % (1ULL << 40)) != 0)
                                *remainder = (bsize % (1ULL << 40)
                                              / ((1ULL << 40) / 10));
                } else {
                if ((bsize % (1ULL << 40)) != 0)
                       block += 1;
		}
        }
        return block;
        
}

int main(int argc, char *argv[])
{
        int ret, args, remainder_total, remainder_avail, remainder_used,
            verbose = 0, human = 0;
        char c_opt, *prog, *path, capacity_unit, used_unit, avail_unit;
        struct statvfs vfs;
        uint64_t capacity, used, free __attribute__((unused)), avail, percent, use_size, total_size;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;
	dbg_info(0);

        while ((c_opt = getopt(argc, argv, "hv")) > 0)
                switch (c_opt) {
                case 'v':
                        verbose = 1;
                        args++;
                        break;
                case 'h':
                        human = 1;
                        args++;
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        exit(1);
                }

        if (argc - args == 1)
                path = argv[args++];
        else
                path = "/";

        if (verbose)
                printf("%s %s\n", prog, path);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("ystatvfs");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_init()'ed\n");

        ret = ly_statvfs(path, &vfs);
        if (ret) {
                fprintf(stderr, "ly_statvfs(%s,...) %s\n", path, strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_statvfs()'ed\n");

        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ly_destroy() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_destroy()'ed\n");

        /* XXX -- yf */
        if (human){
                capacity = human_scale(vfs.f_blocks * vfs.f_frsize,
                                       &capacity_unit, &remainder_total);
                avail = human_scale(vfs.f_bavail * vfs.f_bsize, &avail_unit,
                                    &remainder_avail);
                used = human_scale((vfs.f_blocks*vfs.f_frsize
                                    - vfs.f_bfree *vfs.f_bsize),
                                   &used_unit, &remainder_used);
        }else{
                capacity = vfs.f_blocks * vfs.f_frsize;
                free = vfs.f_bfree * vfs.f_bsize;
                used = (vfs.f_blocks*vfs.f_frsize - vfs.f_bfree *vfs.f_bsize);
                avail = vfs.f_bavail * vfs.f_bsize;
        }

        if ((vfs.f_blocks * vfs.f_frsize || vfs.f_bfree * vfs.f_bsize 
             || (vfs.f_blocks - vfs.f_bfree) * vfs.f_bsize) == 0){
                percent = 0;
        } else {
                use_size = vfs.f_blocks*vfs.f_frsize - vfs.f_bfree *vfs.f_bsize;
                total_size = use_size + vfs.f_bavail * vfs.f_bsize;
                percent = 100 * use_size / total_size
                          + (100 * use_size % total_size != 0);   
                if ((percent < 1) && (vfs.f_blocks*vfs.f_frsize 
                                      - vfs.f_bfree *vfs.f_bsize) > 0 )
                        percent = 1;
        }

        if (human){
                printf("Filesystem\t Total\t Used\t Available\t Percent\n");
                if ((remainder_total) || (remainder_used) || (remainder_avail)){
                        printf("YFS\t %llu.%d%c\t %llu.%d%c\t %llu.%d%c\t %llu%%\n",
                               (unsigned long long)capacity, remainder_total,
                               capacity_unit, (unsigned long long)used,
                               remainder_used, used_unit,
                               (unsigned long long)avail, remainder_avail,
                               avail_unit, (unsigned long long)percent);
		} else { 
                        printf("YFS\t %llu%c\t %llu%c\t %llu%c\t %llu%%\n",
                                (unsigned long long)capacity, capacity_unit,
                                (unsigned long long)used, used_unit,
                                (unsigned long long)avail, avail_unit,
                                (unsigned long long)percent);	
		}
        } else 
                printf("YFS\t %llu\t %llu\t %llu\t %llu%%\n",
                       (unsigned long long)capacity, (unsigned long long)used, 
                       (unsigned long long)avail, (unsigned long long)percent);

        return 0;
}

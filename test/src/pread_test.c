

#include <sys/statvfs.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <semaphore.h>
#include <pthread.h>
#include <errno.h>
#include <signal.h>


#define DBG_SUBSYS S_YFSLIB

#include "sdfs_lib.h"
#include "dbg.h"
#include "ylib.h"

int fd, fd_out;
void *ptr;
off_t size = 3114787;


int main ()
{
        int ret;
        char buf[512];

        ly_init(LY_IO);

        sleep(1);

        fd = ly_open("/gray/Painted.Skin.2008.CN.DVDRip.XviD-PMCG-cd2.idx");

        ret = ly_pread(fd, buf, 512, 0);

        return 0;
}

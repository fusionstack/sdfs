

#include <pthread.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSLIB

#include "yfscli_conf.h"
#include "yfs_file.h"
#include "ylib.h"
#include "dbg.h"

int get_empty_filetable(atable_t ftable, void *ptr)
{
        int fd;

        fd = array_table_insert_empty(ftable, ptr);
        if (fd == 0) {
                array_table_remove(ftable, fd, (void *)&fd, NULL);
                fd = array_table_insert_empty(ftable, ptr);
                YASSERT(fd != 0);
        }

        return fd;
}

void *get_file(atable_t ftable, int fd)
{
        return array_table_find(ftable, fd, (void *)&fd);
}

int put_file(atable_t ftable, int fd)
{
        (void) array_table_remove(ftable, fd, (void *)&fd, NULL);

        return 0;
}

int files_init(atable_t *ftable, uint32_t size)
{
        *ftable = array_create_table(NULL, size);
        if (*ftable)
                return 0;
        else
                return ENOMEM;
}

int files_destroy(atable_t ftable)
{
        array_table_destroy(ftable);

        return 0;
}

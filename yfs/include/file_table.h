#ifndef __FILE_TABLE_H__
#define __FILE_TABLE_H__

#include "array_table.h"

int get_empty_filetable(atable_t ftable, void *ptr);
void *get_file(atable_t ftable, int fd);
int put_file(atable_t ftable, int fd);
int files_init(atable_t *ftable, uint32_t size);
int files_destroy(atable_t ftable);

#endif

#ifndef __WRITE_CRC_H__
#define __WRITE_CRC_H__

#include "yfs_conf.h"
#include "sdfs_buffer.h"

int wcrc_drop(uint32_t _off, uint32_t _size, uint32_t *crc, int count);
int wcrc_verify(uint32_t _off, uint32_t _size, const buffer_t *buf,
                const uint32_t *crc, int count);
int wcrc_sum(uint32_t _off, uint32_t _size, const buffer_t *buf,
             uint32_t *crc, uint32_t *_count);
int wcrc_extern(uint32_t _off, uint32_t _size, uint32_t *crc, int count,
                int (disk_read)(int, char *, int, int), int fd);
int wcrc_count(uint32_t _off, uint32_t _size);

#endif

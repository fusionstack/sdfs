#ifndef __EC_H__
#define __EC_H__


#include "sdfs_conf.h"
#include "sdfs_buffer.h"

//m = k + r
#define EC_MMAX YFS_CHK_REP_MAX
#define EC_KMAX YFS_CHK_REP_MAX

#define STRIP_BLOCK 4096
#define STRIP_ALIGN 64
#define STRIP_MAX (YFS_CHK_SIZE/STRIP_BLOCK)

typedef enum {
    PLUGIN_NULL,
    PLUGIN_EC_ISA,
    PLGUIN_MAX
} ec_plugin_t;

typedef enum {
    TECH_NULL,
    TECH_ISA_SSE,
} ec_technique_t;

typedef struct {
        uint8_t plugin;
        uint8_t tech;
        uint8_t k;
        uint8_t m;
} ec_t;

typedef struct {
        uint8_t idx;
        uint32_t offset;
        uint32_t count;
        buffer_t buf;
} ec_strip_t;

int ec_encode(char **data, char **coding, int blocksize, int m, int k);
int ec_decode(unsigned char *src_in_err, char **data, char **coding, int blocksize, int m, int k);

//uint8_t technique=reed_sol_van;
#endif

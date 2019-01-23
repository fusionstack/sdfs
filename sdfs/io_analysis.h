#ifndef __IO_ANALYSIS_H__
#define __IO_ANALYSIS_H__

#include <stdint.h>

typedef enum {
        ANALYSIS_READ = 1,
        ANALYSIS_WRITE,
} analysis_op_t;

int io_analysis_init(const char *name, int seq);
int io_analysis(analysis_op_t op, int count);

#endif

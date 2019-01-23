#ifndef __PROC_H__
#define __PROC_H__

#include <stdint.h>
#include "sdfs_conf.h"

#define PROC_MONITOR_ON  0

typedef int (*proc_cb_fn)(void *, char *, uint32_t);

typedef struct proc_node {
        int key;                        /* hash table key */
        void *target;                   /* pointer to the variable */
        char buf[1024];                 /* current value of *target, in format char* */
        uint32_t extra;                 /* extra arguments */
        char path[MAX_PATH_LEN];        /* path of the file which to be monitored */
        proc_cb_fn parse;               /* funtion to parse value */
        struct proc_node *next;         /* pointer to next node */
} proc_node_t;

typedef struct hash_table {
        proc_node_t **entrys;
        int size;
        int entrys_num;
        int (*hash)(int);
} hash_table_t;

extern int proc_init();
extern int proc_destroy();
extern int proc(char *path, void *target, char *buf, uint32_t extra, proc_cb_fn parse);

extern int proc_log(const char *name);

#endif

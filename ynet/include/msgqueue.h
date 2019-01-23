#ifndef __MSGQUEUE_H__
#define __MSGQUEUE_H__

#include "net_proto.h"

#define MSGQUEUE_SEG_LEN 10485760
#define MSGQUEUE_SEG_COUNT_MAX 64

typedef struct {
        int fd;
        uint32_t woff;
        uint32_t roff;
} msgqueue_seg_t;

typedef struct {
        sy_rwlock_t rwlock;
        ynet_net_nid_t nid;
        char home[MAX_PATH_LEN];
        uint32_t idx;
        msgqueue_seg_t seg[MSGQUEUE_SEG_COUNT_MAX];
} msgqueue_t;

typedef struct {
        uint32_t len;
        uint32_t crc;
        char buf[0];
} msgqueue_msg_t;

#define msg_for_each(msg, left)                                         \
        for (; left && (unsigned)left > msg->len;                       \
                     left -= (msg->len + sizeof(msgqueue_msg_t)),       \
                     msg = (void *)msg + msg->len + sizeof(msgqueue_msg_t))


int msgqueue_init(msgqueue_t *queue, const char *path, const nid_t *nid);
int msgqueue_push(msgqueue_t *queue, const void *_msg, uint32_t len);
int msgqueue_get(msgqueue_t *queue, void *msg, uint32_t len);
int msgqueue_pop(msgqueue_t *queue, void *msg, uint32_t len);
int msgqueue_load(msgqueue_t *queue, const char *path, const nid_t *nid);
int msgqueue_empty(msgqueue_t *queue);

#endif

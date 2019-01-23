#ifndef __RESPONSE_H__
#define __RESPONSE_H__

#include <stdint.h>

struct http_response {
        uint32_t size;
        uint32_t len;
        uint32_t idx;
        char *buf;
};

extern void start_response(struct http_response *rep);
extern int append_response(struct http_response *rep, char *buf, uint32_t len);
extern int send_response(struct http_response *rep, int sd);

#endif

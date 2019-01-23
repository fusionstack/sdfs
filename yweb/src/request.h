#ifndef __REQUEST_H__
#define __REQUEST_H__

#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdint.h>

enum {
        METHOD_GET,
        METHOD_HEAD,
        METHOD_POST,
        METHOD_PUT,
	METHOD_DELETE,
	METHOD_OPTIONS,
};



#define BAD_REQUEST "Bad Request"
#define BAD_RMSG_FORMAT "%s method is unsupport"


#define MAX_METHOD_INDEX 6

extern char *get_method_str(int m);

/** 
 * @see rfc2616 
 */
struct http_request {
        uint32_t size;
        uint32_t len;
        uint32_t idx;
        char *buf;

        /** Request-line */
        char *path;        /**< Request-URI */
        char *protocol;    /**< HTTP/1.1 */
        char *query;

        char *authorization;
        uint32_t content_length;
        char *content_type;
        char *cookie;
        char *host;
        time_t if_modified_since;
        char *referer;
        char *useragent;
        uint64_t offset;     /**< for resume-on-stop */

        int method;

        char *file;
        char *pathinfo;
        struct stat stat;
};

extern void start_request(struct http_request *req);
extern int  append_request(struct http_request *req, char *buf, uint32_t len);
extern void init_request(struct http_request *req);
extern void get_request_line(struct http_request *req, char **line);
extern void end_request(struct http_request *req);

#endif

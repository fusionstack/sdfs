#ifndef __HTTP_OPS_H__
#define __HTTP_OPS_H__

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "request.h"
#include "job.h"

/** send general response to client.
 * @param code response code
 * @param sd client socket
 */
extern int http_send_response(struct http_request *req, int code, buffer_t *rep);

extern int http_send_ok(struct http_request *req, char *me, char *mt,
                        struct stat *st, buffer_t *rep);
extern int http_send_nomod(struct http_request *req, char *me, char *mt,
                           time_t mtime, buffer_t *rep);
extern int http_send_error(struct http_request *req, int err, char* title,
                           char* extra_header, char* text, buffer_t *rep);
extern int http_send_badreq(struct http_request *req, buffer_t *rep);
extern int http_reply_send(job_t *job, buffer_t *buf, mbuffer_op_t op);

extern void add_listhead(struct http_request *req, buffer_t *rep);

extern void add_listbody(char *filename, char *date, uint64_t size, buffer_t *rep);

extern void add_errtail(struct http_request *req, buffer_t *rep);

#endif



#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <semaphore.h>
#include <pthread.h>
#include <fcntl.h>
#include <errno.h>
#include <aio.h>

#define DBG_SUBSYS S_YWEB

#include "get_version.h"
#include "md_lib.h"
#include "job_dock.h"
#include "http_ops.h"
#include "http_proto.h"
#include "mime.h"
#include "response.h"
#include "request.h"
#include "str.h"
#include "ylib.h"
#include "tdate_parse.h"
#include "sdfs_lib.h"
#include "ynet_rpc.h"
#include "yweb_conf.h"
#include "http_state_machine.h"
#include "configure.h"
#include "proc.h"
#include "dbg.h"

inline void http_exit_handler(int sig)
{
        DWARN("got signal %d, exiting\n", sig);

        srv_running = 0;
}

#if 0

static void dump_http_request(struct http_request *req)
{
        DBUG("(%d %s %s) pathinfo %s file %s %s %d host %s\n", 
             req->method, req->path, req->protocol,
             req->pathinfo, req->file,
             req->content_type, req->content_length,
             req->host);
}

/* 9.6 PUT in rfc2616 */
static int handle_put(struct http_request* http_req, int clisd)
{
        int ret, yfd, nfds, epoll_fd, isexisted;
        struct epoll_event ev, events;
        char buf[MAX_BUF_LEN];
        uint32_t buflen;
        uint64_t offset, left;

        dump_http_request(http_req);

        left = http_req->content_length;
        if (left <= 0)
                return 0;

        offset = 0;

        /* file is existed? */
        ret = ly_getattr(http_req->path, &http_req->stat);
        if (ret) {
                isexisted = 0;
                DBUG("file %s isn't existed.\n", http_req->path);
        }
        else {
                isexisted = 1;
                DBUG("file %s existed. length %llu\n", http_req->path,
                     (long long unsigned)http_req->stat.st_size);

                /* XXX how to define overwrite? -gj */
        }

        /** upload file, i.e. 
         * ly_create -> ly_pwrite -> ly_fsync loop 
         *           |----------<--------|
         */
        yfd = ly_create(http_req->path, 0644);
        if (yfd < 0) {
                ret = -yfd;
                DERROR("create(%s) %s\n", http_req->path, strerror(ret));

                /* send 5xx response */
                http_send_error(http_req, 500, "Internal Server Error",
                                "", "ly_create failed", clisd);
                GOTO(err_ret, ret);
        }
        DBUG(" %s created.\n", http_req->path);

        epoll_fd = epoll_create(1);
        if (epoll_fd == -1) {
                ret = errno;
                GOTO(err_yfd, ret);
        }

        ev.events = Y_EPOLL_EVENTS;
        ev.data.fd = clisd;

        ret = _epoll_ctl(epoll_fd, EPOLL_CTL_ADD, clisd, &ev);
        if (ret == -1) {
                ret = errno;
                GOTO(err_epoll, ret);
        }

        /* send 100-coutinue response, see 8.2.3 in rfc2616 */
        http_send_response(http_req, 100, clisd);

        while (left > 0) {
                nfds = _epoll_wait(epoll_fd, &events, 1,
                                  gloconf.rpc_timeout * 1000);

                DBUG("nfds %d\n", nfds);
                if (nfds == -1) {
                        ret = errno;
                        GOTO(err_epoll, ret);
                } else if (nfds == 0) {
                        ret = ETIME;
                        GOTO(err_epoll, ret);
                }

                buflen = left < MAX_BUF_LEN ? left : MAX_BUF_LEN;

                ret = _recv(clisd, buf, buflen, 0);
                if (ret < 0) {
                        ret = -ret;
                        DERROR(" (%d) %s\n", ret, strerror(ret));
                        GOTO(err_epoll, ret);
                } else if (ret == 0)
                        break;

                buflen = (uint32_t)ret;

                ret = ly_pwrite(yfd, buf, buflen, offset);
                if (ret < 0) {
                        ret = -ret;

                        DERROR("ly_pwrite() (%d) %s\n", ret, strerror(ret));
                        GOTO(err_epoll, ret);
                }

                offset += buflen;
                left -= buflen;
        }

        /* close it at once */
        sy_close(epoll_fd);

        ret = ly_release(yfd);
        if (ret) {
                DERROR("ly_release(%s) %s\n", http_req->path, strerror(ret));

                http_send_error(http_req, 500, 
                                "Internal Server Error", "", 
                                "Could not create file.", clisd);
                GOTO(err_yfd, ret);
        }

        /* all is right */
        if (isexisted)
                http_send_response(http_req, 200, clisd);
        else
                http_send_response(http_req, 201, clisd);

        return 0;

err_epoll:
        sy_close(epoll_fd);
err_yfd:
        ly_release(yfd);
err_ret:
        return ret;
}

void *http_handler(void *arg)
{
        int ret, clisd, epoll_fd, nfds;
        struct http_request http_req;
        struct epoll_event ev, events;
        uint32_t buflen;
        char *method_str, *line, *hdr;

        clisd = *(int *)arg;

#ifndef YWEB_THREAD_POOL
        sem_post(&clisd_sem);
#endif

        DBUG("begin http handler to cli %d\n", clisd);

        start_request(&http_req);

        epoll_fd = epoll_create(1);
        if (epoll_fd == -1) {
                ret = errno;
                GOTO(err_req, ret);
        }

        ev.events = Y_EPOLL_EVENTS;
        ev.data.fd = clisd;

        ret = _epoll_ctl(epoll_fd, EPOLL_CTL_ADD, clisd, &ev);
        if (ret == -1) {
                ret = errno;
                sy_close(epoll_fd);
                GOTO(err_req, ret);
        }

        while (srv_running) {
                char buf[MAX_BUF_LEN];

                nfds = _epoll_wait(epoll_fd, &events, 1,
                                  gloconf.rpc_timeout * 1000);
                if (nfds == -1) {
                        ret = errno;
                        sy_close(epoll_fd);
                        GOTO(err_req, ret);
                } else if (nfds == 0) {
                        ret = ETIME;
                        sy_close(epoll_fd);
                        GOTO(err_req, ret);
                }

                buflen = MAX_BUF_LEN;

                ret = _recv(clisd, buf, buflen, 0);
                if (ret < 0) {
                        ret = -ret;
                        sy_close(epoll_fd);
                        GOTO(err_req, ret);
                } else if (ret == 0) {  /* client close connect */
                        sy_close(epoll_fd);
                        DWARN("remote closed\n");

                        goto out;
                } else
                        buflen = (uint32_t)ret;

                ret = append_request(&http_req, buf, buflen);
                if (ret) {
                        sy_close(epoll_fd);
                        GOTO(err_req, ret);
                }

                if( strstr(http_req.buf, HTTP_REQ_END1) != NULL ||
                    strstr(http_req.buf, HTTP_REQ_END2) != NULL) {
                        sy_close(epoll_fd);
                        break;
                }
        }

        init_request(&http_req);

        /* parse the first line of the request */
        get_request_line(&http_req, &method_str);
        if (method_str == NULL) {
                (void) http_send_badreq(&http_req, clisd);

                ret = EINVAL;
                GOTO(err_req, ret);
        }
        DBUG("method %s\n", method_str);

        http_req.path = strpbrk(method_str, " \t\012\015");
        if (http_req.path == NULL) {
                (void) http_send_badreq(&http_req, clisd);

                ret = EINVAL;
                GOTO(err_req, ret);
        }
        *http_req.path++ = '\0';
        http_req.path += strspn(http_req.path, " \t\012\015");
        DBUG("path %s\n", http_req.path);

        http_req.protocol = strpbrk(http_req.path, " \t\012\015");
        if (http_req.protocol == NULL) {
                (void) http_send_badreq(&http_req, clisd);

                ret = EINVAL;
                GOTO(err_req, ret);
        }
        *http_req.protocol++ = '\0';
        http_req.protocol += strspn(http_req.protocol, " \t\012\015");
        DBUG("protocol %s\n", http_req.protocol);

        http_req.query = strchr(http_req.path, '?');
        if (http_req.query == NULL)
                http_req.query = "";
        else
                *http_req.query++ = '\0';
        DBUG("query %s\n", http_req.query);

        /* parse the rest of the request headers */
        while (srv_running) {
                get_request_line(&http_req, &line);

                if (line == NULL)
                        break;
                else if (strncasecmp(line, "Authorization:", 14) == 0) {
                        hdr = &line[14];
                        hdr += strspn(hdr, " \t");
                        http_req.authorization = hdr;
                } else if (strncasecmp(line, "Content-Length:", 15) == 0) {
                        hdr = &line[15];
                        hdr += strspn(hdr, " \t");
                        http_req.content_length = strtoul(hdr, (char **)NULL,
                                                          10);
                } else if (strncasecmp(line, "Content-Type:", 13) == 0) {
                        hdr = &line[13];
                        hdr += strspn(hdr, " \t");
                        http_req.content_type = hdr;
                } else if (strncasecmp(line, "Cookie:", 7) == 0) {
                        hdr = &line[7];
                        hdr += strspn(hdr, " \t");
                        http_req.cookie = hdr;
                } else if (strncasecmp(line, "Host:", 5) == 0) {
                        hdr = &line[5];
                        hdr += strspn(hdr, " \t");
                        http_req.host = hdr;

                        if (strchr(http_req.host, '/') != NULL
                            || http_req.host[0] == '.') {
                                (void) http_send_badreq(&http_req, clisd);

                                ret = EINVAL;
                                GOTO(err_req, ret);
                        }
                } else if (strncasecmp(line, "If-Modified-Since:", 18) == 0) {
                        hdr = &line[18];
                        hdr += strspn(hdr, " \t");
                        (void) tdate_parse(hdr, &http_req.if_modified_since);
                } else if (strncasecmp(line, "Referer:", 8) == 0) {
                        hdr = &line[8];
                        hdr += strspn(hdr, " \t");
                        http_req.referer = hdr;
                } else if (strncasecmp(line, "User-Agent:", 11) == 0) {
                        hdr = &line[11];
                        hdr += strspn(hdr, " \t");
                        http_req.useragent = hdr;
                } else if (strncasecmp(line, "Range:", 6) == 0) {
                        hdr = &line[6];
                        hdr += strspn(hdr, " \t");

                        DBUG("Range: %s\n", hdr);

                        char *str = strstr(hdr, "bytes=");
                        char *endptr = "\r\n";
                        if (str) {
                                str += 6;
                                http_req.offset = strtoull(str, &endptr, 0);
                        } 
                }
        }

        if (strcasecmp(method_str, get_method_str(METHOD_GET)) == 0)
                http_req.method = METHOD_GET;
        else if (strcasecmp(method_str, get_method_str(METHOD_HEAD)) == 0)
                http_req.method = METHOD_HEAD;
        else if (strcasecmp(method_str, get_method_str(METHOD_PUT)) == 0)
                http_req.method = METHOD_PUT;
        else {
                (void) http_send_error(&http_req, 501, "Not Implemented", "",
                                       "That method is not implemented.",clisd);
                ret = EINVAL;
                GOTO(err_req, ret);
        }
        DBUG("method %s\n", method_str);

        /* verify request-uri */
        str_decode(http_req.path, http_req.path);

        if (http_req.path[0] != '/') {
                (void) http_send_error(&http_req, 400, "Bad Request", "",
                                       "Bad filename.", clisd);

                ret = EINVAL;
                GOTO(err_req, ret);
        }

        http_req.file = &http_req.path[1];

        dir_dotdot(http_req.file);

        if (http_req.file[0] == '\0' || http_req.file[0] == '/'
            || (http_req.file[0] == '.' && http_req.file[1] == '/'
                && (http_req.file[2] == '\0' || http_req.file[2] == '/'))) {
                DERROR("file %s\n", http_req.file);

                (void) http_send_error(&http_req, 400, "Bad Request", "",
                                       "Illegal filename.", clisd);

                ret = EINVAL;
                GOTO(err_req, ret);
        }

        /* request is parsed, now handle it */
        switch (http_req.method) {
        case METHOD_HEAD:
        case METHOD_GET:
                ret = handle_get(&http_req, clisd);
                if (ret)
                        GOTO(err_req, ret);
                break;
        case METHOD_PUT:
                ret = handle_put(&http_req, clisd);
                if (ret)
                        GOTO(err_req, ret);
                break;
        default:
                break;
        }

out:
        end_request(&http_req);

        ret = sy_close(clisd);
        if (ret)
                DERROR("ret (%d) %s\n", ret, strerror(ret));

#ifndef YWEB_THREAD_POOL
        http_srv_thr_running--;
        pthread_exit(NULL);
#endif
        return (void *)0;
err_req:
        end_request(&http_req);
        (void) sy_close(clisd);
#ifndef YWEB_THREAD_POOL
        http_srv_thr_running--;
        pthread_exit((void *)&ret);
#endif
        return (void *)0;
}
#endif

//static int http_srv_shutdown = 0;

static void signal_handler(int sig)
{
	(void) sig;
        DERROR("got signal %d\n", sig);

        jobdock_iterator();
}

int http_srv(int daemon, const char *port)
{
        int ret;

        signal(SIGUSR1, signal_handler);
        signal(SIGUSR2, http_exit_handler);

        mime_init();

        ret = ly_prep(daemon, "yweb", -1);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init(daemon, "yweb", -1);
        if (ret)
                GOTO(err_ret, ret);

        ret = io_analysis_init("ftp", 0);
        if (ret)
                GOTO(err_ret, ret);
        
        ret = network_connect_mond(0);
        if (ret)
                GOTO(err_ret, ret);

        ret = http_listen(port, YNET_RPC_NONBLOCK);
        if (ret)
                GOTO(err_ret, ret);

        ret = rpc_start(); /*begin serivce*/
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_update_status("running", -1);
        if (ret)
                GOTO(err_ret, ret);

        while (srv_running) { //we got nothing to do here
                ret = netable_wait(&ng.mds_nh, 1);
                if (ret) {
                        if (ret == ETIMEDOUT)
                                continue;
                        else
                                GOTO(err_ret, ret);
                }
        }

        ret = ly_update_status("stopping", -1);
        if (ret)
                GOTO(err_ret, ret);

        (void) ly_destroy();

        ret = ly_update_status("stopped", -1);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

int main(int argc, char *argv[])
{
        int ret, daemon = 1, maxcore __attribute__((unused)) = 0;
        char c_opt;
        char *port;

        port = YWEB_SERVICE_DEF;

        while ((c_opt = getopt(argc, argv, "cfp:v")) > 0)
                switch (c_opt) {
                case 'c':
                        maxcore = 1;
                        break;
                case 'f':
                        daemon = 2;
                        break;
                case 'p':
                        port = optarg;
                        break;
                case 'v':
                        get_version();
                        EXIT(0);
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        EXIT(1);
                }

        ret = http_srv(daemon, port);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

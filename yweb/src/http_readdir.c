

#include <sys/types.h>
#include <stdint.h>
#include <dirent.h>

#define DBG_SUBSYS S_YWEB

#include "http_ops.h"
#include "error.h"
//#include "readdir.h"
//#include "nfs_conf.h"
#include "ylib.h"
#include "sdfs_lib.h"
#include "dbg.h"

int http_readdir(struct http_request *http_req, buffer_t *buf)
{
        int ret;
        char date[64];
        off_t offset;
        void *de0, *ptr;
        unsigned delen, len;
        struct dirent *de;
        struct stat stbuf;
        fileinfo_t *md;
        char _buf[MAX_BUF_LEN];
        fileid_t fileid;

        md = (void *)_buf;
        offset = 0;
        de0 = NULL;
        delen = 0;

        ret = http_send_response(http_req, 200, buf);
        if (ret)
                GOTO(err_ret, ret);

        (void) add_listhead(http_req, buf);

        ret = sdfs_lookup_recurive(http_req->path, &fileid);
        if (ret)
                GOTO(err_ret, ret);

        while (srv_running) {
                ret = sdfs_readdirplus(&fileid, offset, &de0, (int *)&delen, EXEC_USS_WEB);
                if (ret) {
                        http_send_error(http_req, 500, 
                                              "Error", NULL,
                                              "server error",
                                              buf);

                        GOTO(err_ret, ret);
                } else if (delen == 0) {
                        break;
                }

                if (delen > 0) {
                        dir_for_each(de0, delen, de, offset) {
                                if (de->d_reclen > delen) {
                                        http_send_error(http_req, 500, 
                                                        "error", NULL,
                                                        "server error",

                                                        buf);
                                        GOTO(err_ret, ret);
                                }

                                len = _strlen(de->d_name);
                                if ((len == 1 && de->d_name[0] == '.') 
                                    || (len == 2 && de->d_name[0] == '.'
                                        && de->d_name[1] == '.'))
                                        goto next;

                                md = (void *)de + de->d_reclen - sizeof(md_proto_t);
                                MD2STAT(md, &stbuf);

                                //DINFO("file %s len %llu\n", depath, (LLU)stbuf.st_size);

                                stat_to_datestr(&stbuf, date);

                                DBUG("%s %s %llu\n", de->d_name,
                                     date,
                                     (unsigned long long)stbuf.st_size);

                                add_listbody(de->d_name, 
                                             date, 
                                             (uint64_t) stbuf.st_size, 
                                             buf);  
                        next:
                                offset = de->d_off;
                                ptr += de->d_reclen;
                                delen -= de->d_reclen;
                        }
                                        
                        yfree((void **)&de0);
                        delen = 0;
                }
        }

        (void) add_errtail(http_req, buf);

        return 0;
err_ret:
        return ret;
}

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define DBG_SUBSYS S_LIBYLIB

#include "configure.h"
#include "ylib.h"
#include "sysutil.h"
#include "dbg.h"

#define MAX_NUM 100

static int __config_readline(addr_t *_addr, const char *buf, const char *pattern)
{
        int i, count;
        char *p, addr[MAX_NAME_LEN], value[MAX_BUF_LEN], *srv;
        char *list[MAX_NUM];

        p = strchr(buf, ' ');

        if (p == NULL)
                return 0;

        memcpy(addr, buf, p - buf);

        addr[p - buf] = '\0';
        srv = p++;

        DBUG("addr %s, srv %s\n", addr, srv);

        count = MAX_NUM;
        _str_split(srv, ' ', list, &count);

        for (i = 0; i < count; i++) {
                if (strncmp(list[i], pattern, strlen(pattern)) == 0) {
                        strcpy(value, list[i]);
                        DBUG("found %s\n", value);
                        break;
                }
        }

        if (i == count) {
                DERROR("%s not found\n", pattern);
                return 0;
        }
        
        count = MAX_NUM;
        p = value + strlen(pattern) + 1;
        p[strlen(p) - 1] = '\0';

        DBUG("p %s\n", p);
        _str_split(p, ',', list, &count);
        
        for (i = 0; i < count; i++) {
                _addr[i].port = atoi(list[i]);
                _addr[i].addr = inet_addr(addr);
                DBUG("addr %s %u port %u\n", addr, _addr[i].addr, i);
        }

        return count;
}

int config_import(addr_t *addr, int *_count, const char *pattern)
{
        int ret, count, total;
        FILE *fp;
        char line[MAX_BUF_LEN];
        //char path[MAX_PATH_LEN];

        fp = fopen(SDFS_HOME"/etc/cluster.conf", "r");
        if (fp == NULL) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        total = 0;
        while (1) {
                if (fgets(line, MAX_BUF_LEN, fp) == NULL) {
                        break;
                }

                DINFO("get line %u %s", total, line);

                count = __config_readline(&addr[total], line, pattern);
                total += count;
        }

        fclose(fp);

        *_count = total;

        return 0;
err_ret:
        return ret;
}

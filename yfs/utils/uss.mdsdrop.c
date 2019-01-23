

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "configure.h"
#include "uss_sysstat.h"
#include "sdfs_lib.h"
#include "dbg.h"

static void __split(char *_addr, char *_port, const char *buf)
{
        char *port;

        port = strchr(buf, ':');

        YASSERT(port);

        memcpy(_addr, buf, port - buf);
        _addr[port - buf] = '\0';
        strcpy(_port, port + 1);

        DBUG("addr %s port %s\n", _addr, _port);
}

static int __help(char *argv[])
{
        printf("%s 192.168.1.1:0\n", argv[0]);

        return EINVAL;
}

int main(int argc, char *argv[])
{
        int ret, count;
        char buf[MAX_BUF_LEN];
        addr_t *addr;
        char _addr[32], _port[32];

        (void) argc;
        (void) argv;

        dbg_info(0);

        if (argc != 2) {
                return __help(argv);
        }

        __split(_addr, _port, argv[1]);

        //drop.addr = inet_addr(_addr);
        //drop.port = atoi(_port);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                GOTO(err_ret, ret);

        ret = ly_init_simple("mdsdrop");
        if (ret)
                GOTO(err_ret, ret);

        addr = (void *)buf;
        ret = config_import(addr, &count, "mond");
        if (ret)
                GOTO(err_ret, ret);

#if 0
        ret = paxos_drop(addr, count, &drop);
        if (ret)
                GOTO(err_ret, ret);
#endif

        return 0;
err_ret:
        return ret;
}

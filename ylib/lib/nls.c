

#define DBG_SUBSYS S_LIBYLIBNLS

#include <errno.h>

#include "sysutil.h"
#include "nls.h"
#include "dbg.h"

struct nls_table *nls_tables = NULL;

inline void nls_register(struct nls_table *nls)
{
        nls->next = nls_tables;
        nls_tables = nls;
}

extern struct nls_table *nls_cp936;

int nls_loadall(void)
{
        nls_register(nls_cp936);

        return 0;
};

int nls_getable(char *charset, struct nls_table **t)
{
        int ret;
        struct nls_table *nls;

        for (nls = nls_tables; nls; nls = nls->next) {
                if (_strcmp(nls->charset, charset) == 0)
                        break;

                if (nls->alias && (_strcmp(nls->alias, charset)))
                        break;
        }

        if (nls == NULL) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        *t = nls;

        return 0;
err_ret:
        return ret;
}

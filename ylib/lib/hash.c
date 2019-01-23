

#include <stdint.h>

#define DBG_SUBSYS S_LIBYLIB

#include "dbg.h"

/*
 * the famous DJB hash function for strings
 */
uint32_t hash_str(const char *str)
{
        uint32_t hash = 5381;
        const char *s;

        for (s = str; *s; s++)
                hash = ((hash << 5) + hash) + *s;

        hash &= ~(1 << 31);     /* strip the highest bit */

        return hash;
}

uint32_t hash_mem(const void *mem, int size)
{
        uint32_t hash = 5381;
        const char *s = mem;
        int i;

        for (i = 0; i < size; i++)
                hash = ((hash << 5) + hash) + s[i];

        hash &= ~(1 << 31);

        return hash;
}

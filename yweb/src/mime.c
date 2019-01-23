

#include <string.h>
#include <stdlib.h>

#define DBG_SUBSYS S_YWEB

#include "sysutil.h"
#include "mime.h"
#include "dbg.h"

struct mime_entry enc_tab[] = {
#include "mime_encodings.h"
};

const int n_enc_tab = sizeof(enc_tab) / sizeof(*enc_tab);

struct mime_entry typ_tab[] = {
#include "mime_types.h"
};

const int n_typ_tab = sizeof(typ_tab) / sizeof(*typ_tab);

/* qsort comparison routine - declared old-style on purpose, for portability */
static int ext_compare(const void *a, const void *b)
{
        return _strcmp(((struct mime_entry *)a)->ext,
                      ((struct mime_entry *)b)->ext);
}

void mime_init(void)
{
        int i;

        /* sort the tables so we can do binary search */
        qsort(enc_tab, n_enc_tab, sizeof(*enc_tab), ext_compare);
        qsort(typ_tab, n_typ_tab, sizeof(*typ_tab), ext_compare);

        /* fill in the lengths */
        for (i = 0; i < n_enc_tab; ++i) {
                enc_tab[i].ext_len = _strlen(enc_tab[i].ext);
                enc_tab[i].val_len = _strlen(enc_tab[i].val);
        }

        for (i = 0; i < n_typ_tab; ++i) {
                typ_tab[i].ext_len = _strlen(typ_tab[i].ext);
                typ_tab[i].val_len = _strlen(typ_tab[i].val);
        }
}

void mime_getype(char* name, char* me, size_t me_size, char **_type)
{
        int me_indexes[100], i, top, bot, mid, r;
        unsigned int n_me_indexes;
        char *prev_dot, *dot, *ext, *type;
        size_t ext_len, me_len;
        char *default_type = "application/octet-stream; charset=%s";

        /* peel off encoding extensions until there are't any more */
        n_me_indexes = 0;
        for (prev_dot = &name[strlen(name)]; ; prev_dot = dot) {
                for (dot = prev_dot - 1; dot >= name && *dot != '.'; --dot)
                        ;

                if (dot < name) {
                        /*
                         * no dot found. No more encoding extensions, and no
                         * type extension either.
                         */
                        type = default_type;
                        goto done;
                }

                ext = dot + 1;
                ext_len = prev_dot - ext;

                /*
                 * search the encoding table. linear search is fine here, there
                 * are only a few entries.
                 */
                for (i = 0; i < n_enc_tab; ++i) {
                        if (ext_len == enc_tab[i].ext_len
                            && strncasecmp(ext, enc_tab[i].ext, ext_len) == 0) {
                                if (n_me_indexes
                                    < sizeof(me_indexes)/sizeof(*me_indexes)) {
                                        me_indexes[n_me_indexes] = i;
                                        ++n_me_indexes;
                                }

                                goto next;
                        }
                }

                /*
                 * no encoding extension found. break and look for a type
                 * extension
                 */
                break;

next:
                ;
        }

        /* binary search for a matching type extension */
        top = n_typ_tab - 1;
        bot = 0;
        while (top >= bot) {
                mid = ( top + bot ) / 2;
                r = strncasecmp(ext, typ_tab[mid].ext, ext_len);
                if (r < 0)
                        top = mid - 1;
                else if (r > 0)
                        bot = mid + 1;
                else {
                        if (ext_len < typ_tab[mid].ext_len)
                                top = mid - 1;
                        else if (ext_len > typ_tab[mid].ext_len)
                                bot = mid + 1;
                        else {
                                type = typ_tab[mid].val;
                                goto done;
                        }
                }
        }

        type = default_type;

done:
        /* the last thing we do is actually generate the mime-encoding header */
        me[0] = '\0';
        me_len = 0;
        for (i = n_me_indexes - 1; i >= 0; --i) {
                if (me_len + enc_tab[me_indexes[i]].val_len + 1 < me_size) {
                        if (me[0] != '\0') {
                                (void)strcpy(&me[me_len], ",");
                                ++me_len;
                        }

                        (void) _strcpy(&me[me_len], enc_tab[me_indexes[i]].val);
                        me_len += enc_tab[me_indexes[i]].val_len;
                }
        }

        *_type = type;
}

#ifndef __MIME_H__
#define __MIME_H__

struct mime_entry {
        char *ext;
        size_t ext_len;
        char *val;
        size_t val_len;
};

extern void mime_init(void);
extern void mime_getype(char* name, char* me, size_t me_size, char **type);

#endif

#ifndef __YUTILS_H
#define __YUTILS_H

#include <string.h>
#include <string>
#include <vector>

typedef std::vector<std::string> StringVector;

static inline int split(char *str, StringVector& args) {
        char *token;

        token = strtok(str, " ");
        while (token) {
                args.push_back(std::string(token));
                token = strtok(NULL, " ");
        }

        return 0;
}

/* Strip whitespace from the start and end of STRING.  Return a pointer
   into STRING. */
static inline char* stripwhite (char *string) {
        register char *s, *t;

        for (s = string; isspace (*s); s++)
                ;

        if (*s == 0)
                return (s);

        t = s + strlen (s) - 1;
        while (t > s && isspace (*t))
                t--;
        *++t = '\0';

        return s;
}

#endif
